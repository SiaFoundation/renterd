package s3

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"

	"go.sia.tech/gofakes3"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/object"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

// maxKeysDefault is the default maxKeys value used in the AWS SDK
const maxKeysDefault = 1000

var (
	_ gofakes3.Backend          = (*s3)(nil)
	_ gofakes3.MultipartBackend = (*s3)(nil)
)

type s3 struct {
	b      bus
	w      worker
	logger *zap.SugaredLogger
}

// ListBuckets returns a list of all buckets owned by the authenticated
// sender of the request.
// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTServiceGET.html
func (s *s3) ListBuckets(ctx context.Context) ([]gofakes3.BucketInfo, error) {
	buckets, err := s.b.ListBuckets(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list buckets: %w", err)
	}
	bucketInfos := make([]gofakes3.BucketInfo, len(buckets))
	for i, bucket := range buckets {
		bucketInfos[i] = gofakes3.BucketInfo{
			Name:         bucket.Name,
			CreationDate: gofakes3.NewContentTime(bucket.CreatedAt),
		}
	}
	return bucketInfos, nil
}

// ListBucket returns the contents of a bucket. Backends should use the
// supplied prefix to limit the contents of the bucket and to sort the
// matched items into the Contents and CommonPrefixes fields.
//
// ListBucket must return a gofakes3.ErrNoSuchBucket error if the bucket
// does not exist. See gofakes3.BucketNotFound() for a convenient way to create one.
//
// The prefix MUST be correctly handled for the backend to be valid. Each
// item you consider returning should be checked using prefix.Match(name),
// even if the prefix is empty. The Backend MUST treat a nil prefix
// identically to a zero prefix.
//
// At this stage, implementers MAY return gofakes3.ErrInternalPageNotImplemented
// if the page argument is non-empty. In this case, gofakes3 may or may
// not, depending on how it was configured, retry the same request with no page.
// We have observed (though not yet confirmed) that simple clients tend to
// work fine if you ignore the pagination request, but this may not suit
// your application. Not all backends bundled with gofakes3 correctly
// support this pagination yet, but that will change.
//
// TODO: This implementation is not ideal because it fetches all objects. We
// will eventually want to support this type of pagination in the bus.
func (s *s3) ListBucket(ctx context.Context, bucketName string, prefix *gofakes3.Prefix, page gofakes3.ListBucketPage) (*gofakes3.ObjectList, error) {
	if prefix == nil {
		prefix = &gofakes3.Prefix{}
	}
	prefix.HasDelimiter = prefix.Delimiter != ""
	if prefix.HasDelimiter && prefix.Delimiter != "/" {
		// NOTE: this is a limitation of the current implementation of the bus.
		return nil, gofakes3.ErrorMessage(gofakes3.ErrNotImplemented, "delimiter must be '/' but was "+prefix.Delimiter)
	}

	// Workaround for empty prefix
	prefix.HasPrefix = prefix.Prefix != ""

	// Adjust MaxKeys
	if page.MaxKeys == 0 {
		page.MaxKeys = maxKeysDefault
	}

	var objects []api.ObjectMetadata
	var err error
	response := gofakes3.NewObjectList()
	if prefix.HasDelimiter {
		// Handle request with delimiter.
		opts := api.GetObjectOptions{}
		if page.HasMarker {
			opts.Marker = page.Marker
			opts.Limit = int(page.MaxKeys)
		}
		var path string // root of bucket
		adjustedPrefix := prefix.Prefix
		if idx := strings.LastIndex(adjustedPrefix, prefix.Delimiter); idx != -1 {
			path = adjustedPrefix[:idx+1]
			adjustedPrefix = adjustedPrefix[idx+1:]
		}
		if adjustedPrefix != "" {
			opts.Prefix = adjustedPrefix
		}
		var res api.ObjectsResponse
		res, err = s.b.Object(ctx, bucketName, path, opts)
		if err != nil && strings.Contains(err.Error(), api.ErrBucketNotFound.Error()) {
			return nil, gofakes3.BucketNotFound(bucketName)
		} else if err != nil {
			return nil, gofakes3.ErrorMessage(gofakes3.ErrInternal, err.Error())
		}
		objects = res.Entries
		response.IsTruncated = res.HasMore
		if response.IsTruncated {
			response.NextMarker = objects[len(objects)-1].Name
		}
	} else {
		// Handle request without delimiter.
		opts := api.ListObjectOptions{
			Limit:  int(page.MaxKeys),
			Marker: page.Marker,
			Prefix: "/" + prefix.Prefix,
		}

		var res api.ObjectsListResponse
		res, err = s.b.ListObjects(ctx, bucketName, opts)
		if err != nil && strings.Contains(err.Error(), api.ErrBucketNotFound.Error()) {
			return nil, gofakes3.BucketNotFound(bucketName)
		} else if err != nil {
			return nil, gofakes3.ErrorMessage(gofakes3.ErrInternal, err.Error())
		}
		objects = res.Objects
		response.IsTruncated = res.HasMore
		response.NextMarker = res.NextMarker
	}
	if err != nil {
		return nil, gofakes3.ErrorMessage(gofakes3.ErrInternal, err.Error())
	}

	// Loop over the entries and add them to the response.
	for _, object := range objects {
		key := strings.TrimPrefix(object.Name, "/")
		if prefix.HasDelimiter && strings.HasSuffix(key, prefix.Delimiter) {
			response.AddPrefix(key)
			continue
		}

		item := &gofakes3.Content{
			Key:          key,
			LastModified: gofakes3.NewContentTime(object.ModTime),
			ETag:         hex.EncodeToString(frand.Bytes(32)), // TODO: don't have that
			Size:         object.Size,
			StorageClass: gofakes3.StorageStandard,
		}
		response.Add(item)
	}

	return response, nil
}

// CreateBucket creates the bucket if it does not already exist. The name
// should be assumed to be a valid name.
//
// If the bucket already exists, a gofakes3.ResourceError with
// gofakes3.ErrBucketAlreadyExists MUST be returned.
func (s *s3) CreateBucket(ctx context.Context, name string) error {
	if err := s.b.CreateBucket(ctx, name, api.CreateBucketOptions{}); err != nil && strings.Contains(err.Error(), api.ErrBucketExists.Error()) {
		return gofakes3.ErrBucketAlreadyExists
	} else if err != nil {
		return gofakes3.ErrorMessage(gofakes3.ErrInternal, err.Error())
	}
	return nil
}

// BucketExists should return a boolean indicating the bucket existence, or
// an error if the backend was unable to determine existence.
//
// TODO: backend could be improved to allow for checking specific dir in root.
func (s *s3) BucketExists(ctx context.Context, name string) (bool, error) {
	_, err := s.b.Bucket(ctx, name)
	if err != nil && strings.Contains(err.Error(), api.ErrBucketNotFound.Error()) {
		return false, nil
	} else if err != nil {
		return false, gofakes3.ErrorMessage(gofakes3.ErrInternal, err.Error())
	}
	return true, nil
}

// DeleteBucket deletes a bucket if and only if it is empty.
//
// If the bucket is not empty, gofakes3.ResourceError with
// gofakes3.ErrBucketNotEmpty MUST be returned.
//
// If the bucket does not exist, gofakes3.ErrNoSuchBucket MUST be returned.
//
// AWS does not validate the bucket's name for anything other than existence.
// TODO: This check is not atomic. The backend needs to be updated to support
// atomically checking whether a bucket is empty.
func (s *s3) DeleteBucket(ctx context.Context, name string) error {
	if err := s.b.DeleteBucket(ctx, name); err != nil && strings.Contains(err.Error(), api.ErrBucketNotEmpty.Error()) {
		return gofakes3.ErrBucketNotEmpty
	} else if err != nil && strings.Contains(err.Error(), api.ErrBucketNotFound.Error()) {
		return gofakes3.BucketNotFound(name)
	} else if err != nil {
		return gofakes3.ErrorMessage(gofakes3.ErrInternal, err.Error())
	}
	return nil
}

// GetObject must return a gofakes3.ErrNoSuchKey error if the object does
// not exist. See gofakes3.KeyNotFound() for a convenient way to create
// one.
//
// If the returned Object is not nil, you MUST call Object.Contents.Close(),
// otherwise you will leak resources. Implementers should return a no-op
// implementation of io.ReadCloser.
//
// If rnge is nil, it is assumed you want the entire object. If rnge is not
// nil, but the underlying backend does not support range requests,
// implementers MUST return ErrNotImplemented.
//
// If the backend is a VersionedBackend, GetObject retrieves the latest version.
// TODO: Range requests starting from the end are not supported yet. Backend
// needs to be updated for that.
func (s *s3) GetObject(ctx context.Context, bucketName, objectName string, rangeRequest *gofakes3.ObjectRangeRequest) (*gofakes3.Object, error) {
	if rangeRequest != nil && rangeRequest.FromEnd {
		return nil, gofakes3.ErrorMessage(gofakes3.ErrNotImplemented, "range request from end not supported")
	}

	opts := api.DownloadObjectOptions{}
	if rangeRequest != nil {
		length := int64(-1)
		if rangeRequest.End >= 0 {
			length = rangeRequest.End - rangeRequest.Start + 1
		}
		opts.Range = api.DownloadRange{Offset: rangeRequest.Start, Length: length}
	}
	res, err := s.w.GetObject(ctx, bucketName, objectName, opts)
	if err != nil && strings.Contains(err.Error(), api.ErrBucketNotFound.Error()) {
		return nil, gofakes3.BucketNotFound(bucketName)
	} else if err != nil && strings.Contains(err.Error(), api.ErrObjectNotFound.Error()) {
		return nil, gofakes3.KeyNotFound(objectName)
	} else if err != nil {
		return nil, gofakes3.ErrorMessage(gofakes3.ErrInternal, err.Error())
	}
	var objectRange *gofakes3.ObjectRange
	if res.Range != nil {
		objectRange = &gofakes3.ObjectRange{
			Start:  res.Range.Offset,
			Length: res.Range.Length,
		}
	}

	// TODO: When we support metadata we need to add it here.
	metadata := map[string]string{
		"Content-Type":  res.ContentType,
		"Last-Modified": res.ModTime.UTC().Format(http.TimeFormat),
	}

	return &gofakes3.Object{
		Name:     gofakes3.URLEncode(objectName),
		Metadata: metadata,
		Size:     res.Size,
		Contents: res.Content,
		Range:    objectRange,
	}, nil
}

// HeadObject fetches the Object from the backend, but reading the Contents
// will return io.EOF immediately.
//
// If the returned Object is not nil, you MUST call Object.Contents.Close(),
// otherwise you will leak resources. Implementers should return a no-op
// implementation of io.ReadCloser.
//
// HeadObject should return a NotFound() error if the object does not
// exist.
func (s *s3) HeadObject(ctx context.Context, bucketName, objectName string) (*gofakes3.Object, error) {
	res, err := s.b.Object(ctx, bucketName, objectName, api.GetObjectOptions{IgnoreDelim: true})
	if err != nil && strings.Contains(err.Error(), api.ErrObjectNotFound.Error()) {
		return nil, gofakes3.KeyNotFound(objectName)
	} else if err != nil {
		return nil, gofakes3.ErrorMessage(gofakes3.ErrInternal, err.Error())
	}
	// TODO: When we support metadata we need to add it here.
	metadata := map[string]string{
		"Content-Type":  mime.TypeByExtension(objectName),
		"Last-Modified": res.Object.LastModified(),
	}
	return &gofakes3.Object{
		Name:     gofakes3.URLEncode(objectName),
		Metadata: metadata,
		Size:     res.Object.Size,
		Contents: io.NopCloser(bytes.NewReader(nil)),
	}, nil
}

// DeleteObject deletes an object from the bucket.
//
// If the backend is a VersionedBackend and versioning is enabled, this
// should introduce a delete marker rather than actually delete the object.
//
// DeleteObject must return a gofakes3.ErrNoSuchBucket error if the bucket
// does not exist. See gofakes3.BucketNotFound() for a convenient way to create one.
// FIXME: confirm with S3 whether this is the correct behaviour.
//
// DeleteObject must not return an error if the object does not exist. Source:
// https://docs.aws.amazon.com/sdk-for-go/api/service/s3/#S3.DeleteObject:
//
//	Removes the null version (if there is one) of an object and inserts a
//	delete marker, which becomes the latest version of the object. If there
//	isn't a null version, Amazon S3 does not remove any objects.
func (s *s3) DeleteObject(ctx context.Context, bucketName, objectName string) (gofakes3.ObjectDeleteResult, error) {
	err := s.b.DeleteObject(ctx, bucketName, objectName, api.DeleteObjectOptions{})
	if err != nil && strings.Contains(err.Error(), api.ErrBucketNotFound.Error()) {
		return gofakes3.ObjectDeleteResult{}, gofakes3.BucketNotFound(bucketName)
	} else if err != nil && !strings.Contains(err.Error(), api.ErrObjectNotFound.Error()) {
		return gofakes3.ObjectDeleteResult{}, gofakes3.ErrorMessage(gofakes3.ErrInternal, err.Error())
	}
	return gofakes3.ObjectDeleteResult{
		IsDeleteMarker: false, // not supported
		VersionID:      "",    // not supported
	}, nil
}

// PutObject should assume that the key is valid. The map containing meta
// may be nil.
//
// The size can be used if the backend needs to read the whole reader; use
// gofakes3.ReadAll() for this job rather than ioutil.ReadAll().
// TODO: Metadata is currently ignored. The backend requires an update to
// support it.
func (s *s3) PutObject(ctx context.Context, bucketName, key string, meta map[string]string, input io.Reader, size int64) (gofakes3.PutObjectResult, error) {
	opts := api.UploadObjectOptions{}
	if ct, ok := meta["Content-Type"]; ok {
		opts.MimeType = ct
	}
	ur, err := s.w.UploadObject(ctx, input, bucketName, key, opts)
	if err != nil && strings.Contains(err.Error(), api.ErrBucketNotFound.Error()) {
		return gofakes3.PutObjectResult{}, gofakes3.BucketNotFound(bucketName)
	} else if err != nil {
		return gofakes3.PutObjectResult{}, gofakes3.ErrorMessage(gofakes3.ErrInternal, err.Error())
	}
	return gofakes3.PutObjectResult{
		ETag:      ur.ETag,
		VersionID: "", // not supported
	}, nil
}

func (s *s3) DeleteMulti(ctx context.Context, bucketName string, objects ...string) (gofakes3.MultiDeleteResult, error) {
	var res gofakes3.MultiDeleteResult
	for _, objectName := range objects {
		err := s.b.DeleteObject(ctx, bucketName, objectName, api.DeleteObjectOptions{})
		if err != nil && !strings.Contains(err.Error(), api.ErrObjectNotFound.Error()) {
			res.Error = append(res.Error, gofakes3.ErrorResult{
				Key:     objectName,
				Code:    gofakes3.ErrInternal,
				Message: err.Error(),
			})
		} else {
			res.Deleted = append(res.Deleted, gofakes3.ObjectID{
				Key:       objectName,
				VersionID: "", // not supported
			})
		}
	}
	return res, nil
}

func (s *s3) CopyObject(ctx context.Context, srcBucket, srcKey, dstBucket, dstKey string, meta map[string]string) (gofakes3.CopyObjectResult, error) {
	var opts api.CopyObjectOptions
	if ct, ok := meta["Content-Type"]; ok {
		opts.MimeType = ct
	}
	obj, err := s.b.CopyObject(ctx, srcBucket, dstBucket, "/"+srcKey, "/"+dstKey, opts)
	if err != nil {
		return gofakes3.CopyObjectResult{}, gofakes3.ErrorMessage(gofakes3.ErrInternal, err.Error())
	}
	return gofakes3.CopyObjectResult{
		ETag:         api.FormatETag(obj.ETag),
		LastModified: gofakes3.NewContentTime(obj.ModTime.UTC()),
	}, nil
}

func (s *s3) CreateMultipartUpload(ctx context.Context, bucket, key string, meta map[string]string) (gofakes3.UploadID, error) {
	opts := api.CreateMultipartOptions{Key: object.NoOpKey}
	if ct, ok := meta["Content-Type"]; ok {
		opts.MimeType = ct
	}
	resp, err := s.b.CreateMultipartUpload(ctx, bucket, "/"+key, opts)
	if err != nil {
		return "", gofakes3.ErrorMessage(gofakes3.ErrInternal, err.Error())
	}
	return gofakes3.UploadID(resp.UploadID), nil
}

func (s *s3) UploadPart(ctx context.Context, bucket, object string, id gofakes3.UploadID, partNumber int, contentLength int64, input io.Reader) (*gofakes3.UploadPartResult, error) {
	res, err := s.w.UploadMultipartUploadPart(ctx, input, bucket, object, string(id), partNumber, api.UploadMultipartUploadPartOptions{
		DisablePreshardingEncryption: true,
	})
	if err != nil {
		return nil, gofakes3.ErrorMessage(gofakes3.ErrInternal, err.Error())
	}
	return &gofakes3.UploadPartResult{ETag: res.ETag}, nil
}

func (s *s3) ListMultipartUploads(ctx context.Context, bucket string, marker *gofakes3.UploadListMarker, prefix gofakes3.Prefix, limit int64) (*gofakes3.ListMultipartUploadsResult, error) {
	prefix.HasPrefix = prefix.Prefix != ""
	prefix.HasDelimiter = prefix.Delimiter != ""
	if prefix.HasDelimiter && prefix.Delimiter != "/" {
		return nil, gofakes3.ErrorMessage(gofakes3.ErrNotImplemented, "delimiter must be '/'")
	} else if prefix.HasPrefix {
		return nil, gofakes3.ErrorMessage(gofakes3.ErrNotImplemented, "prefix not supported")
	} else if marker != nil {
		return nil, gofakes3.ErrorMessage(gofakes3.ErrNotImplemented, "marker not supported")
	}
	resp, err := s.b.MultipartUploads(ctx, bucket, "", "", "", int(limit))
	if err != nil {
		return nil, gofakes3.ErrorMessage(gofakes3.ErrInternal, err.Error())
	}
	var uploads []gofakes3.ListMultipartUploadItem
	for _, upload := range resp.Uploads {
		uploads = append(uploads, gofakes3.ListMultipartUploadItem{
			Key:       upload.Path[1:],
			UploadID:  gofakes3.UploadID(upload.UploadID),
			Initiated: gofakes3.NewContentTime(upload.CreatedAt),
		})
	}
	return &gofakes3.ListMultipartUploadsResult{
		Bucket:             bucket,
		KeyMarker:          "",
		UploadIDMarker:     "",
		NextKeyMarker:      "",
		NextUploadIDMarker: "",
		MaxUploads:         limit,
		Delimiter:          prefix.Delimiter,
		Prefix:             prefix.Prefix,
		CommonPrefixes:     []gofakes3.CommonPrefix{},
		IsTruncated:        false,
		Uploads:            uploads,
	}, nil
}

func (s *s3) ListParts(ctx context.Context, bucket, object string, uploadID gofakes3.UploadID, marker int, limit int64) (*gofakes3.ListMultipartUploadPartsResult, error) {
	resp, err := s.b.MultipartUploadParts(ctx, bucket, "/"+object, string(uploadID), marker, limit)
	if err != nil {
		return nil, gofakes3.ErrorMessage(gofakes3.ErrInternal, err.Error())
	}
	var parts []gofakes3.ListMultipartUploadPartItem
	for _, part := range resp.Parts {
		parts = append(parts, gofakes3.ListMultipartUploadPartItem{
			PartNumber:   part.PartNumber,
			LastModified: gofakes3.NewContentTime(part.LastModified),
			ETag:         part.ETag,
			Size:         part.Size,
		})
	}

	return &gofakes3.ListMultipartUploadPartsResult{
		Bucket:               bucket,
		Key:                  object,
		UploadID:             uploadID,
		PartNumberMarker:     marker,
		NextPartNumberMarker: resp.NextMarker,
		MaxParts:             limit,
		IsTruncated:          resp.HasMore,
		Parts:                parts,
	}, nil
}

func (s *s3) AbortMultipartUpload(ctx context.Context, bucket, object string, id gofakes3.UploadID) error {
	err := s.b.AbortMultipartUpload(ctx, bucket, "/"+object, string(id))
	if err != nil {
		return gofakes3.ErrorMessage(gofakes3.ErrInternal, err.Error())
	}
	return nil
}

func (s *s3) CompleteMultipartUpload(ctx context.Context, bucket, object string, id gofakes3.UploadID, input *gofakes3.CompleteMultipartUploadRequest) (*gofakes3.CompleteMultipartUploadResult, error) {
	var parts []api.MultipartCompletedPart
	for _, part := range input.Parts {
		parts = append(parts, api.MultipartCompletedPart{
			ETag:       part.ETag,
			PartNumber: part.PartNumber,
		})
	}
	resp, err := s.b.CompleteMultipartUpload(ctx, bucket, "/"+object, string(id), parts)
	if err != nil {
		return nil, gofakes3.ErrorMessage(gofakes3.ErrInternal, err.Error())
	}
	return &gofakes3.CompleteMultipartUploadResult{
		ETag: api.FormatETag(resp.ETag),
	}, nil
}
