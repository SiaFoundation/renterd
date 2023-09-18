package s3

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
	"time"

	"github.com/SiaFoundation/gofakes3"
	"go.sia.tech/renterd/api"
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
func (s *s3) ListBuckets() ([]gofakes3.BucketInfo, error) {
	buckets, err := s.b.ListBuckets(context.Background())
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
func (s *s3) ListBucket(bucketName string, prefix *gofakes3.Prefix, page gofakes3.ListBucketPage) (*gofakes3.ObjectList, error) {
	exists, err := s.BucketExists(bucketName)
	if err != nil {
		return nil, err
	} else if !exists {
		return nil, gofakes3.BucketNotFound(bucketName)
	}

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

	// Specify bucket.
	opts := []api.ObjectsOption{api.ObjectsWithBucket(bucketName)}

	// Handle prefix.
	var path string // root of bucket
	if prefix.HasPrefix {
		if idx := strings.LastIndex(prefix.Prefix, "/"); idx != -1 {
			path = prefix.Prefix[:idx+1]
			prefix.Prefix = prefix.Prefix[idx+1:]
		}
		opts = append(opts, api.ObjectsWithPrefix(prefix.Prefix))
	}

	// Handle pagination.
	if page.MaxKeys == 0 {
		page.MaxKeys = maxKeysDefault
	}
	if page.HasMarker {
		opts = append(opts, api.ObjectsWithMarker(page.Marker))
		opts = append(opts, api.ObjectsWithLimit(int(page.MaxKeys)))
	}

	// Fetch all objects of bucket with the given prefix.
	res, err := s.b.Object(context.Background(), path, opts...)
	if err != nil {
		return nil, gofakes3.ErrorMessage(gofakes3.ErrInternal, err.Error())
	}

	// Create the list response
	response := gofakes3.NewObjectList()
	if res.HasMore {
		response.IsTruncated = true
		response.NextMarker = res.Entries[len(res.Entries)-1].Name
	}

	// Loop over the entries and add them to the response.
	for _, object := range res.Entries {
		key := strings.TrimPrefix(object.Name, "/")
		if strings.HasSuffix(key, "/") {
			response.AddPrefix(gofakes3.URLEncode(key))
			continue
		}

		item := &gofakes3.Content{
			Key:          gofakes3.URLEncode(key),
			LastModified: gofakes3.NewContentTime(time.Unix(0, 0).UTC()), // TODO: don't have that
			ETag:         hex.EncodeToString(frand.Bytes(32)),            // TODO: don't have that
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
func (s *s3) CreateBucket(name string) error {
	if err := s.b.CreateBucket(context.Background(), name); err != nil && strings.Contains(err.Error(), api.ErrBucketExists.Error()) {
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
func (s *s3) BucketExists(name string) (bool, error) {
	_, err := s.b.Bucket(context.Background(), name)
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
func (s *s3) DeleteBucket(name string) error {
	if err := s.b.DeleteBucket(context.Background(), name); err != nil && strings.Contains(err.Error(), api.ErrBucketNotEmpty.Error()) {
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
func (s *s3) GetObject(bucketName, objectName string, rangeRequest *gofakes3.ObjectRangeRequest) (*gofakes3.Object, error) {
	if rangeRequest != nil && rangeRequest.FromEnd {
		return nil, gofakes3.ErrorMessage(gofakes3.ErrNotImplemented, "range request from end not supported")
	}

	var opts []api.DownloadObjectOption
	if rangeRequest != nil {
		opts = append(opts, api.DownloadWithRange(rangeRequest.Start, rangeRequest.End))
	}
	res, err := s.w.GetObject(context.Background(), bucketName, objectName, opts...)
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
			Start:  res.Range.Start,
			Length: res.Range.Length,
		}
	}

	// TODO: When we support metadata we need to add it here.
	metadata := map[string]string{
		"Content-Type":  res.ContentType,
		"Last-Modified": res.ModTime.Format(http.TimeFormat),
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
func (s *s3) HeadObject(bucketName, objectName string) (*gofakes3.Object, error) {
	if strings.HasSuffix(objectName, "/") {
		return nil, errors.New("object name must not end with '/'")
	}
	res, err := s.b.Object(context.Background(), objectName, api.ObjectsWithBucket(bucketName))
	if err != nil && strings.Contains(err.Error(), api.ErrObjectNotFound.Error()) {
		return nil, gofakes3.KeyNotFound(objectName)
	}
	// TODO: When we support metadata we need to add it here.
	metadata := map[string]string{
		"Content-Type":  mime.TypeByExtension(objectName),
		"Last-Modified": time.Now().UTC().Format(http.TimeFormat), // TODO: update this when object has metadata
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
func (s *s3) DeleteObject(bucketName, objectName string) (gofakes3.ObjectDeleteResult, error) {
	exists, err := s.BucketExists(bucketName)
	if err != nil {
		return gofakes3.ObjectDeleteResult{}, err
	} else if !exists {
		return gofakes3.ObjectDeleteResult{}, gofakes3.BucketNotFound(bucketName)
	}

	err = s.b.DeleteObject(context.Background(), bucketName, objectName, false)
	if err != nil && !strings.Contains(err.Error(), api.ErrObjectNotFound.Error()) {
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
func (s *s3) PutObject(bucketName, key string, meta map[string]string, input io.Reader, size int64) (gofakes3.PutObjectResult, error) {
	err := s.w.UploadObject(context.Background(), input, key, api.UploadWithBucket(bucketName))
	if err != nil {
		return gofakes3.PutObjectResult{}, gofakes3.ErrorMessage(gofakes3.ErrInternal, err.Error())
	}
	return gofakes3.PutObjectResult{
		VersionID: "", // not supported
	}, nil
}

func (s *s3) DeleteMulti(bucketName string, objects ...string) (gofakes3.MultiDeleteResult, error) {
	var res gofakes3.MultiDeleteResult
	for _, objectName := range objects {
		err := s.b.DeleteObject(context.Background(), objectName, bucketName, false)
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

// TODO: use metadata when we have support for it
func (s *s3) CopyObject(srcBucket, srcKey, dstBucket, dstKey string, meta map[string]string) (gofakes3.CopyObjectResult, error) {
	err := s.b.CopyObject(context.Background(), srcBucket, dstBucket, "/"+srcKey, "/"+dstKey)
	if err != nil {
		return gofakes3.CopyObjectResult{}, gofakes3.ErrorMessage(gofakes3.ErrInternal, err.Error())
	}
	return gofakes3.CopyObjectResult{
		ETag:         "",                                             // TODO: don't have that
		LastModified: gofakes3.NewContentTime(time.Unix(0, 0).UTC()), // TODO: don't have that
	}, nil
}

func (s *s3) CreateMultipartUpload(bucket, object string, meta map[string]string) (gofakes3.UploadID, error) {
	resp, err := s.b.CreateMultipartUpload(context.Background(), bucket, "/"+object)
	if err != nil {
		return "", gofakes3.ErrorMessage(gofakes3.ErrInternal, err.Error())
	}
	return gofakes3.UploadID(resp.UploadID), nil
}

func (s *s3) UploadPart(bucket, object string, id gofakes3.UploadID, partNumber int, contentLength int64, input io.Reader) (etag string, err error) {
	etag, err = s.w.UploadMultipartUploadPart(context.Background(), input, object, string(id), partNumber, api.UploadWithDisabledPreshardingEncryption())
	if err != nil {
		return "", gofakes3.ErrorMessage(gofakes3.ErrInternal, err.Error())
	}
	return etag, nil
}

func (s *s3) ListMultipartUploads(bucket string, marker *gofakes3.UploadListMarker, prefix gofakes3.Prefix, limit int64) (*gofakes3.ListMultipartUploadsResult, error) {
	prefix.HasPrefix = prefix.Prefix != ""
	prefix.HasDelimiter = prefix.Delimiter != ""
	if prefix.HasDelimiter && prefix.Delimiter != "/" {
		return nil, gofakes3.ErrorMessage(gofakes3.ErrNotImplemented, "delimiter must be '/'")
	} else if prefix.HasPrefix {
		return nil, gofakes3.ErrorMessage(gofakes3.ErrNotImplemented, "prefix not supported")
	} else if marker != nil {
		return nil, gofakes3.ErrorMessage(gofakes3.ErrNotImplemented, "marker not supported")
	}
	resp, err := s.b.ListMultipartUploads(context.Background(), bucket, "", "", "", int(limit))
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

func (s *s3) ListParts(bucket, object string, uploadID gofakes3.UploadID, marker int, limit int64) (*gofakes3.ListMultipartUploadPartsResult, error) {
	resp, err := s.b.ListMultipartUploadParts(context.Background(), bucket, "/"+object, string(uploadID), marker, limit)
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
		IsTruncated:          resp.IsTruncated,
		Parts:                parts,
	}, nil
}

func (s *s3) AbortMultipartUpload(bucket, object string, id gofakes3.UploadID) error {
	panic("not implemented")
}

func (s *s3) CompleteMultipartUpload(bucket, object string, id gofakes3.UploadID, input *gofakes3.CompleteMultipartUploadRequest) (versionID gofakes3.VersionID, etag string, err error) {
	var parts []api.MultipartCompletedPart
	for _, part := range input.Parts {
		parts = append(parts, api.MultipartCompletedPart{
			ETag:       part.ETag,
			PartNumber: part.PartNumber,
		})
	}
	resp, err := s.b.CompleteMultipartUpload(context.Background(), bucket, "/"+object, string(id), parts)
	if err != nil {
		return "", "", gofakes3.ErrorMessage(gofakes3.ErrInternal, err.Error())
	}
	return "", resp.ETag, nil
}
