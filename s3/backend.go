package s3

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"mime"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/Mikubill/gofakes3"
	"go.sia.tech/renterd/api"
	"go.uber.org/zap"
	"lukechampine.com/frand"
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
			CreationDate: gofakes3.NewContentTime(time.Unix(0, 0).UTC()), // TODO: don't have that
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
func (s *s3) ListBucket(name string, prefix *gofakes3.Prefix, page gofakes3.ListBucketPage) (*gofakes3.ObjectList, error) {
	if prefix == nil {
		prefix = &gofakes3.Prefix{}
	} else if prefix.HasDelimiter && prefix.Delimiter != "/" {
		// NOTE: this is a limitation of the current implementation of the bus.
		return nil, gofakes3.ErrorMessage(gofakes3.ErrNotImplemented, "delimiter must be '/'")
	}

	// Workaround for empty prefix
	prefix.HasPrefix = prefix.Prefix != ""

	// Handle prefix.
	path := name + "/" // root of bucket
	var opts []api.ObjectsOption
	if prefix.HasPrefix {
		idx := strings.LastIndex(prefix.Prefix, "/")
		if idx == -1 {
			// path remains the same and the prefix becomes the prefix
			opts = append(opts, api.ObjectsWithPrefix(name+"/"+prefix.Prefix))
		} else {
			// part of the prefix becomes the path
			path = name + "/" + prefix.Prefix[:idx+1]
			opts = append(opts, api.ObjectsWithPrefix(prefix.Prefix[idx+1:]))
		}
	}

	// Fetch all objects of bucket with the given prefix.
	_, objects, err := s.b.Object(context.Background(), path, opts...)
	if err != nil {
		return nil, gofakes3.ErrorMessage(gofakes3.ErrInternal, err.Error())
	}

	// Match objects against prefix. If the object is a common prefix (folder),
	// add it as a prefix to the response and otherwise as an object.
	resp := gofakes3.NewObjectList()
	for _, object := range objects {
		objectKey := object.Name[len(name)+2:] // trim bucket name

		if strings.HasSuffix(objectKey, "/") {
			resp.AddPrefix(gofakes3.URLEncode(objectKey))
			continue
		}

		item := &gofakes3.Content{
			Key:          gofakes3.URLEncode(objectKey),
			LastModified: gofakes3.NewContentTime(time.Unix(0, 0).UTC()), // TODO: don't have that
			ETag:         hex.EncodeToString(frand.Bytes(32)),            // TODO: don't have that
			Size:         object.Size,
			StorageClass: gofakes3.StorageStandard,
		}
		resp.Add(item)
	}

	// Sort by alphabet.
	sort.Slice(resp.CommonPrefixes, func(i, j int) bool {
		return resp.CommonPrefixes[i].Prefix < resp.CommonPrefixes[j].Prefix
	})

	// Apply pagination.
	if page.MaxKeys == 0 {
		page.MaxKeys = math.MaxInt64 // no limit specified
	}
	if page.HasMarker {
		// If there is a marker, remove all objects up until and including the
		// marker.
		for i, obj := range resp.Contents {
			if obj.Key == page.Marker {
				resp.Contents = resp.Contents[i+1:]
				break
			}
		}
		for i, obj := range resp.CommonPrefixes {
			if obj.Prefix == page.Marker {
				resp.CommonPrefixes = resp.CommonPrefixes[i+1:]
				break
			}
		}
	}

	response := gofakes3.NewObjectList()
	for _, obj := range resp.CommonPrefixes {
		if page.MaxKeys <= 0 {
			break
		}
		response.AddPrefix(obj.Prefix)
		page.MaxKeys--
	}

	for _, obj := range resp.Contents {
		if page.MaxKeys <= 0 {
			break
		}
		response.Add(obj)
		page.MaxKeys--
	}

	if len(resp.CommonPrefixes)+len(resp.Contents) > int(page.MaxKeys) {
		response.IsTruncated = true
		if len(response.Contents) > 0 {
			response.NextMarker = response.Contents[len(response.Contents)-1].Key
		} else {
			response.NextMarker = response.CommonPrefixes[len(response.CommonPrefixes)-1].Prefix
		}
	}
	return resp, nil
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
	res, err := s.w.GetObject(context.Background(), fmt.Sprintf("%s/%s", bucketName, objectName), opts...)
	if err != nil {
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
	obj, _, err := s.b.Object(context.Background(), fmt.Sprintf("%s/%s", bucketName, objectName))
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
		Size:     obj.Size,
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
	err := s.b.DeleteObject(context.Background(), fmt.Sprintf("%s/%s", bucketName, objectName), false)
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
	err := s.w.UploadObject(context.Background(), input, fmt.Sprintf("%s/%s", bucketName, key))
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
		err := s.b.DeleteObject(context.Background(), fmt.Sprintf("%s/%s", bucketName, objectName), false)
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
// TODO: impelement once we have ability to copy objects in bus
func (s *s3) CopyObject(srcBucket, srcKey, dstBucket, dstKey string, meta map[string]string) (gofakes3.CopyObjectResult, error) {
	return gofakes3.CopyObjectResult{}, gofakes3.ErrorMessage(gofakes3.ErrNotImplemented, "copying objects is not supported")
}
