package s3

import (
	"io"

	"github.com/Mikubill/gofakes3"
	"go.uber.org/zap"
)

type s3 struct {
	w      worker
	logger *zap.SugaredLogger
}

// ListBuckets returns a list of all buckets owned by the authenticated
// sender of the request.
// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTServiceGET.html
func (s *s3) ListBuckets() ([]gofakes3.BucketInfo, error) {
	panic("not implemented")
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
func (s *s3) ListBucket(name string, prefix *gofakes3.Prefix, page gofakes3.ListBucketPage) (*gofakes3.ObjectList, error) {
	panic("not implemented")
}

// CreateBucket creates the bucket if it does not already exist. The name
// should be assumed to be a valid name.
//
// If the bucket already exists, a gofakes3.ResourceError with
// gofakes3.ErrBucketAlreadyExists MUST be returned.
func (s *s3) CreateBucket(name string) error {
	panic("not implemented")
}

// BucketExists should return a boolean indicating the bucket existence, or
// an error if the backend was unable to determine existence.
func (s *s3) BucketExists(name string) (exists bool, err error) {
	panic("not implemented")
}

// DeleteBucket deletes a bucket if and only if it is empty.
//
// If the bucket is not empty, gofakes3.ResourceError with
// gofakes3.ErrBucketNotEmpty MUST be returned.
//
// If the bucket does not exist, gofakes3.ErrNoSuchBucket MUST be returned.
//
// AWS does not validate the bucket's name for anything other than existence.
func (s *s3) DeleteBucket(name string) error {
	panic("not implemented")
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
func (s *s3) GetObject(bucketName, objectName string, rangeRequest *gofakes3.ObjectRangeRequest) (*gofakes3.Object, error) {
	panic("not implemented")
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
	panic("not implemented")
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
	panic("not implemented")
}

// PutObject should assume that the key is valid. The map containing meta
// may be nil.
//
// The size can be used if the backend needs to read the whole reader; use
// gofakes3.ReadAll() for this job rather than ioutil.ReadAll().
func (s *s3) PutObject(bucketName, key string, meta map[string]string, input io.Reader, size int64) (gofakes3.PutObjectResult, error) {
	panic("not implemented")
}

func (s *s3) DeleteMulti(bucketName string, objects ...string) (gofakes3.MultiDeleteResult, error) {
	panic("not implemented")
}

func (s *s3) CopyObject(srcBucket, srcKey, dstBucket, dstKey string, meta map[string]string) (gofakes3.CopyObjectResult, error) {
	panic("not implemented")
}
