package e2e

import (
	"io"
	"time"

	s3aws "github.com/aws/aws-sdk-go/service/s3"
)

type (
	s3TestClient struct {
		s3 *s3aws.S3
	}
)

// request and response types
type (
	bucketInfo struct {
		creationDate time.Time
		name         string
	}

	copyObjectResponse struct {
		lastModified time.Time
		etag         string
	}

	listBucketResponse struct {
		buckets []bucketInfo
	}

	completePart struct {
		partNumber int64
		etag       string
	}

	getObjectResponse struct {
		body io.ReadCloser
		etag string
	}

	headObjectResponse struct {
		contentLength int64
		etag          string
	}

	putObjectOptions struct {
		metadata map[string]string
	}

	putObjectPartOptions struct {
	}

	putObjectResponse struct {
		etag string
	}

	putObjectPartResponse struct {
		etag string
	}

	uploadInfo struct {
	}
)

func (c *s3TestClient) CompleteMultipartUpload(bucket, object, uploadID string, parts []completePart, opts putObjectOptions) (uploadInfo, error) {
	var input s3aws.CompleteMultipartUploadInput
	input.SetBucket(bucket)
	input.SetKey(object)
	input.SetUploadId(uploadID)
	var upload s3aws.CompletedMultipartUpload
	var inputParts []*s3aws.CompletedPart
	for i := range parts {
		inputParts = append(inputParts, &s3aws.CompletedPart{
			PartNumber: &parts[i].partNumber,
		})
		upload.SetParts(inputParts)
	}
	input.SetMultipartUpload(&upload)

	_, err := c.s3.CompleteMultipartUpload(&input)
	if err != nil {
		return uploadInfo{}, err
	}
	return uploadInfo{}, nil
}

func (c *s3TestClient) CopyObject(bucket, srcKey, dstKey string) (copyObjectResponse, error) {
	var input s3aws.CopyObjectInput
	input.SetBucket(bucket)
	input.SetCopySource(srcKey)
	input.SetKey(dstKey)
	resp, err := c.s3.CopyObject(&input)
	if err != nil {
		return copyObjectResponse{}, err
	}
	return copyObjectResponse{
		lastModified: *resp.CopyObjectResult.LastModified,
		etag:         *resp.CopyObjectResult.ETag,
	}, nil
}

func (c *s3TestClient) CreateBucket(bucket string) error {
	var input s3aws.CreateBucketInput
	input.SetBucket(bucket)
	_, err := c.s3.CreateBucket(&input)
	return err
}

func (c *s3TestClient) DeleteBucket(bucket string) error {
	var input s3aws.DeleteBucketInput
	input.SetBucket(bucket)
	_, err := c.s3.DeleteBucket(&input)
	return err
}

func (c *s3TestClient) DeleteObject(bucket, objKey string) error {
	var input s3aws.DeleteObjectInput
	input.SetBucket(bucket)
	input.SetKey(objKey)
	_, err := c.s3.DeleteObject(&input)
	return err
}

func (c *s3TestClient) GetObject(bucket, objKey string) (getObjectResponse, error) {
	var input s3aws.GetObjectInput
	input.SetBucket(bucket)
	input.SetKey(objKey)
	resp, err := c.s3.GetObject(&input)
	if err != nil {
		return getObjectResponse{}, err
	}
	return getObjectResponse{
		etag: *resp.ETag,
		body: resp.Body,
	}, nil
}

func (c *s3TestClient) HeadBucket(bucket string) error {
	var input s3aws.HeadBucketInput
	input.SetBucket(bucket)
	_, err := c.s3.HeadBucket(&input)
	if err != nil {
		return err
	}
	return nil
}

func (c *s3TestClient) HeadObject(bucket, objKey string) (headObjectResponse, error) {
	var input s3aws.HeadObjectInput
	input.SetBucket(bucket)
	input.SetKey(objKey)
	resp, err := c.s3.HeadObject(&input)
	if err != nil {
		return headObjectResponse{}, err
	}
	return headObjectResponse{
		etag:          *resp.ETag,
		contentLength: *resp.ContentLength,
	}, nil
}

func (c *s3TestClient) ListBuckets() (lbr listBucketResponse, err error) {
	resp, err := c.s3.ListBuckets(&s3aws.ListBucketsInput{})
	if err != nil {
		return listBucketResponse{}, err
	}
	for _, b := range resp.Buckets {
		lbr.buckets = append(lbr.buckets, bucketInfo{
			name:         *b.Name,
			creationDate: *b.CreationDate,
		})
	}
	return lbr, nil
}

func (c *s3TestClient) NewMultipartUpload(bucket, objKey string, opts putObjectOptions) (string, error) {
	var input s3aws.CreateMultipartUploadInput
	input.SetBucket(bucket)
	input.SetKey(objKey)
	if opts.metadata != nil {
		md := make(map[string]*string)
		for k := range opts.metadata {
			v := opts.metadata[k] // copy to avoid reference to loop variable
			md[k] = &v
		}
		input.SetMetadata(md)
	}
	resp, err := c.s3.CreateMultipartUpload(&input)
	if err != nil {
		return "", err
	}
	return *resp.UploadId, nil
}

func (c *s3TestClient) PutObject(bucket, objKey string, body io.ReadSeeker) (putObjectResponse, error) {
	contentLength, err := body.Seek(0, io.SeekEnd)
	if err != nil {
		return putObjectResponse{}, err
	} else if _, err := body.Seek(0, io.SeekStart); err != nil {
		return putObjectResponse{}, err
	}
	var input s3aws.PutObjectInput
	input.SetBucket(bucket)
	input.SetBody(body)
	input.SetKey(objKey)
	input.SetContentLength(contentLength)

	resp, err := c.s3.PutObject(&input)
	if err != nil {
		return putObjectResponse{}, err
	}
	return putObjectResponse{
		etag: *resp.ETag,
	}, nil
}

func (c *s3TestClient) PutObjectPart(bucket, objKey, uploadID string, partNum int64, body io.ReadSeeker, opts putObjectPartOptions) (putObjectPartResponse, error) {
	contentLength, err := body.Seek(0, io.SeekEnd)
	if err != nil {
		return putObjectPartResponse{}, err
	} else if _, err := body.Seek(0, io.SeekStart); err != nil {
		return putObjectPartResponse{}, err
	}
	var input s3aws.UploadPartInput
	input.SetBucket(bucket)
	input.SetKey(objKey)
	input.SetUploadId(uploadID)
	input.SetPartNumber(partNum)
	input.SetBody(body)
	input.SetContentLength(contentLength)
	part, err := c.s3.UploadPart(&input)
	if err != nil {
		return putObjectPartResponse{}, err
	}
	return putObjectPartResponse{
		etag: *part.ETag,
	}, nil
}
