package e2e

import (
	"encoding/base64"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go/aws"
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

	listObjectsOptions struct {
		prefix    string
		marker    string
		delimiter string
		maxKeys   int64
	}

	listObjectsResponse struct {
		contents       []headObjectResponse
		commonPrefixes []string
		nextMarker     string
		truncated      bool
	}

	completePart struct {
		partNumber int64
		etag       string
	}

	getObjectOptions struct {
		offset int64
		length int64
	}

	getObjectResponse struct {
		body         io.ReadCloser
		etag         string
		lastModified time.Time
		metadata     map[string]string
	}

	headObjectResponse struct {
		contentLength int64
		etag          string
		key           string
		lastModified  time.Time
		metadata      map[string]string
	}

	multipartUploadInfo struct {
		bucket   string
		key      string
		uploadID string
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
		bucket   string
		etag     string
		key      string
		uploadID string
	}
)

func (c *s3TestClient) Config() aws.Config {
	return c.s3.Config
}

func (c *s3TestClient) AbortMultipartUpload(bucket, key string, uploadID string) error {
	var input s3aws.AbortMultipartUploadInput
	input.SetBucket(bucket)
	input.SetKey(key)
	input.SetUploadId(uploadID)
	_, err := c.s3.AbortMultipartUpload(&input)
	return err
}

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

	resp, err := c.s3.CompleteMultipartUpload(&input)
	if err != nil {
		return uploadInfo{}, err
	}
	return uploadInfo{
		bucket:   *resp.Bucket,
		etag:     *resp.ETag,
		key:      *resp.Key,
		uploadID: uploadID,
	}, nil
}

func (c *s3TestClient) CopyObject(srcBucket, dstBucket, srcKey, dstKey string, opts putObjectOptions) (copyObjectResponse, error) {
	var input s3aws.CopyObjectInput
	input.SetCopySource(fmt.Sprintf("%s/%s", srcBucket, srcKey))
	input.SetBucket(dstBucket)
	input.SetKey(dstKey)
	if opts.metadata != nil {
		md := make(map[string]*string)
		for k := range opts.metadata {
			v := opts.metadata[k]
			md[k] = &v
		}
		input.SetMetadata(md)
	}
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

func (c *s3TestClient) GetObject(bucket, objKey string, opts getObjectOptions) (getObjectResponse, error) {
	var input s3aws.GetObjectInput
	input.SetBucket(bucket)
	input.SetKey(objKey)
	if hasOffset, hasLength := opts.offset > 0, opts.length > 0; hasOffset || hasLength {
		if hasLength {
			input.SetRange(fmt.Sprintf("bytes=%d-%d", opts.offset, opts.offset+opts.length-1))
		} else {
			input.SetRange(fmt.Sprintf("bytes=%d-", opts.offset))
		}
	}
	resp, err := c.s3.GetObject(&input)
	if err != nil {
		return getObjectResponse{}, err
	}
	md := make(map[string]string)
	for k, v := range resp.Metadata {
		if v != nil {
			md[k] = *v
		}
	}
	return getObjectResponse{
		etag:         *resp.ETag,
		body:         resp.Body,
		lastModified: *resp.LastModified,
		metadata:     md,
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
	md := make(map[string]string)
	for k, v := range resp.Metadata {
		if v != nil {
			md[k] = *v
		}
	}
	return headObjectResponse{
		etag:          *resp.ETag,
		contentLength: *resp.ContentLength,
		key:           objKey,
		lastModified:  *resp.LastModified,
		metadata:      md,
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

func (c *s3TestClient) ListMultipartUploads(bucket string) ([]multipartUploadInfo, error) {
	var input s3aws.ListMultipartUploadsInput
	input.SetBucket(bucket)
	resp, err := c.s3.ListMultipartUploads(&input)
	if err != nil {
		return nil, err
	}
	var uploads []multipartUploadInfo
	for _, u := range resp.Uploads {
		uploads = append(uploads, multipartUploadInfo{
			bucket:   bucket,
			key:      *u.Key,
			uploadID: *u.UploadId,
		})
	}
	return uploads, nil
}

type listObjectPartsResponse struct {
	bucket      string
	key         string
	uploadId    string
	objectParts []objectPart
}

type objectPart struct {
	partNumber int64
	size       int64
	etag       string
}

func (c *s3TestClient) ListObjectParts(bucket, objKey, uploadID string) (lopr listObjectPartsResponse, err error) {
	var input s3aws.ListPartsInput
	input.SetBucket(bucket)
	input.SetKey(objKey)
	input.SetUploadId(uploadID)
	resp, err := c.s3.ListParts(&input)
	if err != nil {
		return listObjectPartsResponse{}, err
	}
	lopr.bucket = *resp.Bucket
	lopr.key = *resp.Key
	lopr.uploadId = *resp.UploadId
	for _, p := range resp.Parts {
		lopr.objectParts = append(lopr.objectParts, objectPart{
			partNumber: *p.PartNumber,
			size:       *p.Size,
			etag:       *p.ETag,
		})
	}
	return lopr, err
}

func (c *s3TestClient) ListObjects(bucket string, opts listObjectsOptions) (lor listObjectsResponse, err error) {
	var input s3aws.ListObjectsV2Input
	input.SetBucket(bucket)
	if opts.prefix != "" {
		input.SetPrefix(opts.prefix)
	}
	if opts.marker != "" {
		opts.marker = base64.URLEncoding.EncodeToString([]byte(opts.marker))
		input.SetContinuationToken(opts.marker)
	}
	if opts.delimiter != "" {
		input.SetDelimiter(opts.delimiter)
	}
	if opts.maxKeys != 0 {
		input.SetMaxKeys(opts.maxKeys)
	}
	resp, err := c.s3.ListObjectsV2(&input)
	if err != nil {
		return listObjectsResponse{}, err
	}
	for _, content := range resp.Contents {
		lor.contents = append(lor.contents, headObjectResponse{
			contentLength: *content.Size,
			etag:          *content.ETag,
			key:           *content.Key,
			lastModified:  *content.LastModified,
		})
	}
	for _, prefix := range resp.CommonPrefixes {
		lor.commonPrefixes = append(lor.commonPrefixes, *prefix.Prefix)
	}
	lor.truncated = *resp.IsTruncated
	if resp.NextContinuationToken != nil {
		m, err := base64.URLEncoding.DecodeString(*resp.NextContinuationToken)
		if err != nil {
			return listObjectsResponse{}, err
		}
		lor.nextMarker = string(m)
	}
	return lor, nil
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

func (c *s3TestClient) PutObject(bucket, objKey string, body io.ReadSeeker, opts putObjectOptions) (putObjectResponse, error) {
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

	if opts.metadata != nil {
		md := make(map[string]*string)
		for k := range opts.metadata {
			v := opts.metadata[k] // copy to avoid reference to loop variable
			md[k] = &v
		}
		input.SetMetadata(md)
	}

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
