package mocks

import (
	"context"

	"go.sia.tech/renterd/api"
)

type s3Mock struct{}

func (*s3Mock) CreateBucket(context.Context, string, api.CreateBucketOptions) error {
	return nil
}

func (*s3Mock) DeleteBucket(context.Context, string) error {
	return nil
}

func (*s3Mock) ListBuckets(context.Context) (buckets []api.Bucket, err error) {
	return nil, nil
}

func (*s3Mock) CopyObject(context.Context, string, string, string, string, api.CopyObjectOptions) (om api.ObjectMetadata, err error) {
	return api.ObjectMetadata{}, nil
}

func (*s3Mock) AbortMultipartUpload(context.Context, string, string, string) (err error) {
	return nil
}

func (*s3Mock) CompleteMultipartUpload(context.Context, string, string, string, []api.MultipartCompletedPart, api.CompleteMultipartOptions) (_ api.MultipartCompleteResponse, err error) {
	return api.MultipartCompleteResponse{}, nil
}

func (*s3Mock) CreateMultipartUpload(context.Context, string, string, api.CreateMultipartOptions) (api.MultipartCreateResponse, error) {
	return api.MultipartCreateResponse{}, nil
}

func (*s3Mock) MultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker string, maxUploads int) (resp api.MultipartListUploadsResponse, _ error) {
	return api.MultipartListUploadsResponse{}, nil
}

func (*s3Mock) MultipartUploadParts(ctx context.Context, bucket, object string, uploadID string, marker int, limit int64) (resp api.MultipartListPartsResponse, _ error) {
	return api.MultipartListPartsResponse{}, nil
}

func (*s3Mock) S3Settings(context.Context) (as api.S3Settings, err error) {
	return api.S3Settings{}, nil
}

func (*s3Mock) UpdateSetting(context.Context, string, interface{}) error {
	return nil
}
