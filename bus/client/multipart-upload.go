package client

import (
	"context"
	"fmt"

	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/object"
)

// AbortMultipartUpload aborts a multipart upload.
func (c *Client) AbortMultipartUpload(ctx context.Context, bucket, key string, uploadID string) (err error) {
	err = c.c.WithContext(ctx).POST("/multipart/abort", api.MultipartAbortRequest{
		Bucket:   bucket,
		Key:      key,
		UploadID: uploadID,
	}, nil)
	return
}

// AddMultipartPart adds a part to a multipart upload.
func (c *Client) AddMultipartPart(ctx context.Context, bucket, key, contractSet, eTag, uploadID string, partNumber int, slices []object.SlabSlice) (err error) {
	err = c.c.WithContext(ctx).PUT("/multipart/part", api.MultipartAddPartRequest{
		Bucket:      bucket,
		ETag:        eTag,
		Key:         key,
		ContractSet: contractSet,
		UploadID:    uploadID,
		PartNumber:  partNumber,
		Slices:      slices,
	})
	return
}

// CompleteMultipartUpload completes a multipart upload.
func (c *Client) CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, parts []api.MultipartCompletedPart, opts api.CompleteMultipartOptions) (resp api.MultipartCompleteResponse, err error) {
	err = c.c.WithContext(ctx).POST("/multipart/complete", api.MultipartCompleteRequest{
		Bucket:   bucket,
		Key:      key,
		Metadata: opts.Metadata,
		UploadID: uploadID,
		Parts:    parts,
	}, &resp)
	return
}

// CreateMultipartUpload creates a new multipart upload.
func (c *Client) CreateMultipartUpload(ctx context.Context, bucket, key string, opts api.CreateMultipartOptions) (resp api.MultipartCreateResponse, err error) {
	err = c.c.WithContext(ctx).POST("/multipart/create", api.MultipartCreateRequest{
		Bucket:                      bucket,
		DisableClientSideEncryption: opts.DisableClientSideEncryption,
		Key:                         key,
		MimeType:                    opts.MimeType,
		Metadata:                    opts.Metadata,
	}, &resp)
	return
}

// MultipartUpload returns information about a specific multipart upload.
func (c *Client) MultipartUpload(ctx context.Context, uploadID string) (resp api.MultipartUpload, err error) {
	err = c.c.WithContext(ctx).GET(fmt.Sprintf("/multipart/upload/%s", uploadID), &resp)
	return
}

// MultipartUploads returns information about all multipart uploads.
func (c *Client) MultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker string, maxUploads int) (resp api.MultipartListUploadsResponse, err error) {
	err = c.c.WithContext(ctx).POST("/multipart/listuploads", api.MultipartListUploadsRequest{
		Bucket:         bucket,
		Prefix:         prefix,
		KeyMarker:      keyMarker,
		UploadIDMarker: uploadIDMarker,
		Limit:          maxUploads,
	}, &resp)
	return
}

// MultipartUploadParts returns information about all parts of a multipart upload.
func (c *Client) MultipartUploadParts(ctx context.Context, bucket, key string, uploadID string, partNumberMarker int, limit int64) (resp api.MultipartListPartsResponse, err error) {
	err = c.c.WithContext(ctx).POST("/multipart/listparts", api.MultipartListPartsRequest{
		Bucket:           bucket,
		Key:              key,
		UploadID:         uploadID,
		PartNumberMarker: partNumberMarker,
		Limit:            limit,
	}, &resp)
	return
}
