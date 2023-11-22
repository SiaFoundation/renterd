package client

import (
	"context"
	"fmt"
	"net/url"

	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/object"
)

// AddObject stores the provided object under the given path.
func (c *Client) AddObject(ctx context.Context, bucket, path, contractSet string, o object.Object, opts api.AddObjectOptions) (err error) {
	path = api.ObjectPathEscape(path)
	err = c.c.WithContext(ctx).PUT(fmt.Sprintf("/objects/%s", path), api.ObjectAddRequest{
		Bucket:      bucket,
		ContractSet: contractSet,
		Object:      o,
		MimeType:    opts.MimeType,
		ETag:        opts.ETag,
	})
	return
}

// CopyObject copies the object from the source bucket and path to the
// destination bucket and path.
func (c *Client) CopyObject(ctx context.Context, srcBucket, dstBucket, srcPath, dstPath string, opts api.CopyObjectOptions) (om api.ObjectMetadata, err error) {
	err = c.c.WithContext(ctx).POST("/objects/copy", api.ObjectsCopyRequest{
		SourceBucket:      srcBucket,
		DestinationBucket: dstBucket,
		SourcePath:        srcPath,
		DestinationPath:   dstPath,

		MimeType: opts.MimeType,
	}, &om)
	return
}

// DeleteObject either deletes the object at the given path or if batch=true
// deletes all objects that start with the given path.
func (c *Client) DeleteObject(ctx context.Context, bucket, path string, opts api.DeleteObjectOptions) (err error) {
	values := url.Values{}
	values.Set("bucket", bucket)
	opts.Apply(values)

	path = api.ObjectPathEscape(path)
	err = c.c.WithContext(ctx).DELETE(fmt.Sprintf("/objects/%s?"+values.Encode(), path))
	return
}

// ListOBjects lists objects in the given bucket.
func (c *Client) ListObjects(ctx context.Context, bucket string, opts api.ListObjectOptions) (resp api.ObjectsListResponse, err error) {
	err = c.c.WithContext(ctx).POST("/objects/list", api.ObjectsListRequest{
		Bucket: bucket,
		Limit:  opts.Limit,
		Prefix: opts.Prefix,
		Marker: opts.Marker,
	}, &resp)
	return
}

// Objects returns the object at given path.
func (c *Client) Object(ctx context.Context, bucket, path string, opts api.GetObjectOptions) (res api.ObjectsResponse, err error) {
	values := url.Values{}
	values.Set("bucket", bucket)
	opts.Apply(values)

	path = api.ObjectPathEscape(path)
	path += "?" + values.Encode()

	err = c.c.WithContext(ctx).GET(fmt.Sprintf("/objects/%s", path), &res)
	return
}

// ObjectsBySlabKey returns all objects that reference a given slab.
func (c *Client) ObjectsBySlabKey(ctx context.Context, bucket string, key object.EncryptionKey) (objects []api.ObjectMetadata, err error) {
	values := url.Values{}
	values.Set("bucket", bucket)
	err = c.c.WithContext(ctx).GET(fmt.Sprintf("/slab/%v/objects?"+values.Encode(), key), &objects)
	return
}

// ObjectsStats returns information about the number of objects and their size.
func (c *Client) ObjectsStats() (osr api.ObjectsStatsResponse, err error) {
	err = c.c.GET("/stats/objects", &osr)
	return
}

// RenameObject renames a single object.
func (c *Client) RenameObject(ctx context.Context, bucket, from, to string, force bool) (err error) {
	return c.renameObjects(ctx, bucket, from, to, api.ObjectsRenameModeSingle, force)
}

// RenameObjects renames all objects with the prefix 'from' to the prefix 'to'.
func (c *Client) RenameObjects(ctx context.Context, bucket, from, to string, force bool) (err error) {
	return c.renameObjects(ctx, bucket, from, to, api.ObjectsRenameModeMulti, force)
}

// SearchObjects returns all objects that contains a sub-string in their key.
func (c *Client) SearchObjects(ctx context.Context, bucket string, opts api.SearchObjectOptions) (entries []api.ObjectMetadata, err error) {
	values := url.Values{}
	values.Set("bucket", bucket)
	opts.Apply(values)

	err = c.c.WithContext(ctx).GET("/search/objects?"+values.Encode(), &entries)
	return
}

func (c *Client) renameObjects(ctx context.Context, bucket, from, to, mode string, force bool) (err error) {
	err = c.c.POST("/objects/rename", api.ObjectsRenameRequest{
		Bucket: bucket,
		Force:  force,
		From:   from,
		To:     to,
		Mode:   mode,
	}, nil)
	return
}
