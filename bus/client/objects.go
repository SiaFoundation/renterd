package client

import (
	"context"
	"fmt"
	"net/url"

	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/object"
)

// AddObject stores the provided object under the given path.
func (c *Client) AddObject(ctx context.Context, bucket, path string, o object.Object, opts api.AddObjectOptions) (err error) {
	path = api.ObjectKeyEscape(path)
	err = c.c.WithContext(ctx).PUT(fmt.Sprintf("/object/%s", path), api.AddObjectRequest{
		Bucket:   bucket,
		Object:   o,
		ETag:     opts.ETag,
		MimeType: opts.MimeType,
		Metadata: opts.Metadata,
	})
	return
}

// CopyObject copies the object from the source bucket and path to the
// destination bucket and path.
func (c *Client) CopyObject(ctx context.Context, srcBucket, dstBucket, srcKey, dstKey string, opts api.CopyObjectOptions) (om api.ObjectMetadata, err error) {
	err = c.c.WithContext(ctx).POST("/objects/copy", api.CopyObjectsRequest{
		SourceBucket:      srcBucket,
		DestinationBucket: dstBucket,
		SourceKey:         srcKey,
		DestinationKey:    dstKey,
		MimeType:          opts.MimeType,
		Metadata:          opts.Metadata,
	}, &om)
	return
}

// DeleteObject deletes the object with given key.
func (c *Client) DeleteObject(ctx context.Context, bucket, key string) (err error) {
	values := url.Values{}
	values.Set("bucket", bucket)

	key = api.ObjectKeyEscape(key)
	err = c.c.WithContext(ctx).DELETE(fmt.Sprintf("/object/%s?"+values.Encode(), key))
	return
}

// RemoveObjects removes objects with given prefix.
func (c *Client) RemoveObjects(ctx context.Context, bucket, prefix string) (err error) {
	err = c.c.WithContext(ctx).POST("/objects/remove", api.ObjectsRemoveRequest{
		Bucket: bucket,
		Prefix: prefix,
	}, nil)
	return
}

// Object returns the object at given key.
func (c *Client) Object(ctx context.Context, bucket, key string, opts api.GetObjectOptions) (res api.Object, err error) {
	values := url.Values{}
	values.Set("bucket", bucket)
	opts.Apply(values)

	key = api.ObjectKeyEscape(key)
	key += "?" + values.Encode()

	err = c.c.WithContext(ctx).GET(fmt.Sprintf("/object/%s", key), &res)
	return
}

// Objects lists objects in the given bucket.
func (c *Client) Objects(ctx context.Context, prefix string, opts api.ListObjectOptions) (resp api.ObjectsResponse, err error) {
	values := url.Values{}
	opts.Apply(values)

	prefix = api.ObjectKeyEscape(prefix)
	prefix += "?" + values.Encode()

	err = c.c.WithContext(ctx).GET(fmt.Sprintf("/objects/%s", prefix), &resp)
	return
}

// ObjectsStats returns information about the number of objects and their size.
func (c *Client) ObjectsStats(ctx context.Context, opts api.ObjectsStatsOpts) (osr api.ObjectsStatsResponse, err error) {
	values := url.Values{}
	if opts.Bucket != "" {
		values.Set("bucket", opts.Bucket)
	}
	err = c.c.WithContext(ctx).GET("/stats/objects?"+values.Encode(), &osr)
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

func (c *Client) renameObjects(ctx context.Context, bucket, from, to, mode string, force bool) (err error) {
	err = c.c.WithContext(ctx).POST("/objects/rename", api.ObjectsRenameRequest{
		Bucket: bucket,
		Force:  force,
		From:   from,
		To:     to,
		Mode:   mode,
	}, nil)
	return
}
