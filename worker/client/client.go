package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/jape"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/object"
)

// A Client provides methods for interacting with a renterd API server.
type Client struct {
	c jape.Client
}

// Account returns the account id for a given host.
func (c *Client) Account(ctx context.Context, hostKey types.PublicKey) (account rhpv3.Account, err error) {
	err = c.c.WithContext(ctx).GET(fmt.Sprintf("/account/%s", hostKey), &account)
	return
}

// DownloadObject downloads the object at the given path.
func (c *Client) DownloadObject(ctx context.Context, w io.Writer, bucket, path string, opts api.DownloadObjectOptions) (err error) {
	if strings.HasSuffix(path, "/") {
		return errors.New("the given path is a directory, use ObjectEntries instead")
	}

	path = api.ObjectPathEscape(path)
	body, _, err := c.object(ctx, bucket, path, opts)
	if err != nil {
		return err
	}
	defer body.Close()
	_, err = io.Copy(w, body)
	return err
}

// DownloadStats returns download statistics.
func (c *Client) DownloadStats() (resp api.DownloadStatsResponse, err error) {
	err = c.c.GET("/stats/downloads", &resp)
	return
}

// GetObject returns the object at given path alongside its metadata.
func (c *Client) GetObject(ctx context.Context, bucket, path string, opts api.DownloadObjectOptions) (*api.GetObjectResponse, error) {
	if strings.HasSuffix(path, "/") {
		return nil, errors.New("the given path is a directory, use ObjectEntries instead")
	}

	// Start download.
	path = api.ObjectPathEscape(path)
	body, header, err := c.object(ctx, bucket, path, opts)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_, _ = io.Copy(io.Discard, body)
			_ = body.Close()
		}
	}()

	// Parse header.
	var size int64
	_, err = fmt.Sscan(header.Get("Content-Length"), &size)
	if err != nil {
		return nil, err
	}
	var r *api.DownloadRange
	if cr := header.Get("Content-Range"); cr != "" {
		dr, err := api.ParseDownloadRange(cr)
		if err != nil {
			return nil, err
		}
		r = &dr

		// If a range is set, the size is the size extracted from the range
		// since Content-Length will then only be the length of the returned
		// range.
		size = dr.Size
	}
	// Parse Last-Modified
	modTime, err := time.Parse(http.TimeFormat, header.Get("Last-Modified"))
	if err != nil {
		return nil, err
	}

	return &api.GetObjectResponse{
		Content:     body,
		ContentType: header.Get("Content-Type"),
		ModTime:     modTime.UTC(),
		Range:       r,
		Size:        size,
	}, nil
}

// ID returns the id of the worker.
func (c *Client) ID(ctx context.Context) (id string, err error) {
	err = c.c.WithContext(ctx).GET("/id", &id)
	return
}

// Contracts returns all contracts from the worker. These contracts decorate a
// bus contract with the contract's latest revision.
func (c *Client) Contracts(ctx context.Context, hostTimeout time.Duration) (resp api.ContractsResponse, err error) {
	err = c.c.WithContext(ctx).GET(fmt.Sprintf("/rhp/contracts?hosttimeout=%s", api.DurationMS(hostTimeout)), &resp)
	return
}

// DeleteObject deletes the object at the given path.
func (c *Client) DeleteObject(ctx context.Context, bucket, path string, opts api.DeleteObjectOptions) (err error) {
	values := url.Values{}
	values.Set("bucket", bucket)
	opts.Apply(values)

	path = api.ObjectPathEscape(path)
	err = c.c.WithContext(ctx).DELETE(fmt.Sprintf("/objects/%s?"+values.Encode(), path))
	return
}

// MigrateSlab migrates the specified slab.
func (c *Client) MigrateSlab(ctx context.Context, slab object.Slab, set string) (res api.MigrateSlabResponse, err error) {
	values := make(url.Values)
	values.Set("contractset", set)
	err = c.c.WithContext(ctx).POST("/slab/migrate?"+values.Encode(), slab, &res)
	return
}

// ObjectEntries returns the entries at the given path, which must end in /.
func (c *Client) ObjectEntries(ctx context.Context, bucket, path string, opts api.ObjectEntriesOptions) (entries []api.ObjectMetadata, err error) {
	path = api.ObjectPathEscape(path)
	body, _, err := c.object(ctx, bucket, path, api.DownloadObjectOptions{
		Prefix: opts.Prefix,
		Offset: opts.Offset,
		Limit:  opts.Limit,
	})
	if err != nil {
		return nil, err
	}
	defer io.Copy(io.Discard, body)
	defer body.Close()
	err = json.NewDecoder(body).Decode(&entries)
	return
}

// State returns the current state of the worker.
func (c *Client) State() (state api.WorkerStateResponse, err error) {
	err = c.c.GET("/state", &state)
	return
}

// UploadMultipartUploadPart uploads part of the data for a multipart upload.
func (c *Client) UploadMultipartUploadPart(ctx context.Context, r io.Reader, bucket, path, uploadID string, partNumber int, opts api.UploadMultipartUploadPartOptions) (*api.UploadMultipartUploadPartResponse, error) {
	path = api.ObjectPathEscape(path)
	c.c.Custom("PUT", fmt.Sprintf("/multipart/%s", path), []byte{}, nil)

	values := make(url.Values)
	values.Set("bucket", bucket)
	values.Set("uploadid", uploadID)
	values.Set("partnumber", fmt.Sprint(partNumber))
	opts.Apply(values)

	u, err := url.Parse(fmt.Sprintf("%v/multipart/%v", c.c.BaseURL, path))
	if err != nil {
		panic(err)
	}
	u.RawQuery = values.Encode()
	req, err := http.NewRequestWithContext(ctx, "PUT", u.String(), r)
	if err != nil {
		panic(err)
	}
	req.SetBasicAuth("", c.c.WithContext(ctx).Password)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer io.Copy(io.Discard, resp.Body)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		err, _ := io.ReadAll(resp.Body)
		return nil, errors.New(string(err))
	}
	return &api.UploadMultipartUploadPartResponse{ETag: resp.Header.Get("ETag")}, nil
}

// UploadObject uploads the data in r, creating an object at the given path.
func (c *Client) UploadObject(ctx context.Context, r io.Reader, bucket, path string, opts api.UploadObjectOptions) (*api.UploadObjectResponse, error) {
	path = api.ObjectPathEscape(path)
	c.c.Custom("PUT", fmt.Sprintf("/objects/%s", path), []byte{}, nil)

	values := make(url.Values)
	values.Set("bucket", bucket)
	opts.Apply(values)
	u, err := url.Parse(fmt.Sprintf("%v/objects/%v", c.c.BaseURL, path))
	if err != nil {
		panic(err)
	}
	u.RawQuery = values.Encode()
	req, err := http.NewRequestWithContext(ctx, "PUT", u.String(), r)
	if err != nil {
		panic(err)
	}
	req.SetBasicAuth("", c.c.WithContext(ctx).Password)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer io.Copy(io.Discard, resp.Body)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		err, _ := io.ReadAll(resp.Body)
		return nil, errors.New(string(err))
	}
	return &api.UploadObjectResponse{ETag: resp.Header.Get("ETag")}, nil
}

// UploadStats returns the upload stats.
func (c *Client) UploadStats() (resp api.UploadStatsResponse, err error) {
	err = c.c.GET("/stats/uploads", &resp)
	return
}

// New returns a client that communicates with a renterd worker server
// listening on the specified address.
func New(addr, password string) *Client {
	return &Client{jape.Client{
		BaseURL:  addr,
		Password: password,
	}}
}

func (c *Client) object(ctx context.Context, bucket, path string, opts api.DownloadObjectOptions) (_ io.ReadCloser, _ http.Header, err error) {
	values := url.Values{}
	values.Set("bucket", url.QueryEscape(bucket))
	opts.ApplyValues(values)
	path += "?" + values.Encode()

	c.c.Custom("GET", fmt.Sprintf("/objects/%s", path), nil, (*[]api.ObjectMetadata)(nil))
	req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s/objects/%s", c.c.BaseURL, path), nil)
	if err != nil {
		panic(err)
	}
	req.SetBasicAuth("", c.c.WithContext(ctx).Password)
	opts.ApplyHeaders(req.Header)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, nil, err
	}
	if resp.StatusCode != 200 && resp.StatusCode != 206 {
		err, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		return nil, nil, errors.New(string(err))
	}
	return resp.Body, resp.Header, err
}
