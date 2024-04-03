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

// A Client provides methods for interacting with a worker.
type Client struct {
	c jape.Client
}

// New returns a new worker client.
func New(addr, password string) *Client {
	return &Client{jape.Client{
		BaseURL:  addr,
		Password: password,
	}}
}

// Account returns the account id for a given host.
func (c *Client) Account(ctx context.Context, hostKey types.PublicKey) (account rhpv3.Account, err error) {
	err = c.c.WithContext(ctx).GET(fmt.Sprintf("/account/%s", hostKey), &account)
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

// HeadObject returns the metadata of the object at the given path.
func (c *Client) HeadObject(ctx context.Context, bucket, path string, opts api.HeadObjectOptions) (*api.HeadObjectResponse, error) {
	c.c.Custom("HEAD", fmt.Sprintf("/objects/%s", path), nil, nil)

	values := url.Values{}
	values.Set("bucket", url.QueryEscape(bucket))
	opts.Apply(values)
	path = api.ObjectPathEscape(path)
	path += "?" + values.Encode()

	// TODO: support HEAD in jape client
	req, err := http.NewRequestWithContext(ctx, "HEAD", fmt.Sprintf("%s/objects/%s", c.c.BaseURL, path), http.NoBody)
	if err != nil {
		panic(err)
	}
	req.SetBasicAuth("", c.c.WithContext(ctx).Password)
	opts.ApplyHeaders(req.Header)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 && resp.StatusCode != 206 {
		_ = resp.Body.Close()
		switch resp.StatusCode {
		case http.StatusNotFound:
			return nil, api.ErrObjectNotFound
		default:
			return nil, errors.New(http.StatusText(resp.StatusCode))
		}
	}

	head, err := parseObjectResponseHeaders(resp.Header)
	if err != nil {
		return nil, err
	}
	return &head, nil
}

// GetObject returns the object at given path alongside its metadata.
func (c *Client) GetObject(ctx context.Context, bucket, path string, opts api.DownloadObjectOptions) (*api.GetObjectResponse, error) {
	if strings.HasSuffix(path, "/") {
		return nil, errors.New("the given path is a directory, use ObjectEntries instead")
	}

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

	head, err := parseObjectResponseHeaders(header)
	if err != nil {
		return nil, err
	}

	return &api.GetObjectResponse{
		Content:            body,
		HeadObjectResponse: head,
	}, nil
}

// ID returns the id of the worker.
func (c *Client) ID(ctx context.Context) (id string, err error) {
	err = c.c.WithContext(ctx).GET("/id", &id)
	return
}

// Memory requests the /memory endpoint.
func (c *Client) Memory(ctx context.Context) (resp api.MemoryResponse, err error) {
	err = c.c.WithContext(ctx).GET("/memory", &resp)
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
func (c *Client) ObjectEntries(ctx context.Context, bucket, path string, opts api.GetObjectOptions) (entries []api.ObjectMetadata, err error) {
	path = api.ObjectPathEscape(path)
	body, _, err := c.object(ctx, bucket, path, api.DownloadObjectOptions{
		GetObjectOptions: opts,
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
	if opts.ContentLength != 0 {
		req.ContentLength = opts.ContentLength
	} else if req.ContentLength, err = sizeFromSeeker(r); err != nil {
		return nil, fmt.Errorf("failed to get content length from seeker: %w", err)
	}
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
	opts.ApplyValues(values)
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
	opts.ApplyHeaders(req.Header)
	if opts.ContentLength != 0 {
		req.ContentLength = opts.ContentLength
	} else if req.ContentLength, err = sizeFromSeeker(r); err != nil {
		return nil, fmt.Errorf("failed to get content length from seeker: %w", err)
	}
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

func (c *Client) object(ctx context.Context, bucket, path string, opts api.DownloadObjectOptions) (_ io.ReadCloser, _ http.Header, err error) {
	values := url.Values{}
	values.Set("bucket", url.QueryEscape(bucket))
	opts.ApplyValues(values)
	path += "?" + values.Encode()

	c.c.Custom("GET", fmt.Sprintf("/objects/%s", path), nil, (*[]api.ObjectMetadata)(nil))
	req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s/objects/%s", c.c.BaseURL, path), http.NoBody)
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

func parseObjectResponseHeaders(header http.Header) (api.HeadObjectResponse, error) {
	// parse size
	var size int64
	_, err := fmt.Sscan(header.Get("Content-Length"), &size)
	if err != nil {
		return api.HeadObjectResponse{}, err
	}

	// parse range
	var r *api.DownloadRange
	if cr := header.Get("Content-Range"); cr != "" {
		dr, err := api.ParseDownloadRange(cr)
		if err != nil {
			return api.HeadObjectResponse{}, err
		}
		r = &dr

		// if a range is set, the size is the size extracted from the range
		// since Content-Length will then only be the length of the returned
		// range.
		size = dr.Size
	}

	// parse headers
	headers := make(map[string]string)
	for k, v := range header {
		if len(v) > 0 {
			headers[k] = v[0]
		}
	}

	return api.HeadObjectResponse{
		ContentType:  header.Get("Content-Type"),
		Etag:         trimEtag(header.Get("ETag")),
		LastModified: header.Get("Last-Modified"),
		Range:        r,
		Size:         size,
		Metadata:     api.ExtractObjectUserMetadataFrom(headers),
	}, nil
}

func sizeFromSeeker(r io.Reader) (int64, error) {
	s, ok := r.(io.Seeker)
	if !ok {
		return 0, nil
	}
	size, err := s.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}
	_, err = s.Seek(0, io.SeekStart)
	if err != nil {
		return 0, err
	}
	return size, nil
}

func trimEtag(etag string) string {
	etag = strings.TrimPrefix(etag, "\"")
	return strings.TrimSuffix(etag, "\"")
}
