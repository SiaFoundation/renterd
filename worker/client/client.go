package client

import (
	"context"
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
	"go.sia.tech/renterd/internal/utils"
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
func (c *Client) Account(ctx context.Context, hostKey types.PublicKey) (account api.Account, err error) {
	err = c.c.WithContext(ctx).GET(fmt.Sprintf("/account/%s", hostKey), &account)
	return
}

// Accounts returns all accounts.
func (c *Client) Accounts(ctx context.Context) (accounts []api.Account, err error) {
	err = c.c.WithContext(ctx).GET(fmt.Sprintf("/accounts"), &accounts)
	return
}

// ResetDrift resets the drift of an account to zero.
func (c *Client) ResetDrift(ctx context.Context, id rhpv3.Account) (err error) {
	err = c.c.WithContext(ctx).POST(fmt.Sprintf("/account/%s/resetdrift", id), nil, nil)
	return
}

// DeleteObject deletes the object at the given path.
func (c *Client) DeleteObject(ctx context.Context, bucket, key string) (err error) {
	values := url.Values{}
	values.Set("bucket", bucket)

	key = api.ObjectKeyEscape(key)
	err = c.c.WithContext(ctx).DELETE(fmt.Sprintf("/object/%s?"+values.Encode(), key))
	return
}

// DownloadObject downloads the object at the given key.
func (c *Client) DownloadObject(ctx context.Context, w io.Writer, bucket, key string, opts api.DownloadObjectOptions) (err error) {
	if strings.HasSuffix(key, "/") {
		return errors.New("the given key is a directory, use ObjectEntries instead")
	}

	key = api.ObjectKeyEscape(key)
	body, _, err := c.object(ctx, bucket, key, opts)
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

// HeadObject returns the metadata of the object at the given key.
func (c *Client) HeadObject(ctx context.Context, bucket, key string, opts api.HeadObjectOptions) (*api.HeadObjectResponse, error) {
	c.c.Custom("HEAD", fmt.Sprintf("/object/%s", key), nil, nil)

	values := url.Values{}
	values.Set("bucket", bucket)
	opts.Apply(values)
	key = api.ObjectKeyEscape(key)
	key += "?" + values.Encode()

	// TODO: support HEAD in jape client
	req, err := http.NewRequestWithContext(ctx, "HEAD", fmt.Sprintf("%s/object/%s", c.c.BaseURL, key), http.NoBody)
	if err != nil {
		panic(err)
	}
	req.SetBasicAuth("", c.c.WithContext(ctx).Password)
	opts.ApplyHeaders(req.Header)

	headers, statusCode, err := utils.DoRequest(req, nil)
	if err != nil && statusCode == http.StatusNotFound {
		return nil, api.ErrObjectNotFound
	} else if err != nil {
		return nil, errors.New(http.StatusText(statusCode))
	}

	head, err := parseObjectResponseHeaders(headers)
	if err != nil {
		return nil, err
	}
	return &head, nil
}

// GetObject returns the object at given key alongside its metadata.
func (c *Client) GetObject(ctx context.Context, bucket, key string, opts api.DownloadObjectOptions) (*api.GetObjectResponse, error) {
	if strings.HasSuffix(key, "/") {
		return nil, errors.New("the given path is a directory, use ObjectEntries instead")
	}

	key = api.ObjectKeyEscape(key)
	body, header, err := c.object(ctx, bucket, key, opts)
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
func (c *Client) MigrateSlab(ctx context.Context, slab object.Slab, set string) error {
	values := make(url.Values)
	values.Set("contractset", set)
	return c.c.WithContext(ctx).POST("/slab/migrate?"+values.Encode(), slab, nil)
}

// RemoveObjects removes the object with given prefix.
func (c *Client) RemoveObjects(ctx context.Context, bucket, prefix string) (err error) {
	err = c.c.WithContext(ctx).POST("/objects/remove", api.ObjectsRemoveRequest{
		Bucket: bucket,
		Prefix: prefix,
	}, nil)
	return
}

// State returns the current state of the worker.
func (c *Client) State() (state api.WorkerStateResponse, err error) {
	err = c.c.GET("/state", &state)
	return
}

// UploadMultipartUploadPart uploads part of the data for a multipart upload.
func (c *Client) UploadMultipartUploadPart(ctx context.Context, r io.Reader, bucket, path, uploadID string, partNumber int, opts api.UploadMultipartUploadPartOptions) (*api.UploadMultipartUploadPartResponse, error) {
	path = api.ObjectKeyEscape(path)
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
	header, _, err := utils.DoRequest(req, nil)
	if err != nil {
		return nil, err
	}
	return &api.UploadMultipartUploadPartResponse{ETag: header.Get("ETag")}, nil
}

// UploadObject uploads the data in r, creating an object at the given path.
func (c *Client) UploadObject(ctx context.Context, r io.Reader, bucket, key string, opts api.UploadObjectOptions) (*api.UploadObjectResponse, error) {
	key = api.ObjectKeyEscape(key)
	c.c.Custom("PUT", fmt.Sprintf("/object/%s", key), []byte{}, nil)

	values := make(url.Values)
	values.Set("bucket", bucket)
	opts.ApplyValues(values)
	u, err := url.Parse(fmt.Sprintf("%v/object/%v", c.c.BaseURL, key))
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
	header, _, err := utils.DoRequest(req, nil)
	if err != nil {
		return nil, err
	}
	return &api.UploadObjectResponse{ETag: header.Get("ETag")}, nil
}

// UploadStats returns the upload stats.
func (c *Client) UploadStats() (resp api.UploadStatsResponse, err error) {
	err = c.c.GET("/stats/uploads", &resp)
	return
}

func (c *Client) object(ctx context.Context, bucket, key string, opts api.DownloadObjectOptions) (_ io.ReadCloser, _ http.Header, err error) {
	values := url.Values{}
	values.Set("bucket", bucket)
	key += "?" + values.Encode()

	c.c.Custom("GET", fmt.Sprintf("/object/%s", key), nil, (*[]api.ObjectMetadata)(nil))
	req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s/object/%s", c.c.BaseURL, key), http.NoBody)
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
	var r *api.ContentRange
	if cr := header.Get("Content-Range"); cr != "" {
		dr, err := api.ParseContentRange(cr)
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

	modTime, err := time.Parse(http.TimeFormat, header.Get("Last-Modified"))
	if err != nil {
		return api.HeadObjectResponse{}, fmt.Errorf("failed to parse Last-Modified header: %w", err)
	}
	return api.HeadObjectResponse{
		ContentType:  header.Get("Content-Type"),
		Etag:         trimEtag(header.Get("ETag")),
		LastModified: api.TimeRFC3339(modTime),
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
