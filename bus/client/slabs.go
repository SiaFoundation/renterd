package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/object"
)

// AddPartialSlab adds a partial slab to the bus.
func (c *Client) AddPartialSlab(ctx context.Context, data []byte, minShards, totalShards uint8, contractSet string) (slabs []object.SlabSlice, slabBufferMaxSizeSoftReached bool, err error) {
	c.c.Custom("POST", "/slabs/partial", nil, &api.AddPartialSlabResponse{})
	values := url.Values{}
	values.Set("minShards", fmt.Sprint(minShards))
	values.Set("totalShards", fmt.Sprint(totalShards))
	values.Set("contractSet", contractSet)

	u, err := url.Parse(fmt.Sprintf("%v/slabs/partial", c.c.BaseURL))
	if err != nil {
		panic(err)
	}
	u.RawQuery = values.Encode()
	req, err := http.NewRequestWithContext(ctx, "POST", u.String(), bytes.NewReader(data))
	if err != nil {
		panic(err)
	}
	req.SetBasicAuth("", c.c.WithContext(ctx).Password)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, false, err
	}
	defer io.Copy(io.Discard, resp.Body)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		err, _ := io.ReadAll(resp.Body)
		return nil, false, errors.New(string(err))
	}
	var apsr api.AddPartialSlabResponse
	err = json.NewDecoder(resp.Body).Decode(&apsr)
	if err != nil {
		return nil, false, err
	}
	return apsr.Slabs, apsr.SlabBufferMaxSizeSoftReached, nil
}

// FetchPartialSlab fetches a partial slab from the bus.
func (c *Client) FetchPartialSlab(ctx context.Context, key object.EncryptionKey, offset, length uint32) ([]byte, error) {
	c.c.Custom("GET", fmt.Sprintf("/slabs/partial/%s", key), nil, &[]byte{})
	values := url.Values{}
	values.Set("offset", fmt.Sprint(offset))
	values.Set("length", fmt.Sprint(length))

	u, err := url.Parse(fmt.Sprintf("%s/slabs/partial/%s", c.c.BaseURL, key))
	if err != nil {
		panic(err)
	}
	u.RawQuery = values.Encode()
	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), http.NoBody)
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
	if resp.StatusCode != 200 && resp.StatusCode != 206 {
		err, _ := io.ReadAll(resp.Body)
		return nil, errors.New(string(err))
	}
	return io.ReadAll(resp.Body)
}

// MarkPackedSlabsUploaded marks the given slabs as uploaded.
func (c *Client) MarkPackedSlabsUploaded(ctx context.Context, slabs []api.UploadedPackedSlab) (err error) {
	err = c.c.WithContext(ctx).POST("/slabbuffer/done", api.PackedSlabsRequestPOST{
		Slabs: slabs,
	}, nil)
	return
}

// PackedSlabsForUpload returns packed slabs that are ready to upload.
func (c *Client) PackedSlabsForUpload(ctx context.Context, lockingDuration time.Duration, minShards, totalShards uint8, set string, limit int) (slabs []api.PackedSlab, err error) {
	err = c.c.WithContext(ctx).POST("/slabbuffer/fetch", api.PackedSlabsRequestGET{
		LockingDuration: api.DurationMS(lockingDuration),
		MinShards:       minShards,
		TotalShards:     totalShards,
		ContractSet:     set,
		Limit:           limit,
	}, &slabs)
	return
}

// RefreshHealth recomputes the cached health of all slabs.
func (c *Client) RefreshHealth(ctx context.Context) error {
	return c.c.WithContext(ctx).POST("/slabs/refreshhealth", nil, nil)
}

// Slab returns the slab with the given key from the bus.
func (c *Client) Slab(ctx context.Context, key object.EncryptionKey) (slab object.Slab, err error) {
	err = c.c.WithContext(ctx).GET(fmt.Sprintf("/slab/%s", key), &slab)
	return
}

// SlabBuffers returns information about the number of objects and their size.
func (c *Client) SlabBuffers() (buffers []api.SlabBuffer, err error) {
	err = c.c.GET("/slabbuffers", &buffers)
	return
}

// SlabsForMigration returns up to 'limit' slabs which require migration. A slab
// needs to be migrated if it has sectors on contracts that are not part of the
// given 'set'.
func (c *Client) SlabsForMigration(ctx context.Context, healthCutoff float64, set string, limit int) (slabs []api.UnhealthySlab, err error) {
	var usr api.UnhealthySlabsResponse
	err = c.c.WithContext(ctx).POST("/slabs/migration", api.MigrationSlabsRequest{ContractSet: set, HealthCutoff: healthCutoff, Limit: limit}, &usr)
	if err != nil {
		return
	}
	return usr.Slabs, nil
}

// UpdateSlab updates the given slab in the database.
func (c *Client) UpdateSlab(ctx context.Context, slab object.Slab, contractSet string) (err error) {
	err = c.c.WithContext(ctx).PUT("/slab", api.UpdateSlabRequest{
		ContractSet: contractSet,
		Slab:        slab,
	})
	return
}
