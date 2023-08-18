package worker

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

	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/jape"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/object"
)

// A Client provides methods for interacting with a renterd API server.
type Client struct {
	c jape.Client
}

func (c *Client) ID(ctx context.Context) (id string, err error) {
	err = c.c.WithContext(ctx).GET("/id", &id)
	return
}

// RHPBroadcast broadcasts the latest revision for a contract.
func (c *Client) RHPBroadcast(ctx context.Context, fcid types.FileContractID) (err error) {
	err = c.c.WithContext(ctx).POST(fmt.Sprintf("/rhp/contract/%s/broadcast", fcid), nil, nil)
	return
}

// RHPContractRoots fetches the roots of the contract with given id.
func (c *Client) RHPContractRoots(ctx context.Context, fcid types.FileContractID) (roots []types.Hash256, err error) {
	err = c.c.WithContext(ctx).GET(fmt.Sprintf("/rhp/contract/%s/roots", fcid), &roots)
	return
}

// RHPScan scans a host, returning its current settings.
func (c *Client) RHPScan(ctx context.Context, hostKey types.PublicKey, hostIP string, timeout time.Duration) (resp api.RHPScanResponse, err error) {
	err = c.c.WithContext(ctx).POST("/rhp/scan", api.RHPScanRequest{
		HostKey: hostKey,
		HostIP:  hostIP,
		Timeout: timeout,
	}, &resp)
	return
}

// RHPForm forms a contract with a host.
func (c *Client) RHPForm(ctx context.Context, endHeight uint64, hk types.PublicKey, hostIP string, renterAddress types.Address, renterFunds types.Currency, hostCollateral types.Currency) (rhpv2.ContractRevision, []types.Transaction, error) {
	req := api.RHPFormRequest{
		EndHeight:      endHeight,
		HostCollateral: hostCollateral,
		HostKey:        hk,
		HostIP:         hostIP,
		RenterFunds:    renterFunds,
		RenterAddress:  renterAddress,
	}
	var resp api.RHPFormResponse
	err := c.c.WithContext(ctx).POST("/rhp/form", req, &resp)
	return resp.Contract, resp.TransactionSet, err
}

// RHPRenew renews an existing contract with a host.
func (c *Client) RHPRenew(ctx context.Context, fcid types.FileContractID, endHeight uint64, hk types.PublicKey, siamuxAddr string, hostAddress, renterAddress types.Address, renterFunds, newCollateral types.Currency, windowSize uint64) (rhpv2.ContractRevision, []types.Transaction, error) {
	req := api.RHPRenewRequest{
		ContractID:    fcid,
		EndHeight:     endHeight,
		HostAddress:   hostAddress,
		HostKey:       hk,
		NewCollateral: newCollateral,
		RenterAddress: renterAddress,
		RenterFunds:   renterFunds,
		SiamuxAddr:    siamuxAddr,
		WindowSize:    windowSize,
	}

	var resp api.RHPRenewResponse
	err := c.c.WithContext(ctx).POST("/rhp/renew", req, &resp)
	return resp.Contract, resp.TransactionSet, err
}

// RHPFund funds an ephemeral account using the supplied contract.
func (c *Client) RHPFund(ctx context.Context, contractID types.FileContractID, hostKey types.PublicKey, hostIP, siamuxAddr string, balance types.Currency) (err error) {
	req := api.RHPFundRequest{
		ContractID: contractID,
		HostKey:    hostKey,
		SiamuxAddr: siamuxAddr,
		Balance:    balance,
	}
	err = c.c.WithContext(ctx).POST("/rhp/fund", req, nil)
	return
}

// RHPSync funds an ephemeral account using the supplied contract.
func (c *Client) RHPSync(ctx context.Context, contractID types.FileContractID, hostKey types.PublicKey, hostIP, siamuxAddr string) (err error) {
	req := api.RHPSyncRequest{
		ContractID: contractID,
		HostKey:    hostKey,
		SiamuxAddr: siamuxAddr,
	}
	err = c.c.WithContext(ctx).POST("/rhp/sync", req, nil)
	return
}

// RHPPriceTable fetches a price table for a host.
func (c *Client) RHPPriceTable(ctx context.Context, hostKey types.PublicKey, siamuxAddr string, timeout time.Duration) (pt hostdb.HostPriceTable, err error) {
	req := api.RHPPriceTableRequest{
		HostKey:    hostKey,
		SiamuxAddr: siamuxAddr,
		Timeout:    timeout,
	}
	err = c.c.WithContext(ctx).POST("/rhp/pricetable", req, &pt)
	return
}

// RHPReadRegistry reads a registry value.
func (c *Client) RHPReadRegistry(ctx context.Context, hostKey types.PublicKey, siamuxAddr string, key rhpv3.RegistryKey, payment rhpv3.PayByEphemeralAccountRequest) (resp rhpv3.RegistryValue, err error) {
	req := api.RHPRegistryReadRequest{
		HostKey:     hostKey,
		SiamuxAddr:  siamuxAddr,
		RegistryKey: key,
		Payment:     payment,
	}
	err = c.c.WithContext(ctx).POST("/rhp/registry/read", req, &resp)
	return
}

// RHPUpdateRegistry updates a registry value.
func (c *Client) RHPUpdateRegistry(ctx context.Context, hostKey types.PublicKey, key rhpv3.RegistryKey, value rhpv3.RegistryValue) (err error) {
	req := api.RHPRegistryUpdateRequest{
		HostKey:       hostKey,
		RegistryKey:   key,
		RegistryValue: value,
	}
	err = c.c.WithContext(ctx).POST("/rhp/registry/update", req, nil)
	return
}

// State returns the current state of the worker.
func (c *Client) State() (state api.WorkerStateResponse, err error) {
	err = c.c.GET("/state", &state)
	return
}

// MigrateSlab migrates the specified slab.
func (c *Client) MigrateSlab(ctx context.Context, slab object.Slab, set string) error {
	values := make(url.Values)
	values.Set("contractset", set)

	return c.c.WithContext(ctx).POST("/slab/migrate?"+values.Encode(), slab, nil)
}

// DownloadStats returns the upload stats.
func (c *Client) DownloadStats() (resp api.DownloadStatsResponse, err error) {
	err = c.c.GET("/stats/downloads", &resp)
	return
}

// UploadStats returns the upload stats.
func (c *Client) UploadStats() (resp api.UploadStatsResponse, err error) {
	err = c.c.GET("/stats/uploads", &resp)
	return
}

// UploadObject uploads the data in r, creating an object at the given path.
func (c *Client) UploadObject(ctx context.Context, r io.Reader, path string, opts ...api.UploadOption) (err error) {
	path = strings.TrimPrefix(path, "/")
	c.c.Custom("PUT", fmt.Sprintf("/objects/%s", path), []byte{}, nil)

	values := make(url.Values)
	for _, opt := range opts {
		opt(values)
	}
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
		return err
	}
	defer io.Copy(io.Discard, resp.Body)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		err, _ := io.ReadAll(resp.Body)
		return errors.New(string(err))
	}
	return
}

func (c *Client) object(ctx context.Context, path, prefix string, offset, limit int, w io.Writer, entries *[]api.ObjectMetadata, opts ...api.DownloadObjectOption) (err error) {
	values := url.Values{}
	values.Set("prefix", url.QueryEscape(prefix))
	values.Set("offset", fmt.Sprint(offset))
	values.Set("limit", fmt.Sprint(limit))
	path += "?" + values.Encode()

	c.c.Custom("GET", fmt.Sprintf("/objects/%s", path), nil, (*[]api.ObjectMetadata)(nil))
	req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s/objects/%s", c.c.BaseURL, path), nil)
	if err != nil {
		panic(err)
	}
	req.SetBasicAuth("", c.c.WithContext(ctx).Password)
	for _, opt := range opts {
		opt(req.Header)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer io.Copy(io.Discard, resp.Body)
	defer resp.Body.Close()
	if resp.StatusCode != 200 && resp.StatusCode != 206 {
		err, _ := io.ReadAll(resp.Body)
		return errors.New(string(err))
	}
	if w != nil {
		_, err = io.Copy(w, resp.Body)
	} else {
		err = json.NewDecoder(resp.Body).Decode(entries)
	}
	return
}

// ObjectEntries returns the entries at the given path, which must end in /.
func (c *Client) ObjectEntries(ctx context.Context, path, prefix string, offset, limit int) (entries []api.ObjectMetadata, err error) {
	path = strings.TrimPrefix(path, "/")
	err = c.object(ctx, path, prefix, offset, limit, nil, &entries)
	return
}

// DownloadObject downloads the object at the given path, writing its data to
// w.
func (c *Client) DownloadObject(ctx context.Context, w io.Writer, path string, opts ...api.DownloadObjectOption) (err error) {
	if strings.HasSuffix(path, "/") {
		return errors.New("the given path is a directory, use ObjectEntries instead")
	}

	path = strings.TrimPrefix(path, "/")
	err = c.object(ctx, path, "", 0, -1, w, nil, opts...)
	return
}

// DeleteObject deletes the object at the given path.
func (c *Client) DeleteObject(ctx context.Context, path string, batch bool) (err error) {
	path = strings.TrimPrefix(path, "/")
	values := url.Values{}
	values.Set("batch", fmt.Sprint(batch))
	err = c.c.WithContext(ctx).DELETE(fmt.Sprintf("/objects/%s?"+values.Encode(), path))
	return
}

// Contracts returns all contracts from the worker. These contracts decorate a
// bus contract with the contract's latest revision.
func (c *Client) Contracts(ctx context.Context, hostTimeout time.Duration) (resp api.ContractsResponse, err error) {
	err = c.c.WithContext(ctx).GET(fmt.Sprintf("/rhp/contracts?hosttimeout=%s", api.ParamDuration(hostTimeout)), &resp)
	return
}

// Account returns the account id for a given host.
func (c *Client) Account(ctx context.Context, hostKey types.PublicKey) (account rhpv3.Account, err error) {
	err = c.c.WithContext(ctx).GET(fmt.Sprintf("/account/%s", hostKey), &account)
	return
}

// NewClient returns a client that communicates with a renterd worker server
// listening on the specified address.
func NewClient(addr, password string) *Client {
	return &Client{jape.Client{
		BaseURL:  addr,
		Password: password,
	}}
}
