package worker

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/jape"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/object"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	rhpv3 "go.sia.tech/renterd/rhp/v3"
)

// A Client provides methods for interacting with a renterd API server.
type Client struct {
	c jape.Client
}

// RHPScan scans a host, returning its current settings.
func (c *Client) RHPScan(hostKey types.PublicKey, hostIP string, timeout time.Duration) (resp api.RHPScanResponse, err error) {
	err = c.c.POST("/rhp/scan", api.RHPScanRequest{
		HostKey: hostKey,
		HostIP:  hostIP,
		Timeout: timeout,
	}, &resp)
	return
}

// RHPForm forms a contract with a host.
func (c *Client) RHPForm(endHeight uint64, hk types.PublicKey, hostIP string, renterAddress types.Address, renterFunds types.Currency, hostCollateral types.Currency) (rhpv2.ContractRevision, []types.Transaction, error) {
	req := api.RHPFormRequest{
		EndHeight:      endHeight,
		HostCollateral: hostCollateral,
		HostKey:        hk,
		HostIP:         hostIP,
		RenterFunds:    renterFunds,
		RenterAddress:  renterAddress,
	}
	var resp api.RHPFormResponse
	err := c.c.POST("/rhp/form", req, &resp)
	return resp.Contract, resp.TransactionSet, err
}

// RHPRenew renews an existing contract with a host.
func (c *Client) RHPRenew(fcid types.FileContractID, endHeight uint64, hk types.PublicKey, hostIP string, renterAddress types.Address, renterFunds, hostCollateral types.Currency) (rhpv2.ContractRevision, []types.Transaction, error) {
	req := api.RHPRenewRequest{
		ContractID:     fcid,
		EndHeight:      endHeight,
		HostCollateral: hostCollateral,
		HostKey:        hk,
		HostIP:         hostIP,
		RenterAddress:  renterAddress,
		RenterFunds:    renterFunds,
	}
	var resp api.RHPRenewResponse
	err := c.c.POST("/rhp/renew", req, &resp)
	return resp.Contract, resp.TransactionSet, err
}

// RHPFund funds an ephemeral account using the supplied contract.
func (c *Client) RHPFund(contractID types.FileContractID, hostKey types.PublicKey, amount types.Currency) (err error) {
	req := api.RHPFundRequest{
		ContractID: contractID,
		HostKey:    hostKey,
		Amount:     amount,
	}
	err = c.c.POST("/rhp/fund", req, nil)
	return
}

// RHPReadRegistry reads a registry value.
func (c *Client) RHPReadRegistry(hostKey types.PublicKey, hostIP string, key rhpv3.RegistryKey, payment rhpv3.PayByEphemeralAccountRequest) (resp rhpv3.RegistryValue, err error) {
	req := api.RHPRegistryReadRequest{
		HostKey:     hostKey,
		HostIP:      hostIP,
		RegistryKey: key,
		Payment:     payment,
	}
	err = c.c.POST("/rhp/registry/read", req, &resp)
	return
}

// RHPUpdateRegistry updates a registry value.
func (c *Client) RHPUpdateRegistry(hostKey types.PublicKey, key rhpv3.RegistryKey, value rhpv3.RegistryValue) (err error) {
	req := api.RHPRegistryUpdateRequest{
		HostKey:       hostKey,
		RegistryKey:   key,
		RegistryValue: value,
	}
	err = c.c.POST("/rhp/registry/update", req, nil)
	return
}

// MigrateSlab migrates the specified slab.
func (c *Client) MigrateSlab(slab object.Slab) error {
	return c.c.POST("/slab/migrate", slab, nil)
}

// UploadObject uploads the data in r, creating an object with the given name.
func (c *Client) UploadObject(r io.Reader, name string) (err error) {
	c.c.Custom("PUT", fmt.Sprintf("/objects/%s", name), []byte{}, nil)

	req, err := http.NewRequest("PUT", fmt.Sprintf("%v/objects/%v", c.c.BaseURL, name), r)
	if err != nil {
		panic(err)
	}
	req.SetBasicAuth("", c.c.Password)
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

func (c *Client) object(path string, w io.Writer, entries *[]string) (err error) {
	c.c.Custom("GET", fmt.Sprintf("/objects/%s", path), nil, (*[]string)(nil))

	req, err := http.NewRequest("GET", fmt.Sprintf("%v/objects/%v", c.c.BaseURL, path), nil)
	if err != nil {
		panic(err)
	}
	req.SetBasicAuth("", c.c.Password)
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
	if w != nil {
		_, err = io.Copy(w, resp.Body)
	} else {
		err = json.NewDecoder(resp.Body).Decode(entries)
	}
	return
}

// ObjectEntries returns the entries at the given path, which must end in /.
func (c *Client) ObjectEntries(path string) (entries []string, err error) {
	err = c.object(path, nil, &entries)
	return
}

// DownloadObject downloads the object at the given path, writing its data to
// w.
func (c *Client) DownloadObject(w io.Writer, path string) (err error) {
	err = c.object(path, w, nil)
	return
}

// DeleteObject deletes the object with the given name.
func (c *Client) DeleteObject(name string) (err error) {
	err = c.c.DELETE(fmt.Sprintf("/objects/%s", name))
	return
}

// ActiveContracts returns all active contracts from the worker. These contracts
// decorate a bus contract with the contract's latest revision.
func (c *Client) ActiveContracts(hostTimeout time.Duration) (resp api.ContractsResponse, err error) {
	err = c.c.GET(fmt.Sprintf("/rhp/contracts/active?hosttimeout=%s", api.Duration(hostTimeout)), &resp)
	return
}

// Accounts requests the worker's /accounts endpoint.
func (c *Client) Accounts() (accounts []api.Account, err error) {
	err = c.c.GET("/accounts", &accounts)
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
