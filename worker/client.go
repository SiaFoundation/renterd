package worker

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"

	"go.sia.tech/jape"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	rhpv3 "go.sia.tech/renterd/rhp/v3"
	"go.sia.tech/renterd/slab"
	"go.sia.tech/siad/types"
)

// A Client provides methods for interacting with a renterd API server.
type Client struct {
	c jape.Client
}

// RHPScan scans a host, returning its current settings.
func (c *Client) RHPScan(hostKey PublicKey, hostIP string) (resp RHPScanResponse, err error) {
	err = c.c.POST("/rhp/scan", RHPScanRequest{hostKey, hostIP}, &resp)
	return
}

// RHPPrepareForm prepares a contract formation transaction.
func (c *Client) RHPPrepareForm(renterKey PrivateKey, hostKey PublicKey, renterFunds types.Currency, renterAddress types.UnlockHash, hostCollateral types.Currency, endHeight uint64, hostSettings rhpv2.HostSettings) (types.FileContract, types.Currency, error) {
	req := RHPPrepareFormRequest{
		RenterKey:      renterKey,
		HostKey:        hostKey,
		RenterFunds:    renterFunds,
		RenterAddress:  renterAddress,
		HostCollateral: hostCollateral,
		EndHeight:      endHeight,
		HostSettings:   hostSettings,
	}
	var resp RHPPrepareFormResponse
	err := c.c.POST("/rhp/prepare/form", req, &resp)
	return resp.Contract, resp.Cost, err
}

// RHPPrepareRenew prepares a contract renewal transaction.
func (c *Client) RHPPrepareRenew(contract types.FileContractRevision, renterKey PrivateKey, hostKey PublicKey, renterFunds types.Currency, renterAddress types.UnlockHash, hostCollateral types.Currency, endHeight uint64, hostSettings rhpv2.HostSettings) (types.FileContract, types.Currency, types.Currency, error) {
	req := RHPPrepareRenewRequest{
		Contract:       contract,
		RenterKey:      renterKey,
		HostKey:        hostKey,
		RenterFunds:    renterFunds,
		RenterAddress:  renterAddress,
		HostCollateral: hostCollateral,
		EndHeight:      endHeight,
		HostSettings:   hostSettings,
	}
	var resp RHPPrepareRenewResponse
	err := c.c.POST("/rhp/prepare/renew", req, &resp)
	return resp.Contract, resp.Cost, resp.FinalPayment, err
}

// RHPPreparePayment prepares an ephemeral account payment.
func (c *Client) RHPPreparePayment(account rhpv3.Account, amount types.Currency, key PrivateKey) (resp rhpv3.PayByEphemeralAccountRequest, err error) {
	req := RHPPreparePaymentRequest{
		Account:    account,
		Amount:     amount,
		Expiry:     0, // TODO
		AccountKey: key,
	}
	err = c.c.POST("/rhp/prepare/payment", req, &resp)
	return
}

// RHPForm forms a contract with a host.
func (c *Client) RHPForm(renterKey PrivateKey, hostKey PublicKey, hostIP string, transactionSet []types.Transaction) (rhpv2.Contract, []types.Transaction, error) {
	req := RHPFormRequest{
		RenterKey:      renterKey,
		HostKey:        hostKey,
		HostIP:         hostIP,
		TransactionSet: transactionSet,
	}
	var resp RHPFormResponse
	err := c.c.POST("/rhp/form", req, &resp)
	return resp.Contract, resp.TransactionSet, err
}

// RHPRenew renews an existing contract with a host.
func (c *Client) RHPRenew(renterKey PrivateKey, hostKey PublicKey, hostIP string, contractID types.FileContractID, transactionSet []types.Transaction, finalPayment types.Currency) (rhpv2.Contract, []types.Transaction, error) {
	req := RHPRenewRequest{
		RenterKey:      renterKey,
		HostKey:        hostKey,
		HostIP:         hostIP,
		ContractID:     contractID,
		TransactionSet: transactionSet,
		FinalPayment:   finalPayment,
	}
	var resp RHPRenewResponse
	err := c.c.POST("/rhp/renew", req, &resp)
	return resp.Contract, resp.TransactionSet, err
}

// RHPFund funds an ephemeral account using the supplied contract.
func (c *Client) RHPFund(contract types.FileContractRevision, renterKey PrivateKey, hostKey PublicKey, hostIP string, account rhpv3.Account, amount types.Currency) (err error) {
	req := RHPFundRequest{
		Contract:  contract,
		RenterKey: renterKey,
		HostKey:   hostKey,
		HostIP:    hostIP,
		Account:   account,
		Amount:    amount,
	}
	err = c.c.POST("/rhp/fund", req, nil)
	return
}

// RHPReadRegistry reads a registry value.
func (c *Client) RHPReadRegistry(hostKey PublicKey, hostIP string, key rhpv3.RegistryKey, payment rhpv3.PayByEphemeralAccountRequest) (resp rhpv3.RegistryValue, err error) {
	req := RHPRegistryReadRequest{
		HostKey:     hostKey,
		HostIP:      hostIP,
		RegistryKey: key,
		Payment:     payment,
	}
	err = c.c.POST("/rhp/registry/read", req, &resp)
	return
}

// RHPUpdateRegistry updates a registry value.
func (c *Client) RHPUpdateRegistry(hostKey PublicKey, hostIP string, key rhpv3.RegistryKey, value rhpv3.RegistryValue, payment rhpv3.PayByEphemeralAccountRequest) (err error) {
	req := RHPRegistryUpdateRequest{
		HostKey:       hostKey,
		HostIP:        hostIP,
		RegistryKey:   key,
		RegistryValue: value,
		Payment:       payment,
	}
	err = c.c.POST("/rhp/registry/update", req, nil)
	return
}

// UploadSlab uploads data to a set of hosts. At most m*SectorSize bytes will be
// read from src.
func (c *Client) UploadSlab(src io.Reader, m, n uint8, height uint64, contracts []Contract) (s slab.Slab, err error) {
	c.c.Custom("POST", "/slabs/upload", []byte{}, &SlabsUploadResponse{})

	js, _ := json.Marshal(SlabsUploadRequest{
		MinShards:     m,
		TotalShards:   n,
		Contracts:     contracts,
		CurrentHeight: height,
	})
	body := io.MultiReader(bytes.NewReader(js), io.LimitReader(src, int64(m)*rhpv2.SectorSize))
	req, err := http.NewRequest("POST", fmt.Sprintf("%v%v", c.c.BaseURL, "/slabs/upload"), body)
	if err != nil {
		panic(err)
	}
	req.SetBasicAuth("", c.c.Password)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return slab.Slab{}, err
	}
	defer io.Copy(ioutil.Discard, resp.Body)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		err, _ := ioutil.ReadAll(resp.Body)
		return slab.Slab{}, errors.New(string(err))
	}
	err = json.NewDecoder(resp.Body).Decode(&s)
	return
}

// DownloadSlab downloads data from a set of hosts.
func (c *Client) DownloadSlab(dst io.Writer, s slab.Slice, contracts []Contract) ([]HostInteraction, error) {
	c.c.Custom("POST", "/slabs/download", SlabsDownloadRequest{}, (*[]byte)(nil))

	js, _ := json.Marshal(SlabsDownloadRequest{
		Slab:      s,
		Contracts: contracts,
	})
	req, err := http.NewRequest("POST", fmt.Sprintf("%v%v", c.c.BaseURL, "/slabs/download"), bytes.NewReader(js))
	if err != nil {
		panic(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth("", c.c.Password)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer io.Copy(ioutil.Discard, resp.Body)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		err, _ := ioutil.ReadAll(resp.Body)
		return nil, errors.New(string(err))
	}

	// read response data
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	// write slab data
	dataLen := binary.LittleEndian.Uint64(b[:8])
	_, err = dst.Write(b[8 : dataLen+8])
	if err != nil {
		return nil, err
	}

	// read metadata
	var metadata []HostInteraction
	err = json.NewDecoder(bytes.NewReader(b[dataLen+8:])).Decode(&metadata)
	if err != nil {
		return nil, err
	}
	return metadata, nil
}

// MigrateSlab migrates the specified slab.
func (c *Client) MigrateSlab(s *slab.Slab, from, to []Contract, currentHeight uint64) (err error) {
	req := SlabsMigrateRequest{
		Slab:          *s,
		From:          from,
		To:            to,
		CurrentHeight: currentHeight,
	}
	err = c.c.POST("/slabs/migrate", req, s)
	return
}

// DeleteSlabs deletes the specified slabs.
func (c *Client) DeleteSlabs(slabs []slab.Slab, contracts []Contract) (err error) {
	req := SlabsDeleteRequest{
		Slabs:     slabs,
		Contracts: contracts,
	}
	err = c.c.POST("/slabs/delete", req, nil)
	return
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
	defer io.Copy(ioutil.Discard, resp.Body)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		err, _ := ioutil.ReadAll(resp.Body)
		return errors.New(string(err))
	}
	return
}

// DownloadObject downloads the object with the given name, writing its data to
// w.
func (c *Client) DownloadObject(w io.Writer, name string) (err error) {
	c.c.Custom("GET", fmt.Sprintf("/objects/%s", name), nil, (*[]byte)(nil))

	req, err := http.NewRequest("GET", fmt.Sprintf("%v/objects/%v", c.c.BaseURL, name), nil)
	if err != nil {
		panic(err)
	}
	req.SetBasicAuth("", c.c.Password)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer io.Copy(ioutil.Discard, resp.Body)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		err, _ := ioutil.ReadAll(resp.Body)
		return errors.New(string(err))
	}
	_, err = io.Copy(w, resp.Body)
	return
}

// DeleteObject deletes the object with the given name.
func (c *Client) DeleteObject(name string) (err error) {
	err = c.c.DELETE(fmt.Sprintf("/objects/%s", name))
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
