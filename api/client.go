package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"time"

	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/renterd/object"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	rhpv3 "go.sia.tech/renterd/rhp/v3"
	"go.sia.tech/renterd/slab"
	"go.sia.tech/renterd/wallet"
	"go.sia.tech/siad/types"
)

// A Client provides methods for interacting with a renterd API server.
type Client struct {
	baseURL  string
	password string
}

func (c *Client) req(method string, route string, data, resp interface{}) error {
	var body io.Reader
	if data != nil {
		js, _ := json.Marshal(data)
		body = bytes.NewReader(js)
	}
	req, err := http.NewRequest(method, fmt.Sprintf("%v%v", c.baseURL, route), body)
	if err != nil {
		panic(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth("", c.password)
	r, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer io.Copy(ioutil.Discard, r.Body)
	defer r.Body.Close()
	if r.StatusCode != 200 {
		err, _ := ioutil.ReadAll(r.Body)
		return errors.New(string(err))
	}
	if resp == nil {
		return nil
	}
	return json.NewDecoder(r.Body).Decode(resp)
}

func (c *Client) get(route string, r interface{}) error     { return c.req("GET", route, nil, r) }
func (c *Client) post(route string, d, r interface{}) error { return c.req("POST", route, d, r) }
func (c *Client) put(route string, d interface{}) error     { return c.req("PUT", route, d, nil) }
func (c *Client) delete(route string) error                 { return c.req("DELETE", route, nil, nil) }

// WalletBalance returns the current wallet balance.
func (c *Client) WalletBalance() (types.Currency, error) {
	var resp WalletBalanceResponse
	err := c.get("/wallet/balance", &resp)
	return resp.Siacoins, err
}

// WalletAddress returns an address controlled by the wallet.
func (c *Client) WalletAddress() (resp types.UnlockHash, err error) {
	err = c.get("/wallet/address", &resp)
	return
}

// WalletOutputs returns the set of unspent outputs controlled by the wallet.
func (c *Client) WalletOutputs() (resp []wallet.SiacoinElement, err error) {
	err = c.get("/wallet/outputs", &resp)
	return
}

// WalletTransactions returns all transactions relevant to the wallet.
func (c *Client) WalletTransactions(since time.Time, max int) (resp []wallet.Transaction, err error) {
	err = c.get(fmt.Sprintf("/wallet/transactions?since=%s&max=%d", since.Format(time.RFC3339), max), &resp)
	return
}

// WalletFund funds txn using inputs controlled by the wallet.
func (c *Client) WalletFund(txn *types.Transaction, amount types.Currency) ([]types.Transaction, error) {
	req := WalletFundRequest{
		Transaction: *txn,
		Amount:      amount,
	}
	var resp WalletFundResponse
	err := c.post("/wallet/fund", req, &resp)
	if err != nil {
		return nil, err
	}
	*txn = resp.Transaction
	return resp.DependsOn, nil
}

// SyncerPeers returns the current peers of the syncer.
func (c *Client) SyncerPeers() (resp []SyncerPeer, err error) {
	err = c.get("/syncer/peers", &resp)
	return
}

// SyncerConnect adds the address as a peer of the syncer.
func (c *Client) SyncerConnect(addr string) (err error) {
	err = c.post("/syncer/connect", addr, nil)
	return
}

// RHPScan scans a host, returning its current settings.
func (c *Client) RHPScan(hostKey consensus.PublicKey, hostIP string) (resp rhpv2.HostSettings, err error) {
	err = c.post("/rhp/scan", RHPScanRequest{hostKey, hostIP}, &resp)
	return
}

// RHPPrepareForm prepares a contract formation transaction.
func (c *Client) RHPPrepareForm(renterKey consensus.PrivateKey, hostKey consensus.PublicKey, renterFunds types.Currency, renterAddress types.UnlockHash, hostCollateral types.Currency, endHeight uint64, hostSettings rhpv2.HostSettings) (types.FileContract, types.Currency, error) {
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
	err := c.post("/rhp/prepare/form", req, &resp)
	return resp.Contract, resp.Cost, err
}

// RHPForm forms a contract with a host.
func (c *Client) RHPForm(renterKey consensus.PrivateKey, hostKey consensus.PublicKey, hostIP string, transactionSet []types.Transaction, walletKey consensus.PrivateKey) (rhpv2.Contract, []types.Transaction, error) {
	req := RHPFormRequest{
		RenterKey:      renterKey,
		HostKey:        hostKey,
		HostIP:         hostIP,
		TransactionSet: transactionSet,
		WalletKey:      walletKey,
	}
	var resp RHPFormResponse
	err := c.post("/rhp/form", req, &resp)
	return resp.Contract, resp.TransactionSet, err
}

// RHPPrepareRenew prepares a contract renewal transaction.
func (c *Client) RHPPrepareRenew(contract types.FileContractRevision, renterKey consensus.PrivateKey, hostKey consensus.PublicKey, renterFunds types.Currency, renterAddress types.UnlockHash, hostCollateral types.Currency, endHeight uint64, hostSettings rhpv2.HostSettings) (types.FileContract, types.Currency, types.Currency, error) {
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
	err := c.post("/rhp/prepare/renew", req, &resp)
	return resp.Contract, resp.Cost, resp.FinalPayment, err
}

// RHPRenew renews an existing contract with a host.
func (c *Client) RHPRenew(renterKey consensus.PrivateKey, hostKey consensus.PublicKey, hostIP string, contractID types.FileContractID, transactionSet []types.Transaction, finalPayment types.Currency, walletKey consensus.PrivateKey) (rhpv2.Contract, []types.Transaction, error) {
	req := RHPRenewRequest{
		RenterKey:      renterKey,
		HostKey:        hostKey,
		HostIP:         hostIP,
		ContractID:     contractID,
		TransactionSet: transactionSet,
		FinalPayment:   finalPayment,
		WalletKey:      walletKey,
	}
	var resp RHPRenewResponse
	err := c.post("/rhp/renew", req, &resp)
	return resp.Contract, resp.TransactionSet, err
}

// RHPReadRegistry reads a registry value.
func (c *Client) RHPReadRegistry(key rhpv3.RegistryKey) (resp rhpv3.RegistryValue, err error) {
	err = c.get(fmt.Sprintf("/rhp/registry/%x/%x", key.PublicKey, key.Tweak[:]), &resp)
	return
}

// RHPUpdateRegistry updates a registry value.
func (c *Client) RHPUpdateRegistry(key rhpv3.RegistryKey, value rhpv3.RegistryValue) (err error) {
	err = c.put(fmt.Sprintf("/rhp/registry/%x/%x", key.PublicKey, key.Tweak[:]), value)
	return
}

// Contracts returns the current set of contracts.
func (c *Client) Contracts() (contracts []rhpv2.Contract, err error) {
	err = c.get("/contracts", &contracts)
	return
}

// Contract returns the contract with the given ID.
func (c *Client) Contract(id types.FileContractID) (contract rhpv2.Contract, err error) {
	err = c.get("/contracts/"+id.String(), &contract)
	return
}

// AddContract adds the provided contract to the current contract set.
func (c *Client) AddContract(contract rhpv2.Contract) (err error) {
	err = c.put("/contracts/"+contract.ID().String(), &contract)
	return
}

// DeleteContract deletes the contract with the given ID.
func (c *Client) DeleteContract(id types.FileContractID) (err error) {
	err = c.delete("/contracts/" + id.String())
	return
}

// UploadSlabs uploads data to a set of hosts.
func (c *Client) UploadSlabs(src io.Reader, m, n uint8, contracts []Contract) (slabs []slab.Slab, err error) {
	// apparently, the only way to stream a multipart upload is via io.Pipe :/
	r, w := io.Pipe()
	mw := multipart.NewWriter(w)
	errChan := make(chan error, 1)
	go func() {
		defer w.Close()
		defer mw.Close()
		js, _ := json.Marshal(SlabsUploadRequest{
			MinShards:   m,
			TotalShards: n,
			Contracts:   contracts,
		})
		mw.WriteField("meta", string(js))
		part, _ := mw.CreateFormFile("data", "data")
		_, err := io.Copy(part, src)
		errChan <- err
	}()
	req, err := http.NewRequest("POST", fmt.Sprintf("%v%v", c.baseURL, "/slabs/upload"), r)
	if err != nil {
		panic(err)
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.SetBasicAuth("", c.password)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	} else if err := <-errChan; err != nil {
		return nil, err
	}
	defer io.Copy(ioutil.Discard, resp.Body)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		err, _ := ioutil.ReadAll(resp.Body)
		return nil, errors.New(string(err))
	}
	err = json.NewDecoder(resp.Body).Decode(&slabs)
	return
}

// DownloadSlabs downloads data from a set of hosts.
func (c *Client) DownloadSlabs(dst io.Writer, slabs []slab.Slice, offset, length int64, contracts []Contract) (err error) {
	js, _ := json.Marshal(SlabsDownloadRequest{
		Slabs:     slabs,
		Offset:    offset,
		Length:    length,
		Contracts: contracts,
	})
	req, err := http.NewRequest("POST", fmt.Sprintf("%v%v", c.baseURL, "/slabs/download"), bytes.NewReader(js))
	if err != nil {
		panic(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth("", c.password)
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
	_, err = io.Copy(dst, resp.Body)
	return
}

// DeleteSlabs deletes the specified slabs.
func (c *Client) DeleteSlabs(slabs []slab.Slab, contracts []Contract) (err error) {
	req := SlabsDeleteRequest{
		Slabs:     slabs,
		Contracts: contracts,
	}
	err = c.post("/slabs/delete", req, nil)
	return
}

// Object returns the object with the given name.
func (c *Client) Object(name string) (o object.Object, err error) {
	err = c.get("/objects/"+name, &o)
	return
}

// AddObject stores the provided object under the given name.
func (c *Client) AddObject(name string, o object.Object) (err error) {
	err = c.put("/objects/"+name, &o)
	return
}

// DeleteObject deletes the object with the given name.
func (c *Client) DeleteObject(name string) (err error) {
	err = c.delete("/objects/" + name)
	return
}

// NewClient returns a client that communicates with a renterd server listening
// on the specified address.
func NewClient(addr, password string) *Client {
	return &Client{
		baseURL:  addr,
		password: password,
	}
}
