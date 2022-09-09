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

	"go.sia.tech/renterd/hostdb"
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

// SyncerPeers returns the current peers of the syncer.
func (c *Client) SyncerPeers() (resp []string, err error) {
	err = c.get("/syncer/peers", &resp)
	return
}

// SyncerConnect adds the address as a peer of the syncer.
func (c *Client) SyncerConnect(addr string) (err error) {
	err = c.post("/syncer/connect", addr, nil)
	return
}

// ConsensusTip returns the current tip index.
func (c *Client) ConsensusTip() (resp consensus.ChainIndex, err error) {
	err = c.get("/consensus/tip", &resp)
	return
}

// TransactionPool returns the transactions currently in the pool.
func (c *Client) TransactionPool() (txns []types.Transaction, err error) {
	err = c.get("/txpool/transactions", &txns)
	return
}

// BroadcastTransaction broadcasts the transaction set to the network.
func (c *Client) BroadcastTransaction(txns []types.Transaction) error {
	return c.post("/txpool/broadcast", txns, nil)
}

// WalletBalance returns the current wallet balance.
func (c *Client) WalletBalance() (bal types.Currency, err error) {
	err = c.get("/wallet/balance", &bal)
	return
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
func (c *Client) WalletFund(txn *types.Transaction, amount types.Currency) ([]types.OutputID, []types.Transaction, error) {
	req := WalletFundRequest{
		Transaction: *txn,
		Amount:      amount,
	}
	var resp WalletFundResponse
	err := c.post("/wallet/fund", req, &resp)
	if err != nil {
		return nil, nil, err
	}
	*txn = resp.Transaction
	return resp.ToSign, resp.DependsOn, nil
}

// WalletSign signs txn using the wallet's private key.
func (c *Client) WalletSign(txn *types.Transaction, toSign []types.OutputID, cf types.CoveredFields) error {
	req := WalletSignRequest{
		Transaction:   *txn,
		ToSign:        toSign,
		CoveredFields: cf,
	}
	return c.post("/wallet/sign", req, txn)
}

// WalletDiscard discards the provided txn, make its inputs usable again. This
// should only be called on transactions that will never be broadcast.
func (c *Client) WalletDiscard(txn types.Transaction) error {
	return c.post("/wallet/discard", txn, nil)
}

// WalletPrepareForm funds and signs a contract transaction.
func (c *Client) WalletPrepareForm(renterKey consensus.PrivateKey, hostKey consensus.PublicKey, renterFunds types.Currency, renterAddress types.UnlockHash, hostCollateral types.Currency, endHeight uint64, hostSettings rhpv2.HostSettings) (txns []types.Transaction, err error) {
	req := WalletPrepareFormRequest{
		RenterKey:      renterKey,
		HostKey:        hostKey,
		RenterFunds:    renterFunds,
		RenterAddress:  renterAddress,
		HostCollateral: hostCollateral,
		EndHeight:      endHeight,
		HostSettings:   hostSettings,
	}
	err = c.post("/wallet/prepare/form", req, &txns)
	return
}

// WalletPrepareRenew funds and signs a contract renewal transaction.
func (c *Client) WalletPrepareRenew(contract types.FileContractRevision, renterKey consensus.PrivateKey, hostKey consensus.PublicKey, renterFunds types.Currency, renterAddress types.UnlockHash, hostCollateral types.Currency, endHeight uint64, hostSettings rhpv2.HostSettings) ([]types.Transaction, types.Currency, error) {
	req := WalletPrepareRenewRequest{
		Contract:       contract,
		RenterKey:      renterKey,
		HostKey:        hostKey,
		RenterFunds:    renterFunds,
		RenterAddress:  renterAddress,
		HostCollateral: hostCollateral,
		EndHeight:      endHeight,
		HostSettings:   hostSettings,
	}
	var resp WalletPrepareRenewResponse
	err := c.post("/wallet/prepare/renew", req, &resp)
	return resp.TransactionSet, resp.FinalPayment, err
}

// Hosts returns all hosts known to the server.
func (c *Client) Hosts() (hosts []hostdb.Host, err error) {
	err = c.get("/hosts", &hosts)
	return
}

// Host returns information about a particular host known to the server.
func (c *Client) Host(hostKey consensus.PublicKey) (h hostdb.Host, err error) {
	err = c.get("/hosts/"+hostKey.String(), &h)
	return
}

// SetHostScore sets the score for the supplied host.
func (c *Client) SetHostScore(hostKey consensus.PublicKey, score float64) (err error) {
	err = c.put("/hosts/"+hostKey.String()+"/score", score)
	return
}

// RecordHostInteraction records an interaction for the supplied host.
func (c *Client) RecordHostInteraction(hostKey consensus.PublicKey, i hostdb.Interaction) (err error) {
	err = c.post("/hosts/"+hostKey.String()+"/interaction", i, nil)
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

// RHPPreparePayment prepares an ephemeral account payment.
func (c *Client) RHPPreparePayment(account rhpv3.Account, amount types.Currency, key consensus.PrivateKey) (resp rhpv3.PayByEphemeralAccountRequest, err error) {
	req := RHPPreparePaymentRequest{
		Account:    account,
		Amount:     amount,
		Expiry:     0, // TODO
		AccountKey: key,
	}
	err = c.post("/rhp/prepare/payment", req, &resp)
	return
}

// RHPForm forms a contract with a host.
func (c *Client) RHPForm(renterKey consensus.PrivateKey, hostKey consensus.PublicKey, hostIP string, transactionSet []types.Transaction) (rhpv2.Contract, []types.Transaction, error) {
	req := RHPFormRequest{
		RenterKey:      renterKey,
		HostKey:        hostKey,
		HostIP:         hostIP,
		TransactionSet: transactionSet,
	}
	var resp RHPFormResponse
	err := c.post("/rhp/form", req, &resp)
	return resp.Contract, resp.TransactionSet, err
}

// RHPRenew renews an existing contract with a host.
func (c *Client) RHPRenew(renterKey consensus.PrivateKey, hostKey consensus.PublicKey, hostIP string, contractID types.FileContractID, transactionSet []types.Transaction, finalPayment types.Currency) (rhpv2.Contract, []types.Transaction, error) {
	req := RHPRenewRequest{
		RenterKey:      renterKey,
		HostKey:        hostKey,
		HostIP:         hostIP,
		ContractID:     contractID,
		TransactionSet: transactionSet,
		FinalPayment:   finalPayment,
	}
	var resp RHPRenewResponse
	err := c.post("/rhp/renew", req, &resp)
	return resp.Contract, resp.TransactionSet, err
}

// RHPFund funds an ephemeral account using the supplied contract.
func (c *Client) RHPFund(contract types.FileContractRevision, renterKey consensus.PrivateKey, hostKey consensus.PublicKey, hostIP string, account rhpv3.Account, amount types.Currency) (err error) {
	req := RHPFundRequest{
		Contract:  contract,
		RenterKey: renterKey,
		HostKey:   hostKey,
		HostIP:    hostIP,
		Account:   account,
		Amount:    amount,
	}
	err = c.post("/rhp/fund", req, nil)
	return
}

// RHPReadRegistry reads a registry value.
func (c *Client) RHPReadRegistry(hostKey consensus.PublicKey, hostIP string, key rhpv3.RegistryKey, payment rhpv3.PayByEphemeralAccountRequest) (resp rhpv3.RegistryValue, err error) {
	req := RHPRegistryReadRequest{
		HostKey:     hostKey,
		HostIP:      hostIP,
		RegistryKey: key,
		Payment:     payment,
	}
	err = c.post("/rhp/registry/read", req, &resp)
	return
}

// RHPUpdateRegistry updates a registry value.
func (c *Client) RHPUpdateRegistry(hostKey consensus.PublicKey, hostIP string, key rhpv3.RegistryKey, value rhpv3.RegistryValue, payment rhpv3.PayByEphemeralAccountRequest) (err error) {
	req := RHPRegistryUpdateRequest{
		HostKey:       hostKey,
		HostIP:        hostIP,
		RegistryKey:   key,
		RegistryValue: value,
		Payment:       payment,
	}
	err = c.post("/rhp/registry/update", req, nil)
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
