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

	"go.sia.tech/jape"
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
	c jape.Client
}

// SyncerPeers returns the current peers of the syncer.
func (c *Client) SyncerPeers() (resp []string, err error) {
	err = c.c.GET("/syncer/peers", &resp)
	return
}

// SyncerConnect adds the address as a peer of the syncer.
func (c *Client) SyncerConnect(addr string) (err error) {
	err = c.c.POST("/syncer/connect", addr, nil)
	return
}

// ConsensusTip returns the current tip index.
func (c *Client) ConsensusTip() (resp ChainIndex, err error) {
	err = c.c.GET("/consensus/tip", &resp)
	return
}

// TransactionPool returns the transactions currently in the pool.
func (c *Client) TransactionPool() (txns []types.Transaction, err error) {
	err = c.c.GET("/txpool/transactions", &txns)
	return
}

// BroadcastTransaction broadcasts the transaction set to the network.
func (c *Client) BroadcastTransaction(txns []types.Transaction) error {
	return c.c.POST("/txpool/broadcast", txns, nil)
}

// WalletBalance returns the current wallet balance.
func (c *Client) WalletBalance() (bal types.Currency, err error) {
	err = c.c.GET("/wallet/balance", &bal)
	return
}

// WalletAddress returns an address controlled by the wallet.
func (c *Client) WalletAddress() (resp types.UnlockHash, err error) {
	err = c.c.GET("/wallet/address", &resp)
	return
}

// WalletOutputs returns the set of unspent outputs controlled by the wallet.
func (c *Client) WalletOutputs() (resp []wallet.SiacoinElement, err error) {
	err = c.c.GET("/wallet/outputs", &resp)
	return
}

// WalletTransactions returns all transactions relevant to the wallet.
func (c *Client) WalletTransactions(since time.Time, max int) (resp []wallet.Transaction, err error) {
	err = c.c.GET(fmt.Sprintf("/wallet/transactions?since=%s&max=%d", paramTime(since), max), &resp)
	return
}

// WalletFund funds txn using inputs controlled by the wallet.
func (c *Client) WalletFund(txn *types.Transaction, amount types.Currency) ([]types.OutputID, []types.Transaction, error) {
	req := WalletFundRequest{
		Transaction: *txn,
		Amount:      amount,
	}
	var resp WalletFundResponse
	err := c.c.POST("/wallet/fund", req, &resp)
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
	return c.c.POST("/wallet/sign", req, txn)
}

// WalletSplit returns a transaction that splits the wallet in the desired
// number of outputs of given amount. The transaction needs to be signed and
// then broadcasted to the network.
func (c *Client) WalletSplit(outputs int, amount types.Currency) error {
	req := WalletSplitRequest{
		Amount:  amount,
		Outputs: outputs,
	}
	var resp WalletSplitResponse
	return c.c.POST("/wallet/split", req, &resp)
}

// WalletDiscard discards the provided txn, make its inputs usable again. This
// should only be called on transactions that will never be broadcast.
func (c *Client) WalletDiscard(txn types.Transaction) error {
	return c.c.POST("/wallet/discard", txn, nil)
}

// WalletPrepareForm funds and signs a contract transaction.
func (c *Client) WalletPrepareForm(renterKey PrivateKey, hostKey PublicKey, renterFunds types.Currency, renterAddress types.UnlockHash, hostCollateral types.Currency, endHeight uint64, hostSettings rhpv2.HostSettings) (txns []types.Transaction, err error) {
	req := WalletPrepareFormRequest{
		RenterKey:      renterKey,
		HostKey:        hostKey,
		RenterFunds:    renterFunds,
		RenterAddress:  renterAddress,
		HostCollateral: hostCollateral,
		EndHeight:      endHeight,
		HostSettings:   hostSettings,
	}
	err = c.c.POST("/wallet/prepare/form", req, &txns)
	return
}

// WalletPrepareRenew funds and signs a contract renewal transaction.
func (c *Client) WalletPrepareRenew(contract types.FileContractRevision, renterKey PrivateKey, hostKey PublicKey, renterFunds types.Currency, renterAddress types.UnlockHash, hostCollateral types.Currency, endHeight uint64, hostSettings rhpv2.HostSettings) ([]types.Transaction, types.Currency, error) {
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
	err := c.c.POST("/wallet/prepare/renew", req, &resp)
	return resp.TransactionSet, resp.FinalPayment, err
}

// WalletPending returns the txpool transactions that are relevant to the
// wallet.
func (c *Client) WalletPending() (resp []types.Transaction, err error) {
	err = c.c.GET("/wallet/pending", &resp)
	return
}

// Hosts returns all hosts known to the server.
func (c *Client) Hosts() (hosts []hostdb.Host, err error) {
	err = c.c.GET("/hosts", &hosts)
	return
}

// Host returns information about a particular host known to the server.
func (c *Client) Host(hostKey PublicKey) (h hostdb.Host, err error) {
	err = c.c.GET(fmt.Sprintf("/hosts/%s", hostKey), &h)
	return
}

// SetHostScore sets the score for the supplied host.
func (c *Client) SetHostScore(hostKey PublicKey, score float64) (err error) {
	err = c.c.PUT(fmt.Sprintf("/hosts/%s/score", hostKey), score)
	return
}

// RecordHostInteraction records an interaction for the supplied host.
func (c *Client) RecordHostInteraction(hostKey PublicKey, i hostdb.Interaction) (err error) {
	err = c.c.POST(fmt.Sprintf("/hosts/%s/interaction", hostKey), i, nil)
	return
}

// RHPScan scans a host, returning its current settings.
func (c *Client) RHPScan(hostKey PublicKey, hostIP string) (resp rhpv2.HostSettings, err error) {
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

// Contracts returns the current set of contracts.
func (c *Client) Contracts() (contracts []rhpv2.Contract, err error) {
	err = c.c.GET("/contracts", &contracts)
	return
}

// Contract returns the contract with the given ID.
func (c *Client) Contract(id types.FileContractID) (contract rhpv2.Contract, err error) {
	err = c.c.GET(fmt.Sprintf("/contracts/%s", id), &contract)
	return
}

// AddContract adds the provided contract to the current contract set.
func (c *Client) AddContract(contract rhpv2.Contract) (err error) {
	err = c.c.PUT(fmt.Sprintf("/contracts/%s", contract.ID()), contract)
	return
}

// DeleteContract deletes the contract with the given ID.
func (c *Client) DeleteContract(id types.FileContractID) (err error) {
	err = c.c.DELETE(fmt.Sprintf("/contracts/%s", id))
	return
}

// HostSets returns the names of all host sets.
func (c *Client) HostSets() (sets []string, err error) {
	err = c.c.GET("/hostsets", &sets)
	return
}

// HostSet returns the hosts in the given set.
func (c *Client) HostSet(name string) (hosts []consensus.PublicKey, err error) {
	err = c.c.GET(fmt.Sprintf("/hostsets/%s", name), &hosts)
	return
}

// SetHostSet assigns a name to the given hosts.
func (c *Client) SetHostSet(name string, hosts []consensus.PublicKey) (err error) {
	err = c.c.PUT(fmt.Sprintf("/hostsets/%s", name), hosts)
	return
}

// HostSetContracts returns the latest contract for each host in the given set,
// or nil if no contract exists.
func (c *Client) HostSetContracts(name string) (contracts []*rhpv2.Contract, err error) {
	err = c.c.GET(fmt.Sprintf("/hostsets/%s/contracts", name), &contracts)
	return
}

// HostSetResolves returns the last announced IP for each host in the given set,
// or the empty string if no announcement is known.
func (c *Client) HostSetResolves(name string) (ips []string, err error) {
	err = c.c.GET(fmt.Sprintf("/hostsets/%s/resolve", name), &ips)
	return
}

// UploadSlabs uploads data to a set of hosts.
func (c *Client) UploadSlabs(src io.Reader, m, n uint8, height uint64, contracts []Contract) (slabs []slab.Slab, err error) {
	c.c.Custom("POST", "/slabs/upload", []byte{}, &slabs)

	// apparently, the only way to stream a multipart upload is via io.Pipe :/
	r, w := io.Pipe()
	mw := multipart.NewWriter(w)
	errChan := make(chan error, 1)
	go func() {
		defer w.Close()
		defer mw.Close()
		js, _ := json.Marshal(SlabsUploadRequest{
			MinShards:     m,
			TotalShards:   n,
			Contracts:     contracts,
			CurrentHeight: height,
		})
		mw.WriteField("meta", string(js))
		part, _ := mw.CreateFormFile("data", "data")
		_, err := io.Copy(part, src)
		errChan <- err
	}()
	req, err := http.NewRequest("POST", fmt.Sprintf("%v%v", c.c.BaseURL, "/slabs/upload"), r)
	if err != nil {
		panic(err)
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.SetBasicAuth("", c.c.Password)
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
	c.c.Custom("POST", "/slabs/download", SlabsDownloadRequest{}, (*[]byte)(nil))

	js, _ := json.Marshal(SlabsDownloadRequest{
		Slabs:     slabs,
		Offset:    offset,
		Length:    length,
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

// MigrateSlabs migrates the specified slabs.
func (c *Client) MigrateSlabs(slabs []slab.Slab, from, to []Contract, currentHeight uint64) (err error) {
	req := SlabsMigrateRequest{
		Slabs:         slabs,
		From:          from,
		To:            to,
		CurrentHeight: currentHeight,
	}
	err = c.c.POST("/slabs/migrate", req, nil)
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

func (c *Client) objects(path string) (or ObjectsResponse, err error) {
	err = c.c.GET(fmt.Sprintf("/objects/%s", path), &or)
	return
}

// Object returns the object with the given name.
func (c *Client) Object(name string) (o object.Object, err error) {
	or, err := c.objects(name)
	if err == nil {
		o = *or.Object
	}
	return
}

// ObjectEntries returns the entries at the given path, which must end in /.
func (c *Client) ObjectEntries(path string) (entries []string, err error) {
	or, err := c.objects(path)
	return or.Entries, err
}

// AddObject stores the provided object under the given name.
func (c *Client) AddObject(name string, o object.Object) (err error) {
	err = c.c.PUT(fmt.Sprintf("/objects/%s", name), o)
	return
}

// DeleteObject deletes the object with the given name.
func (c *Client) DeleteObject(name string) (err error) {
	err = c.c.DELETE(fmt.Sprintf("/objects/%s", name))
	return
}

// NewClient returns a client that communicates with a renterd server listening
// on the specified address.
func NewClient(addr, password string) *Client {
	return &Client{jape.Client{
		BaseURL:  addr,
		Password: password,
	}}
}
