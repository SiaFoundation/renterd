package bus

import (
	"fmt"
	"time"

	"go.sia.tech/jape"
	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/renterd/object"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
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

// ConsensusState returns the current block height and whether the node is
// synced.
func (c *Client) ConsensusState() (resp ConsensusState, err error) {
	err = c.c.GET("/consensus/state", &resp)
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

// WalletRedistribute returns a signed transaction that redistributes the money
// in the wallet in the desired number of outputs of given amount.
func (c *Client) WalletRedistribute(outputs int, amount types.Currency) (txn types.Transaction, err error) {
	req := WalletRedistributeRequest{
		Amount:  amount,
		Outputs: outputs,
	}

	err = c.c.POST("/wallet/redistribute", req, &txn)
	return
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

// Hosts returns up to max hosts that have not been interacted with since
// the specified time.
func (c *Client) Hosts(notSince time.Time, max int) (hosts []hostdb.Host, err error) {
	err = c.c.GET(fmt.Sprintf("/hosts?notSince=%v&max=%d", paramTime(notSince), max), &hosts)
	return
}

// AllHosts returns all hosts known to the server.
func (c *Client) AllHosts() (hosts []hostdb.Host, err error) {
	return c.Hosts(time.Now(), -1)
}

// Host returns information about a particular host known to the server.
func (c *Client) Host(hostKey PublicKey) (h hostdb.Host, err error) {
	err = c.c.GET(fmt.Sprintf("/hosts/%s", hostKey), &h)
	return
}

// RecordHostInteraction records an interaction for the supplied host.
func (c *Client) RecordHostInteraction(hostKey PublicKey, i hostdb.Interaction) (err error) {
	err = c.c.POST(fmt.Sprintf("/hosts/%s", hostKey), i, nil)
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

// HostSetContracts returns the latest contract for each host in the given set.
// The ID and HostIP fields may be empty, depending on whether a contract exists
// and a host announcement is known.
func (c *Client) HostSetContracts(name string) (contracts []Contract, err error) {
	err = c.c.GET(fmt.Sprintf("/hostsets/%s/contracts", name), &contracts)
	return
}

func (c *Client) AcquireContractLock(types.FileContractID) (types.FileContractRevision, error) {
	panic("unimplemented")
}
func (c *Client) ReleaseContractLock(types.FileContractID) error {
	panic("unimplemented")
}
func (c *Client) ActiveContracts(maxEndHeight uint64) ([]Contract, error) {
	panic("unimplemented")
}
func (c *Client) AllContracts(currentPeriod uint64) ([]Contract, error) {
	panic("unimplemented")
}
func (c *Client) ContractData(types.FileContractID) (rhpv2.Contract, error) {
	panic("unimplemented")
}
func (c *Client) ContractHistory(types.FileContractID, uint64) ([]Contract, error) {
	panic("unimplemented")
}
func (c *Client) UpdateContractMetadata(types.FileContractID, ContractMetadata) error {
	panic("unimplemented")
}
func (c *Client) RecommendedFee() (types.Currency, error) {
	panic("unimplemented")
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
func (c *Client) AddObject(name string, o object.Object, usedContract map[consensus.PublicKey]types.FileContractID) (err error) {
	err = c.c.PUT(fmt.Sprintf("/objects/%s", name), AddObjectRequest{
		Object:        o,
		UsedContracts: usedContract,
	})
	return
}

// DeleteObject deletes the object with the given name.
func (c *Client) DeleteObject(name string) (err error) {
	err = c.c.DELETE(fmt.Sprintf("/objects/%s", name))
	return
}

// NewClient returns a client that communicates with a renterd store server
// listening on the specified address.
func NewClient(addr, password string) *Client {
	return &Client{jape.Client{
		BaseURL:  addr,
		Password: password,
	}}
}
