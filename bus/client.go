package bus

import (
	"fmt"
	"net/url"
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
	err = c.c.GET(fmt.Sprintf("/hosts?max=%v&notSince=%v", max, paramTime(notSince)), &hosts)
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
func (c *Client) Contracts(orderBy string, limit int) (contracts []rhpv2.Contract, err error) {
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
	err = c.c.PUT(fmt.Sprintf("/contracts/%s/new", contract.ID()), contract)
	return
}

// AddRenewedContract adds the provided contract to the current contract set.
func (c *Client) AddRenewedContract(contract rhpv2.Contract, renewedFrom types.FileContractID) (err error) {
	err = c.c.PUT(fmt.Sprintf("/contracts/%s/renewed", contract.ID()), ContractsIDRenewedRequest{
		Contract:    contract,
		RenewedFrom: renewedFrom,
	})
	return
}

// DeleteContract deletes the contract with the given ID.
func (c *Client) DeleteContract(id types.FileContractID) (err error) {
	err = c.c.DELETE(fmt.Sprintf("/contracts/%s", id))
	return
}

// ContractSets returns the names of all host sets.
func (c *Client) ContractSets() (sets []string, err error) {
	err = c.c.GET("/contractsets", &sets)
	return
}

// HostSet returns the contracts in the given set.
func (c *Client) ContractSet(name string) (hosts []types.FileContractID, err error) {
	err = c.c.GET(fmt.Sprintf("/contractsets/%s", name), &hosts)
	return
}

// SetContractSet assigns a name to the given contracts.
func (c *Client) SetContractSet(name string, contracts []types.FileContractID) (err error) {
	err = c.c.PUT(fmt.Sprintf("/contractsets/%s", name), contracts)
	return
}

// SetContracts returns the full contract for each contract id in the given set.
// The ID and HostIP fields may be empty, depending on whether a contract exists
// and a host announcement is known.
func (c *Client) SetContracts(name string) (contracts []Contract, err error) {
	err = c.c.GET(fmt.Sprintf("/contractsets/%s/contracts", name), &contracts)
	return
}

// AcquireContract acquires a contract for a given amount of time unless
// released manually before that time.
func (c *Client) AcquireContract(fcid types.FileContractID, d time.Duration) (rev types.FileContractRevision, err error) {
	var resp ContractAcquireResponse
	err = c.c.POST(fmt.Sprintf("/contracts/%s/acquire", fcid), ContractAcquireRequest{Duration: d}, &resp)
	rev = resp.Revision
	return
}

// ReleaseContract releases a contract that was previously acquired using AcquireContract.
func (c *Client) ReleaseContract(fcid types.FileContractID) (err error) {
	err = c.c.POST(fmt.Sprintf("/contracts/%s/release", fcid), nil, nil)
	return
}

func (c *Client) DeleteContracts(ids []types.FileContractID) error {
	panic("unimplemented")
}
func (c *Client) ActiveContracts() ([]Contract, error) {
	panic("unimplemented")
}
func (c *Client) SpendingHistory(types.FileContractID, uint64) ([]ContractSpending, error) {
	panic("unimplemented")
}
func (c *Client) ContractMetadata(types.FileContractID) (ContractMetadata, error) {
	panic("unimplemented")
}
func (c *Client) UpdateContractMetadata(types.FileContractID, ContractMetadata) error {
	panic("unimplemented")
}
func (c *Client) RecommendedFee() (types.Currency, error) {
	panic("unimplemented")
}

// ContractsForSlab returns contracts that can be used to download the provided
// slab.
func (c *Client) ContractsForSlab(shards []object.Sector) (contracts []Contract, err error) {
	panic("unimplemented")
}

// Object returns the object at the given path, or, if path ends in '/', the
// entries under that path.
func (c *Client) Object(path string) (o object.Object, entries []string, err error) {
	var or ObjectsResponse
	err = c.c.GET(fmt.Sprintf("/objects/%s", path), &or)
	if or.Object != nil {
		o = *or.Object
	} else {
		entries = or.Entries
	}
	return
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

// MarkSlabsMigrationFailure updates the latest failure time of the given slabs
// to the current time.
func (c *Client) MarkSlabsMigrationFailure(slabIDs []SlabID) (int, error) {
	var resp ObjectsMarkSlabMigrationFailureResponse
	err := c.c.POST("/migration/failed", ObjectsMarkSlabMigrationFailureRequest{
		SlabIDs: slabIDs,
	}, &resp)
	return resp.Updates, err
}

// SlabsForMigration returns up to n slabs which require migration and haven't
// failed migration since failureCutoff.
func (c *Client) SlabsForMigration(n int, failureCutoff time.Time, goodContracts []types.FileContractID) ([]SlabID, error) {
	var values url.Values
	values.Set("cutoff", paramTime(failureCutoff).String())
	values.Set("limit", fmt.Sprint(n))
	values.Set("goodContracts", fmt.Sprint(goodContracts))
	var resp ObjectsMigrateSlabsResponse
	err := c.c.GET("/migration/slabs?%s"+values.Encode(), &resp)
	return resp.SlabIDs, err
}

// SlabForMigration returns a slab and the contracts its stored on.
func (c *Client) SlabForMigration(slabID SlabID) (object.Slab, []MigrationContract, error) {
	var resp ObjectsMigrateSlabResponse
	err := c.c.GET(fmt.Sprintf("/migration/slab/%s", slabID), &resp)
	return resp.Slab, resp.Contracts, err
}

// NewClient returns a client that communicates with a renterd store server
// listening on the specified address.
func NewClient(addr, password string) *Client {
	return &Client{jape.Client{
		BaseURL:  addr,
		Password: password,
	}}
}
