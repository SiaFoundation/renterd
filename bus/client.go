package bus

import (
	"encoding/json"
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

// AcceptBlock submits a block to the consensus manager.
func (c *Client) AcceptBlock(b types.Block) (err error) {
	err = c.c.POST("/consensus/acceptblock", b, nil)
	return
}

// SyncerAddress returns the address the syncer is listening on.
func (c *Client) SyncerAddress() (addr string, err error) {
	err = c.c.GET("/syncer/address", &addr)
	return
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

// estimatedSiacoinTxnSize estimates the txn size of a siacoin txn without file
// contract given its number of outputs.
func estimatedSiacoinTxnSize(nOutputs uint64) uint64 {
	return 1000 + 60*nOutputs
}

// SendSiacoins is a helper method that sends siacoins to the given outputs.
func (c *Client) SendSiacoins(scos []types.SiacoinOutput) (err error) {
	fee, err := c.RecommendedFee()
	if err != nil {
		return err
	}
	fee = fee.Mul64(estimatedSiacoinTxnSize(uint64(len(scos))))

	var value types.Currency
	for _, sco := range scos {
		value = value.Add(sco.Value)
	}
	txn := types.Transaction{
		SiacoinOutputs: scos,
		MinerFees:      []types.Currency{fee},
	}
	toSign, parents, err := c.WalletFund(&txn, value)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = c.WalletDiscard(txn)
		}
	}()
	err = c.WalletSign(&txn, toSign, types.FullCoveredFields)
	if err != nil {
		return err
	}
	return c.BroadcastTransaction(append(parents, txn))
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
func (c *Client) WalletPrepareRenew(contract types.FileContractRevision, renterKey PrivateKey, hostKey PublicKey, renterFunds types.Currency, renterAddress types.UnlockHash, endHeight uint64, hostSettings rhpv2.HostSettings) ([]types.Transaction, types.Currency, error) {
	req := WalletPrepareRenewRequest{
		Contract:      contract,
		RenterKey:     renterKey,
		HostKey:       hostKey,
		RenterFunds:   renterFunds,
		RenterAddress: renterAddress,
		EndHeight:     endHeight,
		HostSettings:  hostSettings,
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

// Contracts returns all contracts in the contract store.
func (c *Client) Contracts() (contracts []Contract, err error) {
	err = c.c.GET("/contracts", &contracts)
	return
}

// Contract returns the contract with the given ID.
func (c *Client) Contract(id types.FileContractID) (contract Contract, err error) {
	err = c.c.GET(fmt.Sprintf("/contracts/%s", id), &contract)
	return
}

// AddContract adds the provided contract to the contract store.
func (c *Client) AddContract(contract rhpv2.ContractRevision, totalCost types.Currency, startHeight uint64) (added Contract, err error) {
	err = c.c.POST(fmt.Sprintf("/contracts/%s/new", contract.ID()), ContractsIDAddRequest{
		Contract:    contract,
		StartHeight: startHeight,
		TotalCost:   totalCost,
	}, &added)
	return
}

// AddRenewedContract adds the provided contract to the contract store.
func (c *Client) AddRenewedContract(contract rhpv2.ContractRevision, totalCost types.Currency, startHeight uint64, renewedFrom types.FileContractID) (renewed Contract, err error) {
	err = c.c.POST(fmt.Sprintf("/contracts/%s/renewed", contract.ID()), ContractsIDRenewedRequest{
		Contract:    contract,
		RenewedFrom: renewedFrom,
		StartHeight: startHeight,
		TotalCost:   totalCost,
	}, &renewed)
	return
}

// DeleteContracts deletes the contracts with the given IDs.
func (c *Client) DeleteContracts(ids []types.FileContractID) error {
	// TODO: batch delete
	for _, id := range ids {
		if err := c.DeleteContract(id); err != nil {
			return err
		}
	}
	return nil
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

// ContractSet returns the contracts in the given set.
func (c *Client) ContractSet(name string) (hosts []Contract, err error) {
	err = c.c.GET(fmt.Sprintf("/contractsets/%s", name), &hosts)
	return
}

// SetContractSet assigns a name to the given contracts.
func (c *Client) SetContractSet(name string, contracts []types.FileContractID) (err error) {
	err = c.c.PUT(fmt.Sprintf("/contractsets/%s", name), contracts)
	return
}

// AcquireContract acquires a contract for a given amount of time unless
// released manually before that time.
func (c *Client) AcquireContract(fcid types.FileContractID, d time.Duration) (locked bool, err error) {
	var resp ContractAcquireResponse
	err = c.c.POST(fmt.Sprintf("/contracts/%s/acquire", fcid), ContractAcquireRequest{Duration: d}, &resp)
	locked = resp.Locked
	return
}

// ReleaseContract releases a contract that was previously acquired using AcquireContract.
func (c *Client) ReleaseContract(fcid types.FileContractID) (err error) {
	err = c.c.POST(fmt.Sprintf("/contracts/%s/release", fcid), nil, nil)
	return
}

// RecommendedFee returns the recommended fee for a txn.
func (c *Client) RecommendedFee() (fee types.Currency, err error) {
	err = c.c.GET("/txpool/recommendedfee", &fee)
	return
}

// ContractsForSlab returns contracts that can be used to download the provided
// slab.
func (c *Client) ContractsForSlab(shards []object.Sector, contractSetName string) ([]Contract, error) {
	// fetch all contracts from the set
	contractIds, err := c.ContractSet(contractSetName)
	if err != nil {
		return nil, err
	}

	// build two maps that indicate whether a contract is needed or usable, a
	// contract is needed when there are shards located on its host, a contract
	// is only usable if its in the contract set
	isNeeded := make(map[string]struct{})
	for _, shard := range shards {
		isNeeded[shard.Host.String()] = struct{}{}
	}
	isUsable := make(map[types.FileContractID]struct{})
	for _, contractId := range contractIds {
		isUsable[contractId.ID] = struct{}{}
	}

	// fetch all contracts and filter them
	contracts, err := c.Contracts()
	if err != nil {
		return nil, err
	}

	filtered := contracts[:0]
	for _, contract := range contracts {
		if _, usable := isUsable[contract.ID]; !usable {
			continue
		}
		if _, needed := isNeeded[contract.HostKey.String()]; !needed {
			continue
		}
		filtered = append(filtered, contract)
	}
	return filtered, nil
}

// Setting returns the value for the setting with given key.
func (c *Client) Setting(key string, resp interface{}) (err error) {
	var value string
	if err := c.c.GET(fmt.Sprintf("/setting/%s", key), &value); err != nil {
		return err
	}
	return json.Unmarshal([]byte(value), &resp)
}

// Settings returns the keys of all settings in the store.
func (c *Client) Settings() (settings []string, err error) {
	err = c.c.GET("/settings", &settings)
	return
}

// UpdateSetting will update or insert the setting for given key with the given value.
func (c *Client) UpdateSetting(key string, value interface{}) error {
	v, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("unable to marshal given setting, err: %v", err)
	}

	return c.c.POST(fmt.Sprintf("/setting/%s/%s", key, url.QueryEscape(string(v))), nil, nil)
}

// GougingSettings returns the gouging settings.
func (c *Client) GougingSettings() (gs GougingSettings, err error) {
	err = c.Setting(SettingGouging, &gs)
	return
}

// UpdateGougingSettings allows configuring the gouging settings.
func (c *Client) UpdateGougingSettings(gs GougingSettings) error {
	return c.UpdateSetting(SettingGouging, gs)
}

// RedundancySettings returns the redundancy settings.
func (c *Client) RedundancySettings() (rs RedundancySettings, err error) {
	err = c.Setting(SettingRedundancy, &rs)
	return
}

// UpdateRedundancySettings allows configuring the redundancy.
func (c *Client) UpdateRedundancySettings(rs RedundancySettings) error {
	return c.UpdateSetting(SettingRedundancy, rs)
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

// SlabsForMigration returns up to n slabs which require migration and haven't
// failed migration since failureCutoff.
func (c *Client) SlabsForMigration(n int, failureCutoff time.Time, goodContracts []types.FileContractID) (slabs []object.Slab, err error) {
	values := url.Values{}
	values.Set("cutoff", paramTime(failureCutoff).String())
	values.Set("limit", fmt.Sprint(n))
	values.Set("goodContracts", fmt.Sprint(goodContracts))
	err = c.c.GET("/migration/slabs?"+values.Encode(), &slabs)
	return
}

// DownloadParams returns parameters used for downloading slabs.
func (c *Client) DownloadParams() (dp DownloadParams, err error) {
	return DownloadParams{
		ContractSet: "autopilot", // TODO
	}, nil
}

// UploadParams returns parameters used for uploading slabs.
func (c *Client) UploadParams() (up UploadParams, err error) {
	rs, err := c.RedundancySettings()
	if err != nil {
		return UploadParams{}, err
	}

	cs, err := c.ConsensusState()
	if err != nil {
		return UploadParams{}, err
	}

	return UploadParams{
		CurrentHeight: cs.BlockHeight,
		MinShards:     uint8(rs.MinShards),   // TODO
		TotalShards:   uint8(rs.TotalShards), // TODO
		ContractSet:   "autopilot",           // TODO
	}, nil
}

// MigrateParams returns parameters used for migrating a slab.
func (c *Client) MigrateParams(slab object.Slab) (up MigrateParams, err error) {
	panic("unimplemented")
}

// NewClient returns a client that communicates with a renterd store server
// listening on the specified address.
func NewClient(addr, password string) *Client {
	return &Client{jape.Client{
		BaseURL:  addr,
		Password: password,
	}}
}
