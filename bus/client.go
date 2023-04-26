package bus

import (
	"context"
	"errors"
	"fmt"
	"math/big"
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
	"go.sia.tech/renterd/wallet"
)

// A Client provides methods for interacting with a renterd API server.
type Client struct {
	c jape.Client
}

// AcceptBlock submits a block to the consensus manager.
func (c *Client) AcceptBlock(ctx context.Context, b types.Block) (err error) {
	err = c.c.WithContext(ctx).POST("/consensus/acceptblock", b, nil)
	return
}

// SyncerAddress returns the address the syncer is listening on.
func (c *Client) SyncerAddress(ctx context.Context) (addr string, err error) {
	err = c.c.WithContext(ctx).GET("/syncer/address", &addr)
	return
}

// SyncerPeers returns the current peers of the syncer.
func (c *Client) SyncerPeers(ctx context.Context) (resp []string, err error) {
	err = c.c.WithContext(ctx).GET("/syncer/peers", &resp)
	return
}

// SyncerConnect adds the address as a peer of the syncer.
func (c *Client) SyncerConnect(ctx context.Context, addr string) (err error) {
	err = c.c.WithContext(ctx).POST("/syncer/connect", addr, nil)
	return
}

// ConsensusState returns the current block height and whether the node is
// synced.
func (c *Client) ConsensusState(ctx context.Context) (resp api.ConsensusState, err error) {
	err = c.c.WithContext(ctx).GET("/consensus/state", &resp)
	return
}

// ConsensusNetwork returns information about the consensus network.
func (c *Client) ConsensusNetwork(ctx context.Context) (resp api.ConsensusNetwork, err error) {
	err = c.c.WithContext(ctx).GET("/consensus/network", &resp)
	return
}

// TransactionPool returns the transactions currently in the pool.
func (c *Client) TransactionPool(ctx context.Context) (txns []types.Transaction, err error) {
	err = c.c.WithContext(ctx).GET("/txpool/transactions", &txns)
	return
}

// BroadcastTransaction broadcasts the transaction set to the network.
func (c *Client) BroadcastTransaction(ctx context.Context, txns []types.Transaction) error {
	return c.c.WithContext(ctx).POST("/txpool/broadcast", txns, nil)
}

// WalletBalance returns the current wallet balance.
func (c *Client) WalletBalance(ctx context.Context) (bal types.Currency, err error) {
	err = c.c.WithContext(ctx).GET("/wallet/balance", &bal)
	return
}

// WalletAddress returns an address controlled by the wallet.
func (c *Client) WalletAddress(ctx context.Context) (resp types.Address, err error) {
	err = c.c.WithContext(ctx).GET("/wallet/address", &resp)
	return
}

// WalletOutputs returns the set of unspent outputs controlled by the wallet.
func (c *Client) WalletOutputs(ctx context.Context) (resp []wallet.SiacoinElement, err error) {
	err = c.c.WithContext(ctx).GET("/wallet/outputs", &resp)
	return
}

// estimatedSiacoinTxnSize estimates the txn size of a siacoin txn without file
// contract given its number of outputs.
func estimatedSiacoinTxnSize(nOutputs uint64) uint64 {
	return 1000 + 60*nOutputs
}

// SendSiacoins is a helper method that sends siacoins to the given outputs.
func (c *Client) SendSiacoins(ctx context.Context, scos []types.SiacoinOutput) (err error) {
	fee, err := c.RecommendedFee(ctx)
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
	toSign, parents, err := c.WalletFund(ctx, &txn, value)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = c.WalletDiscard(ctx, txn)
		}
	}()
	err = c.WalletSign(ctx, &txn, toSign, types.CoveredFields{WholeTransaction: true})
	if err != nil {
		return err
	}
	return c.BroadcastTransaction(ctx, append(parents, txn))
}

// WalletTransactions returns all transactions relevant to the wallet.
func (c *Client) WalletTransactions(ctx context.Context, since time.Time, max int) (resp []wallet.Transaction, err error) {
	err = c.c.WithContext(ctx).GET(fmt.Sprintf("/wallet/transactions?since=%s&max=%d", api.ParamTime(since), max), &resp)
	return
}

// WalletFund funds txn using inputs controlled by the wallet.
func (c *Client) WalletFund(ctx context.Context, txn *types.Transaction, amount types.Currency) ([]types.Hash256, []types.Transaction, error) {
	req := api.WalletFundRequest{
		Transaction: *txn,
		Amount:      amount,
	}
	var resp api.WalletFundResponse
	err := c.c.WithContext(ctx).POST("/wallet/fund", req, &resp)
	if err != nil {
		return nil, nil, err
	}
	*txn = resp.Transaction
	return resp.ToSign, resp.DependsOn, nil
}

// WalletSign signs txn using the wallet's private key.
func (c *Client) WalletSign(ctx context.Context, txn *types.Transaction, toSign []types.Hash256, cf types.CoveredFields) error {
	req := api.WalletSignRequest{
		Transaction:   *txn,
		ToSign:        toSign,
		CoveredFields: cf,
	}
	return c.c.WithContext(ctx).POST("/wallet/sign", req, txn)
}

// WalletRedistribute broadcasts a transaction that redistributes the money in
// the wallet in the desired number of outputs of given amount. If the
// transaction was successfully broadcasted it will return the transaction ID.
func (c *Client) WalletRedistribute(ctx context.Context, outputs int, amount types.Currency) (id types.TransactionID, err error) {
	req := api.WalletRedistributeRequest{
		Amount:  amount,
		Outputs: outputs,
	}

	err = c.c.WithContext(ctx).POST("/wallet/redistribute", req, &id)
	return
}

// WalletDiscard discards the provided txn, make its inputs usable again. This
// should only be called on transactions that will never be broadcast.
func (c *Client) WalletDiscard(ctx context.Context, txn types.Transaction) error {
	return c.c.WithContext(ctx).POST("/wallet/discard", txn, nil)
}

// WalletPrepareForm funds and signs a contract transaction.
func (c *Client) WalletPrepareForm(ctx context.Context, renterAddress types.Address, renterKey types.PrivateKey, renterFunds, hostCollateral types.Currency, hostKey types.PublicKey, hostSettings rhpv2.HostSettings, endHeight uint64) (txns []types.Transaction, err error) {
	req := api.WalletPrepareFormRequest{
		EndHeight:      endHeight,
		HostCollateral: hostCollateral,
		HostKey:        hostKey,
		HostSettings:   hostSettings,
		RenterAddress:  renterAddress,
		RenterFunds:    renterFunds,
		RenterKey:      renterKey,
	}
	err = c.c.WithContext(ctx).POST("/wallet/prepare/form", req, &txns)
	return
}

// WalletPrepareRenew funds and signs a contract renewal transaction.
func (c *Client) WalletPrepareRenew(ctx context.Context, revision types.FileContractRevision, hostAddress, renterAddress types.Address, renterKey types.PrivateKey, renterFunds, newCollateral types.Currency, hostKey types.PublicKey, pt rhpv3.HostPriceTable, endHeight, windowSize uint64) (api.WalletPrepareRenewResponse, error) {
	req := api.WalletPrepareRenewRequest{
		Revision:      revision,
		EndHeight:     endHeight,
		HostAddress:   hostAddress,
		HostKey:       hostKey,
		PriceTable:    pt,
		NewCollateral: newCollateral,
		RenterAddress: renterAddress,
		RenterFunds:   renterFunds,
		RenterKey:     renterKey,
		WindowSize:    windowSize,
	}
	var resp api.WalletPrepareRenewResponse
	err := c.c.WithContext(ctx).POST("/wallet/prepare/renew", req, &resp)
	return resp, err
}

// WalletPending returns the txpool transactions that are relevant to the
// wallet.
func (c *Client) WalletPending(ctx context.Context) (resp []types.Transaction, err error) {
	err = c.c.WithContext(ctx).GET("/wallet/pending", &resp)
	return
}

// Host returns information about a particular host known to the server.
func (c *Client) Host(ctx context.Context, hostKey types.PublicKey) (h hostdb.HostInfo, err error) {
	err = c.c.WithContext(ctx).GET(fmt.Sprintf("/host/%s", hostKey), &h)
	return
}

// Hosts returns 'limit' hosts at given 'offset'.
func (c *Client) Hosts(ctx context.Context, offset, limit int) (hosts []hostdb.Host, err error) {
	values := url.Values{}
	values.Set("offset", fmt.Sprint(offset))
	values.Set("limit", fmt.Sprint(limit))
	err = c.c.WithContext(ctx).GET("/hosts?"+values.Encode(), &hosts)
	return
}

// HostsForScanning returns 'limit' host addresses at given 'offset' which
// haven't been scanned after lastScan.
func (c *Client) HostsForScanning(ctx context.Context, maxLastScan time.Time, offset, limit int) (hosts []hostdb.HostAddress, err error) {
	err = c.c.WithContext(ctx).GET(fmt.Sprintf("/hosts/scanning?offset=%v&limit=%v&lastScan=%s", offset, limit, api.ParamTime(maxLastScan)), &hosts)
	return
}

// RemoveOfflineHosts removes all hosts that have been offline for longer than the given max downtime.
func (c *Client) RemoveOfflineHosts(ctx context.Context, minRecentScanFailures uint64, maxDowntime time.Duration) (removed uint64, err error) {
	err = c.c.WithContext(ctx).POST("/hosts/remove", api.HostsRemoveRequest{
		MaxDowntimeHours:      api.ParamDurationHour(maxDowntime),
		MinRecentScanFailures: minRecentScanFailures,
	}, &removed)
	return
}

// HostAllowlist returns the allowlist.
func (c *Client) HostAllowlist(ctx context.Context) (allowlist []types.PublicKey, err error) {
	err = c.c.WithContext(ctx).GET("/hosts/allowlist", &allowlist)
	return
}

// UpdateHostAllowlist updates the host allowlist, adding and removing the given entries.
func (c *Client) UpdateHostAllowlist(ctx context.Context, add, remove []types.PublicKey, clear bool) (err error) {
	err = c.c.WithContext(ctx).PUT("/hosts/allowlist", api.UpdateAllowlistRequest{Add: add, Remove: remove, Clear: clear})
	return
}

// HostBlocklist returns a host blocklist.
func (c *Client) HostBlocklist(ctx context.Context) (blocklist []string, err error) {
	err = c.c.WithContext(ctx).GET("/hosts/blocklist", &blocklist)
	return
}

// UpdateHostBlocklist updates the host blocklist, adding and removing the given entries.
func (c *Client) UpdateHostBlocklist(ctx context.Context, add, remove []string, clear bool) (err error) {
	err = c.c.WithContext(ctx).PUT("/hosts/blocklist", api.UpdateBlocklistRequest{Add: add, Remove: remove, Clear: clear})
	return
}

// RecordHostInteraction records an interaction for the supplied host.
func (c *Client) RecordInteractions(ctx context.Context, interactions []hostdb.Interaction) (err error) {
	err = c.c.WithContext(ctx).POST("/hosts/interactions", interactions, nil)
	return
}

// RecordContractSpending records contract spending metrics for contrats.
func (c *Client) RecordContractSpending(ctx context.Context, records []api.ContractSpendingRecord) (err error) {
	err = c.c.WithContext(ctx).POST("/contracts/spending", records, nil)
	return
}

// ActiveContracts returns all active contracts in the metadata store.
func (c *Client) ActiveContracts(ctx context.Context) (contracts []api.ContractMetadata, err error) {
	err = c.c.WithContext(ctx).GET("/contracts/active", &contracts)
	return
}

// ArchiveContracts archives the contracts with the given IDs and archival reason.
func (c *Client) ArchiveContracts(ctx context.Context, toArchive map[types.FileContractID]string) (err error) {
	err = c.c.WithContext(ctx).POST("/contracts/archive", toArchive, nil)
	return
}

// Contracts returns the contracts for the given set from the metadata store.
func (c *Client) Contracts(ctx context.Context, set string) (contracts []api.ContractMetadata, err error) {
	if set == "" {
		return nil, errors.New("set cannot be empty")
	}
	err = c.c.WithContext(ctx).GET(fmt.Sprintf("/contracts/set/%s", set), &contracts)
	return
}

// Contract returns the contract with the given ID.
func (c *Client) Contract(ctx context.Context, id types.FileContractID) (contract api.ContractMetadata, err error) {
	err = c.c.WithContext(ctx).GET(fmt.Sprintf("/contract/%s", id), &contract)
	return
}

// ContractSets returns the contract sets of the bus.
func (c *Client) ContractSets(ctx context.Context) (sets []string, err error) {
	err = c.c.WithContext(ctx).GET("/contracts/sets", &sets)
	return
}

// DeleteContractSet removes the contract set from the bus.
func (c *Client) DeleteContractSet(ctx context.Context, set string) (err error) {
	err = c.c.WithContext(ctx).DELETE(fmt.Sprintf("/contracts/set/%s", set))
	return
}

// AddContract adds the provided contract to the metadata store.
func (c *Client) AddContract(ctx context.Context, contract rhpv2.ContractRevision, totalCost types.Currency, startHeight uint64) (added api.ContractMetadata, err error) {
	err = c.c.WithContext(ctx).POST(fmt.Sprintf("/contract/%s", contract.ID()), api.ContractsIDAddRequest{
		Contract:    contract,
		StartHeight: startHeight,
		TotalCost:   totalCost,
	}, &added)
	return
}

// AddRenewedContract adds the provided contract to the metadata store.
func (c *Client) AddRenewedContract(ctx context.Context, contract rhpv2.ContractRevision, totalCost types.Currency, startHeight uint64, renewedFrom types.FileContractID) (renewed api.ContractMetadata, err error) {
	err = c.c.WithContext(ctx).POST(fmt.Sprintf("/contract/%s/renewed", contract.ID()), api.ContractsIDRenewedRequest{
		Contract:    contract,
		RenewedFrom: renewedFrom,
		StartHeight: startHeight,
		TotalCost:   totalCost,
	}, &renewed)
	return
}

// AncestorContracts returns any ancestors of a given active contract.
func (c *Client) AncestorContracts(ctx context.Context, fcid types.FileContractID, minStartHeight uint64) (contracts []api.ArchivedContract, err error) {
	values := url.Values{}
	values.Set("minStartHeight", fmt.Sprint(minStartHeight))
	err = c.c.WithContext(ctx).GET(fmt.Sprintf("/contract/%s/ancestors?"+values.Encode(), fcid), &contracts)
	return
}

// SetContractSet adds the given contracts to the given set.
func (c *Client) SetContractSet(ctx context.Context, set string, contracts []types.FileContractID) (err error) {
	err = c.c.WithContext(ctx).PUT(fmt.Sprintf("/contracts/set/%s", set), contracts)
	return
}

// DeleteContracts deletes the contracts with the given IDs.
func (c *Client) DeleteContracts(ctx context.Context, ids []types.FileContractID) error {
	// TODO: batch delete
	for _, id := range ids {
		if err := c.DeleteContract(ctx, id); err != nil {
			return err
		}
	}
	return nil
}

// DeleteContract deletes the contract with the given ID.
func (c *Client) DeleteContract(ctx context.Context, id types.FileContractID) (err error) {
	err = c.c.WithContext(ctx).DELETE(fmt.Sprintf("/contract/%s", id))
	return
}

// DeleteAllContracts deletes all contracts from the bus.
func (c *Client) DeleteAllContracts(ctx context.Context) (err error) {
	err = c.c.WithContext(ctx).DELETE("/contracts/all")
	return
}

// AcquireContract acquires a contract for a given amount of time unless
// released manually before that time.
func (c *Client) AcquireContract(ctx context.Context, fcid types.FileContractID, priority int, d time.Duration) (lockID uint64, err error) {
	var resp api.ContractAcquireResponse
	err = c.c.WithContext(ctx).POST(fmt.Sprintf("/contract/%s/acquire", fcid), api.ContractAcquireRequest{
		Duration: api.ParamDuration(d),
		Priority: priority,
	}, &resp)
	lockID = resp.LockID
	return
}

// ReleaseContract releases a contract that was previously acquired using AcquireContract.
func (c *Client) ReleaseContract(ctx context.Context, fcid types.FileContractID, lockID uint64) (err error) {
	err = c.c.WithContext(ctx).POST(fmt.Sprintf("/contract/%s/release", fcid), api.ContractReleaseRequest{
		LockID: lockID,
	}, nil)
	return
}

// RecommendedFee returns the recommended fee for a txn.
func (c *Client) RecommendedFee(ctx context.Context) (fee types.Currency, err error) {
	err = c.c.WithContext(ctx).GET("/txpool/recommendedfee", &fee)
	return
}

// Setting returns the value for the setting with given key.
func (c *Client) Setting(ctx context.Context, key string, value interface{}) (err error) {
	err = c.c.WithContext(ctx).GET(fmt.Sprintf("/setting/%s", key), &value)
	return
}

// Settings returns the keys of all settings in the store.
func (c *Client) Settings(ctx context.Context) (settings []string, err error) {
	err = c.c.WithContext(ctx).GET("/settings", &settings)
	return
}

// UpdateSetting will update the given setting under the given key.
func (c *Client) UpdateSetting(ctx context.Context, key string, value interface{}) error {
	return c.c.WithContext(ctx).PUT(fmt.Sprintf("/setting/%s", key), value)
}

// DeleteSetting will delete the setting with given key.
func (c *Client) DeleteSetting(ctx context.Context, key string) error {
	return c.c.WithContext(ctx).DELETE(fmt.Sprintf("/setting/%s", key))
}

// ContractSetSettings returns the contract set settings.
func (c *Client) ContractSetSettings(ctx context.Context) (css api.ContractSetSettings, err error) {
	err = c.Setting(ctx, api.SettingContractSet, &css)
	return
}

// GougingSettings returns the gouging settings.
func (c *Client) GougingSettings(ctx context.Context) (gs api.GougingSettings, err error) {
	err = c.Setting(ctx, api.SettingGouging, &gs)
	return
}

// RedundancySettings returns the redundancy settings.
func (c *Client) RedundancySettings(ctx context.Context) (rs api.RedundancySettings, err error) {
	err = c.Setting(ctx, api.SettingRedundancy, &rs)
	return
}

// SearchHosts returns all hosts that match certain search criteria.
func (c *Client) SearchHosts(ctx context.Context, filterMode string, addressContains string, keyIn []types.PublicKey, offset, limit int) (hosts []hostdb.Host, err error) {
	err = c.c.WithContext(ctx).POST("/search/hosts", api.SearchHostsRequest{
		Offset:          offset,
		Limit:           limit,
		FilterMode:      filterMode,
		AddressContains: addressContains,
		KeyIn:           keyIn,
	}, &hosts)
	return
}

// SearchObjects returns all objects that contains a sub-string in their key.
func (c *Client) SearchObjects(ctx context.Context, key string, offset, limit int) (entries []api.ObjectMetadata, err error) {
	values := url.Values{}
	values.Set("offset", fmt.Sprint(offset))
	values.Set("limit", fmt.Sprint(limit))
	values.Set("key", key)
	err = c.c.WithContext(ctx).GET("/search/objects?"+values.Encode(), &entries)
	return
}

// Object returns the object at the given path with the given prefix, or, if
// path ends in '/', the entries under that path.
func (c *Client) Object(ctx context.Context, path, prefix string, offset, limit int) (o object.Object, entries []api.ObjectMetadata, err error) {
	values := url.Values{}
	values.Set("prefix", prefix)
	values.Set("offset", fmt.Sprint(offset))
	values.Set("limit", fmt.Sprint(limit))
	path = strings.TrimLeft(path, "/")
	var or api.ObjectsResponse
	err = c.c.WithContext(ctx).GET(fmt.Sprintf("/objects/%s?"+values.Encode(), path), &or)
	if or.Object != nil {
		o = *or.Object
	} else {
		entries = or.Entries
	}
	return
}

// AddObject stores the provided object under the given path.
func (c *Client) AddObject(ctx context.Context, path string, o object.Object, usedContract map[types.PublicKey]types.FileContractID) (err error) {
	err = c.c.WithContext(ctx).PUT(fmt.Sprintf("/objects/%s", path), api.AddObjectRequest{
		Object:        o,
		UsedContracts: usedContract,
	})
	return
}

// DeleteObject deletes the object at the given path.
func (c *Client) DeleteObject(ctx context.Context, path string) (err error) {
	err = c.c.WithContext(ctx).DELETE(fmt.Sprintf("/objects/%s", path))
	return
}

// SlabsForMigration returns up to 'limit' slabs which require migration. A slab
// needs to be migrated if it has sectors on contracts that are not part of the
// given 'set'.
func (c *Client) SlabsForMigration(ctx context.Context, healthCutoff float64, set string, limit int) (slabs []object.Slab, err error) {
	err = c.c.WithContext(ctx).POST("/slabs/migration", api.MigrationSlabsRequest{ContractSet: set, HealthCutoff: healthCutoff, Limit: limit}, &slabs)
	return
}

// UpdateSlab updates the given slab in the database.
func (c *Client) UpdateSlab(ctx context.Context, slab object.Slab, usedContracts map[types.PublicKey]types.FileContractID) (err error) {
	err = c.c.WithContext(ctx).PUT("/slab", api.UpdateSlabRequest{
		Slab:          slab,
		UsedContracts: usedContracts,
	})
	return
}

// UploadParams returns parameters used for uploading slabs.
func (c *Client) UploadParams(ctx context.Context) (up api.UploadParams, err error) {
	err = c.c.WithContext(ctx).GET("/params/upload", &up)
	return
}

// GougingParams returns parameters used for performing gouging checks.
func (c *Client) GougingParams(ctx context.Context) (gp api.GougingParams, err error) {
	err = c.c.WithContext(ctx).GET("/params/gouging", &gp)
	return
}

// Account requests the bus's /accounts/:host endpoint.
func (c *Client) Account(ctx context.Context, id rhpv3.Account, host types.PublicKey) (account api.Account, err error) {
	err = c.c.WithContext(ctx).POST(fmt.Sprintf("/accounts/%s", id), api.AccountHandlerPOST{
		HostKey: host,
	}, &account)
	return
}

// Accounts returns the ephemeral accounts known to the bus.
func (c *Client) Accounts(ctx context.Context) (accounts []api.Account, err error) {
	err = c.c.WithContext(ctx).GET("/accounts", &accounts)
	return
}

// AddBalance adds the given amount to an account's balance.
func (c *Client) AddBalance(ctx context.Context, id rhpv3.Account, hk types.PublicKey, amount *big.Int) (err error) {
	err = c.c.WithContext(ctx).POST(fmt.Sprintf("/accounts/%s/add", id), api.AccountsAddBalanceRequest{
		HostKey: hk,
		Amount:  amount,
	}, nil)
	return
}

func (c *Client) LockAccount(ctx context.Context, id rhpv3.Account, hostKey types.PublicKey, exclusive bool, duration time.Duration) (account api.Account, lockID uint64, err error) {
	var resp api.AccountsLockHandlerResponse
	err = c.c.WithContext(ctx).POST(fmt.Sprintf("/accounts/%s/lock", id), api.AccountsLockHandlerRequest{
		HostKey:   hostKey,
		Exclusive: exclusive,
		Duration:  api.ParamDuration(duration),
	}, &resp)
	return resp.Account, resp.LockID, err
}

func (c *Client) UnlockAccount(ctx context.Context, id rhpv3.Account, lockID uint64) (err error) {
	err = c.c.WithContext(ctx).POST(fmt.Sprintf("/accounts/%s/unlock", id), api.AccountsUnlockHandlerRequest{
		LockID: lockID,
	}, nil)
	return
}

// SetBalance sets the given account's balance to a certain amount.
func (c *Client) SetBalance(ctx context.Context, id rhpv3.Account, hk types.PublicKey, amount *big.Int) (err error) {
	err = c.c.WithContext(ctx).POST(fmt.Sprintf("/accounts/%s/update", id), api.AccountsUpdateBalanceRequest{
		HostKey: hk,
		Amount:  amount,
	}, nil)
	return
}

// ScheduleSync sets the requiresSync flag of an account.
func (c *Client) ScheduleSync(ctx context.Context, id rhpv3.Account, hk types.PublicKey) (err error) {
	err = c.c.WithContext(ctx).POST(fmt.Sprintf("/accounts/%s/requiressync", id), api.AccountsRequiresSyncRequest{
		HostKey: hk,
	}, nil)
	return
}

// ResetDrift resets the drift of an account to zero.
func (c *Client) ResetDrift(ctx context.Context, id rhpv3.Account) (err error) {
	err = c.c.WithContext(ctx).POST(fmt.Sprintf("/accounts/%s/resetdrift", id), nil, nil)
	return
}

// FileContractTax asks the bus for the siafund fee that has to be paid for a
// contract with a given payout.
func (c *Client) FileContractTax(ctx context.Context, payout types.Currency) (tax types.Currency, err error) {
	err = c.c.WithContext(ctx).GET(fmt.Sprintf("/consensus/siafundfee/%s", api.ParamCurrency(payout)), &tax)
	return
}

// ObjectsStats returns information about the number of objects and their size.
func (c *Client) ObjectsStats() (osr api.ObjectsStats, err error) {
	err = c.c.GET("/stats/objects", &osr)
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
