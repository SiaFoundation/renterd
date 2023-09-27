package bus

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strings"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/jape"
	"go.sia.tech/renterd/alerts"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/object"
	"go.sia.tech/renterd/wallet"
	"go.sia.tech/renterd/webhooks"
)

// A Client provides methods for interacting with a renterd API server.
type Client struct {
	c jape.Client
}

// Alerts fetches the active alerts from the bus.
func (c *Client) Alerts() (alerts []alerts.Alert, err error) {
	err = c.c.GET("/alerts", &alerts)
	return
}

// DismissAlerts dimisses the alerts with the given IDs.
func (c *Client) DismissAlerts(ctx context.Context, ids ...types.Hash256) error {
	return c.c.WithContext(ctx).POST("/alerts/dismiss", ids, nil)
}

// RegisterAlert dimisses the alerts with the given IDs.
func (c *Client) RegisterAlert(ctx context.Context, alert alerts.Alert) error {
	return c.c.WithContext(ctx).POST("/alerts/register", alert, nil)
}

// CreateBucket creates a new bucket.
func (c *Client) CreateBucket(ctx context.Context, name string, policy api.BucketPolicy) error {
	return c.c.WithContext(ctx).POST("/buckets", api.BucketCreateRequest{
		Name:   name,
		Policy: policy,
	}, nil)
}

// UpdateBucketPolicy updates the policy of an existing bucket.
func (c *Client) UpdateBucketPolicy(ctx context.Context, bucket string, policy api.BucketPolicy) error {
	return c.c.WithContext(ctx).PUT(fmt.Sprintf("/buckets/%s/policy", bucket), api.BucketUpdatePolicyRequest{
		Policy: policy,
	})
}

// DeleteBucket deletes an existing bucket. Fails if the bucket isn't empty.
func (c *Client) DeleteBucket(ctx context.Context, name string) error {
	return c.c.WithContext(ctx).DELETE(fmt.Sprintf("/buckets/%s", name))
}

// Bucket returns information about a specific bucket.
func (c *Client) Bucket(ctx context.Context, name string) (resp api.Bucket, err error) {
	err = c.c.WithContext(ctx).GET(fmt.Sprintf("/buckets/%s", name), &resp)
	return
}

// ListBuckets lists all available buckets.
func (c *Client) ListBuckets(ctx context.Context) (buckets []api.Bucket, err error) {
	err = c.c.WithContext(ctx).GET("/buckets", &buckets)
	return
}

// RegisterWebhook registers a new webhook for the given URL.
func (c *Client) RegisterWebhook(ctx context.Context, url, module, event string) error {
	err := c.c.WithContext(ctx).POST("/webhooks", webhooks.Webhook{
		Event:  event,
		Module: module,
		URL:    url,
	}, nil)
	return err
}

// BroadcastAction broadcasts an action that triggers a webhook.
func (c *Client) BroadcastAction(ctx context.Context, action webhooks.Event) error {
	err := c.c.WithContext(ctx).POST("/webhooks/action", action, nil)
	return err
}

// DeleteWebhook deletes the webhook with the given ID.
func (c *Client) DeleteWebhook(ctx context.Context, url, module, event string) error {
	return c.c.POST("/webhook/delete", webhooks.Webhook{
		URL:    url,
		Module: module,
		Event:  event,
	}, nil)
}

// Webhooks returns all webhooks currently registered.
func (c *Client) Webhooks(ctx context.Context) (resp api.WebHookResponse, err error) {
	err = c.c.WithContext(ctx).GET("/webhooks", &resp)
	return
}

// Autopilots returns all autopilots in the autopilots store.
func (c *Client) Autopilots(ctx context.Context) (autopilots []api.Autopilot, err error) {
	err = c.c.WithContext(ctx).GET("/autopilots", &autopilots)
	return
}

// Autopilot returns the autopilot with the given ID.
func (c *Client) Autopilot(ctx context.Context, id string) (autopilot api.Autopilot, err error) {
	err = c.c.WithContext(ctx).GET(fmt.Sprintf("/autopilots/%s", id), &autopilot)
	return
}

// UpdateAutopilot updates the given autopilot in the store.
func (c *Client) UpdateAutopilot(ctx context.Context, autopilot api.Autopilot) (err error) {
	err = c.c.WithContext(ctx).PUT(fmt.Sprintf("/autopilots/%s", autopilot.ID), autopilot)
	return
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

// Wallet calls the /wallet endpoint on the bus.
func (c *Client) Wallet(ctx context.Context) (resp api.WalletResponse, err error) {
	err = c.c.WithContext(ctx).GET("/wallet", &resp)
	return
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

// SendSiacoins is a helper method that sends siacoins to the given outputs.
func (c *Client) SendSiacoins(ctx context.Context, scos []types.SiacoinOutput) (err error) {
	var value types.Currency
	for _, sco := range scos {
		value = value.Add(sco.Value)
	}
	txn := types.Transaction{
		SiacoinOutputs: scos,
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
func (c *Client) WalletTransactions(ctx context.Context, opts ...api.WalletTransactionsOption) (resp []wallet.Transaction, err error) {
	c.c.Custom("GET", "/wallet/transactions", nil, &resp)

	values := url.Values{}
	for _, opt := range opts {
		opt(values)
	}
	u, err := url.Parse(fmt.Sprintf("%v/wallet/transactions", c.c.BaseURL))
	if err != nil {
		panic(err)
	}
	u.RawQuery = values.Encode()
	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		panic(err)
	}
	err = c.do(req, &resp)
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
func (c *Client) WalletPrepareForm(ctx context.Context, renterAddress types.Address, renterKey types.PublicKey, renterFunds, hostCollateral types.Currency, hostKey types.PublicKey, hostSettings rhpv2.HostSettings, endHeight uint64) (txns []types.Transaction, err error) {
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
	err = c.c.WithContext(ctx).GET(fmt.Sprintf("/hosts/scanning?offset=%v&limit=%v&lastScan=%s", offset, limit, api.TimeRFC3339(maxLastScan)), &hosts)
	return
}

// RemoveOfflineHosts removes all hosts that have been offline for longer than the given max downtime.
func (c *Client) RemoveOfflineHosts(ctx context.Context, minRecentScanFailures uint64, maxDowntime time.Duration) (removed uint64, err error) {
	err = c.c.WithContext(ctx).POST("/hosts/remove", api.HostsRemoveRequest{
		MaxDowntimeHours:      api.DurationH(maxDowntime),
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
func (c *Client) RecordHostScans(ctx context.Context, scans []hostdb.HostScan) (err error) {
	err = c.c.WithContext(ctx).POST("/hosts/scans", api.HostsScanRequest{
		Scans: scans,
	}, nil)
	return
}

// RecordHostInteraction records an interaction for the supplied host.
func (c *Client) RecordPriceTables(ctx context.Context, priceTableUpdates []hostdb.PriceTableUpdate) (err error) {
	err = c.c.WithContext(ctx).POST("/hosts/pricetables", api.HostsPriceTablesRequest{
		PriceTableUpdates: priceTableUpdates,
	}, nil)
	return
}

// RecordContractSpending records contract spending metrics for contracts.
func (c *Client) RecordContractSpending(ctx context.Context, records []api.ContractSpendingRecord) (err error) {
	err = c.c.WithContext(ctx).POST("/contracts/spending", records, nil)
	return
}

// Contracts returns all contracts in the metadata store.
func (c *Client) Contracts(ctx context.Context) (contracts []api.ContractMetadata, err error) {
	err = c.c.WithContext(ctx).GET("/contracts", &contracts)
	return
}

// ArchiveContracts archives the contracts with the given IDs and archival reason.
func (c *Client) ArchiveContracts(ctx context.Context, toArchive map[types.FileContractID]string) (err error) {
	err = c.c.WithContext(ctx).POST("/contracts/archive", toArchive, nil)
	return
}

// ContractSetContracts returns the contracts for the given set from the
// metadata store.
func (c *Client) ContractSetContracts(ctx context.Context, set string) (contracts []api.ContractMetadata, err error) {
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

// ContractRoots returns the sector roots, as well as the ones that are still
// uploading, for the contract with given id.
func (c *Client) ContractRoots(ctx context.Context, fcid types.FileContractID) (roots, uploading []types.Hash256, err error) {
	var resp api.ContractRootsResponse
	if err = c.c.WithContext(ctx).GET(fmt.Sprintf("/contract/%s/roots", fcid), &resp); err != nil {
		return
	}
	return resp.Roots, resp.Uploading, nil
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

// AncestorContracts returns any ancestors of a given contract.
func (c *Client) AncestorContracts(ctx context.Context, fcid types.FileContractID, minStartHeight uint64) (contracts []api.ArchivedContract, err error) {
	values := url.Values{}
	values.Set("minStartHeight", fmt.Sprint(minStartHeight))
	err = c.c.WithContext(ctx).GET(fmt.Sprintf("/contract/%s/ancestors?"+values.Encode(), fcid), &contracts)
	return
}

// RenewedContract returns the renewed contract for the given ID.
func (c *Client) RenewedContract(ctx context.Context, renewedFrom types.FileContractID) (contract api.ContractMetadata, err error) {
	err = c.c.WithContext(ctx).GET(fmt.Sprintf("/contracts/renewed/%s", renewedFrom), &contract)
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
		Duration: api.DurationMS(d),
		Priority: priority,
	}, &resp)
	lockID = resp.LockID
	return
}

// KeepaliveContract extends the duration on an already acquired lock on a
// contract.
func (c *Client) KeepaliveContract(ctx context.Context, fcid types.FileContractID, lockID uint64, d time.Duration) (err error) {
	err = c.c.WithContext(ctx).POST(fmt.Sprintf("/contract/%s/keepalive", fcid), api.ContractKeepaliveRequest{
		Duration: api.DurationMS(d),
		LockID:   lockID,
	}, nil)
	return
}

// ReleaseContract releases a contract that was previously acquired using AcquireContract.
func (c *Client) ReleaseContract(ctx context.Context, fcid types.FileContractID, lockID uint64) (err error) {
	err = c.c.WithContext(ctx).POST(fmt.Sprintf("/contract/%s/release", fcid), api.ContractReleaseRequest{
		LockID: lockID,
	}, nil)
	return
}

// PrunableData returns an overview of all contract sizes, the total size and
// the amount of data that can be pruned.
func (c *Client) PrunableData(ctx context.Context) (prunableData api.ContractsPrunableDataResponse, err error) {
	err = c.c.WithContext(ctx).GET("/contracts/prunable", &prunableData)
	return
}

// ContractSize returns the contract's size.
func (c *Client) ContractSize(ctx context.Context, fcid types.FileContractID) (size api.ContractSize, err error) {
	err = c.c.WithContext(ctx).GET(fmt.Sprintf("/contract/%s/size", fcid), &size)
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
func (c *Client) ContractSetSettings(ctx context.Context) (gs api.ContractSetSetting, err error) {
	err = c.Setting(ctx, api.SettingContractSet, &gs)
	return
}

// GougingSettings returns the gouging settings.
func (c *Client) GougingSettings(ctx context.Context) (gs api.GougingSettings, err error) {
	err = c.Setting(ctx, api.SettingGouging, &gs)
	return
}

// UploadPackingSettings returns the upload packing settings.
func (c *Client) UploadPackingSettings(ctx context.Context) (ups api.UploadPackingSettings, err error) {
	err = c.Setting(ctx, api.SettingUploadPacking, &ups)
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

// ObjectsBySlabKey returns all objects that reference a given slab.
func (c *Client) ObjectsBySlabKey(ctx context.Context, bucket string, key object.EncryptionKey) (objects []api.ObjectMetadata, err error) {
	values := url.Values{}
	values.Set("bucket", bucket)
	err = c.c.WithContext(ctx).GET(fmt.Sprintf("/slab/%v/objects?"+values.Encode(), key), &objects)
	return
}

// ListOBjects lists objects in the given bucket.
func (c *Client) ListObjects(ctx context.Context, bucket, prefix, marker string, limit int) (resp api.ObjectsListResponse, err error) {
	err = c.c.WithContext(ctx).POST("/objects/list", api.ObjectsListRequest{
		Bucket: bucket,
		Limit:  limit,
		Prefix: prefix,
		Marker: marker,
	}, &resp)
	return
}

// SearchObjects returns all objects that contains a sub-string in their key.
func (c *Client) SearchObjects(ctx context.Context, bucket, key string, offset, limit int) (entries []api.ObjectMetadata, err error) {
	values := url.Values{}
	values.Set("offset", fmt.Sprint(offset))
	values.Set("limit", fmt.Sprint(limit))
	values.Set("key", key)
	values.Set("bucket", bucket)
	err = c.c.WithContext(ctx).GET("/search/objects?"+values.Encode(), &entries)
	return
}

func (c *Client) Object(ctx context.Context, path string, opts ...api.ObjectsOption) (res api.ObjectsResponse, err error) {
	path = strings.TrimPrefix(path, "/")
	values := url.Values{}
	for _, opt := range opts {
		opt(values)
	}
	path += "?" + values.Encode()

	err = c.c.WithContext(ctx).GET(fmt.Sprintf("/objects/%s", path), &res)
	return
}

// AddObject stores the provided object under the given path.
func (c *Client) AddObject(ctx context.Context, bucket, path, contractSet string, o object.Object, usedContract map[types.PublicKey]types.FileContractID) (err error) {
	path = strings.TrimPrefix(path, "/")
	err = c.c.WithContext(ctx).PUT(fmt.Sprintf("/objects/%s", path), api.ObjectAddRequest{
		Bucket:        bucket,
		ContractSet:   contractSet,
		Object:        o,
		UsedContracts: usedContract,
	})
	return
}

// CopyObject copies the object from the source bucket and path to the
// destination bucket and path.
func (c *Client) CopyObject(ctx context.Context, srcBucket, dstBucket, srcPath, dstPath string) (om api.ObjectMetadata, err error) {
	err = c.c.WithContext(ctx).POST("/objects/copy", api.ObjectsCopyRequest{
		SourceBucket:      srcBucket,
		DestinationBucket: dstBucket,
		SourcePath:        srcPath,
		DestinationPath:   dstPath,
	}, &om)
	return
}

// DeleteObject either deletes the object at the given path or if batch=true
// deletes all objects that start with the given path.
func (c *Client) DeleteObject(ctx context.Context, bucket, path string, batch bool) (err error) {
	path = strings.TrimPrefix(path, "/")
	values := url.Values{}
	values.Set("batch", fmt.Sprint(batch))
	values.Set("bucket", bucket)
	err = c.c.WithContext(ctx).DELETE(fmt.Sprintf("/objects/%s?"+values.Encode(), path))
	return
}

// RecomputeHealth recomputes the cached health of all slabs.
func (c *Client) RefreshHealth(ctx context.Context) error {
	return c.c.WithContext(ctx).POST("/slabs/refreshhealth", nil, nil)
}

// SlabsForMigration returns up to 'limit' slabs which require migration. A slab
// needs to be migrated if it has sectors on contracts that are not part of the
// given 'set'.
func (c *Client) SlabsForMigration(ctx context.Context, healthCutoff float64, set string, limit int) (slabs []api.UnhealthySlab, err error) {
	var usr api.UnhealthySlabsResponse
	err = c.c.WithContext(ctx).POST("/slabs/migration", api.MigrationSlabsRequest{ContractSet: set, HealthCutoff: healthCutoff, Limit: limit}, &usr)
	if err != nil {
		return
	}
	return usr.Slabs, nil
}

// UpdateSlab updates the given slab in the database.
func (c *Client) UpdateSlab(ctx context.Context, slab object.Slab, contractSet string, usedContracts map[types.PublicKey]types.FileContractID) (err error) {
	err = c.c.WithContext(ctx).PUT("/slab", api.UpdateSlabRequest{
		ContractSet:   contractSet,
		Slab:          slab,
		UsedContracts: usedContracts,
	})
	return
}

// Slab returns the slab with the given key from the bus.
func (c *Client) Slab(ctx context.Context, key object.EncryptionKey) (slab object.Slab, err error) {
	err = c.c.WithContext(ctx).GET(fmt.Sprintf("/slab/%s", key), &slab)
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
		Duration:  api.DurationMS(duration),
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

func (c *Client) PackedSlabsForUpload(ctx context.Context, lockingDuration time.Duration, minShards, totalShards uint8, set string, limit int) (slabs []api.PackedSlab, err error) {
	err = c.c.WithContext(ctx).POST("/slabbuffer/fetch", api.PackedSlabsRequestGET{
		LockingDuration: api.DurationMS(lockingDuration),
		MinShards:       minShards,
		TotalShards:     totalShards,
		ContractSet:     set,
		Limit:           limit,
	}, &slabs)
	return
}

func (c *Client) MarkPackedSlabsUploaded(ctx context.Context, slabs []api.UploadedPackedSlab, usedContracts map[types.PublicKey]types.FileContractID) (err error) {
	err = c.c.WithContext(ctx).POST("/slabbuffer/done", api.PackedSlabsRequestPOST{
		Slabs:         slabs,
		UsedContracts: usedContracts,
	}, nil)
	return
}

// TrackUpload tracks the upload with given id in the bus.
func (c *Client) TrackUpload(ctx context.Context, uID api.UploadID) (err error) {
	err = c.c.WithContext(ctx).POST(fmt.Sprintf("/upload/%s", uID), nil, nil)
	return
}

// AddUploadingSector adds the given sector to the upload with given id.
func (c *Client) AddUploadingSector(ctx context.Context, uID api.UploadID, id types.FileContractID, root types.Hash256) (err error) {
	err = c.c.WithContext(ctx).POST(fmt.Sprintf("/upload/%s/sector", uID), api.UploadSectorRequest{
		ContractID: id,
		Root:       root,
	}, nil)
	return
}

func (c *Client) AddPartialSlab(ctx context.Context, data []byte, minShards, totalShards uint8, contractSet string) (slabs []object.PartialSlab, slabBufferMaxSizeSoftReached bool, err error) {
	c.c.Custom("POST", "/slabs/partial", nil, &api.AddPartialSlabResponse{})
	values := url.Values{}
	values.Set("minShards", fmt.Sprint(minShards))
	values.Set("totalShards", fmt.Sprint(totalShards))
	values.Set("contractSet", contractSet)

	u, err := url.Parse(fmt.Sprintf("%v/slabs/partial", c.c.BaseURL))
	if err != nil {
		panic(err)
	}
	u.RawQuery = values.Encode()
	req, err := http.NewRequestWithContext(ctx, "POST", u.String(), bytes.NewReader(data))
	if err != nil {
		panic(err)
	}
	req.SetBasicAuth("", c.c.WithContext(ctx).Password)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, false, err
	}
	defer io.Copy(io.Discard, resp.Body)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		err, _ := io.ReadAll(resp.Body)
		return nil, false, errors.New(string(err))
	}
	var apsr api.AddPartialSlabResponse
	err = json.NewDecoder(resp.Body).Decode(&apsr)
	if err != nil {
		return nil, false, err
	}
	return apsr.Slabs, apsr.SlabBufferMaxSizeSoftReached, nil
}

func (c *Client) FetchPartialSlab(ctx context.Context, key object.EncryptionKey, offset, length uint32) ([]byte, error) {
	c.c.Custom("GET", fmt.Sprintf("/slabs/partial/%s", key), nil, &[]byte{})
	values := url.Values{}
	values.Set("offset", fmt.Sprint(offset))
	values.Set("length", fmt.Sprint(length))

	u, err := url.Parse(fmt.Sprintf("%s/slabs/partial/%s", c.c.BaseURL, key))
	if err != nil {
		panic(err)
	}
	u.RawQuery = values.Encode()
	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		panic(err)
	}
	req.SetBasicAuth("", c.c.WithContext(ctx).Password)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer io.Copy(io.Discard, resp.Body)
	defer resp.Body.Close()
	if resp.StatusCode != 200 && resp.StatusCode != 206 {
		err, _ := io.ReadAll(resp.Body)
		return nil, errors.New(string(err))
	}
	return io.ReadAll(resp.Body)
}

// FinishUpload marks the given upload as finished.
func (c *Client) FinishUpload(ctx context.Context, uID api.UploadID) (err error) {
	err = c.c.WithContext(ctx).DELETE(fmt.Sprintf("/upload/%s", uID))
	return
}

// ObjectsStats returns information about the number of objects and their size.
func (c *Client) ObjectsStats() (osr api.ObjectsStatsResponse, err error) {
	err = c.c.GET("/stats/objects", &osr)
	return
}

// SlabBuffers returns information about the number of objects and their size.
func (c *Client) SlabBuffers() (buffers []api.SlabBuffer, err error) {
	err = c.c.GET("/slabbuffers", &buffers)
	return
}

// State returns the current state of the bus.
func (c *Client) State() (state api.BusStateResponse, err error) {
	err = c.c.GET("/state", &state)
	return
}

// RenameObject renames a single object.
func (c *Client) RenameObject(ctx context.Context, bucket, from, to string) (err error) {
	return c.renameObjects(ctx, bucket, from, to, api.ObjectsRenameModeSingle)
}

// RenameObjects renames all objects with the prefix 'from' to the prefix 'to'.
func (c *Client) RenameObjects(ctx context.Context, bucket, from, to string) (err error) {
	return c.renameObjects(ctx, bucket, from, to, api.ObjectsRenameModeMulti)
}

func (c *Client) renameObjects(ctx context.Context, bucket, from, to, mode string) (err error) {
	err = c.c.POST("/objects/rename", api.ObjectsRenameRequest{
		Bucket: bucket,
		From:   from,
		To:     to,
		Mode:   mode,
	}, nil)
	return
}

func (c *Client) CreateMultipartUpload(ctx context.Context, bucket, path string, ec object.EncryptionKey) (resp api.MultipartCreateResponse, err error) {
	err = c.c.WithContext(ctx).POST("/multipart/create", api.MultipartCreateRequest{
		Bucket: bucket,
		Key:    ec,
		Path:   path,
	}, &resp)
	return
}

func (c *Client) AddMultipartPart(ctx context.Context, bucket, path, contractSet, uploadID string, partNumber int, slices []object.SlabSlice, partialSlab []object.PartialSlab, etag string, usedContracts map[types.PublicKey]types.FileContractID) (err error) {
	err = c.c.WithContext(ctx).PUT("/multipart/part", api.MultipartAddPartRequest{
		Bucket:        bucket,
		Etag:          etag,
		Path:          path,
		ContractSet:   contractSet,
		UploadID:      uploadID,
		PartNumber:    partNumber,
		Slices:        slices,
		PartialSlabs:  partialSlab,
		UsedContracts: usedContracts,
	})
	return
}

func (c *Client) AbortMultipartUpload(ctx context.Context, bucket, path string, uploadID string) (err error) {
	err = c.c.WithContext(ctx).POST("/multipart/abort", api.MultipartAbortRequest{
		Bucket:   bucket,
		Path:     path,
		UploadID: uploadID,
	}, nil)
	return
}

func (c *Client) CompleteMultipartUpload(ctx context.Context, bucket, path string, uploadID string, parts []api.MultipartCompletedPart) (resp api.MultipartCompleteResponse, err error) {
	err = c.c.WithContext(ctx).POST("/multipart/complete", api.MultipartCompleteRequest{
		Bucket:   bucket,
		Path:     path,
		UploadID: uploadID,
		Parts:    parts,
	}, &resp)
	return
}

func (c *Client) MultipartUpload(ctx context.Context, uploadID string) (resp api.MultipartUpload, err error) {
	err = c.c.WithContext(ctx).GET(fmt.Sprintf("/multipart/upload/%s", uploadID), &resp)
	return
}

func (c *Client) MultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker string, maxUploads int) (resp api.MultipartListUploadsResponse, err error) {
	err = c.c.WithContext(ctx).POST("/multipart/listuploads", api.MultipartListUploadsRequest{
		Bucket:         bucket,
		Prefix:         prefix,
		PathMarker:     keyMarker,
		UploadIDMarker: uploadIDMarker,
		Limit:          maxUploads,
	}, &resp)
	return
}

func (c *Client) MultipartUploadParts(ctx context.Context, bucket, object string, uploadID string, marker int, limit int64) (resp api.MultipartListPartsResponse, err error) {
	err = c.c.WithContext(ctx).POST("/multipart/listparts", api.MultipartListPartsRequest{
		Bucket:           bucket,
		Path:             object,
		UploadID:         uploadID,
		PartNumberMarker: marker,
		Limit:            limit,
	}, &resp)
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

func (c *Client) do(req *http.Request, resp interface{}) error {
	req.Header.Set("Content-Type", "application/json")
	if c.c.Password != "" {
		req.SetBasicAuth("", c.c.Password)
	}
	r, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer io.Copy(io.Discard, r.Body)
	defer r.Body.Close()
	if !(200 <= r.StatusCode && r.StatusCode < 300) {
		err, _ := io.ReadAll(r.Body)
		return errors.New(string(err))
	}
	if resp == nil {
		return nil
	}
	return json.NewDecoder(r.Body).Decode(resp)
}
