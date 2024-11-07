package bus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"runtime"
	"sort"
	"strings"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"

	"go.sia.tech/renterd/internal/prometheus"
	rhp3 "go.sia.tech/renterd/internal/rhp/v3"
	"go.sia.tech/renterd/stores"
	"go.sia.tech/renterd/stores/sql"

	"go.sia.tech/renterd/internal/gouging"
	rhp2 "go.sia.tech/renterd/internal/rhp/v2"

	"go.sia.tech/core/gateway"
	"go.sia.tech/core/types"
	"go.sia.tech/gofakes3"
	"go.sia.tech/jape"
	"go.sia.tech/renterd/alerts"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/build"
	"go.sia.tech/renterd/internal/utils"
	"go.sia.tech/renterd/object"
	"go.sia.tech/renterd/webhooks"
	"go.uber.org/zap"
)

var ErrSettingFieldNotFound = errors.New("setting field not found")

func (b *Bus) accountsFundHandler(jc jape.Context) {
	var req api.AccountsFundRequest
	if jc.Decode(&req) != nil {
		return
	}

	// contract metadata
	cm, err := b.store.Contract(jc.Request.Context(), req.ContractID)
	if jc.Check("failed to fetch contract metadata", err) != nil {
		return
	}

	rk := b.masterKey.DeriveContractKey(cm.HostKey)

	// acquire contract
	lockID, err := b.contractLocker.Acquire(jc.Request.Context(), lockingPriorityFunding, req.ContractID, math.MaxInt64)
	if jc.Check("failed to acquire lock", err) != nil {
		return
	}
	defer b.contractLocker.Release(req.ContractID, lockID)

	// latest revision
	rev, err := b.rhp3Client.Revision(jc.Request.Context(), req.ContractID, cm.HostKey, cm.SiamuxAddr)
	if jc.Check("failed to fetch contract revision", err) != nil {
		return
	}

	// ensure we have at least 2H in the contract to cover the costs
	if types.NewCurrency64(2).Cmp(rev.ValidRenterPayout()) >= 0 {
		jc.Error(fmt.Errorf("insufficient funds to fund account: %v <= %v", rev.ValidRenterPayout(), types.NewCurrency64(2)), http.StatusBadRequest)
		return
	}

	// price table
	pt, err := b.rhp3Client.PriceTable(jc.Request.Context(), cm.HostKey, cm.SiamuxAddr, rhp3.PreparePriceTableContractPayment(&rev, req.AccountID, rk))
	if jc.Check("failed to fetch price table", err) != nil {
		return
	}

	// check only the FundAccountCost
	if types.NewCurrency64(1).Cmp(pt.FundAccountCost) < 0 {
		jc.Error(fmt.Errorf("%w: host is gouging on FundAccountCost", gouging.ErrPriceTableGouging), http.StatusServiceUnavailable)
		return
	}

	// cap the deposit by what's left in the contract
	deposit := req.Amount
	cost := pt.FundAccountCost
	availableFunds := rev.ValidRenterPayout().Sub(cost)
	if deposit.Cmp(availableFunds) > 0 {
		deposit = availableFunds
	}

	// fund the account
	err = b.rhp3Client.FundAccount(jc.Request.Context(), &rev, cm.HostKey, cm.SiamuxAddr, deposit, req.AccountID, pt.HostPriceTable, rk)
	if jc.Check("failed to fund account", err) != nil {
		return
	}

	// record spending
	err = b.store.RecordContractSpending(jc.Request.Context(), []api.ContractSpendingRecord{
		{
			ContractSpending: api.ContractSpending{
				FundAccount: deposit.Add(cost),
			},
			ContractID:     rev.ParentID,
			RevisionNumber: rev.RevisionNumber,
			Size:           rev.Filesize,

			MissedHostPayout:  rev.MissedHostPayout(),
			ValidRenterPayout: rev.ValidRenterPayout(),
		},
	})
	if err != nil {
		b.logger.Error("failed to record contract spending", zap.Error(err))
	}
	jc.Encode(api.AccountsFundResponse{
		Deposit: deposit,
	})
}

func (b *Bus) consensusAcceptBlock(jc jape.Context) {
	var block types.Block
	if jc.Decode(&block) != nil {
		return
	}

	if jc.Check("failed to accept block", b.cm.AddBlocks([]types.Block{block})) != nil {
		return
	}

	if block.V2 == nil {
		b.s.BroadcastHeader(gateway.BlockHeader{
			ParentID:   block.ParentID,
			Nonce:      block.Nonce,
			Timestamp:  block.Timestamp,
			MerkleRoot: block.MerkleRoot(),
		})
	} else {
		b.s.BroadcastV2BlockOutline(gateway.OutlineBlock(block, b.cm.PoolTransactions(), b.cm.V2PoolTransactions()))
	}
}

func (b *Bus) syncerAddrHandler(jc jape.Context) {
	api.WriteResponse(jc, api.SyncerAddrResp(b.s.Addr()))
}

func (b *Bus) syncerPeersHandler(jc jape.Context) {
	var peers []string
	for _, p := range b.s.Peers() {
		peers = append(peers, p.String())
	}
	api.WriteResponse(jc, api.SyncerPeersResp(peers))
}

func (b *Bus) syncerConnectHandler(jc jape.Context) {
	var addr string
	if jc.Decode(&addr) == nil {
		_, err := b.s.Connect(jc.Request.Context(), addr)
		jc.Check("couldn't connect to peer", err)
	}
}

func (b *Bus) consensusStateHandler(jc jape.Context) {
	cs, err := b.consensusState(jc.Request.Context())
	if jc.Check("couldn't fetch consensus state", err) != nil {
		return
	}
	api.WriteResponse(jc, cs)
}

func (b *Bus) consensusNetworkHandler(jc jape.Context) {
	jc.Encode(*b.cm.TipState().Network)
}

func (b *Bus) postSystemSQLite3BackupHandler(jc jape.Context) {
	var req api.BackupRequest
	if jc.Decode(&req) != nil {
		return
	}
	switch req.Database {
	case "main", "metrics":
	default:
		jc.Error(fmt.Errorf("%w: valid values are 'main' and 'metrics'", api.ErrInvalidDatabase), http.StatusBadRequest)
		return
	}
	err := b.store.Backup(jc.Request.Context(), req.Database, req.Path)
	if errors.Is(err, api.ErrBackupNotSupported) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if jc.Check("failed to backup", err) != nil {
		return
	}
}

func (b *Bus) txpoolFeeHandler(jc jape.Context) {
	api.WriteResponse(jc, api.TxPoolFeeResp{Currency: b.cm.RecommendedFee()})
}

func (b *Bus) txpoolTransactionsHandler(jc jape.Context) {
	api.WriteResponse(jc, api.TxPoolTxResp(b.cm.PoolTransactions()))
}

func (b *Bus) txpoolBroadcastHandler(jc jape.Context) {
	var txnSet []types.Transaction
	if jc.Decode(&txnSet) != nil {
		return
	}

	_, err := b.cm.AddPoolTransactions(txnSet)
	if jc.Check("couldn't broadcast transaction set", err) != nil {
		return
	}

	b.s.BroadcastTransactionSet(txnSet)
}

func (b *Bus) bucketsHandlerGET(jc jape.Context) {
	resp, err := b.store.Buckets(jc.Request.Context())
	if jc.Check("couldn't list buckets", err) != nil {
		return
	}
	api.WriteResponse(jc, prometheus.Slice(resp))
}

func (b *Bus) bucketsHandlerPOST(jc jape.Context) {
	var bucket api.BucketCreateRequest
	if jc.Decode(&bucket) != nil {
		return
	} else if bucket.Name == "" {
		jc.Error(errors.New("no name provided"), http.StatusBadRequest)
		return
	} else if jc.Check("failed to create bucket", b.store.CreateBucket(jc.Request.Context(), bucket.Name, bucket.Policy)) != nil {
		return
	}
}

func (b *Bus) bucketsHandlerPolicyPUT(jc jape.Context) {
	var req api.BucketUpdatePolicyRequest
	if jc.Decode(&req) != nil {
		return
	} else if bucket := jc.PathParam("name"); bucket == "" {
		jc.Error(errors.New("no bucket name provided"), http.StatusBadRequest)
		return
	} else if jc.Check("failed to create bucket", b.store.UpdateBucketPolicy(jc.Request.Context(), bucket, req.Policy)) != nil {
		return
	}
}

func (b *Bus) bucketHandlerDELETE(jc jape.Context) {
	var name string
	if jc.DecodeParam("name", &name) != nil {
		return
	} else if name == "" {
		jc.Error(errors.New("no name provided"), http.StatusBadRequest)
		return
	} else if jc.Check("failed to delete bucket", b.store.DeleteBucket(jc.Request.Context(), name)) != nil {
		return
	}
}

func (b *Bus) bucketHandlerGET(jc jape.Context) {
	var name string
	if jc.DecodeParam("name", &name) != nil {
		return
	} else if name == "" {
		jc.Error(errors.New("parameter 'name' is required"), http.StatusBadRequest)
		return
	}
	bucket, err := b.store.Bucket(jc.Request.Context(), name)
	if errors.Is(err, api.ErrBucketNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if jc.Check("failed to fetch bucket", err) != nil {
		return
	}
	jc.Encode(bucket)
}

func (b *Bus) walletHandler(jc jape.Context) {
	address := b.w.Address()
	balance, err := b.w.Balance()
	if jc.Check("couldn't fetch wallet balance", err) != nil {
		return
	}

	api.WriteResponse(jc, api.WalletResponse{
		Balance:    balance,
		Address:    address,
		ScanHeight: b.w.Tip().Height,
	})
}

func (b *Bus) walletEventsHandler(jc jape.Context) {
	var offset int
	if jc.DecodeForm("offset", &offset) != nil {
		return
	} else if offset < 0 {
		jc.Error(api.ErrInvalidOffset, http.StatusBadRequest)
		return
	}

	limit := -1
	if jc.DecodeForm("limit", &limit) != nil {
		return
	} else if limit < -1 {
		jc.Error(api.ErrInvalidLimit, http.StatusBadRequest)
		return
	}

	events, err := b.w.Events(offset, limit)
	if jc.Check("couldn't load events", err) != nil {
		return
	}
	relevant := []types.Address{b.w.Address()}
	for i := range events {
		// NOTE: add the wallet's address to every event. Theoretically,
		// this information should be persisted next to the event but
		// using a SingleAddress the address should always be set because
		// only relevant events are persisted and because the wallet only
		// has one address.
		events[i].Relevant = relevant
	}
	jc.Encode(events)
}

func (b *Bus) walletSendSiacoinsHandler(jc jape.Context) {
	var req api.WalletSendRequest
	if jc.Decode(&req) != nil {
		return
	} else if req.Address == types.VoidAddress {
		jc.Error(errors.New("cannot send to void address"), http.StatusBadRequest)
		return
	}

	// estimate miner fee
	feePerByte := b.cm.RecommendedFee()
	minerFee := feePerByte.Mul64(stdTxnSize)
	if req.SubtractMinerFee {
		var underflow bool
		req.Amount, underflow = req.Amount.SubWithUnderflow(minerFee)
		if underflow {
			jc.Error(fmt.Errorf("amount must be greater than miner fee: %s", minerFee), http.StatusBadRequest)
			return
		}
	}

	// send V2 transaction if we're passed the V2 hardfork allow height
	if b.isPassedV2AllowHeight() {
		txn := types.V2Transaction{
			MinerFee: minerFee,
			SiacoinOutputs: []types.SiacoinOutput{
				{Address: req.Address, Value: req.Amount},
			},
		}
		// fund and sign transaction
		basis, toSign, err := b.w.FundV2Transaction(&txn, req.Amount.Add(minerFee), req.UseUnconfirmed)
		if jc.Check("failed to fund transaction", err) != nil {
			return
		}
		b.w.SignV2Inputs(&txn, toSign)
		basis, txnset, err := b.cm.V2TransactionSet(basis, txn)
		if jc.Check("failed to get parents for funded transaction", err) != nil {
			b.w.ReleaseInputs(nil, []types.V2Transaction{txn})
			return
		}
		// verify the transaction and add it to the transaction pool
		if _, err := b.cm.AddV2PoolTransactions(basis, txnset); jc.Check("failed to add v2 transaction set", err) != nil {
			b.w.ReleaseInputs(nil, []types.V2Transaction{txn})
			return
		}
		// broadcast the transaction
		b.s.BroadcastV2TransactionSet(basis, txnset)
		jc.Encode(txn.ID())
	} else {
		// build transaction
		txn := types.Transaction{
			MinerFees: []types.Currency{minerFee},
			SiacoinOutputs: []types.SiacoinOutput{
				{Address: req.Address, Value: req.Amount},
			},
		}
		toSign, err := b.w.FundTransaction(&txn, req.Amount.Add(minerFee), req.UseUnconfirmed)
		if jc.Check("failed to fund transaction", err) != nil {
			return
		}
		b.w.SignTransaction(&txn, toSign, types.CoveredFields{WholeTransaction: true})
		// shouldn't be necessary to get parents since the transaction is
		// not using unconfirmed outputs, but good practice
		txnset := append(b.cm.UnconfirmedParents(txn), txn)
		// verify the transaction and add it to the transaction pool
		if _, err := b.cm.AddPoolTransactions(txnset); jc.Check("failed to add transaction set", err) != nil {
			b.w.ReleaseInputs([]types.Transaction{txn}, nil)
			return
		}
		// broadcast the transaction
		b.s.BroadcastTransactionSet(txnset)
		jc.Encode(txn.ID())
	}
}

func (b *Bus) walletRedistributeHandler(jc jape.Context) {
	var wfr api.WalletRedistributeRequest
	if jc.Decode(&wfr) != nil {
		return
	}
	if wfr.Outputs == 0 {
		jc.Error(errors.New("'outputs' has to be greater than zero"), http.StatusBadRequest)
		return
	}

	spendableOutputs, err := b.w.SpendableOutputs()
	if jc.Check("couldn't fetch spendable outputs", err) != nil {
		return
	}
	var available int
	for _, so := range spendableOutputs {
		if so.SiacoinOutput.Value.Cmp(wfr.Amount) >= 0 {
			available++
		}
	}
	if available >= wfr.Outputs {
		b.logger.Debugf("no wallet maintenance needed, plenty of outputs available (%v>=%v)", available, wfr.Outputs)
		jc.Encode([]types.TransactionID{})
		return
	}
	wantedOutputs := wfr.Outputs - available

	var ids []types.TransactionID
	if state := b.cm.TipState(); state.Index.Height < state.Network.HardforkV2.AllowHeight {
		// v1 redistribution
		txns, toSign, err := b.w.Redistribute(wantedOutputs, wfr.Amount, b.cm.RecommendedFee())
		if jc.Check("couldn't redistribute money in the wallet into the desired outputs", err) != nil {
			return
		}

		if len(txns) == 0 {
			jc.Encode(ids)
			return
		}

		for i := 0; i < len(txns); i++ {
			b.w.SignTransaction(&txns[i], toSign, types.CoveredFields{WholeTransaction: true})
			ids = append(ids, txns[i].ID())
		}

		_, err = b.cm.AddPoolTransactions(txns)
		if jc.Check("couldn't broadcast the transaction", err) != nil {
			b.w.ReleaseInputs(txns, nil)
			return
		}
	} else {
		// v2 redistribution
		txns, toSign, err := b.w.RedistributeV2(wantedOutputs, wfr.Amount, b.cm.RecommendedFee())
		if jc.Check("couldn't redistribute money in the wallet into the desired outputs", err) != nil {
			return
		}

		if len(txns) == 0 {
			jc.Encode(ids)
			return
		}

		for i := 0; i < len(txns); i++ {
			b.w.SignV2Inputs(&txns[i], toSign[i])
			ids = append(ids, txns[i].ID())
		}

		_, err = b.cm.AddV2PoolTransactions(state.Index, txns)
		if jc.Check("couldn't broadcast the transaction", err) != nil {
			b.w.ReleaseInputs(nil, txns)
			return
		}
	}

	jc.Encode(ids)
}

func (b *Bus) walletPendingHandler(jc jape.Context) {
	events, err := b.w.UnconfirmedEvents()
	if jc.Check("couldn't fetch unconfirmed events", err) != nil {
		return
	}
	jc.Encode(events)
}

func (b *Bus) hostsHandlerPOST(jc jape.Context) {
	var req api.HostsRequest
	if jc.Decode(&req) != nil {
		return
	}

	// validate the usability mode
	switch req.UsabilityMode {
	case api.UsabilityFilterModeUsable:
	case api.UsabilityFilterModeUnusable:
	case api.UsabilityFilterModeAll:
	case "":
		req.UsabilityMode = api.UsabilityFilterModeAll
	default:
		jc.Error(fmt.Errorf("invalid usability mode: '%v', options are 'usable', 'unusable' or an empty string for no filter", req.UsabilityMode), http.StatusBadRequest)
		return
	}

	// validate the filter mode
	switch req.FilterMode {
	case api.HostFilterModeAllowed:
	case api.HostFilterModeBlocked:
	case api.HostFilterModeAll:
	case "":
		req.FilterMode = api.HostFilterModeAllowed
	default:
		jc.Error(fmt.Errorf("invalid filter mode: '%v', options are 'allowed', 'blocked' or an empty string for 'allowed' filter", req.FilterMode), http.StatusBadRequest)
		return
	}

	// validate the offset and limit
	if req.Offset < 0 {
		jc.Error(errors.New("offset must be non-negative"), http.StatusBadRequest)
		return
	}
	if req.Limit < 0 && req.Limit != -1 {
		jc.Error(errors.New("limit must be non-negative or equal to -1 to indicate no limit"), http.StatusBadRequest)
		return
	} else if req.Limit == 0 {
		req.Limit = -1
	}

	hosts, err := b.store.Hosts(jc.Request.Context(), api.HostOptions{
		FilterMode:      req.FilterMode,
		UsabilityMode:   req.UsabilityMode,
		AddressContains: req.AddressContains,
		KeyIn:           req.KeyIn,
		Offset:          req.Offset,
		Limit:           req.Limit,
		MaxLastScan:     req.MaxLastScan,
	})
	if jc.Check(fmt.Sprintf("couldn't fetch hosts %d-%d", req.Offset, req.Offset+req.Limit), err) != nil {
		return
	}
	api.WriteResponse(jc, prometheus.Slice(hosts))
}

func (b *Bus) hostsRemoveHandlerPOST(jc jape.Context) {
	var hrr api.HostsRemoveRequest
	if jc.Decode(&hrr) != nil {
		return
	}
	if hrr.MaxDowntimeHours == 0 {
		jc.Error(errors.New("maxDowntime must be non-zero"), http.StatusBadRequest)
		return
	}
	if hrr.MaxConsecutiveScanFailures == 0 {
		jc.Error(errors.New("maxConsecutiveScanFailures must be non-zero"), http.StatusBadRequest)
		return
	}
	removed, err := b.store.RemoveOfflineHosts(jc.Request.Context(), hrr.MaxConsecutiveScanFailures, time.Duration(hrr.MaxDowntimeHours))
	if jc.Check("couldn't remove offline hosts", err) != nil {
		return
	}
	jc.Encode(removed)
}

func (b *Bus) hostsPubkeyHandlerGET(jc jape.Context) {
	var hostKey types.PublicKey
	if jc.DecodeParam("hostkey", &hostKey) != nil {
		return
	}
	host, err := b.store.Host(jc.Request.Context(), hostKey)
	if errors.Is(err, api.ErrHostNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if jc.Check("couldn't load host", err) == nil {
		jc.Encode(host)
	}
}

func (b *Bus) hostsScanHandlerPOST(jc jape.Context) {
	// only scan hosts if we are online
	if len(b.s.Peers()) == 0 {
		jc.Error(errors.New("not connected to the internet"), http.StatusServiceUnavailable)
		return
	}

	// decode the request
	var hk types.PublicKey
	if jc.DecodeParam("hostkey", &hk) != nil {
		return
	}
	var rsr api.HostScanRequest
	if jc.Decode(&rsr) != nil {
		return
	}

	// fetch host
	h, err := b.store.Host(jc.Request.Context(), hk)
	if jc.Check("failed to fetch host", err) != nil {
		return
	}

	// scan host
	var errStr string
	settings, priceTable, elapsed, err := b.scanHost(jc.Request.Context(), time.Duration(rsr.Timeout), hk, h.NetAddress)
	if err != nil {
		errStr = err.Error()
	}

	jc.Encode(api.HostScanResponse{
		Ping:       api.DurationMS(elapsed),
		PriceTable: priceTable,
		ScanError:  errStr,
		Settings:   settings,
	})
}

func (b *Bus) hostsResetLostSectorsPOST(jc jape.Context) {
	var hostKey types.PublicKey
	if jc.DecodeParam("hostkey", &hostKey) != nil {
		return
	}
	err := b.store.ResetLostSectors(jc.Request.Context(), hostKey)
	if jc.Check("couldn't reset lost sectors", err) != nil {
		return
	}
}

func (b *Bus) contractsSpendingHandlerPOST(jc jape.Context) {
	var records []api.ContractSpendingRecord
	if jc.Decode(&records) != nil {
		return
	}
	if jc.Check("failed to record spending metrics for contract", b.store.RecordContractSpending(jc.Request.Context(), records)) != nil {
		return
	}
}

func (b *Bus) hostsAllowlistHandlerGET(jc jape.Context) {
	allowlist, err := b.store.HostAllowlist(jc.Request.Context())
	if jc.Check("couldn't load allowlist", err) == nil {
		api.WriteResponse(jc, api.AllowListResp(allowlist))
	}
}

func (b *Bus) hostsAllowlistHandlerPUT(jc jape.Context) {
	ctx := jc.Request.Context()
	var req api.UpdateAllowlistRequest
	if jc.Decode(&req) == nil {
		if len(req.Add)+len(req.Remove) > 0 && req.Clear {
			jc.Error(errors.New("cannot add or remove entries while clearing the allowlist"), http.StatusBadRequest)
			return
		} else if jc.Check("couldn't update allowlist entries", b.store.UpdateHostAllowlistEntries(ctx, req.Add, req.Remove, req.Clear)) != nil {
			return
		}
	}
}

func (b *Bus) hostsBlocklistHandlerGET(jc jape.Context) {
	blocklist, err := b.store.HostBlocklist(jc.Request.Context())
	if jc.Check("couldn't load blocklist", err) == nil {
		api.WriteResponse(jc, api.BlockListResp(blocklist))
	}
}

func (b *Bus) hostsBlocklistHandlerPUT(jc jape.Context) {
	ctx := jc.Request.Context()
	var req api.UpdateBlocklistRequest
	if jc.Decode(&req) == nil {
		if len(req.Add)+len(req.Remove) > 0 && req.Clear {
			jc.Error(errors.New("cannot add or remove entries while clearing the blocklist"), http.StatusBadRequest)
			return
		} else if jc.Check("couldn't update blocklist entries", b.store.UpdateHostBlocklistEntries(ctx, req.Add, req.Remove, req.Clear)) != nil {
			return
		}
	}
}

func (b *Bus) contractsHandlerGET(jc jape.Context) {
	var cs string
	if jc.DecodeForm("contractset", &cs) != nil {
		return
	}
	filterMode := api.ContractFilterModeActive
	if jc.DecodeForm("filtermode", &filterMode) != nil {
		return
	}

	switch filterMode {
	case api.ContractFilterModeAll:
	case api.ContractFilterModeActive:
	case api.ContractFilterModeArchived:
	default:
		jc.Error(fmt.Errorf("invalid filter mode: '%v'", filterMode), http.StatusBadRequest)
		return
	}

	contracts, err := b.store.Contracts(jc.Request.Context(), api.ContractsOpts{
		ContractSet: cs,
		FilterMode:  filterMode,
	})
	if jc.Check("couldn't load contracts", err) == nil {
		api.WriteResponse(jc, prometheus.Slice(contracts))
	}
}

func (b *Bus) contractsRenewedIDHandlerGET(jc jape.Context) {
	var id types.FileContractID
	if jc.DecodeParam("id", &id) != nil {
		return
	}

	md, err := b.store.RenewedContract(jc.Request.Context(), id)
	if jc.Check("faild to fetch renewed contract", err) == nil {
		jc.Encode(md)
	}
}

func (b *Bus) contractsArchiveHandlerPOST(jc jape.Context) {
	var toArchive api.ContractsArchiveRequest
	if jc.Decode(&toArchive) != nil {
		return
	}

	if jc.Check("failed to archive contracts", b.store.ArchiveContracts(jc.Request.Context(), toArchive)) == nil {
		for fcid, reason := range toArchive {
			b.broadcastAction(webhooks.Event{
				Module: api.ModuleContract,
				Event:  api.EventArchive,
				Payload: api.EventContractArchive{
					ContractID: fcid,
					Reason:     reason,
					Timestamp:  time.Now().UTC(),
				},
			})
		}
	}
}

func (b *Bus) contractsSetsHandlerGET(jc jape.Context) {
	sets, err := b.store.ContractSets(jc.Request.Context())
	if jc.Check("couldn't fetch contract sets", err) == nil {
		jc.Encode(sets)
	}
}

func (b *Bus) contractsSetHandlerPUT(jc jape.Context) {
	var req api.ContractSetUpdateRequest
	if set := jc.PathParam("set"); set == "" {
		jc.Error(errors.New("path parameter 'set' can not be empty"), http.StatusBadRequest)
		return
	} else if jc.Decode(&req) != nil {
		return
	} else if jc.Check("could not add contracts to set", b.store.UpdateContractSet(jc.Request.Context(), set, req.ToAdd, req.ToRemove)) != nil {
		return
	} else if len(req.ToAdd)+len(req.ToRemove) > 0 {
		b.broadcastAction(webhooks.Event{
			Module: api.ModuleContractSet,
			Event:  api.EventUpdate,
			Payload: api.EventContractSetUpdate{
				Name:      set,
				ToAdd:     req.ToAdd,
				ToRemove:  req.ToRemove,
				Timestamp: time.Now().UTC(),
			},
		})
	}
}

func (b *Bus) contractsSetHandlerDELETE(jc jape.Context) {
	if set := jc.PathParam("set"); set != "" {
		jc.Check("could not remove contract set", b.store.RemoveContractSet(jc.Request.Context(), set))
	}
}

func (b *Bus) contractAcquireHandlerPOST(jc jape.Context) {
	var id types.FileContractID
	if jc.DecodeParam("id", &id) != nil {
		return
	}
	var req api.ContractAcquireRequest
	if jc.Decode(&req) != nil {
		return
	}

	lockID, err := b.contractLocker.Acquire(jc.Request.Context(), req.Priority, id, time.Duration(req.Duration))
	if jc.Check("failed to acquire contract", err) != nil {
		return
	}
	jc.Encode(api.ContractAcquireResponse{
		LockID: lockID,
	})
}

func (b *Bus) contractKeepaliveHandlerPOST(jc jape.Context) {
	var id types.FileContractID
	if jc.DecodeParam("id", &id) != nil {
		return
	}
	var req api.ContractKeepaliveRequest
	if jc.Decode(&req) != nil {
		return
	}

	err := b.contractLocker.KeepAlive(id, req.LockID, time.Duration(req.Duration))
	if jc.Check("failed to extend lock duration", err) != nil {
		return
	}
}

func (b *Bus) contractLatestRevisionHandlerGET(jc jape.Context) {
	var fcid types.FileContractID
	if jc.DecodeParam("id", &fcid) != nil {
		return
	}
	contract, err := b.store.Contract(jc.Request.Context(), fcid)
	if errors.Is(err, api.ErrContractNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if jc.Check("failed to fetch contract", err) != nil {
		return
	}

	if b.isPassedV2AllowHeight() {
		panic("not implemented")
	} else {
		revision, err := b.rhp3Client.Revision(jc.Request.Context(), fcid, contract.HostKey, contract.SiamuxAddr)
		if jc.Check("failed to fetch revision", err) != nil {
			return
		}
		jc.Encode(api.Revision{
			ContractID: revision.ParentID,
			V2FileContract: types.V2FileContract{
				Capacity:         revision.Filesize, // same as size for v1
				Filesize:         revision.Filesize,
				FileMerkleRoot:   revision.FileMerkleRoot,
				ProofHeight:      revision.WindowStart,
				ExpirationHeight: revision.WindowEnd,
				RenterOutput:     revision.ValidRenterOutput(),
				HostOutput:       revision.ValidHostOutput(),
				MissedHostValue:  revision.MissedHostPayout(),
				TotalCollateral:  types.ZeroCurrency, // unknown in v1
				RenterPublicKey:  types.PublicKey(revision.UnlockConditions.PublicKeys[0].Key),
				HostPublicKey:    types.PublicKey(revision.UnlockConditions.PublicKeys[1].Key),
				RevisionNumber:   revision.RevisionNumber,

				RenterSignature: types.Signature{}, // unavailable in v1
				HostSignature:   types.Signature{}, // unavailable in v1
			},
		})
	}
}

func (b *Bus) contractPruneHandlerPOST(jc jape.Context) {
	ctx := jc.Request.Context()

	// decode fcid
	var fcid types.FileContractID
	if jc.DecodeParam("id", &fcid) != nil {
		return
	}

	// decode timeout
	var req api.ContractPruneRequest
	if jc.Decode(&req) != nil {
		return
	}

	// create gouging checker
	gp, err := b.gougingParams(ctx)
	if jc.Check("couldn't fetch gouging parameters", err) != nil {
		return
	}
	gc := gouging.NewChecker(gp.GougingSettings, gp.ConsensusState, nil, nil)

	// apply timeout
	pruneCtx := ctx
	if req.Timeout > 0 {
		var cancel context.CancelFunc
		pruneCtx, cancel = context.WithTimeout(ctx, time.Duration(req.Timeout))
		defer cancel()
	}

	// acquire contract lock indefinitely and defer the release
	lockID, err := b.contractLocker.Acquire(pruneCtx, lockingPriorityPruning, fcid, time.Duration(math.MaxInt64))
	if jc.Check("couldn't acquire contract lock", err) != nil {
		return
	}
	defer func() {
		if err := b.contractLocker.Release(fcid, lockID); err != nil {
			b.logger.Error("failed to release contract lock", zap.Error(err))
		}
	}()

	// fetch the contract from the bus
	c, err := b.store.Contract(ctx, fcid)
	if errors.Is(err, api.ErrContractNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if jc.Check("couldn't fetch contract", err) != nil {
		return
	}

	// build map of uploading sectors
	pending := make(map[types.Hash256]struct{})
	for _, root := range b.sectors.Sectors(fcid) {
		pending[root] = struct{}{}
	}

	// prune the contract
	rk := b.masterKey.DeriveContractKey(c.HostKey)
	rev, spending, pruned, remaining, err := b.rhp2Client.PruneContract(pruneCtx, rk, gc, c.HostIP, c.HostKey, fcid, c.RevisionNumber, func(fcid types.FileContractID, roots []types.Hash256) ([]uint64, error) {
		indices, err := b.store.PrunableContractRoots(ctx, fcid, roots)
		if err != nil {
			return nil, err
		} else if len(indices) > len(roots) {
			return nil, fmt.Errorf("selected %d prunable roots but only %d were provided", len(indices), len(roots))
		}

		filtered := indices[:0]
		for _, index := range indices {
			_, ok := pending[roots[index]]
			if !ok {
				filtered = append(filtered, index)
			}
		}
		indices = filtered
		return indices, nil
	})

	if errors.Is(err, rhp2.ErrNoSectorsToPrune) {
		err = nil // ignore error
	} else if !errors.Is(err, context.Canceled) {
		if jc.Check("couldn't prune contract", err) != nil {
			return
		}
	}

	// record spending
	if !spending.Total().IsZero() {
		b.store.RecordContractSpending(jc.Request.Context(), []api.ContractSpendingRecord{
			{
				ContractSpending: spending,
				ContractID:       fcid,
				RevisionNumber:   rev.RevisionNumber,
				Size:             rev.Filesize,

				MissedHostPayout:  rev.MissedHostPayout(),
				ValidRenterPayout: rev.ValidRenterPayout(),
			},
		})
	}

	// return response
	res := api.ContractPruneResponse{
		ContractSize: rev.Filesize,
		Pruned:       pruned,
		Remaining:    remaining,
	}
	if err != nil {
		res.Error = err.Error()
	}
	jc.Encode(res)
}

func (b *Bus) contractsPrunableDataHandlerGET(jc jape.Context) {
	sizes, err := b.store.ContractSizes(jc.Request.Context())
	if jc.Check("failed to fetch contract sizes", err) != nil {
		return
	}

	// prepare the response
	var contracts []api.ContractPrunableData
	var totalPrunable, totalSize uint64

	// build the response
	for fcid, size := range sizes {
		// adjust the amount of prunable data with the pending uploads, due to
		// how we record contract spending a contract's size might already
		// include pending sectors
		pending := b.sectors.Pending(fcid)
		if pending > size.Prunable {
			size.Prunable = 0
		} else {
			size.Prunable -= pending
		}

		contracts = append(contracts, api.ContractPrunableData{
			ID:           fcid,
			ContractSize: size,
		})
		totalPrunable += size.Prunable
		totalSize += size.Size
	}

	// sort contracts by the amount of prunable data
	sort.Slice(contracts, func(i, j int) bool {
		if contracts[i].Prunable == contracts[j].Prunable {
			return contracts[i].Size > contracts[j].Size
		}
		return contracts[i].Prunable > contracts[j].Prunable
	})

	api.WriteResponse(jc, api.ContractsPrunableDataResponse{
		Contracts:     contracts,
		TotalPrunable: totalPrunable,
		TotalSize:     totalSize,
	})
}

func (b *Bus) contractSizeHandlerGET(jc jape.Context) {
	var id types.FileContractID
	if jc.DecodeParam("id", &id) != nil {
		return
	}

	size, err := b.store.ContractSize(jc.Request.Context(), id)
	if errors.Is(err, api.ErrContractNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if jc.Check("failed to fetch contract size", err) != nil {
		return
	}

	// adjust the amount of prunable data with the pending uploads, due to how
	// we record contract spending a contract's size might already include
	// pending sectors
	pending := b.sectors.Pending(id)
	if pending > size.Prunable {
		size.Prunable = 0
	} else {
		size.Prunable -= pending
	}

	jc.Encode(size)
}

func (b *Bus) contractReleaseHandlerPOST(jc jape.Context) {
	var id types.FileContractID
	if jc.DecodeParam("id", &id) != nil {
		return
	}
	var req api.ContractReleaseRequest
	if jc.Decode(&req) != nil {
		return
	}
	if jc.Check("failed to release contract", b.contractLocker.Release(id, req.LockID)) != nil {
		return
	}
}

func (b *Bus) contractIDHandlerGET(jc jape.Context) {
	var id types.FileContractID
	if jc.DecodeParam("id", &id) != nil {
		return
	}
	c, err := b.store.Contract(jc.Request.Context(), id)
	if jc.Check("couldn't load contract", err) == nil {
		jc.Encode(c)
	}
}

func (b *Bus) contractsHandlerPUT(jc jape.Context) {
	// decode request
	var c api.ContractMetadata
	if jc.Decode(&c) != nil {
		return
	}

	// upsert the contract
	if jc.Check("failed to add contract", b.store.PutContract(jc.Request.Context(), c)) == nil {
		b.broadcastAction(webhooks.Event{
			Module: api.ModuleContract,
			Event:  api.EventAdd,
			Payload: api.EventContractAdd{
				Added:     c,
				Timestamp: time.Now().UTC(),
			},
		})
	}
}

func (b *Bus) contractIDRenewHandlerPOST(jc jape.Context) {
	// apply pessimistic timeout
	ctx, cancel := context.WithTimeout(jc.Request.Context(), 15*time.Minute)
	defer cancel()

	// decode contract id
	var fcid types.FileContractID
	if jc.DecodeParam("id", &fcid) != nil {
		return
	}

	// decode request
	var rrr api.ContractRenewRequest
	if jc.Decode(&rrr) != nil {
		return
	}

	// validate the request
	if rrr.EndHeight == 0 {
		http.Error(jc.ResponseWriter, "EndHeight can not be zero", http.StatusBadRequest)
		return
	} else if rrr.ExpectedNewStorage == 0 {
		http.Error(jc.ResponseWriter, "ExpectedNewStorage can not be zero", http.StatusBadRequest)
		return
	} else if rrr.RenterFunds.IsZero() {
		http.Error(jc.ResponseWriter, "RenterFunds can not be zero", http.StatusBadRequest)
		return
	}

	// fetch the contract
	c, err := b.store.Contract(ctx, fcid)
	if errors.Is(err, api.ErrContractNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if jc.Check("couldn't fetch contract", err) != nil {
		return
	}

	// fetch the host
	h, err := b.store.Host(ctx, c.HostKey)
	if jc.Check("couldn't fetch host", err) != nil {
		return
	}

	// fetch consensus state
	cs := b.cm.TipState()

	// fetch gouging parameters
	gp, err := b.gougingParams(ctx)
	if jc.Check("could not get gouging parameters", err) != nil {
		return
	}

	// send V2 transaction if we're passed the V2 hardfork allow height
	var newRevision rhpv2.ContractRevision
	var contractPrice, initialRenterFunds types.Currency
	if b.isPassedV2AllowHeight() {
		panic("not implemented")
	} else {
		newRevision, contractPrice, initialRenterFunds, err = b.renewContract(ctx, cs, gp, c, h.Settings, rrr.RenterFunds, rrr.MinNewCollateral, rrr.EndHeight, rrr.ExpectedNewStorage)
		if errors.Is(err, api.ErrMaxFundAmountExceeded) {
			jc.Error(err, http.StatusBadRequest)
			return
		} else if jc.Check("couldn't renew contract", err) != nil {
			return
		}
	}

	// add the renewal
	metadata, err := b.addRenewal(ctx, fcid, newRevision, contractPrice, initialRenterFunds, cs.Index.Height, api.ContractStatePending)
	if jc.Check("couldn't add renewal", err) == nil {
		jc.Encode(metadata)
	}
}

func (b *Bus) contractIDRootsHandlerGET(jc jape.Context) {
	var id types.FileContractID
	if jc.DecodeParam("id", &id) != nil {
		return
	}

	roots, err := b.store.ContractRoots(jc.Request.Context(), id)
	if jc.Check("couldn't fetch contract sectors", err) == nil {
		jc.Encode(api.ContractRootsResponse{
			Roots:     roots,
			Uploading: b.sectors.Sectors(id),
		})
	}
}

func (b *Bus) contractIDHandlerDELETE(jc jape.Context) {
	var id types.FileContractID
	if jc.DecodeParam("id", &id) != nil {
		return
	}
	jc.Check("couldn't remove contract", b.store.ArchiveContract(jc.Request.Context(), id, api.ContractArchivalReasonRemoved))
}

func (b *Bus) contractsAllHandlerDELETE(jc jape.Context) {
	jc.Check("couldn't remove contracts", b.store.ArchiveAllContracts(jc.Request.Context(), api.ContractArchivalReasonRemoved))
}

func (b *Bus) objectHandlerGET(jc jape.Context) {
	key := jc.PathParam("key")
	var bucket string
	if jc.DecodeForm("bucket", &bucket) != nil {
		return
	} else if bucket == "" {
		jc.Error(api.ErrBucketMissing, http.StatusBadRequest)
		return
	}

	var onlymetadata bool
	if jc.DecodeForm("onlymetadata", &onlymetadata) != nil {
		return
	}

	var o api.Object
	var err error

	if onlymetadata {
		o, err = b.store.ObjectMetadata(jc.Request.Context(), bucket, key)
	} else {
		o, err = b.store.Object(jc.Request.Context(), bucket, key)
	}
	if errors.Is(err, api.ErrObjectNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if jc.Check("couldn't load object", err) != nil {
		return
	}
	jc.Encode(o)
}

func (b *Bus) objectsHandlerGET(jc jape.Context) {
	var bucket, marker, delim, sortBy, sortDir, substring string
	if jc.DecodeForm("bucket", &bucket) != nil {
		return
	}

	if jc.DecodeForm("delimiter", &delim) != nil {
		return
	}
	limit := -1
	if jc.DecodeForm("limit", &limit) != nil {
		return
	}
	if jc.DecodeForm("marker", &marker) != nil {
		return
	}
	if jc.DecodeForm("sortby", &sortBy) != nil {
		return
	}
	if jc.DecodeForm("sortdir", &sortDir) != nil {
		return
	}
	if jc.DecodeForm("substring", &substring) != nil {
		return
	}
	var slabEncryptionKey object.EncryptionKey
	if jc.DecodeForm("slabencryptionkey", &slabEncryptionKey) != nil {
		return
	}

	resp, err := b.store.Objects(jc.Request.Context(), bucket, jc.PathParam("prefix"), substring, delim, sortBy, sortDir, marker, limit, slabEncryptionKey)
	if errors.Is(err, api.ErrUnsupportedDelimiter) {
		jc.Error(err, http.StatusBadRequest)
		return
	} else if jc.Check("failed to query objects", err) != nil {
		return
	}
	api.WriteResponse(jc, resp)
}

func (b *Bus) objectHandlerPUT(jc jape.Context) {
	var aor api.AddObjectRequest
	if jc.Decode(&aor) != nil {
		return
	} else if aor.Bucket == "" {
		jc.Error(api.ErrBucketMissing, http.StatusBadRequest)
		return
	}
	jc.Check("couldn't store object", b.store.UpdateObject(jc.Request.Context(), aor.Bucket, jc.PathParam("key"), aor.ContractSet, aor.ETag, aor.MimeType, aor.Metadata, aor.Object))
}

func (b *Bus) objectsCopyHandlerPOST(jc jape.Context) {
	var orr api.CopyObjectsRequest
	if jc.Decode(&orr) != nil {
		return
	}
	om, err := b.store.CopyObject(jc.Request.Context(), orr.SourceBucket, orr.DestinationBucket, orr.SourceKey, orr.DestinationKey, orr.MimeType, orr.Metadata)
	if jc.Check("couldn't copy object", err) != nil {
		return
	}

	jc.ResponseWriter.Header().Set("Last-Modified", om.ModTime.Std().Format(http.TimeFormat))
	jc.ResponseWriter.Header().Set("ETag", api.FormatETag(om.ETag))
	jc.Encode(om)
}

func (b *Bus) objectsRemoveHandlerPOST(jc jape.Context) {
	var orr api.ObjectsRemoveRequest
	if jc.Decode(&orr) != nil {
		return
	} else if orr.Bucket == "" {
		jc.Error(api.ErrBucketMissing, http.StatusBadRequest)
		return
	}

	if orr.Prefix == "" {
		jc.Error(errors.New("prefix cannot be empty"), http.StatusBadRequest)
		return
	}

	jc.Check("failed to remove objects", b.store.RemoveObjects(jc.Request.Context(), orr.Bucket, orr.Prefix))
}

func (b *Bus) objectsRenameHandlerPOST(jc jape.Context) {
	var orr api.ObjectsRenameRequest
	if jc.Decode(&orr) != nil {
		return
	} else if orr.Bucket == "" {
		jc.Error(api.ErrBucketMissing, http.StatusBadRequest)
		return
	}
	if orr.Mode == api.ObjectsRenameModeSingle {
		// Single object rename.
		if strings.HasSuffix(orr.From, "/") || strings.HasSuffix(orr.To, "/") {
			jc.Error(fmt.Errorf("can't rename dirs with mode %v", orr.Mode), http.StatusBadRequest)
			return
		}
		jc.Check("couldn't rename object", b.store.RenameObject(jc.Request.Context(), orr.Bucket, orr.From, orr.To, orr.Force))
		return
	} else if orr.Mode == api.ObjectsRenameModeMulti {
		// Multi object rename.
		if !strings.HasSuffix(orr.From, "/") || !strings.HasSuffix(orr.To, "/") {
			jc.Error(fmt.Errorf("can't rename file with mode %v", orr.Mode), http.StatusBadRequest)
			return
		}
		jc.Check("couldn't rename objects", b.store.RenameObjects(jc.Request.Context(), orr.Bucket, orr.From, orr.To, orr.Force))
		return
	} else {
		// Invalid mode.
		jc.Error(fmt.Errorf("invalid mode: %v", orr.Mode), http.StatusBadRequest)
		return
	}
}

func (b *Bus) objectHandlerDELETE(jc jape.Context) {
	var bucket string
	if jc.DecodeForm("bucket", &bucket) != nil {
		return
	} else if bucket == "" {
		jc.Error(api.ErrBucketMissing, http.StatusBadRequest)
		return
	}
	err := b.store.RemoveObject(jc.Request.Context(), bucket, jc.PathParam("key"))
	if errors.Is(err, api.ErrObjectNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	}
	jc.Check("couldn't delete object", err)
}

func (b *Bus) slabbuffersHandlerGET(jc jape.Context) {
	buffers, err := b.store.SlabBuffers(jc.Request.Context())
	if jc.Check("couldn't get slab buffers info", err) != nil {
		return
	}
	api.WriteResponse(jc, api.SlabBuffersResp(buffers))
}

func (b *Bus) objectsStatshandlerGET(jc jape.Context) {
	opts := api.ObjectsStatsOpts{}
	if jc.DecodeForm("bucket", &opts.Bucket) != nil {
		return
	}
	info, err := b.store.ObjectsStats(jc.Request.Context(), opts)
	if jc.Check("couldn't get objects stats", err) != nil {
		return
	}
	jc.Encode(info)
}

func (b *Bus) packedSlabsHandlerFetchPOST(jc jape.Context) {
	var psrg api.PackedSlabsRequestGET
	if jc.Decode(&psrg) != nil {
		return
	}
	if psrg.MinShards == 0 || psrg.TotalShards == 0 {
		jc.Error(fmt.Errorf("min_shards and total_shards must be non-zero"), http.StatusBadRequest)
		return
	}
	if psrg.LockingDuration == 0 {
		jc.Error(fmt.Errorf("locking_duration must be non-zero"), http.StatusBadRequest)
		return
	}
	if psrg.ContractSet == "" {
		jc.Error(fmt.Errorf("contract_set must be non-empty"), http.StatusBadRequest)
		return
	}
	slabs, err := b.store.PackedSlabsForUpload(jc.Request.Context(), time.Duration(psrg.LockingDuration), psrg.MinShards, psrg.TotalShards, psrg.ContractSet, psrg.Limit)
	if jc.Check("couldn't get packed slabs", err) != nil {
		return
	}
	jc.Encode(slabs)
}

func (b *Bus) packedSlabsHandlerDonePOST(jc jape.Context) {
	var psrp api.PackedSlabsRequestPOST
	if jc.Decode(&psrp) != nil {
		return
	}
	jc.Check("failed to mark packed slab(s) as uploaded", b.store.MarkPackedSlabsUploaded(jc.Request.Context(), psrp.Slabs))
}

func (b *Bus) settingsGougingHandlerGET(jc jape.Context) {
	if gs, err := b.fetchSetting(jc.Request.Context(), stores.SettingGouging); err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	} else if gsc, ok := gs.(api.GougingSettings); !ok {
		panic("unexpected value") // developer error
	} else {
		jc.Encode(gsc)
	}
}

func (b *Bus) settingsGougingHandlerPATCH(jc jape.Context) {
	jc.Custom((*map[string]any)(nil), api.GougingSettings{})

	// decode patch
	var patch map[string]any
	if jc.Decode(&patch) != nil {
		return
	}

	// apply patch
	update, err := b.patchSetting(jc.Request.Context(), stores.SettingGouging, patch)
	if errors.Is(err, ErrSettingFieldNotFound) {
		jc.Error(err, http.StatusBadRequest)
	} else if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	} else if gs, ok := update.(api.GougingSettings); !ok {
		panic("unexpected setting") // developer error
	} else {
		jc.Encode(gs)
	}
}

func (b *Bus) settingsGougingHandlerPUT(jc jape.Context) {
	var gs api.GougingSettings
	if jc.Decode(&gs) != nil {
		return
	} else if err := gs.Validate(); err != nil {
		jc.Error(fmt.Errorf("couldn't update gouging settings, error: %v", err), http.StatusBadRequest)
		return
	} else if err := b.updateSetting(jc.Request.Context(), stores.SettingGouging, gs); err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}
}

func (b *Bus) settingsPinnedHandlerGET(jc jape.Context) {
	if ps, err := b.fetchSetting(jc.Request.Context(), stores.SettingPinned); err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	} else if psc, ok := ps.(api.PinnedSettings); !ok {
		panic("unexpected value") // developer error
	} else {
		jc.Encode(psc)
	}
}

func (b *Bus) settingsPinnedHandlerPATCH(jc jape.Context) {
	jc.Custom((*map[string]any)(nil), api.PinnedSettings{})

	// decode patch
	var patch map[string]any
	if jc.Decode(&patch) != nil {
		return
	}

	// apply patch
	update, err := b.patchSetting(jc.Request.Context(), stores.SettingPinned, patch)
	if errors.Is(err, ErrSettingFieldNotFound) {
		jc.Error(err, http.StatusBadRequest)
		return
	} else if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	} else if gs, ok := update.(api.PinnedSettings); !ok {
		panic("unexpected setting") // developer error
	} else {
		jc.Encode(gs)
	}
}

func (b *Bus) settingsPinnedHandlerPUT(jc jape.Context) {
	var ps api.PinnedSettings
	if jc.Decode(&ps) != nil {
		return
	} else if err := ps.Validate(); err != nil {
		jc.Error(fmt.Errorf("couldn't update pinned settings, error: %v", err), http.StatusBadRequest)
		return
	} else if ps.Enabled() && !b.explorer.Enabled() {
		jc.Error(fmt.Errorf("can't enable price pinning, %w", api.ErrExplorerDisabled), http.StatusBadRequest)
		return
	} else if err := b.updateSetting(jc.Request.Context(), stores.SettingPinned, ps); err != nil {
		jc.Error(err, http.StatusInternalServerError)
	}
}

func (b *Bus) settingsUploadHandlerGET(jc jape.Context) {
	if us, err := b.fetchSetting(jc.Request.Context(), stores.SettingUpload); err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	} else if usc, ok := us.(api.UploadSettings); !ok {
		panic("unexpected value") // developer error
	} else {
		jc.Encode(usc)
	}
}

func (b *Bus) settingsUploadHandlerPATCH(jc jape.Context) {
	jc.Custom((*map[string]any)(nil), api.UploadSettings{})

	// decode patch
	var patch map[string]any
	if jc.Decode(&patch) != nil {
		return
	}

	// apply patch
	update, err := b.patchSetting(jc.Request.Context(), stores.SettingUpload, patch)
	if errors.Is(err, ErrSettingFieldNotFound) {
		jc.Error(err, http.StatusBadRequest)
		return
	} else if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	} else if gs, ok := update.(api.UploadSettings); !ok {
		panic("unexpected setting") // developer error
	} else {
		jc.Encode(gs)
	}
}

func (b *Bus) settingsUploadHandlerPUT(jc jape.Context) {
	var us api.UploadSettings
	if jc.Decode(&us) != nil {
		return
	} else if err := us.Validate(); err != nil {
		jc.Error(fmt.Errorf("couldn't update upload settings, error: %v", err), http.StatusBadRequest)
		return
	} else if err := b.updateSetting(jc.Request.Context(), stores.SettingUpload, us); err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}
}

func (b *Bus) settingsS3HandlerGET(jc jape.Context) {
	if s3s, err := b.fetchSetting(jc.Request.Context(), stores.SettingS3); err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	} else if s3sc, ok := s3s.(api.S3Settings); !ok {
		panic("unexpected value") // developer error
	} else {
		jc.Encode(s3sc)
	}
}

func (b *Bus) settingsS3HandlerPATCH(jc jape.Context) {
	jc.Custom((*map[string]any)(nil), api.S3Settings{})

	// decode patch
	var patch map[string]any
	if jc.Decode(&patch) != nil {
		return
	}

	// apply patch
	update, err := b.patchSetting(jc.Request.Context(), stores.SettingS3, patch)
	if errors.Is(err, ErrSettingFieldNotFound) {
		jc.Error(err, http.StatusBadRequest)
		return
	} else if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	} else if gs, ok := update.(api.S3Settings); !ok {
		panic("unexpected setting") // developer error
	} else {
		jc.Encode(gs)
	}
}

func (b *Bus) settingsS3HandlerPUT(jc jape.Context) {
	var s3s api.S3Settings
	if jc.Decode(&s3s) != nil {
		return
	} else if err := s3s.Validate(); err != nil {
		jc.Error(fmt.Errorf("couldn't update S3 settings, error: %v", err), http.StatusBadRequest)
		return
	} else if err := b.updateSetting(jc.Request.Context(), stores.SettingS3, s3s); err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}
}

func (b *Bus) sectorsHostRootHandlerDELETE(jc jape.Context) {
	var hk types.PublicKey
	var root types.Hash256
	if jc.DecodeParam("hk", &hk) != nil {
		return
	} else if jc.DecodeParam("root", &root) != nil {
		return
	}
	n, err := b.store.DeleteHostSector(jc.Request.Context(), hk, root)
	if jc.Check("failed to mark sector as lost", err) != nil {
		return
	} else if n > 0 {
		b.logger.Infow("successfully marked sector as lost", "hk", hk, "root", root)
	}
}

func (b *Bus) slabHandlerGET(jc jape.Context) {
	var key object.EncryptionKey
	if jc.DecodeParam("key", &key) != nil {
		return
	}
	slab, err := b.store.Slab(jc.Request.Context(), key)
	if errors.Is(err, api.ErrSlabNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}
	jc.Encode(slab)
}

func (b *Bus) slabHandlerPUT(jc jape.Context) {
	var key object.EncryptionKey
	if jc.DecodeParam("key", &key) != nil {
		return
	}

	var sectors []api.UploadedSector
	if jc.Decode(&sectors) != nil {
		return
	}

	// validate the sectors
	for _, s := range sectors {
		if s.Root == (types.Hash256{}) {
			jc.Error(errors.New("root can not be empty"), http.StatusBadRequest)
			return
		} else if s.ContractID == (types.FileContractID{}) {
			jc.Error(errors.New("contractID can not be empty"), http.StatusBadRequest)
			return
		}
	}

	err := b.store.UpdateSlab(jc.Request.Context(), key, sectors)
	if errors.Is(err, api.ErrSlabNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if errors.Is(err, api.ErrUnknownSector) {
		jc.Error(err, http.StatusBadRequest)
		return
	} else if errors.Is(err, api.ErrContractNotFound) {
		jc.Error(err, http.StatusBadRequest)
		return
	} else if err != nil {
		jc.Error(fmt.Errorf("%v: %w", "couldn't update slab", err), http.StatusInternalServerError)
		return
	}
}

func (b *Bus) slabsRefreshHealthHandlerPOST(jc jape.Context) {
	jc.Check("failed to recompute health", b.store.RefreshHealth(jc.Request.Context()))
}

func (b *Bus) slabsMigrationHandlerPOST(jc jape.Context) {
	var msr api.MigrationSlabsRequest
	if jc.Decode(&msr) == nil {
		if slabs, err := b.store.UnhealthySlabs(jc.Request.Context(), msr.HealthCutoff, msr.ContractSet, msr.Limit); jc.Check("couldn't fetch slabs for migration", err) == nil {
			jc.Encode(api.UnhealthySlabsResponse{
				Slabs: slabs,
			})
		}
	}
}

func (b *Bus) slabsPartialHandlerGET(jc jape.Context) {
	jc.Custom(nil, []byte{})

	var key object.EncryptionKey
	if jc.DecodeParam("key", &key) != nil {
		return
	}

	var offset int
	if jc.DecodeForm("offset", &offset) != nil {
		return
	} else if offset < 0 {
		jc.Error(api.ErrInvalidOffset, http.StatusBadRequest)
		return
	}

	var length int
	if jc.DecodeForm("length", &length) != nil {
		return
	} else if length <= 0 {
		jc.Error(api.ErrInvalidLength, http.StatusBadRequest)
		return
	}

	data, err := b.store.FetchPartialSlab(jc.Request.Context(), key, uint32(offset), uint32(length))
	if errors.Is(err, api.ErrObjectNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}
	jc.ResponseWriter.Write(data)
}

func (b *Bus) slabsPartialHandlerPOST(jc jape.Context) {
	var minShards int
	if jc.DecodeForm("minshards", &minShards) != nil {
		return
	}
	var totalShards int
	if jc.DecodeForm("totalshards", &totalShards) != nil {
		return
	}
	var contractSet string
	if jc.DecodeForm("contractset", &contractSet) != nil {
		return
	}
	if minShards <= 0 || totalShards <= minShards {
		jc.Error(errors.New("minShards must be positive and totalShards must be greater than minShards"), http.StatusBadRequest)
		return
	}
	if totalShards > math.MaxUint8 {
		jc.Error(fmt.Errorf("totalShards must be less than or equal to %d", math.MaxUint8), http.StatusBadRequest)
		return
	}
	if contractSet == "" {
		jc.Error(errors.New("parameter 'contractSet' is required"), http.StatusBadRequest)
		return
	}
	data, err := io.ReadAll(jc.Request.Body)
	if jc.Check("failed to read request body", err) != nil {
		return
	}
	slabs, bufferSize, err := b.store.AddPartialSlab(jc.Request.Context(), data, uint8(minShards), uint8(totalShards), contractSet)
	if jc.Check("failed to add partial slab", err) != nil {
		return
	}
	us, err := b.store.UploadSettings(jc.Request.Context())
	if err != nil {
		jc.Error(fmt.Errorf("could not get upload packing settings: %w", err), http.StatusInternalServerError)
		return
	}
	jc.Encode(api.AddPartialSlabResponse{
		Slabs:                        slabs,
		SlabBufferMaxSizeSoftReached: bufferSize >= us.Packing.SlabBufferMaxSizeSoft,
	})
}

func (b *Bus) autopilotConfigHandlerGET(jc jape.Context) {
	cfg, err := b.store.AutopilotConfig(jc.Request.Context())
	if jc.Check("failed to fetch autopilot config", err) != nil {
		return
	}
	jc.Encode(cfg)
}

func (b *Bus) autopilotConfigHandlerPUT(jc jape.Context) {
	var cfg api.AutopilotConfig
	if jc.Decode(&cfg) != nil {
		return
	}

	if cfg.Enabled {
		err := errors.Join(cfg.Contracts.Validate(), cfg.Hosts.Validate())
		if err != nil {
			jc.Error(err, http.StatusBadRequest)
			return
		}
	}

	jc.Check("failed to update autopilot config", b.store.UpdateAutopilotConfig(jc.Request.Context(), cfg))
}

func (b *Bus) autopilotPeriodHandlerGET(jc jape.Context) {
	period, err := b.store.AutopilotPeriod(jc.Request.Context())
	if jc.Check("failed to fetch current period", err) != nil {
		return
	}
	jc.Encode(period)
}

func (b *Bus) autopilotPeriodHandlerPUT(jc jape.Context) {
	var period uint64
	if jc.Decode(&period) != nil {
		return
	}

	jc.Check("failed to update autopilot period", b.store.UpdateAutopilotPeriod(jc.Request.Context(), period))
}

func (b *Bus) contractIDAncestorsHandler(jc jape.Context) {
	var fcid types.FileContractID
	if jc.DecodeParam("id", &fcid) != nil {
		return
	}
	var minStartHeight uint64
	if jc.DecodeForm("minstartheight", &minStartHeight) != nil {
		return
	}
	ancestors, err := b.store.AncestorContracts(jc.Request.Context(), fcid, uint64(minStartHeight))
	if jc.Check("failed to fetch ancestor contracts", err) != nil {
		return
	}
	jc.Encode(ancestors)
}

func (b *Bus) contractIDBroadcastHandler(jc jape.Context) {
	var fcid types.FileContractID
	if jc.DecodeParam("id", &fcid) != nil {
		return
	}

	txnID, err := b.broadcastContract(jc.Request.Context(), fcid)
	if jc.Check("failed to broadcast contract revision", err) == nil {
		jc.Encode(txnID)
	}
}

func (b *Bus) paramsHandlerUploadGET(jc jape.Context) {
	gp, err := b.gougingParams(jc.Request.Context())
	if jc.Check("could not get gouging parameters", err) != nil {
		return
	}

	var uploadPacking bool
	var contractSet string
	us, err := b.store.UploadSettings(jc.Request.Context())
	if jc.Check("could not get upload settings", err) == nil {
		contractSet = us.DefaultContractSet
		uploadPacking = us.Packing.Enabled
	}

	api.WriteResponse(jc, api.UploadParams{
		ContractSet:   contractSet,
		CurrentHeight: b.cm.TipState().Index.Height,
		GougingParams: gp,
		UploadPacking: uploadPacking,
	})
}

func (b *Bus) consensusState(ctx context.Context) (api.ConsensusState, error) {
	index, err := b.cs.ChainIndex(ctx)
	if err != nil {
		return api.ConsensusState{}, err
	}

	var synced bool
	block, found := b.cm.Block(index.ID)
	if found {
		synced = utils.IsSynced(block)
	}

	return api.ConsensusState{
		BlockHeight:   index.Height,
		LastBlockTime: api.TimeRFC3339(block.Timestamp),
		Synced:        synced,
	}, nil
}

func (b *Bus) paramsHandlerGougingGET(jc jape.Context) {
	gp, err := b.gougingParams(jc.Request.Context())
	if jc.Check("could not get gouging parameters", err) != nil {
		return
	}
	api.WriteResponse(jc, gp)
}

func (b *Bus) gougingParams(ctx context.Context) (api.GougingParams, error) {
	gs, err := b.store.GougingSettings(ctx)
	if errors.Is(err, sql.ErrSettingNotFound) {
		gs = api.DefaultGougingSettings
	} else if err != nil {
		return api.GougingParams{}, err
	}

	us, err := b.store.UploadSettings(ctx)
	if errors.Is(err, sql.ErrSettingNotFound) {
		us = api.DefaultUploadSettings(b.cm.TipState().Network.Name)
	} else if err != nil {
		return api.GougingParams{}, err
	}

	cs, err := b.consensusState(ctx)
	if err != nil {
		return api.GougingParams{}, err
	}

	return api.GougingParams{
		ConsensusState:     cs,
		GougingSettings:    gs,
		RedundancySettings: us.Redundancy,
	}, nil
}

func (b *Bus) handleGETAlerts(jc jape.Context) {
	var severity alerts.Severity
	if jc.DecodeForm("severity", &severity) != nil {
		return
	}

	var offset int
	if jc.DecodeForm("offset", &offset) != nil {
		return
	} else if offset < 0 {
		jc.Error(api.ErrInvalidOffset, http.StatusBadRequest)
		return
	}

	limit := -1
	if jc.DecodeForm("limit", &limit) != nil {
		return
	} else if limit < -1 {
		jc.Error(api.ErrInvalidLimit, http.StatusBadRequest)
		return
	}

	ar, err := b.alertMgr.Alerts(jc.Request.Context(), alerts.AlertsOpts{
		Offset:   offset,
		Limit:    limit,
		Severity: severity,
	})
	if jc.Check("failed to fetch alerts", err) != nil {
		return
	}
	api.WriteResponse(jc, ar)
}

func (b *Bus) handlePOSTAlertsDismiss(jc jape.Context) {
	var ids []types.Hash256
	if jc.Decode(&ids) != nil {
		return
	}
	jc.Check("failed to dismiss alerts", b.alertMgr.DismissAlerts(jc.Request.Context(), ids...))
}

func (b *Bus) handlePOSTAlertsRegister(jc jape.Context) {
	var alert alerts.Alert
	if jc.Decode(&alert) != nil {
		return
	}
	jc.Check("failed to register alert", b.alertMgr.RegisterAlert(jc.Request.Context(), alert))
}

func (b *Bus) accountsHandlerGET(jc jape.Context) {
	var owner string
	if jc.DecodeForm("owner", &owner) != nil {
		return
	}
	accounts, err := b.store.Accounts(jc.Request.Context(), owner)
	if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}
	jc.Encode(accounts)
}

func (b *Bus) accountsHandlerPOST(jc jape.Context) {
	var req api.AccountsSaveRequest
	if jc.Decode(&req) != nil {
		return
	}
	for _, acc := range req.Accounts {
		if acc.Owner == "" {
			jc.Error(errors.New("acocunts need to have a valid 'Owner'"), http.StatusBadRequest)
			return
		}
	}
	if b.store.SaveAccounts(jc.Request.Context(), req.Accounts) != nil {
		return
	}
}

func (b *Bus) hostsCheckHandlerPUT(jc jape.Context) {
	var hk types.PublicKey
	if jc.DecodeParam("hostkey", &hk) != nil {
		return
	}
	var hc api.HostChecks
	if jc.Check("failed to decode host check", jc.Decode(&hc)) != nil {
		return
	}

	err := b.store.UpdateHostCheck(jc.Request.Context(), hk, hc)
	if jc.Check("failed to update host check", err) != nil {
		return
	}
}

func (b *Bus) broadcastAction(e webhooks.Event) {
	log := b.logger.With("event", e.Event).With("module", e.Module)
	err := b.webhooksMgr.BroadcastAction(context.Background(), e)
	if err != nil {
		log.With(zap.Error(err)).Error("failed to broadcast action")
	} else {
		log.Debug("successfully broadcast action")
	}
}

func (b *Bus) contractTaxHandlerGET(jc jape.Context) {
	var payout types.Currency
	if jc.DecodeParam("payout", (*api.ParamCurrency)(&payout)) != nil {
		return
	}
	cs := b.cm.TipState()
	jc.Encode(cs.FileContractTax(types.FileContract{Payout: payout}))
}

func (b *Bus) stateHandlerGET(jc jape.Context) {
	api.WriteResponse(jc, api.BusStateResponse{
		StartTime: api.TimeRFC3339(b.startTime),
		BuildState: api.BuildState{
			Version:   build.Version(),
			Commit:    build.Commit(),
			OS:        runtime.GOOS,
			BuildTime: api.TimeRFC3339(build.BuildTime()),
		},
		Explorer: api.ExplorerState{
			Enabled: b.explorer.Enabled(),
			URL:     b.explorer.BaseURL(),
		},
		Network: b.cm.TipState().Network.Name,
	})
}

func (b *Bus) uploadTrackHandlerPOST(jc jape.Context) {
	var id api.UploadID
	if jc.DecodeParam("id", &id) == nil {
		jc.Check("failed to track upload", b.sectors.StartUpload(id))
	}
}

func (b *Bus) uploadAddSectorHandlerPOST(jc jape.Context) {
	var id api.UploadID
	if jc.DecodeParam("id", &id) != nil {
		return
	}
	var req api.UploadSectorRequest
	if jc.Decode(&req) != nil {
		return
	}
	jc.Check("failed to add sector", b.sectors.AddSector(id, req.ContractID, req.Root))
}

func (b *Bus) uploadFinishedHandlerDELETE(jc jape.Context) {
	var id api.UploadID
	if jc.DecodeParam("id", &id) == nil {
		b.sectors.FinishUpload(id)
	}
}

func (b *Bus) webhookActionHandlerPost(jc jape.Context) {
	var action webhooks.Event
	if jc.Check("failed to decode action", jc.Decode(&action)) != nil {
		return
	}
	b.broadcastAction(action)
}

func (b *Bus) webhookHandlerDelete(jc jape.Context) {
	var wh webhooks.Webhook
	if jc.Decode(&wh) != nil {
		return
	}
	err := b.webhooksMgr.Delete(jc.Request.Context(), wh)
	if errors.Is(err, webhooks.ErrWebhookNotFound) {
		jc.Error(fmt.Errorf("webhook for URL %v and event %v.%v not found", wh.URL, wh.Module, wh.Event), http.StatusNotFound)
		return
	} else if jc.Check("failed to delete webhook", err) != nil {
		return
	}
}

func (b *Bus) webhookHandlerGet(jc jape.Context) {
	webhooks, queueInfos := b.webhooksMgr.Info()
	jc.Encode(api.WebhookResponse{
		Queues:   queueInfos,
		Webhooks: webhooks,
	})
}

func (b *Bus) webhookHandlerPost(jc jape.Context) {
	var req webhooks.Webhook
	if jc.Decode(&req) != nil {
		return
	}

	err := b.webhooksMgr.Register(jc.Request.Context(), webhooks.Webhook{
		Event:   req.Event,
		Module:  req.Module,
		URL:     req.URL,
		Headers: req.Headers,
	})
	if err != nil {
		jc.Error(fmt.Errorf("failed to add Webhook: %w", err), http.StatusInternalServerError)
		return
	}
}

func (b *Bus) metricsHandlerDELETE(jc jape.Context) {
	metric := jc.PathParam("key")
	if metric == "" {
		jc.Error(errors.New("parameter 'metric' is required"), http.StatusBadRequest)
		return
	}

	var cutoff time.Time
	if jc.DecodeForm("cutoff", (*api.TimeRFC3339)(&cutoff)) != nil {
		return
	} else if cutoff.IsZero() {
		jc.Error(errors.New("parameter 'cutoff' is required"), http.StatusBadRequest)
		return
	}

	err := b.store.PruneMetrics(jc.Request.Context(), metric, cutoff)
	if jc.Check("failed to prune metrics", err) != nil {
		return
	}
}

func (b *Bus) metricsHandlerPUT(jc jape.Context) {
	jc.Custom((*interface{})(nil), nil)

	key := jc.PathParam("key")
	switch key {
	case api.MetricContractPrune:
		// TODO: jape hack - remove once jape can handle decoding multiple different request types
		var req api.ContractPruneMetricRequestPUT
		if err := json.NewDecoder(jc.Request.Body).Decode(&req); err != nil {
			jc.Error(fmt.Errorf("couldn't decode request type (%T): %w", req, err), http.StatusBadRequest)
			return
		} else if jc.Check("failed to record contract prune metric", b.store.RecordContractPruneMetric(jc.Request.Context(), req.Metrics...)) != nil {
			return
		}
	case api.MetricContractSetChurn:
		// TODO: jape hack - remove once jape can handle decoding multiple different request types
		var req api.ContractSetChurnMetricRequestPUT
		if err := json.NewDecoder(jc.Request.Body).Decode(&req); err != nil {
			jc.Error(fmt.Errorf("couldn't decode request type (%T): %w", req, err), http.StatusBadRequest)
			return
		} else if jc.Check("failed to record contract churn metric", b.store.RecordContractSetChurnMetric(jc.Request.Context(), req.Metrics...)) != nil {
			return
		}
	default:
		jc.Error(fmt.Errorf("unknown metric key '%s'", key), http.StatusBadRequest)
		return
	}
}

func (b *Bus) metricsHandlerGET(jc jape.Context) {
	// parse mandatory query parameters
	var start time.Time
	if jc.DecodeForm("start", (*api.TimeRFC3339)(&start)) != nil {
		return
	} else if start.IsZero() {
		jc.Error(errors.New("parameter 'start' is required"), http.StatusBadRequest)
		return
	}

	var n uint64
	if jc.DecodeForm("n", &n) != nil {
		return
	} else if n == 0 {
		if jc.Request.FormValue("n") == "" {
			jc.Error(errors.New("parameter 'n' is required"), http.StatusBadRequest)
		} else {
			jc.Error(errors.New("'n' has to be greater than zero"), http.StatusBadRequest)
		}
		return
	}

	var interval time.Duration
	if jc.DecodeForm("interval", (*api.DurationMS)(&interval)) != nil {
		return
	} else if interval == 0 {
		jc.Error(errors.New("parameter 'interval' is required"), http.StatusBadRequest)
		return
	}

	// parse optional query parameters
	var metrics interface{}
	var err error
	key := jc.PathParam("key")
	switch key {
	case api.MetricContract:
		var opts api.ContractMetricsQueryOpts
		if jc.DecodeForm("contractid", &opts.ContractID) != nil {
			return
		} else if jc.DecodeForm("hostkey", &opts.HostKey) != nil {
			return
		}
		metrics, err = b.metrics(jc.Request.Context(), key, start, n, interval, opts)
	case api.MetricContractPrune:
		var opts api.ContractPruneMetricsQueryOpts
		if jc.DecodeForm("contractid", &opts.ContractID) != nil {
			return
		} else if jc.DecodeForm("hostkey", &opts.HostKey) != nil {
			return
		} else if jc.DecodeForm("hostversion", &opts.HostVersion) != nil {
			return
		}
		metrics, err = b.metrics(jc.Request.Context(), key, start, n, interval, opts)
	case api.MetricContractSet:
		var opts api.ContractSetMetricsQueryOpts
		if jc.DecodeForm("name", &opts.Name) != nil {
			return
		}
		metrics, err = b.metrics(jc.Request.Context(), key, start, n, interval, opts)
	case api.MetricContractSetChurn:
		var opts api.ContractSetChurnMetricsQueryOpts
		if jc.DecodeForm("name", &opts.Name) != nil {
			return
		} else if jc.DecodeForm("direction", &opts.Direction) != nil {
			return
		} else if jc.DecodeForm("reason", &opts.Reason) != nil {
			return
		}
		metrics, err = b.metrics(jc.Request.Context(), key, start, n, interval, opts)
	case api.MetricWallet:
		var opts api.WalletMetricsQueryOpts
		metrics, err = b.metrics(jc.Request.Context(), key, start, n, interval, opts)
	default:
		jc.Error(fmt.Errorf("unknown metric '%s'", key), http.StatusBadRequest)
		return
	}
	if errors.Is(err, api.ErrMaxIntervalsExceeded) {
		jc.Error(err, http.StatusBadRequest)
		return
	} else if jc.Check(fmt.Sprintf("failed to fetch '%s' metrics", key), err) != nil {
		return
	}
	jc.Encode(metrics)
}

func (b *Bus) metrics(ctx context.Context, key string, start time.Time, n uint64, interval time.Duration, opts interface{}) (interface{}, error) {
	switch key {
	case api.MetricContract:
		return b.store.ContractMetrics(ctx, start, n, interval, opts.(api.ContractMetricsQueryOpts))
	case api.MetricContractPrune:
		return b.store.ContractPruneMetrics(ctx, start, n, interval, opts.(api.ContractPruneMetricsQueryOpts))
	case api.MetricContractSet:
		return b.store.ContractSetMetrics(ctx, start, n, interval, opts.(api.ContractSetMetricsQueryOpts))
	case api.MetricContractSetChurn:
		return b.store.ContractSetChurnMetrics(ctx, start, n, interval, opts.(api.ContractSetChurnMetricsQueryOpts))
	case api.MetricWallet:
		return b.store.WalletMetrics(ctx, start, n, interval, opts.(api.WalletMetricsQueryOpts))
	}
	return nil, fmt.Errorf("unknown metric '%s'", key)
}

func (b *Bus) multipartHandlerCreatePOST(jc jape.Context) {
	var req api.MultipartCreateRequest
	if jc.Decode(&req) != nil {
		return
	}

	var key object.EncryptionKey
	if req.DisableClientSideEncryption {
		key = object.NoOpKey
	} else {
		key = object.GenerateEncryptionKey(object.EncryptionKeyTypeSalted)
	}

	resp, err := b.store.CreateMultipartUpload(jc.Request.Context(), req.Bucket, req.Key, key, req.MimeType, req.Metadata)
	if jc.Check("failed to create multipart upload", err) != nil {
		return
	}
	jc.Encode(resp)
}

func (b *Bus) multipartHandlerAbortPOST(jc jape.Context) {
	var req api.MultipartAbortRequest
	if jc.Decode(&req) != nil {
		return
	}
	err := b.store.AbortMultipartUpload(jc.Request.Context(), req.Bucket, req.Key, req.UploadID)
	if jc.Check("failed to abort multipart upload", err) != nil {
		return
	}
}

func (b *Bus) multipartHandlerCompletePOST(jc jape.Context) {
	var req api.MultipartCompleteRequest
	if jc.Decode(&req) != nil {
		return
	}
	resp, err := b.store.CompleteMultipartUpload(jc.Request.Context(), req.Bucket, req.Key, req.UploadID, req.Parts, api.CompleteMultipartOptions{
		Metadata: req.Metadata,
	})
	if jc.Check("failed to complete multipart upload", err) != nil {
		return
	}
	jc.Encode(resp)
}

func (b *Bus) multipartHandlerUploadPartPUT(jc jape.Context) {
	var req api.MultipartAddPartRequest
	if jc.Decode(&req) != nil {
		return
	}
	if req.Bucket == "" {
		jc.Error(api.ErrBucketMissing, http.StatusBadRequest)
		return
	} else if req.ContractSet == "" {
		jc.Error(errors.New("contract_set must be non-empty"), http.StatusBadRequest)
		return
	} else if req.ETag == "" {
		jc.Error(errors.New("etag must be non-empty"), http.StatusBadRequest)
		return
	} else if req.PartNumber <= 0 || req.PartNumber > gofakes3.MaxUploadPartNumber {
		jc.Error(fmt.Errorf("part_number must be between 1 and %d", gofakes3.MaxUploadPartNumber), http.StatusBadRequest)
		return
	} else if req.UploadID == "" {
		jc.Error(errors.New("upload_id must be non-empty"), http.StatusBadRequest)
		return
	}
	err := b.store.AddMultipartPart(jc.Request.Context(), req.Bucket, req.Key, req.ContractSet, req.ETag, req.UploadID, req.PartNumber, req.Slices)
	if jc.Check("failed to upload part", err) != nil {
		return
	}
}

func (b *Bus) multipartHandlerUploadGET(jc jape.Context) {
	resp, err := b.store.MultipartUpload(jc.Request.Context(), jc.PathParam("id"))
	if jc.Check("failed to get multipart upload", err) != nil {
		return
	}
	jc.Encode(resp)
}

func (b *Bus) multipartHandlerListUploadsPOST(jc jape.Context) {
	var req api.MultipartListUploadsRequest
	if jc.Decode(&req) != nil {
		return
	}
	resp, err := b.store.MultipartUploads(jc.Request.Context(), req.Bucket, req.Prefix, req.KeyMarker, req.UploadIDMarker, req.Limit)
	if jc.Check("failed to list multipart uploads", err) != nil {
		return
	}
	jc.Encode(resp)
}

func (b *Bus) multipartHandlerListPartsPOST(jc jape.Context) {
	var req api.MultipartListPartsRequest
	if jc.Decode(&req) != nil {
		return
	}
	resp, err := b.store.MultipartUploadParts(jc.Request.Context(), req.Bucket, req.Key, req.UploadID, req.PartNumberMarker, int64(req.Limit))
	if jc.Check("failed to list multipart upload parts", err) != nil {
		return
	}
	jc.Encode(resp)
}

func (b *Bus) contractsFormHandler(jc jape.Context) {
	// apply pessimistic timeout
	ctx, cancel := context.WithTimeout(jc.Request.Context(), 15*time.Minute)
	defer cancel()

	// decode the request
	var rfr api.ContractFormRequest
	if jc.Decode(&rfr) != nil {
		return
	}

	// validate the request
	if rfr.EndHeight == 0 {
		http.Error(jc.ResponseWriter, "EndHeight can not be zero", http.StatusBadRequest)
		return
	} else if rfr.HostKey == (types.PublicKey{}) {
		http.Error(jc.ResponseWriter, "HostKey must be provided", http.StatusBadRequest)
		return
	} else if rfr.HostCollateral.IsZero() {
		http.Error(jc.ResponseWriter, "HostCollateral can not be zero", http.StatusBadRequest)
		return
	} else if rfr.HostIP == "" {
		http.Error(jc.ResponseWriter, "HostIP must be provided", http.StatusBadRequest)
		return
	} else if rfr.RenterFunds.IsZero() {
		http.Error(jc.ResponseWriter, "RenterFunds can not be zero", http.StatusBadRequest)
		return
	} else if rfr.RenterAddress == (types.Address{}) {
		http.Error(jc.ResponseWriter, "RenterAddress must be provided", http.StatusBadRequest)
		return
	}

	// fetch gouging parameters
	gp, err := b.gougingParams(ctx)
	if jc.Check("could not get gouging parameters", err) != nil {
		return
	}
	gc := gouging.NewChecker(gp.GougingSettings, gp.ConsensusState, nil, nil)

	// fetch host settings
	settings, err := b.rhp2Client.Settings(ctx, rfr.HostKey, rfr.HostIP)
	if jc.Check("couldn't fetch host settings", err) != nil {
		return
	}

	// check gouging
	breakdown := gc.CheckSettings(settings)
	if breakdown.Gouging() {
		jc.Error(fmt.Errorf("failed to form contract, gouging check failed: %v", breakdown), http.StatusBadRequest)
		return
	}

	// send V2 transaction if we're passed the V2 hardfork allow height
	var rev rhpv2.ContractRevision
	if b.isPassedV2AllowHeight() {
		panic("not implemented")
	} else {
		rev, err = b.formContract(
			ctx,
			settings,
			rfr.RenterAddress,
			rfr.RenterFunds,
			rfr.HostCollateral,
			rfr.HostKey,
			rfr.HostIP,
			rfr.EndHeight,
		)
		if jc.Check("couldn't form contract", err) != nil {
			return
		}
	}

	// add the contract
	metadata, err := b.addContract(
		ctx,
		rev,
		rev.Revision.MissedHostPayout().Sub(rfr.HostCollateral),
		rfr.RenterFunds,
		b.cm.Tip().Height,
		api.ContractStatePending,
	)
	if jc.Check("couldn't add contract", err) != nil {
		return
	}

	// return the contract
	jc.Encode(metadata)
}
