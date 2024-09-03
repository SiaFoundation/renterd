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

	rhp3 "go.sia.tech/renterd/internal/rhp/v3"

	ibus "go.sia.tech/renterd/internal/bus"
	"go.sia.tech/renterd/internal/gouging"

	"go.sia.tech/core/gateway"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
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

func (b *Bus) fetchSetting(ctx context.Context, key string, value interface{}) error {
	if val, err := b.ss.Setting(ctx, key); err != nil {
		return fmt.Errorf("could not get contract set settings: %w", err)
	} else if err := json.Unmarshal([]byte(val), &value); err != nil {
		b.logger.Panicf("failed to unmarshal %v settings '%s': %v", key, val, err)
	}
	return nil
}

func (b *Bus) accountsFundHandler(jc jape.Context) {
	var req api.AccountsFundRequest
	if jc.Decode(&req) != nil {
		return
	}

	// contract metadata
	cm, err := b.ms.Contract(jc.Request.Context(), req.ContractID)
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
	rev, err := b.rhp3.Revision(jc.Request.Context(), req.ContractID, cm.HostKey, cm.SiamuxAddr)
	if jc.Check("failed to fetch contract revision", err) != nil {
		return
	}

	// ensure we have at least 2H in the contract to cover the costs
	if types.NewCurrency64(2).Cmp(rev.ValidRenterPayout()) >= 0 {
		jc.Error(fmt.Errorf("insufficient funds to fund account: %v <= %v", rev.ValidRenterPayout(), types.NewCurrency64(2)), http.StatusBadRequest)
		return
	}

	// price table
	pt, err := b.rhp3.PriceTable(jc.Request.Context(), cm.HostKey, cm.SiamuxAddr, rhp3.PreparePriceTableContractPayment(&rev, req.AccountID, rk))
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
	err = b.rhp3.FundAccount(jc.Request.Context(), &rev, cm.HostKey, cm.SiamuxAddr, deposit, req.AccountID, pt.HostPriceTable, rk)
	if jc.Check("failed to fund account", err) != nil {
		return
	}

	// record spending
	err = b.ms.RecordContractSpending(jc.Request.Context(), []api.ContractSpendingRecord{
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
	jc.Encode(b.s.Addr())
}

func (b *Bus) syncerPeersHandler(jc jape.Context) {
	var peers []string
	for _, p := range b.s.Peers() {
		peers = append(peers, p.String())
	}
	jc.Encode(peers)
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
	jc.Encode(cs)
}

func (b *Bus) consensusNetworkHandler(jc jape.Context) {
	jc.Encode(api.ConsensusNetwork{
		Name: b.cm.TipState().Network.Name,
	})
}

func (b *Bus) txpoolFeeHandler(jc jape.Context) {
	jc.Encode(b.cm.RecommendedFee())
}

func (b *Bus) txpoolTransactionsHandler(jc jape.Context) {
	jc.Encode(b.cm.PoolTransactions())
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
	resp, err := b.ms.ListBuckets(jc.Request.Context())
	if jc.Check("couldn't list buckets", err) != nil {
		return
	}
	jc.Encode(resp)
}

func (b *Bus) bucketsHandlerPOST(jc jape.Context) {
	var bucket api.BucketCreateRequest
	if jc.Decode(&bucket) != nil {
		return
	} else if bucket.Name == "" {
		jc.Error(errors.New("no name provided"), http.StatusBadRequest)
		return
	} else if jc.Check("failed to create bucket", b.ms.CreateBucket(jc.Request.Context(), bucket.Name, bucket.Policy)) != nil {
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
	} else if jc.Check("failed to create bucket", b.ms.UpdateBucketPolicy(jc.Request.Context(), bucket, req.Policy)) != nil {
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
	} else if jc.Check("failed to delete bucket", b.ms.DeleteBucket(jc.Request.Context(), name)) != nil {
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
	bucket, err := b.ms.Bucket(jc.Request.Context(), name)
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

	tip := b.w.Tip()
	jc.Encode(api.WalletResponse{
		ScanHeight:  tip.Height,
		Address:     address,
		Confirmed:   balance.Confirmed,
		Spendable:   balance.Spendable,
		Unconfirmed: balance.Unconfirmed,
		Immature:    balance.Immature,
	})
}

func (b *Bus) walletTransactionsHandler(jc jape.Context) {
	offset := 0
	limit := -1
	if jc.DecodeForm("offset", &offset) != nil ||
		jc.DecodeForm("limit", &limit) != nil {
		return
	}

	// TODO: deprecate these parameters when moving to v2.0.0
	var before, since time.Time
	if jc.DecodeForm("before", (*api.TimeRFC3339)(&before)) != nil ||
		jc.DecodeForm("since", (*api.TimeRFC3339)(&since)) != nil {
		return
	}

	// convertToTransaction converts wallet event data to a Transaction.
	convertToTransaction := func(kind string, data wallet.EventData) (txn types.Transaction, ok bool) {
		ok = true
		switch kind {
		case wallet.EventTypeMinerPayout,
			wallet.EventTypeFoundationSubsidy,
			wallet.EventTypeSiafundClaim:
			payout, _ := data.(wallet.EventPayout)
			txn = types.Transaction{SiacoinOutputs: []types.SiacoinOutput{payout.SiacoinElement.SiacoinOutput}}
		case wallet.EventTypeV1Transaction:
			v1Txn, _ := data.(wallet.EventV1Transaction)
			txn = types.Transaction(v1Txn.Transaction)
		case wallet.EventTypeV1ContractResolution:
			fce, _ := data.(wallet.EventV1ContractResolution)
			txn = types.Transaction{
				FileContracts:  []types.FileContract{fce.Parent.FileContract},
				SiacoinOutputs: []types.SiacoinOutput{fce.SiacoinElement.SiacoinOutput},
			}
		default:
			ok = false
		}
		return
	}

	// convertToTransactions converts wallet events to API transactions.
	convertToTransactions := func(events []wallet.Event) []api.Transaction {
		var transactions []api.Transaction
		for _, e := range events {
			if txn, ok := convertToTransaction(e.Type, e.Data); ok {
				transactions = append(transactions, api.Transaction{
					Raw:       txn,
					Index:     e.Index,
					ID:        types.TransactionID(e.ID),
					Inflow:    e.SiacoinInflow(),
					Outflow:   e.SiacoinOutflow(),
					Timestamp: e.Timestamp,
				})
			}
		}
		return transactions
	}

	if before.IsZero() && since.IsZero() {
		events, err := b.w.Events(offset, limit)
		if jc.Check("couldn't load transactions", err) == nil {
			jc.Encode(convertToTransactions(events))
		}
		return
	}

	// TODO: remove this when 'before' and 'since' are deprecated, until then we
	// fetch all transactions and paginate manually if either is specified
	events, err := b.w.Events(0, -1)
	if jc.Check("couldn't load transactions", err) != nil {
		return
	}
	filtered := events[:0]
	for _, txn := range events {
		if (before.IsZero() || txn.Timestamp.Before(before)) &&
			(since.IsZero() || txn.Timestamp.After(since)) {
			filtered = append(filtered, txn)
		}
	}
	events = filtered
	if limit == 0 || limit == -1 {
		jc.Encode(convertToTransactions(events[offset:]))
	} else {
		jc.Encode(convertToTransactions(events[offset : offset+limit]))
	}
}

func (b *Bus) walletOutputsHandler(jc jape.Context) {
	utxos, err := b.w.SpendableOutputs()
	if jc.Check("couldn't load outputs", err) == nil {
		// convert to siacoin elements
		elements := make([]api.SiacoinElement, len(utxos))
		for i, sce := range utxos {
			elements[i] = api.SiacoinElement{
				ID: sce.StateElement.ID,
				SiacoinOutput: types.SiacoinOutput{
					Value:   sce.SiacoinOutput.Value,
					Address: sce.SiacoinOutput.Address,
				},
				MaturityHeight: sce.MaturityHeight,
			}
		}
		jc.Encode(elements)
	}
}

func (b *Bus) walletFundHandler(jc jape.Context) {
	var wfr api.WalletFundRequest
	if jc.Decode(&wfr) != nil {
		return
	}
	txn := wfr.Transaction

	if len(txn.MinerFees) == 0 {
		// if no fees are specified, we add some
		fee := b.cm.RecommendedFee().Mul64(b.cm.TipState().TransactionWeight(txn))
		txn.MinerFees = []types.Currency{fee}
	}

	toSign, err := b.w.FundTransaction(&txn, wfr.Amount.Add(txn.MinerFees[0]), wfr.UseUnconfirmedTxns)
	if jc.Check("couldn't fund transaction", err) != nil {
		return
	}

	jc.Encode(api.WalletFundResponse{
		Transaction: txn,
		ToSign:      toSign,
		DependsOn:   b.cm.UnconfirmedParents(txn),
	})
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

func (b *Bus) walletSignHandler(jc jape.Context) {
	var wsr api.WalletSignRequest
	if jc.Decode(&wsr) != nil {
		return
	}
	b.w.SignTransaction(&wsr.Transaction, wsr.ToSign, wsr.CoveredFields)
	jc.Encode(wsr.Transaction)
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

	var ids []types.TransactionID
	if state := b.cm.TipState(); state.Index.Height < state.Network.HardforkV2.AllowHeight {
		// v1 redistribution
		txns, toSign, err := b.w.Redistribute(wfr.Outputs, wfr.Amount, b.cm.RecommendedFee())
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
		txns, toSign, err := b.w.RedistributeV2(wfr.Outputs, wfr.Amount, b.cm.RecommendedFee())
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

func (b *Bus) walletDiscardHandler(jc jape.Context) {
	var txn types.Transaction
	if jc.Decode(&txn) == nil {
		b.w.ReleaseInputs([]types.Transaction{txn}, nil)
	}
}

func (b *Bus) walletPendingHandler(jc jape.Context) {
	isRelevant := func(txn types.Transaction) bool {
		addr := b.w.Address()
		for _, sci := range txn.SiacoinInputs {
			if sci.UnlockConditions.UnlockHash() == addr {
				return true
			}
		}
		for _, sco := range txn.SiacoinOutputs {
			if sco.Address == addr {
				return true
			}
		}
		return false
	}

	txns := b.cm.PoolTransactions()
	relevant := txns[:0]
	for _, txn := range txns {
		if isRelevant(txn) {
			relevant = append(relevant, txn)
		}
	}
	jc.Encode(relevant)
}

func (b *Bus) hostsHandlerGETDeprecated(jc jape.Context) {
	offset := 0
	limit := -1
	if jc.DecodeForm("offset", &offset) != nil || jc.DecodeForm("limit", &limit) != nil {
		return
	}

	// fetch hosts
	hosts, err := b.hs.SearchHosts(jc.Request.Context(), "", api.HostFilterModeAllowed, api.UsabilityFilterModeAll, "", nil, offset, limit)
	if jc.Check(fmt.Sprintf("couldn't fetch hosts %d-%d", offset, offset+limit), err) != nil {
		return
	}
	jc.Encode(hosts)
}

func (b *Bus) searchHostsHandlerPOST(jc jape.Context) {
	var req api.SearchHostsRequest
	if jc.Decode(&req) != nil {
		return
	}

	// TODO: on the next major release:
	// - properly default search params (currently no defaults are set)
	// - properly validate and return 400 (currently validation is done in autopilot and the store)

	hosts, err := b.hs.SearchHosts(jc.Request.Context(), req.AutopilotID, req.FilterMode, req.UsabilityMode, req.AddressContains, req.KeyIn, req.Offset, req.Limit)
	if jc.Check(fmt.Sprintf("couldn't fetch hosts %d-%d", req.Offset, req.Offset+req.Limit), err) != nil {
		return
	}
	jc.Encode(hosts)
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
	removed, err := b.hs.RemoveOfflineHosts(jc.Request.Context(), hrr.MaxConsecutiveScanFailures, time.Duration(hrr.MaxDowntimeHours))
	if jc.Check("couldn't remove offline hosts", err) != nil {
		return
	}
	jc.Encode(removed)
}

func (b *Bus) hostsScanningHandlerGET(jc jape.Context) {
	offset := 0
	limit := -1
	maxLastScan := time.Now()
	if jc.DecodeForm("offset", &offset) != nil || jc.DecodeForm("limit", &limit) != nil || jc.DecodeForm("lastScan", (*api.TimeRFC3339)(&maxLastScan)) != nil {
		return
	}
	hosts, err := b.hs.HostsForScanning(jc.Request.Context(), maxLastScan, offset, limit)
	if jc.Check(fmt.Sprintf("couldn't fetch hosts %d-%d", offset, offset+limit), err) != nil {
		return
	}
	jc.Encode(hosts)
}

func (b *Bus) hostsPubkeyHandlerGET(jc jape.Context) {
	var hostKey types.PublicKey
	if jc.DecodeParam("hostkey", &hostKey) != nil {
		return
	}
	host, err := b.hs.Host(jc.Request.Context(), hostKey)
	if jc.Check("couldn't load host", err) == nil {
		jc.Encode(host)
	}
}

func (b *Bus) hostsResetLostSectorsPOST(jc jape.Context) {
	var hostKey types.PublicKey
	if jc.DecodeParam("hostkey", &hostKey) != nil {
		return
	}
	err := b.hs.ResetLostSectors(jc.Request.Context(), hostKey)
	if jc.Check("couldn't reset lost sectors", err) != nil {
		return
	}
}

func (b *Bus) hostsScanHandlerPOST(jc jape.Context) {
	var req api.HostsScanRequest
	if jc.Decode(&req) != nil {
		return
	}
	if jc.Check("failed to record scans", b.hs.RecordHostScans(jc.Request.Context(), req.Scans)) != nil {
		return
	}
}

func (b *Bus) hostsPricetableHandlerPOST(jc jape.Context) {
	var req api.HostsPriceTablesRequest
	if jc.Decode(&req) != nil {
		return
	}
	if jc.Check("failed to record interactions", b.hs.RecordPriceTables(jc.Request.Context(), req.PriceTableUpdates)) != nil {
		return
	}
}

func (b *Bus) contractsSpendingHandlerPOST(jc jape.Context) {
	var records []api.ContractSpendingRecord
	if jc.Decode(&records) != nil {
		return
	}
	if jc.Check("failed to record spending metrics for contract", b.ms.RecordContractSpending(jc.Request.Context(), records)) != nil {
		return
	}
}

func (b *Bus) hostsAllowlistHandlerGET(jc jape.Context) {
	allowlist, err := b.hs.HostAllowlist(jc.Request.Context())
	if jc.Check("couldn't load allowlist", err) == nil {
		jc.Encode(allowlist)
	}
}

func (b *Bus) hostsAllowlistHandlerPUT(jc jape.Context) {
	ctx := jc.Request.Context()
	var req api.UpdateAllowlistRequest
	if jc.Decode(&req) == nil {
		if len(req.Add)+len(req.Remove) > 0 && req.Clear {
			jc.Error(errors.New("cannot add or remove entries while clearing the allowlist"), http.StatusBadRequest)
			return
		} else if jc.Check("couldn't update allowlist entries", b.hs.UpdateHostAllowlistEntries(ctx, req.Add, req.Remove, req.Clear)) != nil {
			return
		}
	}
}

func (b *Bus) hostsBlocklistHandlerGET(jc jape.Context) {
	blocklist, err := b.hs.HostBlocklist(jc.Request.Context())
	if jc.Check("couldn't load blocklist", err) == nil {
		jc.Encode(blocklist)
	}
}

func (b *Bus) hostsBlocklistHandlerPUT(jc jape.Context) {
	ctx := jc.Request.Context()
	var req api.UpdateBlocklistRequest
	if jc.Decode(&req) == nil {
		if len(req.Add)+len(req.Remove) > 0 && req.Clear {
			jc.Error(errors.New("cannot add or remove entries while clearing the blocklist"), http.StatusBadRequest)
			return
		} else if jc.Check("couldn't update blocklist entries", b.hs.UpdateHostBlocklistEntries(ctx, req.Add, req.Remove, req.Clear)) != nil {
			return
		}
	}
}

func (b *Bus) contractsHandlerGET(jc jape.Context) {
	var cs string
	if jc.DecodeForm("contractset", &cs) != nil {
		return
	}
	contracts, err := b.ms.Contracts(jc.Request.Context(), api.ContractsOpts{
		ContractSet: cs,
	})
	if jc.Check("couldn't load contracts", err) == nil {
		jc.Encode(contracts)
	}
}

func (b *Bus) contractsRenewedIDHandlerGET(jc jape.Context) {
	var id types.FileContractID
	if jc.DecodeParam("id", &id) != nil {
		return
	}

	md, err := b.ms.RenewedContract(jc.Request.Context(), id)
	if jc.Check("faild to fetch renewed contract", err) == nil {
		jc.Encode(md)
	}
}

func (b *Bus) contractsArchiveHandlerPOST(jc jape.Context) {
	var toArchive api.ContractsArchiveRequest
	if jc.Decode(&toArchive) != nil {
		return
	}

	if jc.Check("failed to archive contracts", b.ms.ArchiveContracts(jc.Request.Context(), toArchive)) == nil {
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
	sets, err := b.ms.ContractSets(jc.Request.Context())
	if jc.Check("couldn't fetch contract sets", err) == nil {
		jc.Encode(sets)
	}
}

func (b *Bus) contractsSetHandlerPUT(jc jape.Context) {
	var contractIds []types.FileContractID
	if set := jc.PathParam("set"); set == "" {
		jc.Error(errors.New("path parameter 'set' can not be empty"), http.StatusBadRequest)
		return
	} else if jc.Decode(&contractIds) != nil {
		return
	} else if jc.Check("could not add contracts to set", b.ms.SetContractSet(jc.Request.Context(), set, contractIds)) != nil {
		return
	} else {
		b.broadcastAction(webhooks.Event{
			Module: api.ModuleContractSet,
			Event:  api.EventUpdate,
			Payload: api.EventContractSetUpdate{
				Name:        set,
				ContractIDs: contractIds,
				Timestamp:   time.Now().UTC(),
			},
		})
	}
}

func (b *Bus) contractsSetHandlerDELETE(jc jape.Context) {
	if set := jc.PathParam("set"); set != "" {
		jc.Check("could not remove contract set", b.ms.RemoveContractSet(jc.Request.Context(), set))
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

func (b *Bus) contractsPrunableDataHandlerGET(jc jape.Context) {
	sizes, err := b.ms.ContractSizes(jc.Request.Context())
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

	jc.Encode(api.ContractsPrunableDataResponse{
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

	size, err := b.ms.ContractSize(jc.Request.Context(), id)
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
	c, err := b.ms.Contract(jc.Request.Context(), id)
	if jc.Check("couldn't load contract", err) == nil {
		jc.Encode(c)
	}
}

func (b *Bus) contractIDHandlerPOST(jc jape.Context) {
	var id types.FileContractID
	var req api.ContractAddRequest
	if jc.DecodeParam("id", &id) != nil || jc.Decode(&req) != nil {
		return
	} else if req.Contract.ID() != id {
		http.Error(jc.ResponseWriter, "contract ID mismatch", http.StatusBadRequest)
		return
	} else if req.TotalCost.IsZero() {
		http.Error(jc.ResponseWriter, "TotalCost can not be zero", http.StatusBadRequest)
		return
	}

	a, err := b.addContract(jc.Request.Context(), req.Contract, req.ContractPrice, req.TotalCost, req.StartHeight, req.State)
	if jc.Check("couldn't store contract", err) != nil {
		return
	}
	jc.Encode(a)
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
	} else if rrr.ExpectedNewStorage == 0 {
		http.Error(jc.ResponseWriter, "ExpectedNewStorage can not be zero", http.StatusBadRequest)
	} else if rrr.MaxFundAmount.IsZero() {
		http.Error(jc.ResponseWriter, "MaxFundAmount can not be zero", http.StatusBadRequest)
	} else if rrr.MinNewCollateral.IsZero() {
		http.Error(jc.ResponseWriter, "MinNewCollateral can not be zero", http.StatusBadRequest)
	} else if rrr.RenterFunds.IsZero() {
		http.Error(jc.ResponseWriter, "RenterFunds can not be zero", http.StatusBadRequest)
		return
	}

	// fetch the contract
	c, err := b.ms.Contract(ctx, fcid)
	if errors.Is(err, api.ErrContractNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if jc.Check("couldn't fetch contract", err) != nil {
		return
	}

	// fetch the host
	h, err := b.hs.Host(ctx, c.HostKey)
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
	var contractPrice, fundAmount types.Currency
	if b.isPassedV2AllowHeight() {
		panic("not implemented")
	} else {
		newRevision, contractPrice, fundAmount, err = b.renewContract(ctx, cs, gp, c, h.Settings, rrr.RenterFunds, rrr.MinNewCollateral, rrr.MaxFundAmount, rrr.EndHeight, rrr.ExpectedNewStorage)
		if errors.Is(err, api.ErrMaxFundAmountExceeded) {
			jc.Error(err, http.StatusBadRequest)
			return
		} else if jc.Check("couldn't renew contract", err) != nil {
			return
		}
	}

	// add renewal contract to store
	metadata, err := b.addRenewedContract(ctx, fcid, newRevision, contractPrice, fundAmount, cs.Index.Height, api.ContractStatePending)
	if jc.Check("couldn't store contract", err) != nil {
		return
	}

	// send the response
	jc.Encode(metadata)
}

func (b *Bus) contractIDRenewedHandlerPOST(jc jape.Context) {
	var id types.FileContractID
	var req api.ContractRenewedRequest
	if jc.DecodeParam("id", &id) != nil || jc.Decode(&req) != nil {
		return
	}
	if req.Contract.ID() != id {
		http.Error(jc.ResponseWriter, "contract ID mismatch", http.StatusBadRequest)
		return
	}
	if req.TotalCost.IsZero() {
		http.Error(jc.ResponseWriter, "TotalCost can not be zero", http.StatusBadRequest)
		return
	}
	if req.State == "" {
		req.State = api.ContractStatePending
	}
	r, err := b.addRenewedContract(jc.Request.Context(), req.RenewedFrom, req.Contract, req.ContractPrice, req.TotalCost, req.StartHeight, req.State)
	if jc.Check("couldn't store contract", err) != nil {
		return
	}

	jc.Encode(r)
}

func (b *Bus) contractIDRootsHandlerGET(jc jape.Context) {
	var id types.FileContractID
	if jc.DecodeParam("id", &id) != nil {
		return
	}

	roots, err := b.ms.ContractRoots(jc.Request.Context(), id)
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
	jc.Check("couldn't remove contract", b.ms.ArchiveContract(jc.Request.Context(), id, api.ContractArchivalReasonRemoved))
}

func (b *Bus) contractsAllHandlerDELETE(jc jape.Context) {
	jc.Check("couldn't remove contracts", b.ms.ArchiveAllContracts(jc.Request.Context(), api.ContractArchivalReasonRemoved))
}

func (b *Bus) searchObjectsHandlerGET(jc jape.Context) {
	offset := 0
	limit := -1
	var key string
	if jc.DecodeForm("offset", &offset) != nil || jc.DecodeForm("limit", &limit) != nil || jc.DecodeForm("key", &key) != nil {
		return
	}
	bucket := api.DefaultBucketName
	if jc.DecodeForm("bucket", &bucket) != nil {
		return
	}
	keys, err := b.ms.SearchObjects(jc.Request.Context(), bucket, key, offset, limit)
	if jc.Check("couldn't list objects", err) != nil {
		return
	}
	jc.Encode(keys)
}

func (b *Bus) objectsHandlerGET(jc jape.Context) {
	var ignoreDelim bool
	if jc.DecodeForm("ignoreDelim", &ignoreDelim) != nil {
		return
	}
	path := jc.PathParam("path")
	if strings.HasSuffix(path, "/") && !ignoreDelim {
		b.objectEntriesHandlerGET(jc, path)
		return
	}
	bucket := api.DefaultBucketName
	if jc.DecodeForm("bucket", &bucket) != nil {
		return
	}
	var onlymetadata bool
	if jc.DecodeForm("onlymetadata", &onlymetadata) != nil {
		return
	}

	var o api.Object
	var err error
	if onlymetadata {
		o, err = b.ms.ObjectMetadata(jc.Request.Context(), bucket, path)
	} else {
		o, err = b.ms.Object(jc.Request.Context(), bucket, path)
	}
	if errors.Is(err, api.ErrObjectNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if jc.Check("couldn't load object", err) != nil {
		return
	}
	jc.Encode(api.ObjectsResponse{Object: &o})
}

func (b *Bus) objectEntriesHandlerGET(jc jape.Context, path string) {
	bucket := api.DefaultBucketName
	if jc.DecodeForm("bucket", &bucket) != nil {
		return
	}

	var prefix string
	if jc.DecodeForm("prefix", &prefix) != nil {
		return
	}

	var sortBy string
	if jc.DecodeForm("sortBy", &sortBy) != nil {
		return
	}

	var sortDir string
	if jc.DecodeForm("sortDir", &sortDir) != nil {
		return
	}

	var marker string
	if jc.DecodeForm("marker", &marker) != nil {
		return
	}

	var offset int
	if jc.DecodeForm("offset", &offset) != nil {
		return
	}
	limit := -1
	if jc.DecodeForm("limit", &limit) != nil {
		return
	}

	// look for object entries
	entries, hasMore, err := b.ms.ObjectEntries(jc.Request.Context(), bucket, path, prefix, sortBy, sortDir, marker, offset, limit)
	if jc.Check("couldn't list object entries", err) != nil {
		return
	}

	jc.Encode(api.ObjectsResponse{Entries: entries, HasMore: hasMore})
}

func (b *Bus) objectsHandlerPUT(jc jape.Context) {
	var aor api.AddObjectRequest
	if jc.Decode(&aor) != nil {
		return
	} else if aor.Bucket == "" {
		aor.Bucket = api.DefaultBucketName
	}
	jc.Check("couldn't store object", b.ms.UpdateObject(jc.Request.Context(), aor.Bucket, jc.PathParam("path"), aor.ContractSet, aor.ETag, aor.MimeType, aor.Metadata, aor.Object))
}

func (b *Bus) objectsCopyHandlerPOST(jc jape.Context) {
	var orr api.CopyObjectsRequest
	if jc.Decode(&orr) != nil {
		return
	}
	om, err := b.ms.CopyObject(jc.Request.Context(), orr.SourceBucket, orr.DestinationBucket, orr.SourcePath, orr.DestinationPath, orr.MimeType, orr.Metadata)
	if jc.Check("couldn't copy object", err) != nil {
		return
	}

	jc.ResponseWriter.Header().Set("Last-Modified", om.ModTime.Std().Format(http.TimeFormat))
	jc.ResponseWriter.Header().Set("ETag", api.FormatETag(om.ETag))
	jc.Encode(om)
}

func (b *Bus) objectsListHandlerPOST(jc jape.Context) {
	var req api.ObjectsListRequest
	if jc.Decode(&req) != nil {
		return
	}
	if req.Bucket == "" {
		req.Bucket = api.DefaultBucketName
	}
	resp, err := b.ms.ListObjects(jc.Request.Context(), req.Bucket, req.Prefix, req.SortBy, req.SortDir, req.Marker, req.Limit)
	if errors.Is(err, api.ErrMarkerNotFound) {
		jc.Error(err, http.StatusBadRequest)
		return
	} else if jc.Check("couldn't list objects", err) != nil {
		return
	}
	jc.Encode(resp)
}

func (b *Bus) objectsRenameHandlerPOST(jc jape.Context) {
	var orr api.ObjectsRenameRequest
	if jc.Decode(&orr) != nil {
		return
	} else if orr.Bucket == "" {
		orr.Bucket = api.DefaultBucketName
	}
	if orr.Mode == api.ObjectsRenameModeSingle {
		// Single object rename.
		if strings.HasSuffix(orr.From, "/") || strings.HasSuffix(orr.To, "/") {
			jc.Error(fmt.Errorf("can't rename dirs with mode %v", orr.Mode), http.StatusBadRequest)
			return
		}
		jc.Check("couldn't rename object", b.ms.RenameObject(jc.Request.Context(), orr.Bucket, orr.From, orr.To, orr.Force))
		return
	} else if orr.Mode == api.ObjectsRenameModeMulti {
		// Multi object rename.
		if !strings.HasSuffix(orr.From, "/") || !strings.HasSuffix(orr.To, "/") {
			jc.Error(fmt.Errorf("can't rename file with mode %v", orr.Mode), http.StatusBadRequest)
			return
		}
		jc.Check("couldn't rename objects", b.ms.RenameObjects(jc.Request.Context(), orr.Bucket, orr.From, orr.To, orr.Force))
		return
	} else {
		// Invalid mode.
		jc.Error(fmt.Errorf("invalid mode: %v", orr.Mode), http.StatusBadRequest)
		return
	}
}

func (b *Bus) objectsHandlerDELETE(jc jape.Context) {
	var batch bool
	if jc.DecodeForm("batch", &batch) != nil {
		return
	}
	bucket := api.DefaultBucketName
	if jc.DecodeForm("bucket", &bucket) != nil {
		return
	}
	var err error
	if batch {
		err = b.ms.RemoveObjects(jc.Request.Context(), bucket, jc.PathParam("path"))
	} else {
		err = b.ms.RemoveObject(jc.Request.Context(), bucket, jc.PathParam("path"))
	}
	if errors.Is(err, api.ErrObjectNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	}
	jc.Check("couldn't delete object", err)
}

func (b *Bus) slabbuffersHandlerGET(jc jape.Context) {
	buffers, err := b.ms.SlabBuffers(jc.Request.Context())
	if jc.Check("couldn't get slab buffers info", err) != nil {
		return
	}
	jc.Encode(buffers)
}

func (b *Bus) objectsStatshandlerGET(jc jape.Context) {
	opts := api.ObjectsStatsOpts{}
	if jc.DecodeForm("bucket", &opts.Bucket) != nil {
		return
	}
	info, err := b.ms.ObjectsStats(jc.Request.Context(), opts)
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
	slabs, err := b.ms.PackedSlabsForUpload(jc.Request.Context(), time.Duration(psrg.LockingDuration), psrg.MinShards, psrg.TotalShards, psrg.ContractSet, psrg.Limit)
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
	jc.Check("failed to mark packed slab(s) as uploaded", b.ms.MarkPackedSlabsUploaded(jc.Request.Context(), psrp.Slabs))
}

func (b *Bus) sectorsHostRootHandlerDELETE(jc jape.Context) {
	var hk types.PublicKey
	var root types.Hash256
	if jc.DecodeParam("hk", &hk) != nil {
		return
	} else if jc.DecodeParam("root", &root) != nil {
		return
	}
	n, err := b.ms.DeleteHostSector(jc.Request.Context(), hk, root)
	if jc.Check("failed to mark sector as lost", err) != nil {
		return
	} else if n > 0 {
		b.logger.Infow("successfully marked sector as lost", "hk", hk, "root", root)
	}
}

func (b *Bus) slabObjectsHandlerGET(jc jape.Context) {
	var key object.EncryptionKey
	if jc.DecodeParam("key", &key) != nil {
		return
	}
	bucket := api.DefaultBucketName
	if jc.DecodeForm("bucket", &bucket) != nil {
		return
	}
	objects, err := b.ms.ObjectsBySlabKey(jc.Request.Context(), bucket, key)
	if jc.Check("failed to retrieve objects by slab", err) != nil {
		return
	}
	jc.Encode(objects)
}

func (b *Bus) slabHandlerGET(jc jape.Context) {
	var key object.EncryptionKey
	if jc.DecodeParam("key", &key) != nil {
		return
	}
	slab, err := b.ms.Slab(jc.Request.Context(), key)
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
	var usr api.UpdateSlabRequest
	if jc.Decode(&usr) == nil {
		jc.Check("couldn't update slab", b.ms.UpdateSlab(jc.Request.Context(), usr.Slab, usr.ContractSet))
	}
}

func (b *Bus) slabsRefreshHealthHandlerPOST(jc jape.Context) {
	jc.Check("failed to recompute health", b.ms.RefreshHealth(jc.Request.Context()))
}

func (b *Bus) slabsMigrationHandlerPOST(jc jape.Context) {
	var msr api.MigrationSlabsRequest
	if jc.Decode(&msr) == nil {
		if slabs, err := b.ms.UnhealthySlabs(jc.Request.Context(), msr.HealthCutoff, msr.ContractSet, msr.Limit); jc.Check("couldn't fetch slabs for migration", err) == nil {
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
	}
	var length int
	if jc.DecodeForm("length", &length) != nil {
		return
	}
	if length <= 0 || offset < 0 {
		jc.Error(fmt.Errorf("length must be positive and offset must be non-negative"), http.StatusBadRequest)
		return
	}
	data, err := b.ms.FetchPartialSlab(jc.Request.Context(), key, uint32(offset), uint32(length))
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
	if jc.DecodeForm("minShards", &minShards) != nil {
		return
	}
	var totalShards int
	if jc.DecodeForm("totalShards", &totalShards) != nil {
		return
	}
	var contractSet string
	if jc.DecodeForm("contractSet", &contractSet) != nil {
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
	slabs, bufferSize, err := b.ms.AddPartialSlab(jc.Request.Context(), data, uint8(minShards), uint8(totalShards), contractSet)
	if jc.Check("failed to add partial slab", err) != nil {
		return
	}
	var pus api.UploadPackingSettings
	if err := b.fetchSetting(jc.Request.Context(), api.SettingUploadPacking, &pus); err != nil && !errors.Is(err, api.ErrSettingNotFound) {
		jc.Error(fmt.Errorf("could not get upload packing settings: %w", err), http.StatusInternalServerError)
		return
	}
	jc.Encode(api.AddPartialSlabResponse{
		Slabs:                        slabs,
		SlabBufferMaxSizeSoftReached: bufferSize >= pus.SlabBufferMaxSizeSoft,
	})
}

func (b *Bus) settingsHandlerGET(jc jape.Context) {
	if settings, err := b.ss.Settings(jc.Request.Context()); jc.Check("couldn't load settings", err) == nil {
		jc.Encode(settings)
	}
}

func (b *Bus) settingKeyHandlerGET(jc jape.Context) {
	jc.Custom(nil, (any)(nil))

	key := jc.PathParam("key")
	if key == "" {
		jc.Error(errors.New("path parameter 'key' can not be empty"), http.StatusBadRequest)
		return
	}

	setting, err := b.ss.Setting(jc.Request.Context(), jc.PathParam("key"))
	if errors.Is(err, api.ErrSettingNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}
	resp := []byte(setting)

	// populate autopilots of price pinning settings with defaults for better DX
	if key == api.SettingPricePinning {
		var pps api.PricePinSettings
		err = json.Unmarshal([]byte(setting), &pps)
		if jc.Check("failed to unmarshal price pinning settings", err) != nil {
			return
		} else if pps.Autopilots == nil {
			pps.Autopilots = make(map[string]api.AutopilotPins)
		}
		// populate the Autopilots map with the current autopilots
		aps, err := b.as.Autopilots(jc.Request.Context())
		if jc.Check("failed to fetch autopilots", err) != nil {
			return
		}
		for _, ap := range aps {
			if _, exists := pps.Autopilots[ap.ID]; !exists {
				pps.Autopilots[ap.ID] = api.AutopilotPins{}
			}
		}
		// encode the settings back
		resp, err = json.Marshal(pps)
		if jc.Check("failed to marshal price pinning settings", err) != nil {
			return
		}
	}
	jc.ResponseWriter.Header().Set("Content-Type", "application/json")
	jc.ResponseWriter.Write(resp)
}

func (b *Bus) settingKeyHandlerPUT(jc jape.Context) {
	key := jc.PathParam("key")
	if key == "" {
		jc.Error(errors.New("path parameter 'key' can not be empty"), http.StatusBadRequest)
		return
	}

	var value interface{}
	if jc.Decode(&value) != nil {
		return
	}

	data, err := json.Marshal(value)
	if err != nil {
		jc.Error(fmt.Errorf("couldn't marshal the given value, error: %v", err), http.StatusBadRequest)
		return
	}

	switch key {
	case api.SettingGouging:
		var gs api.GougingSettings
		if err := json.Unmarshal(data, &gs); err != nil {
			jc.Error(fmt.Errorf("couldn't update gouging settings, invalid request body, %t", value), http.StatusBadRequest)
			return
		} else if err := gs.Validate(); err != nil {
			jc.Error(fmt.Errorf("couldn't update gouging settings, error: %v", err), http.StatusBadRequest)
			return
		}
		b.pinMgr.TriggerUpdate()
	case api.SettingRedundancy:
		var rs api.RedundancySettings
		if err := json.Unmarshal(data, &rs); err != nil {
			jc.Error(fmt.Errorf("couldn't update redundancy settings, invalid request body"), http.StatusBadRequest)
			return
		} else if err := rs.Validate(); err != nil {
			jc.Error(fmt.Errorf("couldn't update redundancy settings, error: %v", err), http.StatusBadRequest)
			return
		}
	case api.SettingS3Authentication:
		var s3as api.S3AuthenticationSettings
		if err := json.Unmarshal(data, &s3as); err != nil {
			jc.Error(fmt.Errorf("couldn't update s3 authentication settings, invalid request body"), http.StatusBadRequest)
			return
		} else if err := s3as.Validate(); err != nil {
			jc.Error(fmt.Errorf("couldn't update s3 authentication settings, error: %v", err), http.StatusBadRequest)
			return
		}
	case api.SettingPricePinning:
		var pps api.PricePinSettings
		if err := json.Unmarshal(data, &pps); err != nil {
			jc.Error(fmt.Errorf("couldn't update price pinning settings, invalid request body"), http.StatusBadRequest)
			return
		} else if err := pps.Validate(); err != nil {
			jc.Error(fmt.Errorf("couldn't update price pinning settings, invalid settings, error: %v", err), http.StatusBadRequest)
			return
		} else if pps.Enabled {
			if _, err := ibus.NewForexClient(pps.ForexEndpointURL).SiacoinExchangeRate(jc.Request.Context(), pps.Currency); err != nil {
				jc.Error(fmt.Errorf("couldn't update price pinning settings, forex API unreachable,error: %v", err), http.StatusBadRequest)
				return
			}
		}
		b.pinMgr.TriggerUpdate()
	}

	if jc.Check("could not update setting", b.ss.UpdateSetting(jc.Request.Context(), key, string(data))) == nil {
		b.broadcastAction(webhooks.Event{
			Module: api.ModuleSetting,
			Event:  api.EventUpdate,
			Payload: api.EventSettingUpdate{
				Key:       key,
				Update:    value,
				Timestamp: time.Now().UTC(),
			},
		})
	}
}

func (b *Bus) settingKeyHandlerDELETE(jc jape.Context) {
	key := jc.PathParam("key")
	if key == "" {
		jc.Error(errors.New("path parameter 'key' can not be empty"), http.StatusBadRequest)
		return
	}

	if jc.Check("could not delete setting", b.ss.DeleteSetting(jc.Request.Context(), key)) == nil {
		b.broadcastAction(webhooks.Event{
			Module: api.ModuleSetting,
			Event:  api.EventDelete,
			Payload: api.EventSettingDelete{
				Key:       key,
				Timestamp: time.Now().UTC(),
			},
		})
	}
}

func (b *Bus) contractIDAncestorsHandler(jc jape.Context) {
	var fcid types.FileContractID
	if jc.DecodeParam("id", &fcid) != nil {
		return
	}
	var minStartHeight uint64
	if jc.DecodeForm("minStartHeight", &minStartHeight) != nil {
		return
	}
	ancestors, err := b.ms.AncestorContracts(jc.Request.Context(), fcid, uint64(minStartHeight))
	if jc.Check("failed to fetch ancestor contracts", err) != nil {
		return
	}
	jc.Encode(ancestors)
}

func (b *Bus) paramsHandlerUploadGET(jc jape.Context) {
	gp, err := b.gougingParams(jc.Request.Context())
	if jc.Check("could not get gouging parameters", err) != nil {
		return
	}

	var contractSet string
	var css api.ContractSetSetting
	if err := b.fetchSetting(jc.Request.Context(), api.SettingContractSet, &css); err != nil && !errors.Is(err, api.ErrSettingNotFound) {
		jc.Error(fmt.Errorf("could not get contract set settings: %w", err), http.StatusInternalServerError)
		return
	} else if err == nil {
		contractSet = css.Default
	}

	var uploadPacking bool
	var pus api.UploadPackingSettings
	if err := b.fetchSetting(jc.Request.Context(), api.SettingUploadPacking, &pus); err != nil && !errors.Is(err, api.ErrSettingNotFound) {
		jc.Error(fmt.Errorf("could not get upload packing settings: %w", err), http.StatusInternalServerError)
		return
	} else if err == nil {
		uploadPacking = pus.Enabled
	}

	jc.Encode(api.UploadParams{
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
	jc.Encode(gp)
}

func (b *Bus) gougingParams(ctx context.Context) (api.GougingParams, error) {
	var gs api.GougingSettings
	if gss, err := b.ss.Setting(ctx, api.SettingGouging); err != nil {
		return api.GougingParams{}, err
	} else if err := json.Unmarshal([]byte(gss), &gs); err != nil {
		b.logger.Panicf("failed to unmarshal gouging settings '%s': %v", gss, err)
	}

	var rs api.RedundancySettings
	if rss, err := b.ss.Setting(ctx, api.SettingRedundancy); err != nil {
		return api.GougingParams{}, err
	} else if err := json.Unmarshal([]byte(rss), &rs); err != nil {
		b.logger.Panicf("failed to unmarshal redundancy settings '%s': %v", rss, err)
	}

	cs, err := b.consensusState(ctx)
	if err != nil {
		return api.GougingParams{}, err
	}

	return api.GougingParams{
		ConsensusState:     cs,
		GougingSettings:    gs,
		RedundancySettings: rs,
		TransactionFee:     b.cm.RecommendedFee(),
	}, nil
}

func (b *Bus) handleGETAlertsDeprecated(jc jape.Context) {
	ar, err := b.alertMgr.Alerts(jc.Request.Context(), alerts.AlertsOpts{Offset: 0, Limit: -1})
	if jc.Check("failed to fetch alerts", err) != nil {
		return
	}
	jc.Encode(ar.Alerts)
}

func (b *Bus) handleGETAlerts(jc jape.Context) {
	if jc.Request.FormValue("offset") == "" && jc.Request.FormValue("limit") == "" {
		b.handleGETAlertsDeprecated(jc)
		return
	}
	offset, limit := 0, -1
	var severity alerts.Severity
	if jc.DecodeForm("offset", &offset) != nil {
		return
	} else if jc.DecodeForm("limit", &limit) != nil {
		return
	} else if offset < 0 {
		jc.Error(errors.New("offset must be non-negative"), http.StatusBadRequest)
		return
	} else if jc.DecodeForm("severity", &severity) != nil {
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
	jc.Encode(ar)
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
	accounts, err := b.accounts.Accounts(jc.Request.Context(), owner)
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
	if b.accounts.SaveAccounts(jc.Request.Context(), req.Accounts) != nil {
		return
	}
}

func (b *Bus) autopilotsListHandlerGET(jc jape.Context) {
	if autopilots, err := b.as.Autopilots(jc.Request.Context()); jc.Check("failed to fetch autopilots", err) == nil {
		jc.Encode(autopilots)
	}
}

func (b *Bus) autopilotsHandlerGET(jc jape.Context) {
	var id string
	if jc.DecodeParam("id", &id) != nil {
		return
	}
	ap, err := b.as.Autopilot(jc.Request.Context(), id)
	if errors.Is(err, api.ErrAutopilotNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	}
	if jc.Check("couldn't load object", err) != nil {
		return
	}

	jc.Encode(ap)
}

func (b *Bus) autopilotsHandlerPUT(jc jape.Context) {
	var id string
	if jc.DecodeParam("id", &id) != nil {
		return
	}

	var ap api.Autopilot
	if jc.Decode(&ap) != nil {
		return
	}

	if ap.ID != id {
		jc.Error(errors.New("id in path and body don't match"), http.StatusBadRequest)
		return
	}

	if jc.Check("failed to update autopilot", b.as.UpdateAutopilot(jc.Request.Context(), ap)) == nil {
		b.pinMgr.TriggerUpdate()
	}
}

func (b *Bus) autopilotHostCheckHandlerPUT(jc jape.Context) {
	var id string
	if jc.DecodeParam("id", &id) != nil {
		return
	}
	var hk types.PublicKey
	if jc.DecodeParam("hostkey", &hk) != nil {
		return
	}
	var hc api.HostCheck
	if jc.Check("failed to decode host check", jc.Decode(&hc)) != nil {
		return
	}

	err := b.hs.UpdateHostCheck(jc.Request.Context(), id, hk, hc)
	if errors.Is(err, api.ErrAutopilotNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if jc.Check("failed to update host", err) != nil {
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
	jc.Encode(api.BusStateResponse{
		StartTime: api.TimeRFC3339(b.startTime),
		BuildState: api.BuildState{
			Version:   build.Version(),
			Commit:    build.Commit(),
			OS:        runtime.GOOS,
			BuildTime: api.TimeRFC3339(build.BuildTime()),
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

	err := b.mtrcs.PruneMetrics(jc.Request.Context(), metric, cutoff)
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
		} else if jc.Check("failed to record contract prune metric", b.mtrcs.RecordContractPruneMetric(jc.Request.Context(), req.Metrics...)) != nil {
			return
		}
	case api.MetricContractSetChurn:
		// TODO: jape hack - remove once jape can handle decoding multiple different request types
		var req api.ContractSetChurnMetricRequestPUT
		if err := json.NewDecoder(jc.Request.Body).Decode(&req); err != nil {
			jc.Error(fmt.Errorf("couldn't decode request type (%T): %w", req, err), http.StatusBadRequest)
			return
		} else if jc.Check("failed to record contract churn metric", b.mtrcs.RecordContractSetChurnMetric(jc.Request.Context(), req.Metrics...)) != nil {
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
		if jc.DecodeForm("contractID", &opts.ContractID) != nil {
			return
		} else if jc.DecodeForm("hostKey", &opts.HostKey) != nil {
			return
		}
		metrics, err = b.metrics(jc.Request.Context(), key, start, n, interval, opts)
	case api.MetricContractPrune:
		var opts api.ContractPruneMetricsQueryOpts
		if jc.DecodeForm("contractID", &opts.ContractID) != nil {
			return
		} else if jc.DecodeForm("hostKey", &opts.HostKey) != nil {
			return
		} else if jc.DecodeForm("hostVersion", &opts.HostVersion) != nil {
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
		return b.mtrcs.ContractMetrics(ctx, start, n, interval, opts.(api.ContractMetricsQueryOpts))
	case api.MetricContractPrune:
		return b.mtrcs.ContractPruneMetrics(ctx, start, n, interval, opts.(api.ContractPruneMetricsQueryOpts))
	case api.MetricContractSet:
		return b.mtrcs.ContractSetMetrics(ctx, start, n, interval, opts.(api.ContractSetMetricsQueryOpts))
	case api.MetricContractSetChurn:
		return b.mtrcs.ContractSetChurnMetrics(ctx, start, n, interval, opts.(api.ContractSetChurnMetricsQueryOpts))
	case api.MetricWallet:
		return b.mtrcs.WalletMetrics(ctx, start, n, interval, opts.(api.WalletMetricsQueryOpts))
	}
	return nil, fmt.Errorf("unknown metric '%s'", key)
}

func (b *Bus) multipartHandlerCreatePOST(jc jape.Context) {
	var req api.MultipartCreateRequest
	if jc.Decode(&req) != nil {
		return
	}

	var key object.EncryptionKey
	if req.GenerateKey {
		key = object.GenerateEncryptionKey()
	} else if req.Key == nil {
		key = object.NoOpKey
	} else {
		key = *req.Key
	}

	resp, err := b.ms.CreateMultipartUpload(jc.Request.Context(), req.Bucket, req.Path, key, req.MimeType, req.Metadata)
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
	err := b.ms.AbortMultipartUpload(jc.Request.Context(), req.Bucket, req.Path, req.UploadID)
	if jc.Check("failed to abort multipart upload", err) != nil {
		return
	}
}

func (b *Bus) multipartHandlerCompletePOST(jc jape.Context) {
	var req api.MultipartCompleteRequest
	if jc.Decode(&req) != nil {
		return
	}
	resp, err := b.ms.CompleteMultipartUpload(jc.Request.Context(), req.Bucket, req.Path, req.UploadID, req.Parts, api.CompleteMultipartOptions{
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
		req.Bucket = api.DefaultBucketName
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
	err := b.ms.AddMultipartPart(jc.Request.Context(), req.Bucket, req.Path, req.ContractSet, req.ETag, req.UploadID, req.PartNumber, req.Slices)
	if jc.Check("failed to upload part", err) != nil {
		return
	}
}

func (b *Bus) multipartHandlerUploadGET(jc jape.Context) {
	resp, err := b.ms.MultipartUpload(jc.Request.Context(), jc.PathParam("id"))
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
	resp, err := b.ms.MultipartUploads(jc.Request.Context(), req.Bucket, req.Prefix, req.PathMarker, req.UploadIDMarker, req.Limit)
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
	resp, err := b.ms.MultipartUploadParts(jc.Request.Context(), req.Bucket, req.Path, req.UploadID, req.PartNumberMarker, int64(req.Limit))
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
	gc := gouging.NewChecker(gp.GougingSettings, gp.ConsensusState, gp.TransactionFee, nil, nil)

	// fetch host settings
	settings, err := b.rhp2.Settings(ctx, rfr.HostKey, rfr.HostIP)
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
	var contract rhpv2.ContractRevision
	if b.isPassedV2AllowHeight() {
		panic("not implemented")
	} else {
		contract, err = b.formContract(
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

	// store the contract
	metadata, err := b.addContract(
		ctx,
		contract,
		contract.Revision.MissedHostPayout().Sub(rfr.HostCollateral),
		rfr.RenterFunds,
		b.cm.Tip().Height,
		api.ContractStatePending,
	)
	if jc.Check("couldn't store contract", err) != nil {
		return
	}

	// return the contract
	jc.Encode(metadata)
}
