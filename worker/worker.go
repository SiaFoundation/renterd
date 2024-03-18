package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"net"
	"net/http"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gotd/contrib/http_range"
	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/jape"
	"go.sia.tech/renterd/alerts"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/build"
	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/internal/utils"
	"go.sia.tech/renterd/object"
	"go.sia.tech/renterd/webhooks"
	"go.sia.tech/renterd/worker/client"
	"go.sia.tech/siad/modules"
	"go.uber.org/zap"
	"golang.org/x/crypto/blake2b"
)

const (
	batchSizeDeleteSectors = uint64(500000) // ~16MiB of roots
	batchSizeFetchSectors  = uint64(130000) // ~4MiB of roots

	defaultLockTimeout          = time.Minute
	defaultRevisionFetchTimeout = 30 * time.Second

	lockingPriorityActiveContractRevision = 100
	lockingPriorityRenew                  = 80
	lockingPriorityFunding                = 40
	lockingPrioritySyncing                = 30
	lockingPriorityPruning                = 20

	lockingPriorityBlockedUpload    = 15
	lockingPriorityUpload           = 10
	lockingPriorityBackgroundUpload = 5
)

var (
	ErrShuttingDown = errors.New("worker is shutting down")
)

// re-export the client
type Client struct {
	*client.Client
}

func NewClient(address, password string) *Client {
	return &Client{
		Client: client.New(address, password),
	}
}

type (
	Bus interface {
		alerts.Alerter
		ConsensusState
		webhooks.Broadcaster

		AccountStore
		ContractLocker
		ContractStore
		HostStore
		ObjectStore
		SettingStore

		Syncer
		Wallet
	}

	// An AccountStore manages ephemaral accounts state.
	AccountStore interface {
		Accounts(ctx context.Context) ([]api.Account, error)
		AddBalance(ctx context.Context, id rhpv3.Account, hk types.PublicKey, amt *big.Int) error

		LockAccount(ctx context.Context, id rhpv3.Account, hostKey types.PublicKey, exclusive bool, duration time.Duration) (api.Account, uint64, error)
		UnlockAccount(ctx context.Context, id rhpv3.Account, lockID uint64) error

		ResetDrift(ctx context.Context, id rhpv3.Account) error
		SetBalance(ctx context.Context, id rhpv3.Account, hk types.PublicKey, amt *big.Int) error
		ScheduleSync(ctx context.Context, id rhpv3.Account, hk types.PublicKey) error
	}

	ContractStore interface {
		Contract(ctx context.Context, id types.FileContractID) (api.ContractMetadata, error)
		ContractSize(ctx context.Context, id types.FileContractID) (api.ContractSize, error)
		ContractRoots(ctx context.Context, id types.FileContractID) ([]types.Hash256, []types.Hash256, error)
		Contracts(ctx context.Context, opts api.ContractsOpts) ([]api.ContractMetadata, error)
		RenewedContract(ctx context.Context, renewedFrom types.FileContractID) (api.ContractMetadata, error)
	}

	HostStore interface {
		RecordHostScans(ctx context.Context, scans []hostdb.HostScan) error
		RecordPriceTables(ctx context.Context, priceTableUpdate []hostdb.PriceTableUpdate) error
		RecordContractSpending(ctx context.Context, records []api.ContractSpendingRecord) error

		Host(ctx context.Context, hostKey types.PublicKey) (hostdb.HostInfo, error)
	}

	ObjectStore interface {
		// NOTE: used for download
		DeleteHostSector(ctx context.Context, hk types.PublicKey, root types.Hash256) error
		FetchPartialSlab(ctx context.Context, key object.EncryptionKey, offset, length uint32) ([]byte, error)
		Slab(ctx context.Context, key object.EncryptionKey) (object.Slab, error)

		// NOTE: used for upload
		AddObject(ctx context.Context, bucket, path, contractSet string, o object.Object, opts api.AddObjectOptions) error
		AddMultipartPart(ctx context.Context, bucket, path, contractSet, ETag, uploadID string, partNumber int, slices []object.SlabSlice) (err error)
		AddPartialSlab(ctx context.Context, data []byte, minShards, totalShards uint8, contractSet string) (slabs []object.SlabSlice, slabBufferMaxSizeSoftReached bool, err error)
		AddUploadingSector(ctx context.Context, uID api.UploadID, id types.FileContractID, root types.Hash256) error
		FinishUpload(ctx context.Context, uID api.UploadID) error
		MarkPackedSlabsUploaded(ctx context.Context, slabs []api.UploadedPackedSlab) error
		TrackUpload(ctx context.Context, uID api.UploadID) error
		UpdateSlab(ctx context.Context, s object.Slab, contractSet string) error

		// NOTE: used by worker
		Bucket(_ context.Context, bucket string) (api.Bucket, error)
		Object(ctx context.Context, bucket, path string, opts api.GetObjectOptions) (api.ObjectsResponse, error)
		DeleteObject(ctx context.Context, bucket, path string, opts api.DeleteObjectOptions) error
		MultipartUpload(ctx context.Context, uploadID string) (resp api.MultipartUpload, err error)
		PackedSlabsForUpload(ctx context.Context, lockingDuration time.Duration, minShards, totalShards uint8, set string, limit int) ([]api.PackedSlab, error)
	}

	SettingStore interface {
		GougingParams(ctx context.Context) (api.GougingParams, error)
		UploadParams(ctx context.Context) (api.UploadParams, error)
	}

	Syncer interface {
		BroadcastTransaction(ctx context.Context, txns []types.Transaction) error
		SyncerPeers(ctx context.Context) (resp []string, err error)
	}

	Wallet interface {
		WalletDiscard(ctx context.Context, txn types.Transaction) error
		WalletFund(ctx context.Context, txn *types.Transaction, amount types.Currency, useUnconfirmedTxns bool) ([]types.Hash256, []types.Transaction, error)
		WalletPrepareForm(ctx context.Context, renterAddress types.Address, renterKey types.PublicKey, renterFunds, hostCollateral types.Currency, hostKey types.PublicKey, hostSettings rhpv2.HostSettings, endHeight uint64) (txns []types.Transaction, err error)
		WalletPrepareRenew(ctx context.Context, revision types.FileContractRevision, hostAddress, renterAddress types.Address, renterKey types.PrivateKey, renterFunds, minNewCollateral types.Currency, pt rhpv3.HostPriceTable, endHeight, windowSize, expectedStorage uint64) (api.WalletPrepareRenewResponse, error)
		WalletSign(ctx context.Context, txn *types.Transaction, toSign []types.Hash256, cf types.CoveredFields) error
	}

	ConsensusState interface {
		ConsensusState(ctx context.Context) (api.ConsensusState, error)
	}
)

// deriveSubKey can be used to derive a sub-masterkey from the worker's
// masterkey to use for a specific purpose. Such as deriving more keys for
// ephemeral accounts.
func (w *worker) deriveSubKey(purpose string) types.PrivateKey {
	seed := blake2b.Sum256(append(w.masterKey[:], []byte(purpose)...))
	pk := types.NewPrivateKeyFromSeed(seed[:])
	for i := range seed {
		seed[i] = 0
	}
	return pk
}

// TODO: deriving the renter key from the host key using the master key only
// works if we persist a hash of the renter's master key in the database and
// compare it on startup, otherwise there's no way of knowing the derived key is
// usuable
// NOTE: Instead of hashing the masterkey and comparing, we could use random
// bytes + the HMAC thereof as the salt. e.g. 32 bytes + 32 bytes HMAC. Then
// whenever we read a specific salt we can verify that is was created with a
// given key. That would eventually allow different masterkeys to coexist in the
// same bus.
//
// TODO: instead of deriving a renter key use a randomly generated salt so we're
// not limited to one key per host
func (w *worker) deriveRenterKey(hostKey types.PublicKey) types.PrivateKey {
	seed := blake2b.Sum256(append(w.deriveSubKey("renterkey"), hostKey[:]...))
	pk := types.NewPrivateKeyFromSeed(seed[:])
	for i := range seed {
		seed[i] = 0
	}
	return pk
}

// A worker talks to Sia hosts to perform contract and storage operations within
// a renterd system.
type worker struct {
	alerts alerts.Alerter

	allowPrivateIPs bool
	id              string
	bus             Bus
	masterKey       [32]byte
	startTime       time.Time

	downloadManager *downloadManager
	uploadManager   *uploadManager

	accounts        *accounts
	priceTables     *priceTables
	transportPoolV3 *transportPoolV3

	uploadsMu            sync.Mutex
	uploadingPackedSlabs map[string]struct{}

	contractSpendingRecorder ContractSpendingRecorder
	contractLockingDuration  time.Duration

	shutdownCtx       context.Context
	shutdownCtxCancel context.CancelFunc

	logger *zap.SugaredLogger
}

func (w *worker) isStopped() bool {
	select {
	case <-w.shutdownCtx.Done():
		return true
	default:
	}
	return false
}

func (w *worker) withRevision(ctx context.Context, fetchTimeout time.Duration, fcid types.FileContractID, hk types.PublicKey, siamuxAddr string, lockPriority int, fn func(rev types.FileContractRevision) error) error {
	return w.withContractLock(ctx, fcid, lockPriority, func() error {
		h := w.Host(hk, fcid, siamuxAddr)
		rev, err := h.FetchRevision(ctx, fetchTimeout)
		if err != nil {
			return err
		}
		return fn(rev)
	})
}

func (w *worker) registerAlert(a alerts.Alert) {
	ctx, cancel := context.WithTimeout(w.shutdownCtx, time.Minute)
	if err := w.alerts.RegisterAlert(ctx, a); err != nil {
		w.logger.Errorf("failed to register alert, err: %v", err)
	}
	cancel()
}

func (w *worker) rhpScanHandler(jc jape.Context) {
	ctx := jc.Request.Context()

	// decode the request
	var rsr api.RHPScanRequest
	if jc.Decode(&rsr) != nil {
		return
	}

	// only scan hosts if we are online
	peers, err := w.bus.SyncerPeers(ctx)
	if jc.Check("failed to fetch peers from bus", err) != nil {
		return
	}
	if len(peers) == 0 {
		jc.Error(errors.New("not connected to the internet"), http.StatusServiceUnavailable)
		return
	}

	// scan host
	var errStr string
	settings, priceTable, elapsed, err := w.scanHost(ctx, time.Duration(rsr.Timeout), rsr.HostKey, rsr.HostIP)
	if err != nil {
		errStr = err.Error()
	}

	jc.Encode(api.RHPScanResponse{
		Ping:       api.DurationMS(elapsed),
		PriceTable: priceTable,
		ScanError:  errStr,
		Settings:   settings,
	})
}

func (w *worker) fetchContracts(ctx context.Context, metadatas []api.ContractMetadata, timeout time.Duration) (contracts []api.Contract, errs HostErrorSet) {
	errs = make(HostErrorSet)

	// create requests channel
	reqs := make(chan api.ContractMetadata)

	// create worker function
	var mu sync.Mutex
	worker := func() {
		for md := range reqs {
			var revision types.FileContractRevision
			err := w.withRevision(ctx, timeout, md.ID, md.HostKey, md.SiamuxAddr, lockingPriorityActiveContractRevision, func(rev types.FileContractRevision) error {
				revision = rev
				return nil
			})
			mu.Lock()
			if err != nil {
				errs[md.HostKey] = err
				contracts = append(contracts, api.Contract{
					ContractMetadata: md,
				})
			} else {
				contracts = append(contracts, api.Contract{
					ContractMetadata: md,
					Revision:         &revision,
				})
			}
			mu.Unlock()
		}
	}

	// launch all workers
	var wg sync.WaitGroup
	for t := 0; t < 20 && t < len(metadatas); t++ {
		wg.Add(1)
		go func() {
			worker()
			wg.Done()
		}()
	}

	// launch all requests
	for _, metadata := range metadatas {
		reqs <- metadata
	}
	close(reqs)

	// wait until they're done
	wg.Wait()
	return
}

func (w *worker) rhpPriceTableHandler(jc jape.Context) {
	ctx := jc.Request.Context()

	// decode the request
	var rptr api.RHPPriceTableRequest
	if jc.Decode(&rptr) != nil {
		return
	}

	// apply timeout
	if rptr.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(rptr.Timeout))
		defer cancel()
	}

	// defer interaction recording
	var err error
	var hpt hostdb.HostPriceTable
	defer func() {
		w.bus.RecordPriceTables(ctx, []hostdb.PriceTableUpdate{
			{
				HostKey:    rptr.HostKey,
				Success:    isSuccessfulInteraction(err),
				Timestamp:  time.Now(),
				PriceTable: hpt,
			},
		})
	}()

	err = w.transportPoolV3.withTransportV3(ctx, rptr.HostKey, rptr.SiamuxAddr, func(ctx context.Context, t *transportV3) error {
		hpt, err = RPCPriceTable(ctx, t, func(pt rhpv3.HostPriceTable) (rhpv3.PaymentMethod, error) { return nil, nil })
		return err
	})

	if jc.Check("could not get price table", err) != nil {
		return
	}
	jc.Encode(hpt)
}

func (w *worker) discardTxnOnErr(txn types.Transaction, errContext string, err *error) {
	discardTxnOnErr(w.shutdownCtx, w.bus, w.logger, txn, errContext, err)
}

func (w *worker) rhpFormHandler(jc jape.Context) {
	ctx := jc.Request.Context()

	// decode the request
	var rfr api.RHPFormRequest
	if jc.Decode(&rfr) != nil {
		return
	}

	// check renter funds is not zero
	if rfr.RenterFunds.IsZero() {
		http.Error(jc.ResponseWriter, "RenterFunds can not be zero", http.StatusBadRequest)
		return
	}

	// apply a pessimistic timeout on contract formations
	ctx, cancel := context.WithTimeout(ctx, 15*time.Minute)
	defer cancel()

	gp, err := w.bus.GougingParams(ctx)
	if jc.Check("could not get gouging parameters", err) != nil {
		return
	}

	hostIP, hostKey, renterFunds := rfr.HostIP, rfr.HostKey, rfr.RenterFunds
	renterAddress, endHeight, hostCollateral := rfr.RenterAddress, rfr.EndHeight, rfr.HostCollateral
	renterKey := w.deriveRenterKey(hostKey)

	var contract rhpv2.ContractRevision
	var txnSet []types.Transaction
	ctx = WithGougingChecker(ctx, w.bus, gp)
	err = w.withTransportV2(ctx, rfr.HostKey, hostIP, func(t *rhpv2.Transport) (err error) {
		hostSettings, err := RPCSettings(ctx, t)
		if err != nil {
			return err
		}
		// NOTE: we overwrite the NetAddress with the host address here since we
		// just used it to dial the host we know it's valid
		hostSettings.NetAddress = hostIP

		gc, err := GougingCheckerFromContext(ctx, false)
		if err != nil {
			return err
		}
		if breakdown := gc.Check(&hostSettings, nil); breakdown.Gouging() {
			return fmt.Errorf("failed to form contract, gouging check failed: %v", breakdown)
		}

		renterTxnSet, err := w.bus.WalletPrepareForm(ctx, renterAddress, renterKey.PublicKey(), renterFunds, hostCollateral, hostKey, hostSettings, endHeight)
		if err != nil {
			return err
		}
		defer w.discardTxnOnErr(renterTxnSet[len(renterTxnSet)-1], "rhpFormHandler", &err)

		contract, txnSet, err = RPCFormContract(ctx, t, renterKey, renterTxnSet)
		if err != nil {
			return err
		}
		return
	})
	if jc.Check("couldn't form contract", err) != nil {
		return
	}

	// broadcast the transaction set
	err = w.bus.BroadcastTransaction(ctx, txnSet)
	if err != nil && !isErrDuplicateTransactionSet(err) {
		w.logger.Errorf("failed to broadcast formation txn set: %v", err)
	}

	jc.Encode(api.RHPFormResponse{
		ContractID:     contract.ID(),
		Contract:       contract,
		TransactionSet: txnSet,
	})
}

func (w *worker) rhpBroadcastHandler(jc jape.Context) {
	ctx := jc.Request.Context()

	// decode the fcid
	var fcid types.FileContractID
	if jc.DecodeParam("id", &fcid) != nil {
		return
	}

	// Acquire lock before fetching revision.
	unlocker, err := w.acquireContractLock(ctx, fcid, lockingPriorityActiveContractRevision)
	if jc.Check("could not acquire revision lock", err) != nil {
		return
	}
	defer unlocker.Release(ctx)

	// Fetch contract from bus.
	c, err := w.bus.Contract(ctx, fcid)
	if jc.Check("could not get contract", err) != nil {
		return
	}
	rk := w.deriveRenterKey(c.HostKey)

	rev, err := w.FetchSignedRevision(ctx, c.HostIP, c.HostKey, rk, fcid, time.Minute)
	if jc.Check("could not fetch revision", err) != nil {
		return
	}
	// Create txn with revision.
	txn := types.Transaction{
		FileContractRevisions: []types.FileContractRevision{rev.Revision},
		Signatures:            rev.Signatures[:],
	}
	// Fund the txn. We pass 0 here since we only need the wallet to fund
	// the fee.
	toSign, parents, err := w.bus.WalletFund(ctx, &txn, types.ZeroCurrency, true)
	if jc.Check("failed to fund transaction", err) != nil {
		return
	}
	// Sign the txn.
	err = w.bus.WalletSign(ctx, &txn, toSign, types.CoveredFields{
		WholeTransaction: true,
	})
	if jc.Check("failed to sign transaction", err) != nil {
		_ = w.bus.WalletDiscard(ctx, txn)
		return
	}
	// Broadcast the txn.
	txnSet := append(parents, txn)
	err = w.bus.BroadcastTransaction(ctx, txnSet)
	if jc.Check("failed to broadcast transaction", err) != nil {
		_ = w.bus.WalletDiscard(ctx, txn)
		return
	}
}

func (w *worker) rhpPruneContractHandlerPOST(jc jape.Context) {
	ctx := jc.Request.Context()

	// decode fcid
	var fcid types.FileContractID
	if jc.DecodeParam("id", &fcid) != nil {
		return
	}

	// decode timeout
	var pcr api.RHPPruneContractRequest
	if jc.Decode(&pcr) != nil {
		return
	}

	// apply timeout
	if pcr.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(pcr.Timeout))
		defer cancel()
	}

	// fetch the contract from the bus
	contract, err := w.bus.Contract(ctx, fcid)
	if errors.Is(err, api.ErrContractNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if jc.Check("couldn't fetch contract", err) != nil {
		return
	}

	// return early if there's no data to prune
	size, err := w.bus.ContractSize(ctx, fcid)
	if jc.Check("couldn't fetch contract size", err) != nil {
		return
	} else if size.Prunable == 0 {
		jc.Encode(api.RHPPruneContractResponse{})
		return
	}

	// fetch gouging params
	gp, err := w.bus.GougingParams(ctx)
	if jc.Check("could not fetch gouging parameters", err) != nil {
		return
	}

	// attach gouging checker
	ctx = WithGougingChecker(ctx, w.bus, gp)

	// prune the contract
	pruned, remaining, err := w.PruneContract(ctx, contract.HostIP, contract.HostKey, fcid, contract.RevisionNumber)
	if err != nil && !errors.Is(err, ErrNoSectorsToPrune) && pruned == 0 {
		err = fmt.Errorf("failed to prune contract %v; %w", fcid, err)
		jc.Error(err, http.StatusInternalServerError)
		return
	}

	res := api.RHPPruneContractResponse{
		Pruned:    pruned,
		Remaining: remaining,
	}
	if err != nil {
		res.Error = err.Error()
	}
	jc.Encode(res)
}

func (w *worker) rhpContractRootsHandlerGET(jc jape.Context) {
	ctx := jc.Request.Context()

	// decode fcid
	var id types.FileContractID
	if jc.DecodeParam("id", &id) != nil {
		return
	}

	// fetch the contract from the bus
	c, err := w.bus.Contract(ctx, id)
	if errors.Is(err, api.ErrContractNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if jc.Check("couldn't fetch contract", err) != nil {
		return
	}

	// fetch gouging params
	gp, err := w.bus.GougingParams(ctx)
	if jc.Check("couldn't fetch gouging parameters from bus", err) != nil {
		return
	}

	// attach gouging checker to the context
	ctx = WithGougingChecker(ctx, w.bus, gp)

	// fetch the roots from the host
	roots, err := w.FetchContractRoots(ctx, c.HostIP, c.HostKey, id, c.RevisionNumber)
	if jc.Check("couldn't fetch contract roots from host", err) == nil {
		jc.Encode(roots)
	}
}

func (w *worker) rhpRenewHandler(jc jape.Context) {
	ctx := jc.Request.Context()

	// decode request
	var rrr api.RHPRenewRequest
	if jc.Decode(&rrr) != nil {
		return
	}

	// check renter funds is not zero
	if rrr.RenterFunds.IsZero() {
		http.Error(jc.ResponseWriter, "RenterFunds can not be zero", http.StatusBadRequest)
		return
	}

	// attach gouging checker
	gp, err := w.bus.GougingParams(ctx)
	if jc.Check("could not get gouging parameters", err) != nil {
		return
	}
	ctx = WithGougingChecker(ctx, w.bus, gp)

	// renew the contract
	var renewed rhpv2.ContractRevision
	var txnSet []types.Transaction
	var contractPrice types.Currency
	if jc.Check("couldn't renew contract", w.withRevision(ctx, defaultRevisionFetchTimeout, rrr.ContractID, rrr.HostKey, rrr.SiamuxAddr, lockingPriorityRenew, func(_ types.FileContractRevision) (err error) {
		h := w.Host(rrr.HostKey, rrr.ContractID, rrr.SiamuxAddr)
		renewed, txnSet, contractPrice, err = h.RenewContract(ctx, rrr)
		return err
	})) != nil {
		return
	}

	// broadcast the transaction set
	err = w.bus.BroadcastTransaction(ctx, txnSet)
	if err != nil && !isErrDuplicateTransactionSet(err) {
		w.logger.Errorf("failed to broadcast renewal txn set: %v", err)
	}

	// send the response
	jc.Encode(api.RHPRenewResponse{
		ContractID:     renewed.ID(),
		Contract:       renewed,
		ContractPrice:  contractPrice,
		TransactionSet: txnSet,
	})
}

func (w *worker) rhpFundHandler(jc jape.Context) {
	ctx := jc.Request.Context()

	// decode request
	var rfr api.RHPFundRequest
	if jc.Decode(&rfr) != nil {
		return
	}

	// attach gouging checker
	gp, err := w.bus.GougingParams(ctx)
	if jc.Check("could not get gouging parameters", err) != nil {
		return
	}
	ctx = WithGougingChecker(ctx, w.bus, gp)

	// fund the account
	jc.Check("couldn't fund account", w.withRevision(ctx, defaultRevisionFetchTimeout, rfr.ContractID, rfr.HostKey, rfr.SiamuxAddr, lockingPriorityFunding, func(rev types.FileContractRevision) (err error) {
		h := w.Host(rfr.HostKey, rev.ParentID, rfr.SiamuxAddr)
		err = h.FundAccount(ctx, rfr.Balance, &rev)
		if isBalanceMaxExceeded(err) {
			// sync the account
			err = h.SyncAccount(ctx, &rev)
			if err != nil {
				w.logger.Infof(fmt.Sprintf("failed to sync account: %v", err), "host", rfr.HostKey)
				return
			}

			// try funding the account again
			err = h.FundAccount(ctx, rfr.Balance, &rev)
			if err != nil {
				w.logger.Errorw(fmt.Sprintf("failed to fund account after syncing: %v", err), "host", rfr.HostKey, "balance", rfr.Balance)
			}
		}
		return
	}))
}

func (w *worker) rhpSyncHandler(jc jape.Context) {
	ctx := jc.Request.Context()

	// decode the request
	var rsr api.RHPSyncRequest
	if jc.Decode(&rsr) != nil {
		return
	}

	// attach gouging checker
	up, err := w.bus.UploadParams(ctx)
	if jc.Check("couldn't fetch upload parameters from bus", err) != nil {
		return
	}
	ctx = WithGougingChecker(ctx, w.bus, up.GougingParams)

	// sync the account
	h := w.Host(rsr.HostKey, rsr.ContractID, rsr.SiamuxAddr)
	jc.Check("couldn't sync account", w.withRevision(ctx, defaultRevisionFetchTimeout, rsr.ContractID, rsr.HostKey, rsr.SiamuxAddr, lockingPrioritySyncing, func(rev types.FileContractRevision) error {
		return h.SyncAccount(ctx, &rev)
	}))
}

func (w *worker) slabMigrateHandler(jc jape.Context) {
	ctx := jc.Request.Context()

	// decode the slab
	var slab object.Slab
	if jc.Decode(&slab) != nil {
		return
	}

	// fetch the upload parameters
	up, err := w.bus.UploadParams(ctx)
	if jc.Check("couldn't fetch upload parameters from bus", err) != nil {
		return
	}

	// NOTE: migrations do not use the default contract set but instead require
	// the user to specify the contract set through the query string parameter,
	// this to avoid accidentally migration to the default set if the autopilot
	// configuration is missing a contract set
	up.ContractSet = ""

	// decode the contract set from the query string
	var contractset string
	if jc.DecodeForm("contractset", &contractset) != nil {
		return
	} else if contractset != "" {
		up.ContractSet = contractset
	}

	// cancel the migration if no contract set is specified
	if up.ContractSet == "" {
		jc.Error(fmt.Errorf("migrations require the contract set to be passed as a query string parameter; %w", api.ErrContractSetNotSpecified), http.StatusBadRequest)
		return
	}

	// cancel the upload if consensus is not synced
	if !up.ConsensusState.Synced {
		w.logger.Errorf("migration cancelled, err: %v", api.ErrConsensusNotSynced)
		jc.Error(api.ErrConsensusNotSynced, http.StatusServiceUnavailable)
		return
	}

	// attach gouging checker to the context
	ctx = WithGougingChecker(ctx, w.bus, up.GougingParams)

	// fetch all contracts
	dlContracts, err := w.bus.Contracts(ctx, api.ContractsOpts{})
	if jc.Check("couldn't fetch contracts from bus", err) != nil {
		return
	}

	// fetch upload contracts
	ulContracts, err := w.bus.Contracts(ctx, api.ContractsOpts{ContractSet: up.ContractSet})
	if jc.Check("couldn't fetch contracts from bus", err) != nil {
		return
	}

	// migrate the slab
	numShardsMigrated, surchargeApplied, err := w.migrate(ctx, &slab, up.ContractSet, dlContracts, ulContracts, up.CurrentHeight)
	if err != nil {
		jc.Encode(api.MigrateSlabResponse{
			NumShardsMigrated: numShardsMigrated,
			SurchargeApplied:  surchargeApplied,
			Error:             err.Error(),
		})
		return
	}

	jc.Encode(api.MigrateSlabResponse{
		NumShardsMigrated: numShardsMigrated,
		SurchargeApplied:  surchargeApplied,
	})
}

func (w *worker) downloadsStatsHandlerGET(jc jape.Context) {
	stats := w.downloadManager.Stats()

	// prepare downloaders stats
	var healthy uint64
	var dss []api.DownloaderStats
	for hk, stat := range stats.downloaders {
		if stat.healthy {
			healthy++
		}
		dss = append(dss, api.DownloaderStats{
			HostKey:                    hk,
			AvgSectorDownloadSpeedMBPS: stat.avgSpeedMBPS,
			NumDownloads:               stat.numDownloads,
		})
	}
	sort.SliceStable(dss, func(i, j int) bool {
		return dss[i].AvgSectorDownloadSpeedMBPS > dss[j].AvgSectorDownloadSpeedMBPS
	})

	// encode response
	jc.Encode(api.DownloadStatsResponse{
		AvgDownloadSpeedMBPS: math.Ceil(stats.avgDownloadSpeedMBPS*100) / 100,
		AvgOverdrivePct:      math.Floor(stats.avgOverdrivePct*100*100) / 100,
		HealthyDownloaders:   healthy,
		NumDownloaders:       uint64(len(stats.downloaders)),
		DownloadersStats:     dss,
	})
}

func (w *worker) uploadsStatsHandlerGET(jc jape.Context) {
	stats := w.uploadManager.Stats()

	// prepare upload stats
	var uss []api.UploaderStats
	for hk, mbps := range stats.uploadSpeedsMBPS {
		uss = append(uss, api.UploaderStats{
			HostKey:                  hk,
			AvgSectorUploadSpeedMBPS: mbps,
		})
	}
	sort.SliceStable(uss, func(i, j int) bool {
		return uss[i].AvgSectorUploadSpeedMBPS > uss[j].AvgSectorUploadSpeedMBPS
	})

	// encode response
	jc.Encode(api.UploadStatsResponse{
		AvgSlabUploadSpeedMBPS: math.Ceil(stats.avgSlabUploadSpeedMBPS*100) / 100,
		AvgOverdrivePct:        math.Floor(stats.avgOverdrivePct*100*100) / 100,
		HealthyUploaders:       stats.healthyUploaders,
		NumUploaders:           stats.numUploaders,
		UploadersStats:         uss,
	})
}

func (w *worker) objectsHandlerHEAD(jc jape.Context) {
	// parse bucket
	bucket := api.DefaultBucketName
	if jc.DecodeForm("bucket", &bucket) != nil {
		return
	}
	var ignoreDelim bool
	if jc.DecodeForm("ignoreDelim", &ignoreDelim) != nil {
		return
	}

	// parse path
	path := jc.PathParam("path")
	if !ignoreDelim && (path == "" || strings.HasSuffix(path, "/")) {
		jc.Error(errors.New("HEAD requests can only be performed on objects, not directories"), http.StatusBadRequest)
		return
	}

	// fetch object metadata
	res, err := w.bus.Object(jc.Request.Context(), bucket, path, api.GetObjectOptions{
		IgnoreDelim:  ignoreDelim,
		OnlyMetadata: true,
	})
	if utils.IsErr(err, api.ErrObjectNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	} else if res.Object == nil {
		jc.Error(api.ErrObjectNotFound, http.StatusInternalServerError) // should never happen but checking because we deref. later
		return
	}

	// serve the content to ensure we're setting the exact same headers as we
	// would for a GET request
	status, err := serveContent(jc.ResponseWriter, jc.Request, *res.Object, func(io.Writer, int64, int64) error { return nil })
	if errors.Is(err, http_range.ErrInvalid) || errors.Is(err, errMultiRangeNotSupported) {
		jc.Error(err, http.StatusBadRequest)
	} else if errors.Is(err, http_range.ErrNoOverlap) {
		jc.Error(err, http.StatusRequestedRangeNotSatisfiable)
	} else if err != nil {
		jc.Error(err, status)
	}
}

func (w *worker) objectsHandlerGET(jc jape.Context) {
	jc.Custom(nil, []api.ObjectMetadata{})

	ctx := jc.Request.Context()

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
	var off int
	if jc.DecodeForm("offset", &off) != nil {
		return
	}
	limit := -1
	if jc.DecodeForm("limit", &limit) != nil {
		return
	}
	var ignoreDelim bool
	if jc.DecodeForm("ignoreDelim", &ignoreDelim) != nil {
		return
	}

	opts := api.GetObjectOptions{
		Prefix:      prefix,
		Marker:      marker,
		Offset:      off,
		Limit:       limit,
		IgnoreDelim: ignoreDelim,
		SortBy:      sortBy,
		SortDir:     sortDir,
	}

	path := jc.PathParam("path")
	res, err := w.bus.Object(ctx, bucket, path, opts)
	if utils.IsErr(err, api.ErrObjectNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if jc.Check("couldn't get object or entries", err) != nil {
		return
	}
	if path == "" || strings.HasSuffix(path, "/") {
		jc.Encode(res.Entries)
		return
	}

	// return early if the object is empty
	if len(res.Object.Slabs) == 0 {
		return
	}

	// fetch gouging params
	gp, err := w.bus.GougingParams(ctx)
	if jc.Check("couldn't fetch gouging parameters from bus", err) != nil {
		return
	}

	// fetch all contracts
	contracts, err := w.bus.Contracts(ctx, api.ContractsOpts{})
	if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}

	// create a download function
	downloadFn := func(wr io.Writer, offset, length int64) (err error) {
		ctx = WithGougingChecker(ctx, w.bus, gp)
		err = w.downloadManager.DownloadObject(ctx, wr, *res.Object.Object, uint64(offset), uint64(length), contracts)
		if err != nil {
			w.logger.Error(err)
			if !errors.Is(err, ErrShuttingDown) &&
				!errors.Is(err, errDownloadCancelled) &&
				!errors.Is(err, io.ErrClosedPipe) {
				w.registerAlert(newDownloadFailedAlert(bucket, path, prefix, marker, offset, length, int64(len(contracts)), err))
			}
		}
		return
	}

	// serve the content
	status, err := serveContent(jc.ResponseWriter, jc.Request, *res.Object, downloadFn)
	if errors.Is(err, http_range.ErrInvalid) || errors.Is(err, errMultiRangeNotSupported) {
		jc.Error(err, http.StatusBadRequest)
	} else if errors.Is(err, http_range.ErrNoOverlap) {
		jc.Error(err, http.StatusRequestedRangeNotSatisfiable)
	} else if err != nil {
		jc.Error(err, status)
	}
}

func (w *worker) objectsHandlerPUT(jc jape.Context) {
	jc.Custom((*[]byte)(nil), nil)
	ctx := jc.Request.Context()

	// grab the path
	path := jc.PathParam("path")

	// fetch the upload parameters
	up, err := w.bus.UploadParams(ctx)
	if jc.Check("couldn't fetch upload parameters from bus", err) != nil {
		return
	}

	// decode the contract set from the query string
	var contractset string
	if jc.DecodeForm("contractset", &contractset) != nil {
		return
	} else if contractset != "" {
		up.ContractSet = contractset
	}

	// decode the mimetype from the query string
	var mimeType string
	if jc.DecodeForm("mimetype", &mimeType) != nil {
		return
	}

	// decode the bucket from the query string
	bucket := api.DefaultBucketName
	if jc.DecodeForm("bucket", &bucket) != nil {
		return
	}

	// return early if the bucket does not exist
	_, err = w.bus.Bucket(ctx, bucket)
	if utils.IsErr(err, api.ErrBucketNotFound) {
		jc.Error(fmt.Errorf("bucket '%s' not found; %w", bucket, err), http.StatusNotFound)
		return
	}

	// cancel the upload if no contract set is specified
	if up.ContractSet == "" {
		jc.Error(api.ErrContractSetNotSpecified, http.StatusBadRequest)
		return
	}

	// cancel the upload if consensus is not synced
	if !up.ConsensusState.Synced {
		w.logger.Errorf("upload cancelled, err: %v", api.ErrConsensusNotSynced)
		jc.Error(api.ErrConsensusNotSynced, http.StatusServiceUnavailable)
		return
	}

	// allow overriding the redundancy settings
	rs := up.RedundancySettings
	if jc.DecodeForm("minshards", &rs.MinShards) != nil {
		return
	}
	if jc.DecodeForm("totalshards", &rs.TotalShards) != nil {
		return
	}
	if jc.Check("invalid redundancy settings", rs.Validate()) != nil {
		return
	}

	// parse headers and extract object meta
	metadata := make(api.ObjectUserMetadata)
	for k, v := range jc.Request.Header {
		if strings.HasPrefix(strings.ToLower(k), strings.ToLower(api.ObjectMetadataPrefix)) && len(v) > 0 {
			metadata[k[len(api.ObjectMetadataPrefix):]] = v[0]
		}
	}

	// build options
	opts := []UploadOption{
		WithBlockHeight(up.CurrentHeight),
		WithContractSet(up.ContractSet),
		WithMimeType(mimeType),
		WithPacking(up.UploadPacking),
		WithRedundancySettings(up.RedundancySettings),
		WithObjectUserMetadata(metadata),
	}

	// attach gouging checker to the context
	ctx = WithGougingChecker(ctx, w.bus, up.GougingParams)

	// fetch contracts
	contracts, err := w.bus.Contracts(ctx, api.ContractsOpts{ContractSet: up.ContractSet})
	if jc.Check("couldn't fetch contracts from bus", err) != nil {
		return
	}

	// upload the object
	params := defaultParameters(bucket, path)
	eTag, err := w.upload(ctx, jc.Request.Body, contracts, params, opts...)
	if err := jc.Check("couldn't upload object", err); err != nil {
		if err != nil {
			w.logger.Error(err)
			if !errors.Is(err, ErrShuttingDown) && !errors.Is(err, errUploadInterrupted) && !errors.Is(err, context.Canceled) {
				w.registerAlert(newUploadFailedAlert(bucket, path, up.ContractSet, mimeType, rs.MinShards, rs.TotalShards, len(contracts), up.UploadPacking, false, err))
			}
		}
		return
	}

	// set etag header
	jc.ResponseWriter.Header().Set("ETag", api.FormatETag(eTag))
}

func (w *worker) multipartUploadHandlerPUT(jc jape.Context) {
	jc.Custom((*[]byte)(nil), nil)
	ctx := jc.Request.Context()

	// grab the path
	path := jc.PathParam("path")

	// fetch the upload parameters
	up, err := w.bus.UploadParams(ctx)
	if jc.Check("couldn't fetch upload parameters from bus", err) != nil {
		return
	}

	// attach gouging checker to the context
	ctx = WithGougingChecker(ctx, w.bus, up.GougingParams)

	// cancel the upload if no contract set is specified
	if up.ContractSet == "" {
		jc.Error(api.ErrContractSetNotSpecified, http.StatusBadRequest)
		return
	}

	// cancel the upload if consensus is not synced
	if !up.ConsensusState.Synced {
		w.logger.Errorf("upload cancelled, err: %v", api.ErrConsensusNotSynced)
		jc.Error(api.ErrConsensusNotSynced, http.StatusServiceUnavailable)
		return
	}

	// decode the contract set from the query string
	var contractset string
	if jc.DecodeForm("contractset", &contractset) != nil {
		return
	} else if contractset != "" {
		up.ContractSet = contractset
	}

	// decode the bucket from the query string
	bucket := api.DefaultBucketName
	if jc.DecodeForm("bucket", &bucket) != nil {
		return
	}

	// return early if the bucket does not exist
	_, err = w.bus.Bucket(ctx, bucket)
	if utils.IsErr(err, api.ErrBucketNotFound) {
		jc.Error(fmt.Errorf("bucket '%s' not found; %w", bucket, err), http.StatusNotFound)
		return
	}

	// decode the upload id
	var uploadID string
	if jc.DecodeForm("uploadid", &uploadID) != nil {
		return
	} else if uploadID == "" {
		jc.Error(errors.New("upload id not specified"), http.StatusBadRequest)
		return
	}

	// decode the part number
	var partNumber int
	if jc.DecodeForm("partnumber", &partNumber) != nil {
		return
	}

	// allow overriding the redundancy settings
	rs := up.RedundancySettings
	if jc.DecodeForm("minshards", &rs.MinShards) != nil {
		return
	}
	if jc.DecodeForm("totalshards", &rs.TotalShards) != nil {
		return
	}
	if jc.Check("invalid redundancy settings", rs.Validate()) != nil {
		return
	}

	// get the offset
	var offset int
	if jc.DecodeForm("offset", &offset) != nil {
		return
	} else if offset < 0 {
		jc.Error(errors.New("offset must be positive"), http.StatusBadRequest)
		return
	}

	// fetch upload from bus
	upload, err := w.bus.MultipartUpload(ctx, uploadID)
	if utils.IsErr(err, api.ErrMultipartUploadNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if jc.Check("failed to fetch multipart upload", err) != nil {
		return
	}

	// built options
	opts := []UploadOption{
		WithBlockHeight(up.CurrentHeight),
		WithContractSet(up.ContractSet),
		WithPacking(up.UploadPacking),
		WithRedundancySettings(up.RedundancySettings),
		WithCustomKey(upload.Key),
	}

	// make sure only one of the following is set
	if encryptionEnabled := !upload.Key.IsNoopKey(); encryptionEnabled && jc.Request.FormValue("offset") == "" {
		jc.Error(errors.New("if object encryption (pre-erasure coding) wasn't disabled by creating the multipart upload with the no-op key, the offset needs to be set"), http.StatusBadRequest)
		return
	} else if encryptionEnabled {
		opts = append(opts, WithCustomEncryptionOffset(uint64(offset)))
	}

	// attach gouging checker to the context
	ctx = WithGougingChecker(ctx, w.bus, up.GougingParams)

	// fetch contracts
	contracts, err := w.bus.Contracts(ctx, api.ContractsOpts{ContractSet: up.ContractSet})
	if jc.Check("couldn't fetch contracts from bus", err) != nil {
		return
	}

	// upload the multipart
	params := multipartParameters(bucket, path, uploadID, partNumber)
	eTag, err := w.upload(ctx, jc.Request.Body, contracts, params, opts...)
	if jc.Check("couldn't upload object", err) != nil {
		if err != nil {
			w.logger.Error(err)
			if !errors.Is(err, ErrShuttingDown) && !errors.Is(err, errUploadInterrupted) {
				w.registerAlert(newUploadFailedAlert(bucket, path, up.ContractSet, "", rs.MinShards, rs.TotalShards, len(contracts), up.UploadPacking, true, err))
			}
		}
		return
	}

	// set etag header
	jc.ResponseWriter.Header().Set("ETag", api.FormatETag(eTag))
}

func (w *worker) objectsHandlerDELETE(jc jape.Context) {
	var batch bool
	if jc.DecodeForm("batch", &batch) != nil {
		return
	}
	var bucket string
	if jc.DecodeForm("bucket", &bucket) != nil {
		return
	}
	err := w.bus.DeleteObject(jc.Request.Context(), bucket, jc.PathParam("path"), api.DeleteObjectOptions{Batch: batch})
	if utils.IsErr(err, api.ErrObjectNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	}
	jc.Check("couldn't delete object", err)
}

func (w *worker) rhpContractsHandlerGET(jc jape.Context) {
	ctx := jc.Request.Context()

	// fetch contracts
	busContracts, err := w.bus.Contracts(ctx, api.ContractsOpts{})
	if jc.Check("failed to fetch contracts from bus", err) != nil {
		return
	}
	if len(busContracts) == 0 {
		jc.Encode(api.ContractsResponse{Contracts: nil})
		return
	}

	var hosttimeout time.Duration
	if jc.DecodeForm("hosttimeout", (*api.DurationMS)(&hosttimeout)) != nil {
		return
	}

	gp, err := w.bus.GougingParams(ctx)
	if jc.Check("could not get gouging parameters", err) != nil {
		return
	}
	ctx = WithGougingChecker(ctx, w.bus, gp)

	contracts, errs := w.fetchContracts(ctx, busContracts, hosttimeout)
	resp := api.ContractsResponse{Contracts: contracts}
	if errs != nil {
		resp.Error = errs.Error()
	}
	jc.Encode(resp)
}

func (w *worker) idHandlerGET(jc jape.Context) {
	jc.Encode(w.id)
}

func (w *worker) memoryGET(jc jape.Context) {
	jc.Encode(api.MemoryResponse{
		Download: w.downloadManager.mm.Status(),
		Upload:   w.uploadManager.mm.Status(),
	})
}

func (w *worker) accountHandlerGET(jc jape.Context) {
	var hostKey types.PublicKey
	if jc.DecodeParam("hostkey", &hostKey) != nil {
		return
	}
	account := rhpv3.Account(w.accounts.deriveAccountKey(hostKey).PublicKey())
	jc.Encode(account)
}

func (w *worker) stateHandlerGET(jc jape.Context) {
	jc.Encode(api.WorkerStateResponse{
		ID:        w.id,
		StartTime: api.TimeRFC3339(w.startTime),
		BuildState: api.BuildState{
			Network:   build.NetworkName(),
			Version:   build.Version(),
			Commit:    build.Commit(),
			OS:        runtime.GOOS,
			BuildTime: api.TimeRFC3339(build.BuildTime()),
		},
	})
}

// New returns an HTTP handler that serves the worker API.
func New(masterKey [32]byte, id string, b Bus, contractLockingDuration, busFlushInterval, downloadOverdriveTimeout, uploadOverdriveTimeout time.Duration, downloadMaxOverdrive, uploadMaxOverdrive, downloadMaxMemory, uploadMaxMemory uint64, allowPrivateIPs bool, l *zap.Logger) (*worker, error) {
	if contractLockingDuration == 0 {
		return nil, errors.New("contract lock duration must be positive")
	}
	if busFlushInterval == 0 {
		return nil, errors.New("bus flush interval must be positive")
	}
	if downloadOverdriveTimeout == 0 {
		return nil, errors.New("download overdrive timeout must be positive")
	}
	if uploadOverdriveTimeout == 0 {
		return nil, errors.New("upload overdrive timeout must be positive")
	}
	if downloadMaxMemory == 0 {
		return nil, errors.New("downloadMaxMemory cannot be 0")
	}
	if uploadMaxMemory == 0 {
		return nil, errors.New("uploadMaxMemory cannot be 0")
	}

	l = l.Named("worker").Named(id)
	ctx, cancel := context.WithCancel(context.Background())
	w := &worker{
		alerts:                  alerts.WithOrigin(b, fmt.Sprintf("worker.%s", id)),
		allowPrivateIPs:         allowPrivateIPs,
		contractLockingDuration: contractLockingDuration,
		id:                      id,
		bus:                     b,
		masterKey:               masterKey,
		logger:                  l.Sugar(),
		startTime:               time.Now(),
		uploadingPackedSlabs:    make(map[string]struct{}),
		shutdownCtx:             ctx,
		shutdownCtxCancel:       cancel,
	}

	w.initAccounts(b)
	w.initPriceTables()
	w.initTransportPool()

	w.initDownloadManager(downloadMaxMemory, downloadMaxOverdrive, downloadOverdriveTimeout, l.Named("downloadmanager").Sugar())
	w.initUploadManager(uploadMaxMemory, uploadMaxOverdrive, uploadOverdriveTimeout, l.Named("uploadmanager").Sugar())

	w.initContractSpendingRecorder(busFlushInterval)
	return w, nil
}

// Handler returns an HTTP handler that serves the worker API.
func (w *worker) Handler() http.Handler {
	return jape.Mux(map[string]jape.Handler{
		"GET    /account/:hostkey": w.accountHandlerGET,
		"GET    /id":               w.idHandlerGET,

		"GET /memory": w.memoryGET,

		"GET    /rhp/contracts":              w.rhpContractsHandlerGET,
		"POST   /rhp/contract/:id/broadcast": w.rhpBroadcastHandler,
		"POST   /rhp/contract/:id/prune":     w.rhpPruneContractHandlerPOST,
		"GET    /rhp/contract/:id/roots":     w.rhpContractRootsHandlerGET,
		"POST   /rhp/scan":                   w.rhpScanHandler,
		"POST   /rhp/form":                   w.rhpFormHandler,
		"POST   /rhp/renew":                  w.rhpRenewHandler,
		"POST   /rhp/fund":                   w.rhpFundHandler,
		"POST   /rhp/sync":                   w.rhpSyncHandler,
		"POST   /rhp/pricetable":             w.rhpPriceTableHandler,

		"GET    /stats/downloads": w.downloadsStatsHandlerGET,
		"GET    /stats/uploads":   w.uploadsStatsHandlerGET,
		"POST   /slab/migrate":    w.slabMigrateHandler,

		"HEAD   /objects/*path": w.objectsHandlerHEAD,
		"GET    /objects/*path": w.objectsHandlerGET,
		"PUT    /objects/*path": w.objectsHandlerPUT,
		"DELETE /objects/*path": w.objectsHandlerDELETE,

		"PUT    /multipart/*path": w.multipartUploadHandlerPUT,

		"GET    /state": w.stateHandlerGET,
	})
}

// Shutdown shuts down the worker.
func (w *worker) Shutdown(ctx context.Context) error {
	// cancel shutdown context
	w.shutdownCtxCancel()

	// stop uploads and downloads
	w.downloadManager.Stop()
	w.uploadManager.Stop()

	// stop recorders
	w.contractSpendingRecorder.Stop(ctx)
	return nil
}

func (w *worker) scanHost(ctx context.Context, timeout time.Duration, hostKey types.PublicKey, hostIP string) (rhpv2.HostSettings, rhpv3.HostPriceTable, time.Duration, error) {
	logger := w.logger.With("host", hostKey).With("hostIP", hostIP).With("timeout", timeout)
	// prepare a helper for scanning
	scan := func() (rhpv2.HostSettings, rhpv3.HostPriceTable, time.Duration, error) {
		// apply timeout
		scanCtx := ctx
		var cancel context.CancelFunc
		if timeout > 0 {
			scanCtx, cancel = context.WithTimeout(scanCtx, timeout)
			defer cancel()
		}
		// resolve hostIP. We don't want to scan hosts on private networks.
		if !w.allowPrivateIPs {
			host, _, err := net.SplitHostPort(hostIP)
			if err != nil {
				return rhpv2.HostSettings{}, rhpv3.HostPriceTable{}, 0, err
			}
			addrs, err := (&net.Resolver{}).LookupIPAddr(scanCtx, host)
			if err != nil {
				return rhpv2.HostSettings{}, rhpv3.HostPriceTable{}, 0, err
			}
			for _, addr := range addrs {
				if isPrivateIP(addr.IP) {
					return rhpv2.HostSettings{}, rhpv3.HostPriceTable{}, 0, api.ErrHostOnPrivateNetwork
				}
			}
		}

		// fetch the host settings
		start := time.Now()
		var settings rhpv2.HostSettings
		err := w.withTransportV2(scanCtx, hostKey, hostIP, func(t *rhpv2.Transport) error {
			var err error
			if settings, err = RPCSettings(scanCtx, t); err != nil {
				return fmt.Errorf("failed to fetch host settings: %w", err)
			}
			// NOTE: we overwrite the NetAddress with the host address here
			// since we just used it to dial the host we know it's valid
			settings.NetAddress = hostIP
			return nil
		})
		elapsed := time.Since(start)
		if err != nil {
			return settings, rhpv3.HostPriceTable{}, elapsed, err
		}

		// fetch the host pricetable
		var pt rhpv3.HostPriceTable
		err = w.transportPoolV3.withTransportV3(scanCtx, hostKey, settings.SiamuxAddr(), func(ctx context.Context, t *transportV3) error {
			if hpt, err := RPCPriceTable(ctx, t, func(pt rhpv3.HostPriceTable) (rhpv3.PaymentMethod, error) { return nil, nil }); err != nil {
				return fmt.Errorf("failed to fetch host price table: %w", err)
			} else {
				pt = hpt.HostPriceTable
				return nil
			}
		})
		return settings, pt, elapsed, err
	}

	// scan: first try
	settings, pt, duration, err := scan()
	if err != nil {
		// scan: second try
		select {
		case <-ctx.Done():
			return rhpv2.HostSettings{}, rhpv3.HostPriceTable{}, 0, ctx.Err()
		case <-time.After(time.Second):
		}
		settings, pt, duration, err = scan()

		logger = logger.With("elapsed", duration)
		if err == nil {
			logger.Info("successfully scanned host on second try")
		} else if !isErrHostUnreachable(err) {
			logger.Infow("failed to scan host", zap.Error(err))
		}
	}

	// check if the scan failed due to a shutdown - shouldn't be necessary but
	// just in case since recording a failed scan might have serious
	// repercussions
	select {
	case <-ctx.Done():
		return rhpv2.HostSettings{}, rhpv3.HostPriceTable{}, 0, ctx.Err()
	default:
	}

	// record host scan - make sure this isn't interrupted by the same context
	// used to time out the scan itself because otherwise we won't be able to
	// record scans that timed out.
	recordCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	scanErr := w.bus.RecordHostScans(recordCtx, []hostdb.HostScan{
		{
			HostKey:    hostKey,
			Success:    isSuccessfulInteraction(err),
			Timestamp:  time.Now(),
			Settings:   settings,
			PriceTable: pt,
		},
	})
	if scanErr != nil {
		logger.Errorw("failed to record host scan", zap.Error(scanErr))
	}
	return settings, pt, duration, err
}

func discardTxnOnErr(ctx context.Context, bus Bus, l *zap.SugaredLogger, txn types.Transaction, errContext string, err *error) {
	if *err == nil {
		return
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	if dErr := bus.WalletDiscard(ctx, txn); dErr != nil {
		l.Errorf("%w: %v, failed to discard txn: %v", *err, errContext, dErr)
	}
	cancel()
}

func isErrHostUnreachable(err error) bool {
	return utils.IsErr(err, os.ErrDeadlineExceeded) ||
		utils.IsErr(err, context.DeadlineExceeded) ||
		utils.IsErr(err, api.ErrHostOnPrivateNetwork) ||
		utils.IsErr(err, errors.New("no route to host")) ||
		utils.IsErr(err, errors.New("no such host")) ||
		utils.IsErr(err, errors.New("connection refused")) ||
		utils.IsErr(err, errors.New("unknown port")) ||
		utils.IsErr(err, errors.New("cannot assign requested address"))
}

func isErrDuplicateTransactionSet(err error) bool {
	return utils.IsErr(err, modules.ErrDuplicateTransactionSet)
}
