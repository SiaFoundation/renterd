package autopilot

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"runtime"
	"sync"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/jape"
	"go.sia.tech/renterd/alerts"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/autopilot/contractor"
	"go.sia.tech/renterd/autopilot/scanner"
	"go.sia.tech/renterd/build"
	"go.sia.tech/renterd/config"
	"go.sia.tech/renterd/internal/utils"
	"go.sia.tech/renterd/object"
	"go.sia.tech/renterd/webhooks"
	"go.uber.org/zap"
)

type Bus interface {
	alerts.Alerter
	webhooks.Broadcaster

	// accounts
	Accounts(ctx context.Context, owner string) (accounts []api.Account, err error)

	// autopilot
	Autopilot(ctx context.Context) (api.Autopilot, error)
	UpdateCurrentPeriod(ctx context.Context, period uint64) error

	// consensus
	ConsensusNetwork(ctx context.Context) (consensus.Network, error)
	ConsensusState(ctx context.Context) (api.ConsensusState, error)

	// contracts
	AncestorContracts(ctx context.Context, id types.FileContractID, minStartHeight uint64) ([]api.ContractMetadata, error)
	ArchiveContracts(ctx context.Context, toArchive map[types.FileContractID]string) error
	BroadcastContract(ctx context.Context, fcid types.FileContractID) (types.TransactionID, error)
	Contract(ctx context.Context, id types.FileContractID) (api.ContractMetadata, error)
	Contracts(ctx context.Context, opts api.ContractsOpts) (contracts []api.ContractMetadata, err error)
	FileContractTax(ctx context.Context, payout types.Currency) (types.Currency, error)
	FormContract(ctx context.Context, renterAddress types.Address, renterFunds types.Currency, hostKey types.PublicKey, hostIP string, hostCollateral types.Currency, endHeight uint64) (api.ContractMetadata, error)
	ContractRevision(ctx context.Context, fcid types.FileContractID) (api.Revision, error)
	RenewContract(ctx context.Context, fcid types.FileContractID, endHeight uint64, renterFunds, minNewCollateral types.Currency, expectedNewStorage uint64) (api.ContractMetadata, error)
	UpdateContractSet(ctx context.Context, set string, toAdd, toRemove []types.FileContractID) error
	UpdateContractUsability(ctx context.Context, contractID types.FileContractID, usability string) (err error)
	PrunableData(ctx context.Context) (prunableData api.ContractsPrunableDataResponse, err error)
	PruneContract(ctx context.Context, id types.FileContractID, timeout time.Duration) (api.ContractPruneResponse, error)

	// hostdb
	Host(ctx context.Context, hostKey types.PublicKey) (api.Host, error)
	Hosts(ctx context.Context, opts api.HostOptions) ([]api.Host, error)
	RemoveOfflineHosts(ctx context.Context, maxConsecutiveScanFailures uint64, maxDowntime time.Duration) (uint64, error)
	UpdateHostCheck(ctx context.Context, hostKey types.PublicKey, hostCheck api.HostChecks) error

	// metrics
	RecordContractSetChurnMetric(ctx context.Context, metrics ...api.ContractSetChurnMetric) error
	RecordContractPruneMetric(ctx context.Context, metrics ...api.ContractPruneMetric) error

	// buckets
	ListBuckets(ctx context.Context) ([]api.Bucket, error)

	// objects
	Objects(ctx context.Context, prefix string, opts api.ListObjectOptions) (resp api.ObjectsResponse, err error)
	RefreshHealth(ctx context.Context) error
	Slab(ctx context.Context, key object.EncryptionKey) (object.Slab, error)
	SlabsForMigration(ctx context.Context, healthCutoff float64, set string, limit int) ([]api.UnhealthySlab, error)

	// scanner
	ScanHost(ctx context.Context, hostKey types.PublicKey, timeout time.Duration) (resp api.HostScanResponse, err error)

	// settings
	GougingSettings(ctx context.Context) (gs api.GougingSettings, err error)
	UploadSettings(ctx context.Context) (us api.UploadSettings, err error)

	// syncer
	SyncerPeers(ctx context.Context) (resp []string, err error)

	// txpool
	RecommendedFee(ctx context.Context) (types.Currency, error)
	TransactionPool(ctx context.Context) (txns []types.Transaction, err error)

	// wallet
	Wallet(ctx context.Context) (api.WalletResponse, error)
	WalletPending(ctx context.Context) (resp []wallet.Event, err error)
	WalletRedistribute(ctx context.Context, outputs int, amount types.Currency) (ids []types.TransactionID, err error)
}

type Autopilot struct {
	alerts  alerts.Alerter
	bus     Bus
	logger  *zap.SugaredLogger
	workers *workerPool

	c *contractor.Contractor
	m *migrator
	s scanner.Scanner

	tickerDuration time.Duration
	wg             sync.WaitGroup

	startStopMu       sync.Mutex
	startTime         time.Time
	shutdownCtx       context.Context
	shutdownCtxCancel context.CancelFunc
	ticker            *time.Ticker
	triggerChan       chan bool

	mu               sync.Mutex
	pruning          bool
	pruningLastStart time.Time
	pruningAlertIDs  map[types.FileContractID]types.Hash256

	maintenanceTxnIDs []types.TransactionID
}

// New initializes an Autopilot.
func New(cfg config.Autopilot, bus Bus, workers []Worker, logger *zap.Logger) (_ *Autopilot, err error) {
	logger = logger.Named("autopilot")
	shutdownCtx, shutdownCtxCancel := context.WithCancel(context.Background())
	ap := &Autopilot{
		alerts:  alerts.WithOrigin(bus, "autopilot"),
		bus:     bus,
		logger:  logger.Sugar(),
		workers: newWorkerPool(workers),

		shutdownCtx:       shutdownCtx,
		shutdownCtxCancel: shutdownCtxCancel,

		tickerDuration: cfg.Heartbeat,

		pruningAlertIDs: make(map[types.FileContractID]types.Hash256),
	}

	ap.s, err = scanner.New(ap.bus, cfg.ScannerBatchSize, cfg.ScannerNumThreads, cfg.ScannerInterval, logger)
	if err != nil {
		return
	}

	ap.c = contractor.New(bus, bus, ap.logger, cfg.RevisionSubmissionBuffer, cfg.RevisionBroadcastInterval)
	ap.m = newMigrator(ap, cfg.MigrationHealthCutoff, cfg.MigratorParallelSlabsPerWorker)

	return ap, nil
}

// Handler returns an HTTP handler that serves the autopilot api.
func (ap *Autopilot) Handler() http.Handler {
	return jape.Mux(map[string]jape.Handler{
		"POST   /config/evaluate": ap.configEvaluateHandlerPOST,
		"GET    /state":           ap.stateHandlerGET,
		"POST   /trigger":         ap.triggerHandlerPOST,
	})
}

func (ap *Autopilot) configEvaluateHandlerPOST(jc jape.Context) {
	ctx := jc.Request.Context()

	// decode request
	var req api.ConfigEvaluationRequest
	if jc.Decode(&req) != nil {
		return
	}

	// fetch necessary information
	reqCfg := req.AutopilotConfig
	gs := req.GougingSettings
	rs := req.RedundancySettings
	cs, err := ap.bus.ConsensusState(ctx)
	if jc.Check("failed to get consensus state", err) != nil {
		return
	}

	// fetch hosts
	hosts, err := ap.bus.Hosts(ctx, api.HostOptions{})
	if jc.Check("failed to get hosts", err) != nil {
		return
	}

	// evaluate the config
	res, err := contractor.EvaluateConfig(reqCfg, cs, rs, gs, hosts)
	if errors.Is(err, contractor.ErrMissingRequiredFields) {
		jc.Error(err, http.StatusBadRequest)
		return
	} else if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}
	jc.Encode(res)
}

func (ap *Autopilot) Run() {
	ap.startStopMu.Lock()
	if ap.isRunning() {
		ap.startStopMu.Unlock()
		return
	}
	ap.startTime = time.Now()
	ap.triggerChan = make(chan bool, 1)
	ap.ticker = time.NewTicker(ap.tickerDuration)

	ap.wg.Add(1)
	defer ap.wg.Done()
	ap.startStopMu.Unlock()

	// block until the autopilot is online
	if online := ap.blockUntilOnline(); !online {
		ap.logger.Error("autopilot stopped before it was able to come online")
		return
	}

	// schedule a trigger when the wallet receives its first deposit
	if err := ap.tryScheduleTriggerWhenFunded(); err != nil {
		if !errors.Is(err, context.Canceled) {
			ap.logger.Error(err)
		}
		return
	}

	var forceScan bool
	for !ap.isStopped() {
		ap.logger.Info("autopilot iteration starting")
		tickerFired := make(chan struct{})
		ap.workers.withWorker(func(w Worker) {
			defer ap.logger.Info("autopilot iteration ended")

			// initiate a host scan - no need to be synced or configured for scanning
			ap.s.Scan(ap.shutdownCtx, ap.bus, forceScan)

			// reset forceScans
			forceScan = false

			// block until consensus is synced
			if synced, blocked, interrupted := ap.blockUntilSynced(ap.ticker.C); !synced {
				if interrupted {
					close(tickerFired)
					return
				}
				ap.logger.Info("autopilot stopped before consensus was synced")
				return
			} else if blocked {
				if scanning, _ := ap.s.Status(); !scanning {
					ap.s.Scan(ap.shutdownCtx, ap.bus, true)
				}
			}

			// block until the autopilot is enabled
			if enabled, interrupted := ap.blockUntilEnabled(ap.ticker.C); !enabled {
				if interrupted {
					close(tickerFired)
					return
				}
				ap.logger.Info("autopilot stopped before it was able to confirm it was enabled in the bus")
				return
			}

			// fetch autopilot
			autopilot, err := ap.bus.Autopilot(ap.shutdownCtx)
			if err != nil {
				ap.logger.Errorf("aborting maintenance, failed to fetch autopilot", zap.Error(err))
				return
			}

			// update the scanner with the hosts config
			ap.s.UpdateHostsConfig(autopilot.Hosts)

			// Log worker id chosen for this maintenance iteration.
			workerID, err := w.ID(ap.shutdownCtx)
			if err != nil {
				ap.logger.Errorf("aborting maintenance, failed to fetch worker id, err: %v", err)
				return
			}
			ap.logger.Infof("using worker %s for iteration", workerID)

			// perform wallet maintenance
			err = ap.performWalletMaintenance(ap.shutdownCtx)
			if err != nil {
				ap.logger.Errorf("wallet maintenance failed, err: %v", err)
			}

			// build maintenance state
			buildState, err := ap.buildState(ap.shutdownCtx)
			if err != nil {
				ap.logger.Errorf("aborting maintenance, failed to build state, err: %v", err)
				return
			}

			// perform maintenance
			setChanged, err := ap.c.PerformContractMaintenance(ap.shutdownCtx, buildState)
			if err != nil && utils.IsErr(err, context.Canceled) {
				return
			} else if err != nil {
				ap.logger.Errorf("contract maintenance failed, err: %v", err)
			}
			maintenanceSuccess := err == nil

			// upon success, notify the migrator. The health of slabs might have
			// changed.
			if maintenanceSuccess && setChanged {
				ap.m.SignalMaintenanceFinished()
			}

			// migration
			ap.m.tryPerformMigrations(ap.workers)

			// pruning
			if autopilot.Contracts.Prune {
				ap.tryPerformPruning()
			} else {
				ap.logger.Info("pruning disabled")
			}
		})

		select {
		case <-ap.shutdownCtx.Done():
			return
		case forceScan = <-ap.triggerChan:
			ap.logger.Info("autopilot iteration triggered")
			ap.ticker.Reset(ap.tickerDuration)
		case <-ap.ticker.C:
		case <-tickerFired:
		}
	}
}

// Shutdown shuts down the autopilot.
func (ap *Autopilot) Shutdown(ctx context.Context) error {
	ap.startStopMu.Lock()
	defer ap.startStopMu.Unlock()

	if ap.isRunning() {
		ap.ticker.Stop()
		ap.shutdownCtxCancel()
		close(ap.triggerChan)
		ap.wg.Wait()
		ap.s.Shutdown(ctx)
		ap.startTime = time.Time{}
	}
	return nil
}

func (ap *Autopilot) StartTime() time.Time {
	ap.startStopMu.Lock()
	defer ap.startStopMu.Unlock()
	return ap.startTime
}

func (ap *Autopilot) Trigger(forceScan bool) bool {
	ap.startStopMu.Lock()
	defer ap.startStopMu.Unlock()

	select {
	case ap.triggerChan <- forceScan:
		return true
	default:
		return false
	}
}

func (ap *Autopilot) Uptime() (dur time.Duration) {
	ap.startStopMu.Lock()
	defer ap.startStopMu.Unlock()
	if ap.isRunning() {
		dur = time.Since(ap.startTime)
	}
	return
}

func (ap *Autopilot) blockUntilEnabled(interrupt <-chan time.Time) (enabled, interrupted bool) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var once sync.Once

	for {
		autopilot, err := ap.bus.Autopilot(ap.shutdownCtx)
		if err != nil && !errors.Is(err, context.Canceled) {
			ap.logger.Errorf("unable to fetch autopilot from the bus, err: %v", err)
		}

		if err != nil || !autopilot.Enabled {
			once.Do(func() { ap.logger.Info("autopilot is waiting to be enabled...") })
			select {
			case <-ap.shutdownCtx.Done():
				return false, false
			case <-interrupt:
				return false, true
			case <-ticker.C:
				continue
			}
		}
		return true, false
	}
}

func (ap *Autopilot) blockUntilOnline() (online bool) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var once sync.Once

	for {
		ctx, cancel := context.WithTimeout(ap.shutdownCtx, 30*time.Second)
		peers, err := ap.bus.SyncerPeers(ctx)
		online = len(peers) > 0
		cancel()

		if utils.IsErr(err, context.Canceled) {
			return
		} else if err != nil {
			ap.logger.Errorf("failed to get peers, err: %v", err)
		} else if !online {
			once.Do(func() { ap.logger.Info("autopilot is waiting on the bus to connect to peers...") })
		}

		if err != nil || !online {
			select {
			case <-ap.shutdownCtx.Done():
				return
			case <-ticker.C:
				continue
			}
		}
		return
	}
}

func (ap *Autopilot) blockUntilSynced(interrupt <-chan time.Time) (synced, blocked, interrupted bool) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var once sync.Once

	for {
		// try and fetch consensus
		ctx, cancel := context.WithTimeout(ap.shutdownCtx, 30*time.Second)
		cs, err := ap.bus.ConsensusState(ctx)
		synced = cs.Synced
		cancel()

		// if an error occurred, or if we're not synced, we continue
		if utils.IsErr(err, context.Canceled) {
			return
		} else if err != nil {
			ap.logger.Errorf("failed to get consensus state, err: %v", err)
		} else if !synced {
			once.Do(func() { ap.logger.Info("autopilot is waiting for consensus to sync...") })
		}

		if err != nil || !synced {
			blocked = true
			select {
			case <-ap.shutdownCtx.Done():
				return
			case <-interrupt:
				interrupted = true
				return
			case <-ticker.C:
				continue
			}
		}
		return
	}
}

func (ap *Autopilot) tryScheduleTriggerWhenFunded() error {
	// apply sane timeout
	ctx, cancel := context.WithTimeout(ap.shutdownCtx, time.Minute)
	defer cancel()

	// no need to schedule a trigger if the wallet is already funded
	wallet, err := ap.bus.Wallet(ctx)
	if err != nil {
		return err
	} else if !wallet.Confirmed.Add(wallet.Unconfirmed).IsZero() {
		return nil
	}

	// spin a goroutine that triggers the autopilot when we receive a deposit
	ap.logger.Info("autopilot loop trigger is scheduled for when the wallet receives a deposit")
	ap.wg.Add(1)
	go func() {
		defer ap.wg.Done()

		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ap.shutdownCtx.Done():
				return
			case <-ticker.C:
			}

			// fetch wallet info
			ctx, cancel := context.WithTimeout(ap.shutdownCtx, 30*time.Second)
			if wallet, err = ap.bus.Wallet(ctx); err != nil {
				ap.logger.Errorf("failed to get wallet info, err: %v", err)
			}
			cancel()

			// if we have received a deposit, trigger the autopilot
			if !wallet.Confirmed.Add(wallet.Unconfirmed).IsZero() {
				if ap.Trigger(false) {
					return
				}
			}
		}
	}()

	return nil
}

func (ap *Autopilot) isRunning() bool {
	return !ap.startTime.IsZero()
}

func (ap *Autopilot) isStopped() bool {
	select {
	case <-ap.shutdownCtx.Done():
		return true
	default:
		return false
	}
}

func (ap *Autopilot) performWalletMaintenance(ctx context.Context) error {
	if ap.isStopped() {
		return nil // skip contract maintenance if we're not synced
	}

	ap.logger.Info("performing wallet maintenance")

	autopilot, err := ap.bus.Autopilot(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch autopilot: %w", err)
	}
	w, err := ap.bus.Wallet(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch wallet: %w", err)
	}

	// convenience variables
	b := ap.bus
	l := ap.logger
	cfg := autopilot.AutopilotConfig
	renewWindow := cfg.Contracts.RenewWindow

	// no contracts - nothing to do
	if cfg.Contracts.Amount == 0 {
		l.Warn("wallet maintenance skipped, no contracts wanted")
		return nil
	}

	// fetch consensus state
	cs, err := ap.bus.ConsensusState(ctx)
	if err != nil {
		l.Warnf("wallet maintenance skipped, fetching consensus state failed with err: %v", err)
		return err
	}

	// fetch wallet balance
	wallet, err := b.Wallet(ctx)
	if err != nil {
		l.Warnf("wallet maintenance skipped, fetching wallet balance failed with err: %v", err)
		return err
	}
	balance := wallet.Confirmed

	// register an alert if balance is low
	if balance.Cmp(contractor.InitialContractFunding.Mul64(cfg.Contracts.Amount)) < 0 {
		ap.RegisterAlert(ctx, newAccountLowBalanceAlert(w.Address, balance, contractor.InitialContractFunding, cs.BlockHeight, renewWindow, autopilot.EndHeight()))
	} else {
		ap.DismissAlert(ctx, alertLowBalanceID)
	}

	// pending maintenance transaction - nothing to do
	pending, err := b.WalletPending(ctx)
	if err != nil {
		return nil
	}
	for _, txn := range pending {
		for _, mTxnID := range ap.maintenanceTxnIDs {
			if mTxnID == types.TransactionID(txn.ID) {
				l.Debugf("wallet maintenance skipped, pending transaction found with id %v", mTxnID)
				return nil
			}
		}
	}

	// figure out the amount per output
	wantedNumOutputs := 10
	amount := contractor.InitialContractFunding.Div64(uint64(wantedNumOutputs))

	// redistribute outputs
	ids, err := b.WalletRedistribute(ctx, wantedNumOutputs, amount)
	if err != nil {
		return fmt.Errorf("failed to redistribute wallet into %d outputs of amount %v, balance %v, err %v", wantedNumOutputs, amount, balance, err)
	}

	l.Debugf("wallet maintenance succeeded, txns %v", ids)
	ap.maintenanceTxnIDs = ids
	return nil
}

func (ap *Autopilot) triggerHandlerPOST(jc jape.Context) {
	var req api.AutopilotTriggerRequest
	if jc.Decode(&req) != nil {
		return
	}
	jc.Encode(api.AutopilotTriggerResponse{
		Triggered: ap.Trigger(req.ForceScan),
	})
}

func (ap *Autopilot) stateHandlerGET(jc jape.Context) {
	ap.mu.Lock()
	pruning, pLastStart := ap.pruning, ap.pruningLastStart // TODO: move to a 'pruner' type
	ap.mu.Unlock()
	migrating, mLastStart := ap.m.Status()
	scanning, sLastStart := ap.s.Status()

	autopilot, err := ap.bus.Autopilot(jc.Request.Context())
	if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}

	jc.Encode(api.AutopilotStateResponse{
		Enabled:            autopilot.Enabled,
		Migrating:          migrating,
		MigratingLastStart: api.TimeRFC3339(mLastStart),
		Pruning:            pruning,
		PruningLastStart:   api.TimeRFC3339(pLastStart),
		Scanning:           scanning,
		ScanningLastStart:  api.TimeRFC3339(sLastStart),
		UptimeMS:           api.DurationMS(ap.Uptime()),

		StartTime: api.TimeRFC3339(ap.StartTime()),
		BuildState: api.BuildState{
			Version:   build.Version(),
			Commit:    build.Commit(),
			OS:        runtime.GOOS,
			BuildTime: api.TimeRFC3339(build.BuildTime()),
		},
	})
}

func (ap *Autopilot) buildState(ctx context.Context) (*contractor.MaintenanceState, error) {
	// fetch autopilot
	autopilot, err := ap.bus.Autopilot(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not fetch autopilot, err: %v", err)
	}

	// fetch consensus state
	cs, err := ap.bus.ConsensusState(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not fetch consensus state, err: %v", err)
	} else if !cs.Synced {
		return nil, errors.New("consensus not synced")
	}

	// fetch upload settings
	us, err := ap.bus.UploadSettings(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not fetch upload settings, err: %v", err)
	}

	// fetch gouging settings
	gs, err := ap.bus.GougingSettings(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not fetch gouging settings, err: %v", err)
	}

	// fetch recommended transaction fee
	fee, err := ap.bus.RecommendedFee(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not fetch fee, err: %v", err)
	}

	// fetch our wallet address
	wi, err := ap.bus.Wallet(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not fetch wallet address, err: %v", err)
	}
	address := wi.Address

	// no need to try and form contracts if wallet is completely empty
	skipContractFormations := wi.Confirmed.IsZero() && wi.Unconfirmed.IsZero()
	if skipContractFormations {
		ap.logger.Warn("contract formations skipped, wallet is empty")
	}

	// update current period if necessary
	if cs.BlockHeight > 0 {
		if autopilot.CurrentPeriod == 0 {
			err := ap.bus.UpdateCurrentPeriod(ctx, cs.BlockHeight)
			if err != nil {
				return nil, err
			}
			autopilot.CurrentPeriod = cs.BlockHeight
			ap.logger.Infof("initialised current period to %d", autopilot.CurrentPeriod)
		} else if nextPeriod := computeNextPeriod(cs.BlockHeight, autopilot.CurrentPeriod, autopilot.Contracts.Period); nextPeriod != autopilot.CurrentPeriod {
			err := ap.bus.UpdateCurrentPeriod(ctx, nextPeriod)
			if err != nil {
				return nil, err
			}
			ap.logger.Infof("updated current period from %d to %d", autopilot.CurrentPeriod, nextPeriod)
			autopilot.CurrentPeriod = nextPeriod
		}
	} else if !skipContractFormations {
		skipContractFormations = true
	}

	return &contractor.MaintenanceState{
		GS: gs,
		RS: us.Redundancy,
		AP: autopilot,

		Address:                address,
		Fee:                    fee,
		SkipContractFormations: skipContractFormations,
	}, nil
}

func computeNextPeriod(bh, currentPeriod, period uint64) uint64 {
	prevPeriod := currentPeriod
	nextPeriod := prevPeriod
	for bh >= nextPeriod+period {
		nextPeriod += period
	}
	return nextPeriod
}
