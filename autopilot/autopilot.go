package autopilot

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/jape"
	"go.sia.tech/renterd/alerts"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/autopilot/contractor"
	"go.sia.tech/renterd/build"
	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/internal/utils"
	"go.sia.tech/renterd/object"
	"go.sia.tech/renterd/wallet"
	"go.sia.tech/renterd/webhooks"
	"go.uber.org/zap"
)

type Bus interface {
	alerts.Alerter
	webhooks.Broadcaster

	// Accounts
	Account(ctx context.Context, id rhpv3.Account, hostKey types.PublicKey) (account api.Account, err error)
	Accounts(ctx context.Context) (accounts []api.Account, err error)

	// Autopilots
	Autopilot(ctx context.Context, id string) (autopilot api.Autopilot, err error)
	UpdateAutopilot(ctx context.Context, autopilot api.Autopilot) error

	// consensus
	ConsensusState(ctx context.Context) (api.ConsensusState, error)

	// contracts
	AddContract(ctx context.Context, c rhpv2.ContractRevision, contractPrice, totalCost types.Currency, startHeight uint64, state string) (api.ContractMetadata, error)
	AddRenewedContract(ctx context.Context, c rhpv2.ContractRevision, contractPrice, totalCost types.Currency, startHeight uint64, renewedFrom types.FileContractID, state string) (api.ContractMetadata, error)
	AncestorContracts(ctx context.Context, id types.FileContractID, minStartHeight uint64) ([]api.ArchivedContract, error)
	ArchiveContracts(ctx context.Context, toArchive map[types.FileContractID]string) error
	Contract(ctx context.Context, id types.FileContractID) (api.ContractMetadata, error)
	Contracts(ctx context.Context, opts api.ContractsOpts) (contracts []api.ContractMetadata, err error)
	FileContractTax(ctx context.Context, payout types.Currency) (types.Currency, error)
	SetContractSet(ctx context.Context, set string, contracts []types.FileContractID) error
	PrunableData(ctx context.Context) (prunableData api.ContractsPrunableDataResponse, err error)

	// hostdb
	Host(ctx context.Context, hostKey types.PublicKey) (hostdb.HostInfo, error)
	HostsForScanning(ctx context.Context, opts api.HostsForScanningOptions) ([]hostdb.HostAddress, error)
	RemoveOfflineHosts(ctx context.Context, minRecentScanFailures uint64, maxDowntime time.Duration) (uint64, error)
	SearchHosts(ctx context.Context, opts api.SearchHostOptions) ([]hostdb.HostInfo, error)

	// metrics
	RecordContractSetChurnMetric(ctx context.Context, metrics ...api.ContractSetChurnMetric) error
	RecordContractPruneMetric(ctx context.Context, metrics ...api.ContractPruneMetric) error

	// buckets
	ListBuckets(ctx context.Context) ([]api.Bucket, error)

	// objects
	ObjectsBySlabKey(ctx context.Context, bucket string, key object.EncryptionKey) (objects []api.ObjectMetadata, err error)
	RefreshHealth(ctx context.Context) error
	Slab(ctx context.Context, key object.EncryptionKey) (object.Slab, error)
	SlabsForMigration(ctx context.Context, healthCutoff float64, set string, limit int) ([]api.UnhealthySlab, error)

	// settings
	UpdateSetting(ctx context.Context, key string, value interface{}) error
	GougingSettings(ctx context.Context) (gs api.GougingSettings, err error)
	RedundancySettings(ctx context.Context) (rs api.RedundancySettings, err error)

	// syncer
	SyncerPeers(ctx context.Context) (resp []string, err error)

	// txpool
	RecommendedFee(ctx context.Context) (types.Currency, error)
	TransactionPool(ctx context.Context) (txns []types.Transaction, err error)

	// wallet
	Wallet(ctx context.Context) (api.WalletResponse, error)
	WalletDiscard(ctx context.Context, txn types.Transaction) error
	WalletOutputs(ctx context.Context) (resp []wallet.SiacoinElement, err error)
	WalletPending(ctx context.Context) (resp []types.Transaction, err error)
	WalletRedistribute(ctx context.Context, outputs int, amount types.Currency) (ids []types.TransactionID, err error)
}

type Autopilot struct {
	id string

	alerts  alerts.Alerter
	bus     Bus
	logger  *zap.SugaredLogger
	workers *workerPool

	a *accounts
	c *contractor.Contractor
	m *migrator
	s *scanner

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

	maintenanceTxnIDs []types.TransactionID
}

// New initializes an Autopilot.
func New(id string, bus Bus, workers []Worker, logger *zap.Logger, heartbeat time.Duration, scannerScanInterval time.Duration, scannerBatchSize, scannerNumThreads uint64, migrationHealthCutoff float64, accountsRefillInterval time.Duration, revisionSubmissionBuffer, migratorParallelSlabsPerWorker uint64, revisionBroadcastInterval time.Duration) (*Autopilot, error) {
	shutdownCtx, shutdownCtxCancel := context.WithCancel(context.Background())

	ap := &Autopilot{
		alerts:  alerts.WithOrigin(bus, fmt.Sprintf("autopilot.%s", id)),
		id:      id,
		bus:     bus,
		logger:  logger.Sugar().Named("autopilot").Named(id),
		workers: newWorkerPool(workers),

		shutdownCtx:       shutdownCtx,
		shutdownCtxCancel: shutdownCtxCancel,

		tickerDuration: heartbeat,
	}
	scanner, err := newScanner(
		ap,
		scannerBatchSize,
		scannerNumThreads,
		scannerScanInterval,
		scannerTimeoutInterval,
		scannerTimeoutMinTimeout,
	)
	if err != nil {
		return nil, err
	}

	ap.s = scanner
	ap.c = contractor.New(bus, ap.logger, revisionSubmissionBuffer, revisionBroadcastInterval)
	ap.m = newMigrator(ap, migrationHealthCutoff, migratorParallelSlabsPerWorker)
	ap.a = newAccounts(ap, ap.bus, ap.bus, ap.workers, ap.logger, accountsRefillInterval)

	return ap, nil
}

func (ap *Autopilot) Config(ctx context.Context) (api.Autopilot, error) {
	return ap.bus.Autopilot(ctx, ap.id)
}

// Handler returns an HTTP handler that serves the autopilot api.
func (ap *Autopilot) Handler() http.Handler {
	return jape.Mux(map[string]jape.Handler{
		"GET    /config":        ap.configHandlerGET,
		"PUT    /config":        ap.configHandlerPUT,
		"POST   /config":        ap.configHandlerPOST,
		"POST   /hosts":         ap.hostsHandlerPOST,
		"GET    /host/:hostKey": ap.hostHandlerGET,
		"GET    /state":         ap.stateHandlerGET,
		"POST   /trigger":       ap.triggerHandlerPOST,
	})
}

func (ap *Autopilot) configHandlerPOST(jc jape.Context) {
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
	fee, err := ap.bus.RecommendedFee(ctx)
	if jc.Check("failed to get recommended fee", err) != nil {
		return
	}
	cfg, err := ap.Config(ctx)
	if jc.Check("failed to get autopilot config", err) != nil {
		return
	}

	// fetch hosts
	hosts, err := ap.bus.SearchHosts(ctx, api.SearchHostOptions{Limit: -1, FilterMode: api.HostFilterModeAllowed})
	if jc.Check("failed to get hosts", err) != nil {
		return
	}

	// evaluate the config
	jc.Encode(contractor.EvaluateConfig(reqCfg.Contracts, cs, fee, cfg.CurrentPeriod, rs, gs, hosts))
}

func (ap *Autopilot) Run() error {
	ap.startStopMu.Lock()
	if ap.isRunning() {
		ap.startStopMu.Unlock()
		return errors.New("already running")
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
		return nil
	}

	// schedule a trigger when the wallet receives its first deposit
	if err := ap.tryScheduleTriggerWhenFunded(); err != nil {
		if !errors.Is(err, context.Canceled) {
			ap.logger.Error(err)
		}
		return nil
	}

	var forceScan bool
	var launchAccountRefillsOnce sync.Once
	for !ap.isStopped() {
		ap.logger.Info("autopilot iteration starting")
		tickerFired := make(chan struct{})
		ap.workers.withWorker(func(w Worker) {
			defer ap.logger.Info("autopilot iteration ended")

			// initiate a host scan - no need to be synced or configured for scanning
			ap.s.tryUpdateTimeout()
			ap.s.tryPerformHostScan(ap.shutdownCtx, w, forceScan)

			// reset forceScan
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
					ap.s.tryPerformHostScan(ap.shutdownCtx, w, true)
				}
			}

			// block until the autopilot is configured
			if configured, interrupted := ap.blockUntilConfigured(ap.ticker.C); !configured {
				if interrupted {
					close(tickerFired)
					return
				}
				ap.logger.Info("autopilot stopped before it was able to confirm it was configured in the bus")
				return
			}

			// fetch configuration
			autopilot, err := ap.Config(ap.shutdownCtx)
			if err != nil {
				ap.logger.Errorf("aborting maintenance, failed to fetch autopilot config", zap.Error(err))
				return
			}

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
			state, err := ap.buildState(ap.shutdownCtx)
			if err != nil {
				ap.logger.Errorf("aborting maintenance, failed to build state, err: %v", err)
				return
			}

			// perform maintenance
			setChanged, err := ap.c.PerformContractMaintenance(ap.shutdownCtx, w, state)
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

			// launch account refills after successful contract maintenance.
			if maintenanceSuccess {
				launchAccountRefillsOnce.Do(func() {
					ap.logger.Debug("account refills loop launched")
					go ap.a.refillWorkersAccountsLoop(ap.shutdownCtx)
				})
			}

			// migration
			ap.m.tryPerformMigrations(ap.workers)

			// pruning
			if autopilot.Config.Contracts.Prune {
				ap.tryPerformPruning(ap.workers)
			} else {
				ap.logger.Debug("pruning disabled")
			}
		})

		select {
		case <-ap.shutdownCtx.Done():
			return nil
		case forceScan = <-ap.triggerChan:
			ap.logger.Info("autopilot iteration triggered")
			ap.ticker.Reset(ap.tickerDuration)
		case <-ap.ticker.C:
		case <-tickerFired:
		}
	}
	return nil
}

// Shutdown shuts down the autopilot.
func (ap *Autopilot) Shutdown(_ context.Context) error {
	ap.startStopMu.Lock()
	defer ap.startStopMu.Unlock()

	if ap.isRunning() {
		ap.ticker.Stop()
		ap.shutdownCtxCancel()
		close(ap.triggerChan)
		ap.wg.Wait()
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

func (ap *Autopilot) blockUntilConfigured(interrupt <-chan time.Time) (configured, interrupted bool) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var once sync.Once

	for {
		// try and fetch the config
		ctx, cancel := context.WithTimeout(ap.shutdownCtx, 30*time.Second)
		_, err := ap.bus.Autopilot(ctx, ap.id)
		cancel()

		// if the config was not found, or we were unable to fetch it, keep blocking
		if utils.IsErr(err, context.Canceled) {
			return
		} else if utils.IsErr(err, api.ErrAutopilotNotFound) {
			once.Do(func() { ap.logger.Info("autopilot is waiting to be configured...") })
		} else if err != nil {
			ap.logger.Errorf("autopilot is unable to fetch its configuration from the bus, err: %v", err)
		}
		if err != nil {
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

	autopilot, err := ap.Config(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch autopilot config: %w", err)
	}
	w, err := ap.bus.Wallet(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch wallet: %w", err)
	}

	// convenience variables
	b := ap.bus
	l := ap.logger
	cfg := autopilot.Config
	renewWindow := cfg.Contracts.RenewWindow

	// no contracts - nothing to do
	if cfg.Contracts.Amount == 0 {
		l.Warn("wallet maintenance skipped, no contracts wanted")
		return nil
	}

	// no allowance - nothing to do
	if cfg.Contracts.Allowance.IsZero() {
		l.Warn("wallet maintenance skipped, no allowance set")
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
	if balance.Cmp(cfg.Contracts.Allowance) < 0 {
		ap.RegisterAlert(ctx, newAccountLowBalanceAlert(w.Address, balance, cfg.Contracts.Allowance, cs.BlockHeight, renewWindow, autopilot.EndHeight()))
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
			if mTxnID == txn.ID() {
				l.Debugf("wallet maintenance skipped, pending transaction found with id %v", mTxnID)
				return nil
			}
		}
	}

	wantedNumOutputs := 10

	// enough outputs - nothing to do
	available, err := b.WalletOutputs(ctx)
	if err != nil {
		return err
	}
	if uint64(len(available)) >= uint64(wantedNumOutputs) {
		l.Debugf("no wallet maintenance needed, plenty of outputs available (%v>=%v)", len(available), uint64(wantedNumOutputs))
		return nil
	}
	wantedNumOutputs -= len(available)

	// figure out the amount per output
	amount := cfg.Contracts.Allowance.Div64(uint64(wantedNumOutputs))

	// redistribute outputs
	ids, err := b.WalletRedistribute(ctx, wantedNumOutputs, amount)
	if err != nil {
		return fmt.Errorf("failed to redistribute wallet into %d outputs of amount %v, balance %v, err %v", wantedNumOutputs, amount, balance, err)
	}

	l.Debugf("wallet maintenance succeeded, txns %v", ids)
	ap.maintenanceTxnIDs = ids
	return nil
}

func (ap *Autopilot) configHandlerGET(jc jape.Context) {
	autopilot, err := ap.bus.Autopilot(jc.Request.Context(), ap.id)
	if utils.IsErr(err, api.ErrAutopilotNotFound) {
		jc.Error(errors.New("autopilot is not configured yet"), http.StatusNotFound)
		return
	}

	if jc.Check("failed to get autopilot config", err) == nil {
		jc.Encode(autopilot.Config)
	}
}

func (ap *Autopilot) configHandlerPUT(jc jape.Context) {
	// decode and validate the config
	var cfg api.AutopilotConfig
	if jc.Decode(&cfg) != nil {
		return
	} else if err := cfg.Validate(); jc.Check("invalid autopilot config", err) != nil {
		return
	}

	// fetch the autopilot and update its config
	var contractSetChanged bool
	autopilot, err := ap.bus.Autopilot(jc.Request.Context(), ap.id)
	if utils.IsErr(err, api.ErrAutopilotNotFound) {
		autopilot = api.Autopilot{ID: ap.id, Config: cfg}
	} else {
		if autopilot.Config.Contracts.Set != cfg.Contracts.Set {
			contractSetChanged = true
		}
		autopilot.Config = cfg
	}

	// update the autopilot and interrupt migrations if necessary
	if err := jc.Check("failed to update autopilot config", ap.bus.UpdateAutopilot(jc.Request.Context(), autopilot)); err == nil && contractSetChanged {
		ap.m.SignalMaintenanceFinished()
	}
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

func (ap *Autopilot) hostHandlerGET(jc jape.Context) {
	var hostKey types.PublicKey
	if jc.DecodeParam("hostKey", &hostKey) != nil {
		return
	}

	state, err := ap.buildState(jc.Request.Context())
	if jc.Check("failed to build state", err) != nil {
		return
	}

	host, err := ap.c.HostInfo(jc.Request.Context(), hostKey, state)
	if jc.Check("failed to get host info", err) != nil {
		return
	}
	jc.Encode(host)
}

func (ap *Autopilot) stateHandlerGET(jc jape.Context) {
	ap.mu.Lock()
	pruning, pLastStart := ap.pruning, ap.pruningLastStart // TODO: move to a 'pruner' type
	ap.mu.Unlock()
	migrating, mLastStart := ap.m.Status()
	scanning, sLastStart := ap.s.Status()
	_, err := ap.bus.Autopilot(jc.Request.Context(), ap.id)
	if err != nil && !strings.Contains(err.Error(), api.ErrAutopilotNotFound.Error()) {
		jc.Error(err, http.StatusInternalServerError)
		return
	}

	jc.Encode(api.AutopilotStateResponse{
		Configured:         err == nil,
		Migrating:          migrating,
		MigratingLastStart: api.TimeRFC3339(mLastStart),
		Pruning:            pruning,
		PruningLastStart:   api.TimeRFC3339(pLastStart),
		Scanning:           scanning,
		ScanningLastStart:  api.TimeRFC3339(sLastStart),
		UptimeMS:           api.DurationMS(ap.Uptime()),

		StartTime: api.TimeRFC3339(ap.StartTime()),
		BuildState: api.BuildState{
			Network:   build.NetworkName(),
			Version:   build.Version(),
			Commit:    build.Commit(),
			OS:        runtime.GOOS,
			BuildTime: api.TimeRFC3339(build.BuildTime()),
		},
	})
}

func (ap *Autopilot) hostsHandlerPOST(jc jape.Context) {
	var req api.SearchHostsRequest
	if jc.Decode(&req) != nil {
		return
	}
	state, err := ap.buildState(jc.Request.Context())
	if jc.Check("failed to build state", err) != nil {
		return
	}
	hosts, err := ap.c.HostInfos(jc.Request.Context(), state, req.FilterMode, req.UsabilityMode, req.AddressContains, req.KeyIn, req.Offset, req.Limit)
	if jc.Check("failed to get host info", err) != nil {
		return
	}
	jc.Encode(hosts)
}

func (ap *Autopilot) buildState(ctx context.Context) (*contractor.State, error) {
	// fetch the autopilot from the bus
	autopilot, err := ap.Config(ctx)
	if err != nil {
		return nil, err
	}

	// fetch consensus state
	cs, err := ap.bus.ConsensusState(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not fetch consensus state, err: %v", err)
	}

	// fetch redundancy settings
	rs, err := ap.bus.RedundancySettings(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not fetch redundancy settings, err: %v", err)
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

	// update current period if necessary
	if cs.Synced {
		if autopilot.CurrentPeriod == 0 {
			autopilot.CurrentPeriod = cs.BlockHeight
			err := ap.bus.UpdateAutopilot(ctx, autopilot)
			if err != nil {
				return nil, err
			}
			ap.logger.Infof("initialised current period to %d", autopilot.CurrentPeriod)
		} else if nextPeriod := autopilot.CurrentPeriod + autopilot.Config.Contracts.Period; cs.BlockHeight >= nextPeriod {
			prevPeriod := autopilot.CurrentPeriod
			autopilot.CurrentPeriod = nextPeriod
			err := ap.bus.UpdateAutopilot(ctx, autopilot)
			if err != nil {
				return nil, err
			}
			ap.logger.Infof("updated current period from %d to %d", prevPeriod, nextPeriod)
		}
	}

	return &contractor.State{
		GS: gs,
		RS: rs,
		AP: autopilot,

		Address: address,
		Fee:     fee,
	}, nil
}
