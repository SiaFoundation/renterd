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
	"go.sia.tech/renterd/build"
	"go.sia.tech/renterd/hostdb"
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
	Contracts(ctx context.Context) (contracts []api.ContractMetadata, err error)
	ContractSetContracts(ctx context.Context, set string) ([]api.ContractMetadata, error)
	FileContractTax(ctx context.Context, payout types.Currency) (types.Currency, error)
	SetContractSet(ctx context.Context, set string, contracts []types.FileContractID) error
	PrunableData(ctx context.Context) (prunableData api.ContractsPrunableDataResponse, err error)

	// hostdb
	Host(ctx context.Context, hostKey types.PublicKey) (hostdb.HostInfo, error)
	Hosts(ctx context.Context, opts api.GetHostsOptions) ([]hostdb.Host, error)
	HostsForScanning(ctx context.Context, opts api.HostsForScanningOptions) ([]hostdb.HostAddress, error)
	RemoveOfflineHosts(ctx context.Context, minRecentScanFailures uint64, maxDowntime time.Duration) (uint64, error)
	SearchHosts(ctx context.Context, opts api.SearchHostOptions) ([]hostdb.Host, error)

	// metrics
	RecordContractSetChurnMetric(ctx context.Context, metrics ...api.ContractSetChurnMetric) error
	RecordContractPruneMetric(ctx context.Context, metrics ...api.ContractPruneMetric) error

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
	c *contractor
	m *migrator
	s *scanner

	tickerDuration time.Duration
	wg             sync.WaitGroup

	stateMu sync.Mutex
	state   state

	startStopMu       sync.Mutex
	startTime         time.Time
	shutdownCtx       context.Context
	shutdownCtxCancel context.CancelFunc
	ticker            *time.Ticker
	triggerChan       chan bool
}

// state holds a bunch of variables that are used by the autopilot and updated
type state struct {
	gs  api.GougingSettings
	rs  api.RedundancySettings
	cfg api.AutopilotConfig

	address types.Address
	fee     types.Currency
	period  uint64
}

// New initializes an Autopilot.
func New(id string, bus Bus, workers []Worker, logger *zap.Logger, heartbeat time.Duration, scannerScanInterval time.Duration, scannerBatchSize, scannerNumThreads uint64, migrationHealthCutoff float64, accountsRefillInterval time.Duration, revisionSubmissionBuffer, migratorParallelSlabsPerWorker uint64, revisionBroadcastInterval time.Duration) (*Autopilot, error) {
	shutdownCtx, shutdownCtxCancel := context.WithCancel(context.Background())

	ap := &Autopilot{
		alerts:  alerts.WithOrigin(bus, fmt.Sprintf("autopilot.%s", id)),
		id:      id,
		bus:     bus,
		logger:  logger.Sugar().Named(api.DefaultAutopilotID),
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
	ap.c = newContractor(ap, revisionSubmissionBuffer, revisionBroadcastInterval)
	ap.m = newMigrator(ap, migrationHealthCutoff, migratorParallelSlabsPerWorker)
	ap.a = newAccounts(ap, ap.bus, ap.bus, ap.workers, ap.logger, accountsRefillInterval)

	return ap, nil
}

// Handler returns an HTTP handler that serves the autopilot api.
func (ap *Autopilot) Handler() http.Handler {
	return jape.Mux(map[string]jape.Handler{
		"GET    /config":        ap.configHandlerGET,
		"PUT    /config":        ap.configHandlerPUT,
		"POST   /hosts":         ap.hostsHandlerPOST,
		"GET    /host/:hostKey": ap.hostHandlerGET,
		"GET    /state":         ap.stateHandlerGET,
		"POST   /trigger":       ap.triggerHandlerPOST,
	})
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
		ap.logger.Error(err)
		return nil
	}

	var forceScan bool
	var launchAccountRefillsOnce sync.Once
	for {
		ap.logger.Info("autopilot iteration starting")
		tickerFired := make(chan struct{})
		ap.workers.withWorker(func(w Worker) {
			defer ap.logger.Info("autopilot iteration ended")

			// create a new context for this iteration
			ctx, cancel := context.WithCancel(ap.shutdownCtx)
			defer cancel()

			// initiate a host scan - no need to be synced or configured for scanning
			ap.s.tryUpdateTimeout()
			ap.s.tryPerformHostScan(ctx, w, forceScan)

			// reset forceScan
			forceScan = false

			// block until consensus is synced
			if synced, blocked, interrupted := ap.blockUntilSynced(ap.ticker.C); !synced {
				if interrupted {
					close(tickerFired)
					return
				}
				ap.logger.Error("autopilot stopped before consensus was synced")
				return
			} else if blocked {
				if scanning, _ := ap.s.Status(); !scanning {
					ap.s.tryPerformHostScan(ctx, w, true)
				}
			}

			// block until the autopilot is configured
			if configured, interrupted := ap.blockUntilConfigured(ap.ticker.C); !configured {
				if interrupted {
					close(tickerFired)
					return
				}
				ap.logger.Error("autopilot stopped before it was able to confirm it was configured in the bus")
				return
			}

			// Log worker id chosen for this maintenance iteration.
			workerID, err := w.ID(ctx)
			if err != nil {
				ap.logger.Errorf("aborting maintenance, failed to fetch worker id, err: %v", err)
				return
			}
			ap.logger.Infof("using worker %s for iteration", workerID)

			// update the loop state
			//
			// NOTE: it is important this is the first action we perform in this
			// iteration of the loop, keeping a state object ensures we use the
			// same state throughout the entire iteration and we don't needless
			// fetch the same information twice
			err = ap.updateState(ctx)
			if err != nil {
				ap.logger.Errorf("failed to update state, err: %v", err)
				return
			}

			// perform wallet maintenance
			err = ap.c.performWalletMaintenance(ctx)
			if err != nil {
				ap.logger.Errorf("wallet maintenance failed, err: %v", err)
			}

			// perform maintenance
			setChanged, err := ap.c.performContractMaintenance(ctx, w)
			if err != nil && isErr(err, context.Canceled) {
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
			ap.m.tryPerformMigrations(ctx, ap.workers)

			// pruning
			if ap.state.cfg.Contracts.Prune {
				ap.c.tryPerformPruning(ctx, ap.workers)
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

func (ap *Autopilot) State() state {
	ap.stateMu.Lock()
	defer ap.stateMu.Unlock()
	return ap.state
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
		if isErr(err, context.Canceled) {
			return
		} else if isErr(err, api.ErrAutopilotNotFound) {
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

		if isErr(err, context.Canceled) {
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
		if isErr(err, context.Canceled) {
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
	ctx, cancel := context.WithTimeout(ap.shutdownCtx, 30*time.Second)
	wallet, err := ap.bus.Wallet(ctx)
	cancel()

	// no need to schedule a trigger if the wallet is already funded
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

func (ap *Autopilot) updateState(ctx context.Context) error {
	// fetch the autopilot from the bus
	autopilot, err := ap.bus.Autopilot(ctx, ap.id)
	if err != nil {
		return err
	}

	// fetch consensus state
	cs, err := ap.bus.ConsensusState(ctx)
	if err != nil {
		return fmt.Errorf("could not fetch consensus state, err: %v", err)
	}

	// fetch redundancy settings
	rs, err := ap.bus.RedundancySettings(ctx)
	if err != nil {
		return fmt.Errorf("could not fetch redundancy settings, err: %v", err)
	}

	// fetch gouging settings
	gs, err := ap.bus.GougingSettings(ctx)
	if err != nil {
		return fmt.Errorf("could not fetch gouging settings, err: %v", err)
	}

	// fetch recommended transaction fee
	fee, err := ap.bus.RecommendedFee(ctx)
	if err != nil {
		return fmt.Errorf("could not fetch fee, err: %v", err)
	}

	// fetch our wallet address
	wi, err := ap.bus.Wallet(ctx)
	if err != nil {
		return fmt.Errorf("could not fetch wallet address, err: %v", err)
	}
	address := wi.Address

	// update current period if necessary
	if cs.Synced {
		if autopilot.CurrentPeriod == 0 {
			autopilot.CurrentPeriod = cs.BlockHeight
			err := ap.bus.UpdateAutopilot(ctx, autopilot)
			if err != nil {
				return err
			}
			ap.logger.Infof("initialised current period to %d", autopilot.CurrentPeriod)
		} else if nextPeriod := autopilot.CurrentPeriod + autopilot.Config.Contracts.Period; cs.BlockHeight >= nextPeriod {
			prevPeriod := autopilot.CurrentPeriod
			autopilot.CurrentPeriod = nextPeriod
			err := ap.bus.UpdateAutopilot(ctx, autopilot)
			if err != nil {
				return err
			}
			ap.logger.Infof("updated current period from %d to %d", prevPeriod, nextPeriod)
		}
	}

	// update the state
	ap.stateMu.Lock()
	ap.state = state{
		gs:  gs,
		rs:  rs,
		cfg: autopilot.Config,

		address: address,
		fee:     fee,
		period:  autopilot.CurrentPeriod,
	}
	ap.stateMu.Unlock()
	return nil
}

func (ap *Autopilot) isStopped() bool {
	select {
	case <-ap.shutdownCtx.Done():
		return true
	default:
		return false
	}
}

func (ap *Autopilot) configHandlerGET(jc jape.Context) {
	autopilot, err := ap.bus.Autopilot(jc.Request.Context(), ap.id)
	if err != nil && strings.Contains(err.Error(), api.ErrAutopilotNotFound.Error()) {
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
	if err != nil && strings.Contains(err.Error(), api.ErrAutopilotNotFound.Error()) {
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

	host, err := ap.c.HostInfo(jc.Request.Context(), hostKey)
	if jc.Check("failed to get host info", err) != nil {
		return
	}
	jc.Encode(host)
}

func (ap *Autopilot) stateHandlerGET(jc jape.Context) {
	pruning, pLastStart := ap.c.Status()
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
	hosts, err := ap.c.HostInfos(jc.Request.Context(), req.FilterMode, req.UsabilityMode, req.AddressContains, req.KeyIn, req.Offset, req.Limit)
	if jc.Check("failed to get host info", err) != nil {
		return
	}
	jc.Encode(hosts)
}
