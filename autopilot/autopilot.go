package autopilot

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/jape"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/object"
	"go.sia.tech/renterd/tracing"
	"go.sia.tech/renterd/wallet"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

type Bus interface {
	// Accounts
	Account(ctx context.Context, id rhpv3.Account, host types.PublicKey) (account api.Account, err error)
	Accounts(ctx context.Context) (accounts []api.Account, err error)

	// Autopilots
	Autopilot(ctx context.Context, id string) (autopilot api.Autopilot, err error)
	UpdateAutopilot(ctx context.Context, autopilot api.Autopilot) error

	// wallet
	WalletAddress(ctx context.Context) (types.Address, error)
	WalletBalance(ctx context.Context) (types.Currency, error)
	WalletDiscard(ctx context.Context, txn types.Transaction) error
	WalletFund(ctx context.Context, txn *types.Transaction, amount types.Currency) ([]types.Hash256, []types.Transaction, error)
	WalletOutputs(ctx context.Context) (resp []wallet.SiacoinElement, err error)
	WalletPending(ctx context.Context) (resp []types.Transaction, err error)
	WalletRedistribute(ctx context.Context, outputs int, amount types.Currency) (id types.TransactionID, err error)

	// hostdb
	Host(ctx context.Context, hostKey types.PublicKey) (hostdb.HostInfo, error)
	Hosts(ctx context.Context, offset, limit int) ([]hostdb.Host, error)
	SearchHosts(ctx context.Context, filterMode, addressContains string, keyIn []types.PublicKey, offset, limit int) ([]hostdb.Host, error)
	HostsForScanning(ctx context.Context, maxLastScan time.Time, offset, limit int) ([]hostdb.HostAddress, error)
	RecordInteractions(ctx context.Context, interactions []hostdb.Interaction) error
	RemoveOfflineHosts(ctx context.Context, minRecentScanFailures uint64, maxDowntime time.Duration) (uint64, error)

	// contracts
	Contracts(ctx context.Context) (contracts []api.ContractMetadata, err error)
	AddContract(ctx context.Context, c rhpv2.ContractRevision, totalCost types.Currency, startHeight uint64) (api.ContractMetadata, error)
	AddRenewedContract(ctx context.Context, c rhpv2.ContractRevision, totalCost types.Currency, startHeight uint64, renewedFrom types.FileContractID) (api.ContractMetadata, error)
	AncestorContracts(ctx context.Context, id types.FileContractID, minStartHeight uint64) ([]api.ArchivedContract, error)
	ArchiveContracts(ctx context.Context, toArchive map[types.FileContractID]string) error
	ContractSetContracts(ctx context.Context, set string) ([]api.ContractMetadata, error)
	FileContractTax(ctx context.Context, payout types.Currency) (types.Currency, error)
	SetContractSet(ctx context.Context, set string, contracts []types.FileContractID) error

	// txpool
	RecommendedFee(ctx context.Context) (types.Currency, error)
	TransactionPool(ctx context.Context) (txns []types.Transaction, err error)

	// consensus
	ConsensusState(ctx context.Context) (api.ConsensusState, error)

	// objects
	Slab(ctx context.Context, key object.EncryptionKey) (object.Slab, error)
	SlabsForMigration(ctx context.Context, healthCutoff float64, set string, limit int) ([]api.UnhealthySlab, error)

	// settings
	UpdateSetting(ctx context.Context, key string, value interface{}) error
	GougingSettings(ctx context.Context) (gs api.GougingSettings, err error)
	RedundancySettings(ctx context.Context) (rs api.RedundancySettings, err error)
}

type Worker interface {
	Account(ctx context.Context, hostKey types.PublicKey) (rhpv3.Account, error)
	Contracts(ctx context.Context, hostTimeout time.Duration) (api.ContractsResponse, error)
	ID(ctx context.Context) (string, error)
	MigrateSlab(ctx context.Context, s object.Slab) error
	RHPForm(ctx context.Context, endHeight uint64, hk types.PublicKey, hostIP string, renterAddress types.Address, renterFunds types.Currency, hostCollateral types.Currency) (rhpv2.ContractRevision, []types.Transaction, error)
	RHPFund(ctx context.Context, contractID types.FileContractID, hostKey types.PublicKey, hostIP, siamuxAddr string, balance types.Currency) (err error)
	RHPPriceTable(ctx context.Context, hostKey types.PublicKey, siamuxAddr string) (hostdb.HostPriceTable, error)
	RHPRenew(ctx context.Context, fcid types.FileContractID, endHeight uint64, hk types.PublicKey, hostIP string, hostAddress, renterAddress types.Address, renterFunds, newCollateral types.Currency, windowSize uint64) (rhpv2.ContractRevision, []types.Transaction, error)
	RHPScan(ctx context.Context, hostKey types.PublicKey, hostIP string, timeout time.Duration) (api.RHPScanResponse, error)
	RHPSync(ctx context.Context, contractID types.FileContractID, hostKey types.PublicKey, hostIP, siamuxAddr string) (err error)
}

type Autopilot struct {
	id string

	bus     Bus
	logger  *zap.SugaredLogger
	workers *workerPool

	mu    sync.Mutex
	state state

	a *accounts
	c *contractor
	m *migrator
	s *scanner

	tickerDuration time.Duration
	wg             sync.WaitGroup

	startStopMu sync.Mutex
	running     bool
	ticker      *time.Ticker
	triggerChan chan bool
	stopChan    chan struct{}
}

// state holds a bunch of variables that are used by the autopilot and updated
type state struct {
	cs  api.ConsensusState
	gs  api.GougingSettings
	rs  api.RedundancySettings
	cfg api.AutopilotConfig

	fee    types.Currency
	period uint64
}

// workerPool contains all workers known to the autopilot.  Users can call
// withWorker to execute a function with a worker of the pool or withWorkers to
// sequentially run a function on all workers.  Due to the RWMutex this will
// never block during normal operations. However, during an update of the
// workerpool, this allows us to guarantee that all workers have finished their
// tasks by calling  acquiring an exclusive lock on the pool before updating it.
// That way the caller who updated the pool can rely on the autopilot not using
// a worker that was removed during the update after the update operation
// returns.
type workerPool struct {
	mu      sync.RWMutex
	workers []Worker
}

func newWorkerPool(workers []Worker) *workerPool {
	return &workerPool{
		workers: workers,
	}
}

func (wp *workerPool) withWorker(workerFunc func(Worker)) {
	wp.mu.RLock()
	defer wp.mu.RUnlock()
	workerFunc(wp.workers[frand.Intn(len(wp.workers))])
}

func (wp *workerPool) withWorkers(workerFunc func([]Worker)) {
	wp.mu.RLock()
	defer wp.mu.RUnlock()
	workerFunc(wp.workers)
}

// Handler returns an HTTP handler that serves the autopilot api.
func (ap *Autopilot) Handler() http.Handler {
	return jape.Mux(tracing.TracedRoutes("autopilot", map[string]jape.Handler{
		"GET    /config":        ap.configHandlerGET,
		"PUT    /config":        ap.configHandlerPUT,
		"POST   /debug/trigger": ap.triggerHandlerPOST,
		"POST   /hosts":         ap.hostsHandlerPOST,
		"GET    /host/:hostKey": ap.hostHandlerGET,
	}))
}

func (ap *Autopilot) Run() error {
	ap.startStopMu.Lock()
	if ap.running {
		ap.startStopMu.Unlock()
		return errors.New("already running")
	}
	ap.running = true
	ap.stopChan = make(chan struct{})
	ap.triggerChan = make(chan bool)
	ap.ticker = time.NewTicker(ap.tickerDuration)

	ap.wg.Add(1)
	defer ap.wg.Done()
	ap.startStopMu.Unlock()

	// register the autopilot in the bus
	if err := ap.registerWithBus(); err != nil {
		ap.logger.Errorf("failed to register autopilot with bus, err %v", err)
		return err
	}

	// block until consensus is synced
	if !ap.blockUntilSynced() {
		ap.logger.Error("autopilot stopped before consensus was synced")
		return nil
	}

	var forceScan bool
	var launchAccountRefillsOnce sync.Once
	for {
		ap.logger.Info("autopilot iteration starting")
		ap.workers.withWorker(func(w Worker) {
			defer ap.logger.Info("autopilot iteration ended")
			ctx, span := tracing.Tracer.Start(context.Background(), "Autopilot Iteration")
			defer span.End()

			// Trace/Log worker id chosen for this maintenance iteration.
			workerID, err := w.ID(ctx)
			if err != nil {
				ap.logger.Errorf("failed to fetch worker id - abort maintenance", err)
				return
			}
			span.SetAttributes(attribute.String("worker", workerID))
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

			// initiate a host scan
			ap.s.tryUpdateTimeout()
			ap.s.tryPerformHostScan(ctx, w, forceScan)

			// do not continue if we are not synced
			if !ap.isSynced() {
				ap.logger.Debug("iteration interrupted, consensus not synced")
				return
			}

			// perform wallet maintenance
			err = ap.c.performWalletMaintenance(ctx)
			if err != nil {
				ap.logger.Errorf("wallet maintenance failed, err: %v", err)
			}

			// perform maintenance
			setChanged, err := ap.c.performContractMaintenance(ctx, w)
			if err != nil {
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
					go ap.a.refillWorkersAccountsLoop(ap.stopChan)
				})
			} else {
				ap.logger.Errorf("contract maintenance failed, err: %v", err)
			}

			// migration
			ap.m.tryPerformMigrations(ctx, ap.workers)
		})

		select {
		case <-ap.stopChan:
			return nil
		case forceScan = <-ap.triggerChan:
			ap.logger.Info("autopilot iteration triggered")
			ap.ticker.Reset(ap.tickerDuration)
		case <-ap.ticker.C:
			forceScan = false
		}
	}
}

// Shutdown shuts down the autopilot.
func (ap *Autopilot) Shutdown(_ context.Context) error {
	ap.startStopMu.Lock()
	defer ap.startStopMu.Unlock()

	if ap.running {
		ap.ticker.Stop()
		close(ap.stopChan)
		close(ap.triggerChan)
		ap.wg.Wait()
		ap.running = false
	}
	return nil
}

func (ap *Autopilot) State() state {
	ap.mu.Lock()
	defer ap.mu.Unlock()
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

func (ap *Autopilot) blockUntilSynced() bool {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ap.stopChan:
			return false
		case <-ticker.C:
			if synced := func() bool {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()

				cs, err := ap.bus.ConsensusState(ctx)
				if err != nil {
					ap.logger.Errorf("failed to get consensus state, err: %v", err)
				}

				return cs.Synced
			}(); synced {
				return true
			}
		}
	}
}

func (ap *Autopilot) registerWithBus() error {
	// apply sane timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// ensure autopilot is registered with the bus
	_, err := ap.bus.Autopilot(ctx, ap.id)
	if err != nil && strings.Contains(err.Error(), api.ErrAutopilotNotFound.Error()) {
		err = ap.bus.UpdateAutopilot(ctx, api.Autopilot{ID: ap.id, Config: api.DefaultAutopilotConfig()})
	}
	return err
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
	ap.mu.Lock()
	ap.state = state{
		cs:  cs,
		gs:  gs,
		rs:  rs,
		cfg: autopilot.Config,

		fee:    fee,
		period: autopilot.CurrentPeriod,
	}
	ap.mu.Unlock()
	return nil
}

func (ap *Autopilot) isSynced() bool {
	return ap.State().cs.Synced
}

func (ap *Autopilot) isStopped() bool {
	select {
	case <-ap.stopChan:
		return true
	default:
		return false
	}
}

func (ap *Autopilot) configHandlerGET(jc jape.Context) {
	autopilot, err := ap.bus.Autopilot(jc.Request.Context(), ap.id)
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
	autopilot, err := ap.bus.Autopilot(jc.Request.Context(), ap.id)
	if strings.Contains(err.Error(), api.ErrAutopilotNotFound.Error()) {
		autopilot = api.Autopilot{ID: ap.id, Config: cfg}
	} else {
		autopilot.Config = cfg
	}

	// update the autopilot
	jc.Check("failed to update autopilot config", ap.bus.UpdateAutopilot(jc.Request.Context(), autopilot))
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

// New initializes an Autopilot.
func New(id string, bus Bus, workers []Worker, logger *zap.Logger, heartbeat time.Duration, scannerScanInterval time.Duration, scannerBatchSize, scannerMinRecentFailures, scannerNumThreads uint64, migrationHealthCutoff float64, accountsRefillInterval time.Duration) (*Autopilot, error) {
	if id == "" {
		return nil, errors.New("id cannot be empty")
	}

	ap := &Autopilot{
		id:      id,
		bus:     bus,
		logger:  logger.Sugar().Named("autopilot"),
		workers: newWorkerPool(workers),

		tickerDuration: heartbeat,
	}
	scanner, err := newScanner(
		ap,
		scannerBatchSize,
		scannerMinRecentFailures,
		scannerNumThreads,
		scannerScanInterval,
		scannerTimeoutInterval,
		scannerTimeoutMinTimeout,
	)
	if err != nil {
		return nil, err
	}

	ap.s = scanner
	ap.c = newContractor(ap)
	ap.m = newMigrator(ap, migrationHealthCutoff)
	ap.a = newAccounts(ap, ap.bus, ap.bus, ap.workers, ap.logger, accountsRefillInterval)

	return ap, nil
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
