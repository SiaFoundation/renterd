package autopilot

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/jape"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/bus"
	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/internal/tracing"
	"go.sia.tech/renterd/object"
	"go.sia.tech/renterd/wallet"
	"go.uber.org/zap"
)

type Store interface {
	Config() api.AutopilotConfig
	SetConfig(c api.AutopilotConfig) error
}

type Bus interface {
	// wallet
	WalletAddress(ctx context.Context) (types.Address, error)
	WalletBalance(ctx context.Context) (types.Currency, error)
	WalletDiscard(ctx context.Context, txn types.Transaction) error
	WalletFund(ctx context.Context, txn *types.Transaction, amount types.Currency) ([]types.Hash256, []types.Transaction, error)
	WalletOutputs(ctx context.Context) (resp []wallet.SiacoinElement, err error)
	WalletPending(ctx context.Context) (resp []types.Transaction, err error)
	WalletPrepareForm(ctx context.Context, renterAddress types.Address, renterKey types.PrivateKey, renterFunds, hostCollateral types.Currency, hostKey types.PublicKey, hostSettings rhpv2.HostSettings, endHeight uint64) (txns []types.Transaction, err error)
	WalletPrepareRenew(ctx context.Context, contract types.FileContractRevision, renterAddress types.Address, renterKey types.PrivateKey, renterFunds, newCollateral types.Currency, hostKey types.PublicKey, hostSettings rhpv2.HostSettings, endHeight uint64) ([]types.Transaction, types.Currency, error)
	WalletRedistribute(ctx context.Context, outputs int, amount types.Currency) (id types.TransactionID, err error)
	WalletSign(ctx context.Context, txn *types.Transaction, toSign []types.Hash256, cf types.CoveredFields) error

	// hostdb
	Host(ctx context.Context, hostKey types.PublicKey) (hostdb.Host, error)
	Hosts(ctx context.Context, offset, limit int) ([]hostdb.Host, error)
	HostsForScanning(ctx context.Context, maxLastScan time.Time, offset, limit int) ([]hostdb.HostAddress, error)
	RecordInteractions(ctx context.Context, interactions []hostdb.Interaction) error
	RemoveOfflineHosts(ctx context.Context, minRecentScanFailures uint64, maxDowntime time.Duration) (uint64, error)

	// contracts
	ActiveContracts(ctx context.Context) (contracts []api.ContractMetadata, err error)
	AddContract(ctx context.Context, c rhpv2.ContractRevision, totalCost types.Currency, startHeight uint64) (api.ContractMetadata, error)
	AddRenewedContract(ctx context.Context, c rhpv2.ContractRevision, totalCost types.Currency, startHeight uint64, renewedFrom types.FileContractID) (api.ContractMetadata, error)
	AncestorContracts(ctx context.Context, id types.FileContractID, minStartHeight uint64) ([]api.ArchivedContract, error)
	Contracts(ctx context.Context, set string) ([]api.ContractMetadata, error)
	DeleteContracts(ctx context.Context, ids []types.FileContractID) error
	SetContractSet(ctx context.Context, set string, contracts []types.FileContractID) error

	// txpool
	RecommendedFee(ctx context.Context) (types.Currency, error)
	TransactionPool(ctx context.Context) (txns []types.Transaction, err error)

	// consensus
	ConsensusState(ctx context.Context) (api.ConsensusState, error)

	// objects
	SlabsForMigration(ctx context.Context, healthCutoff float64, set string, limit int) ([]object.Slab, error)

	// settings
	UpdateSetting(ctx context.Context, key string, value string) error
	GougingSettings(ctx context.Context) (gs api.GougingSettings, err error)
	RedundancySettings(ctx context.Context) (rs api.RedundancySettings, err error)
}

type Worker interface {
	Accounts(ctx context.Context) (accounts []api.Account, err error)
	ActiveContracts(ctx context.Context, hostTimeout time.Duration) (api.ContractsResponse, error)
	MigrateSlab(ctx context.Context, s object.Slab) error
	RHPForm(ctx context.Context, endHeight uint64, hk types.PublicKey, hostIP string, renterAddress types.Address, renterFunds types.Currency, hostCollateral types.Currency) (rhpv2.ContractRevision, []types.Transaction, error)
	RHPFund(ctx context.Context, contractID types.FileContractID, hostKey types.PublicKey, amount types.Currency) (err error)
	RHPRenew(ctx context.Context, fcid types.FileContractID, endHeight uint64, hk types.PublicKey, hostIP string, renterAddress types.Address, renterFunds, newCollateral types.Currency) (rhpv2.ContractRevision, []types.Transaction, error)
	RHPScan(ctx context.Context, hostKey types.PublicKey, hostIP string, timeout time.Duration) (api.RHPScanResponse, error)
}

type Autopilot struct {
	bus    Bus
	logger *zap.SugaredLogger
	store  Store
	worker Worker

	a *accounts
	c *contractor
	m *migrator
	s *scanner

	tickerDuration time.Duration
	wg             sync.WaitGroup

	startStopMu sync.Mutex
	running     bool
	ticker      *time.Ticker
	triggerChan chan struct{}
	stopChan    chan struct{}
}

// Actions returns the autopilot actions that have occurred since the given time.
func (ap *Autopilot) Actions(since time.Time, max int) []api.Action {
	panic("unimplemented")
}

// Config returns the autopilot's current configuration.
func (ap *Autopilot) Config() api.AutopilotConfig {
	return ap.store.Config()
}

// SetConfig updates the autopilot's configuration.
func (ap *Autopilot) SetConfig(c api.AutopilotConfig) error {
	return ap.store.SetConfig(c)
}

func (ap *Autopilot) Run() error {
	ap.startStopMu.Lock()
	if ap.running {
		ap.startStopMu.Unlock()
		return errors.New("already running")
	}
	ap.running = true
	ap.stopChan = make(chan struct{})
	ap.triggerChan = make(chan struct{})
	ap.ticker = time.NewTicker(ap.tickerDuration)

	ap.wg.Add(1)
	defer ap.wg.Done()
	ap.startStopMu.Unlock()

	// update the contract set setting
	err := ap.bus.UpdateSetting(context.Background(), bus.SettingContractSet, ap.store.Config().Contracts.Set)
	if err != nil {
		ap.logger.Errorf("failed to update contract set setting, err: %v", err)
	}

	var launchAccountRefillsOnce sync.Once
	for {
		select {
		case <-ap.stopChan:
			return nil
		case <-ap.triggerChan:
			ap.logger.Info("autopilot iteration triggered")
			ap.ticker.Reset(ap.tickerDuration)
		case <-ap.ticker.C:
			ap.logger.Info("autopilot iteration starting")
		}

		func() {
			defer ap.logger.Info("autopilot iteration ended")
			ctx, span := tracing.Tracer.Start(context.Background(), "Autopilot Iteration")
			defer span.End()

			// use the same config for the entire iteration
			cfg := ap.store.Config()

			// update the contract set setting
			err := ap.bus.UpdateSetting(ctx, bus.SettingContractSet, cfg.Contracts.Set)
			if err != nil {
				ap.logger.Errorf("failed to update contract set setting, err: %v", err)
			}

			// initiate a host scan
			ap.s.tryUpdateTimeout()
			ap.s.tryPerformHostScan(ctx, cfg)

			// fetch consensus state
			cs, err := ap.bus.ConsensusState(ctx)
			if err != nil {
				ap.logger.Errorf("iteration interrupted, could not fetch consensus state, err: %v", err)
				return
			}

			// do not continue if we are not synced
			if !cs.Synced {
				ap.logger.Debug("iteration interrupted, consensus not synced")
				return
			}

			// update current period
			ap.c.updateCurrentPeriod(cfg, cs)

			// perform wallet maintenance
			err = ap.c.performWalletMaintenance(ctx, cfg, cs)
			if err != nil {
				ap.logger.Errorf("wallet maintenance failed, err: %v", err)
			}

			// perform maintenance
			err = ap.c.performContractMaintenance(ctx, cfg, cs)
			if err != nil {
				//ap.logger.Errorf("contract maintenance failed, err: %v", err)
			}
			maintenanceSuccess := err == nil

			// launch account refills after successful contract maintenance.
			if maintenanceSuccess {
				ap.a.UpdateContracts(ctx, cfg)
				launchAccountRefillsOnce.Do(func() {
					ap.logger.Debug("account refills loop launched")
					go ap.a.refillWorkersAccountsLoop(ap.stopChan)
				})
			}

			// migration
			ap.m.tryPerformMigrations(ctx, cfg)
		}()
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

func (ap *Autopilot) Trigger() bool {
	ap.startStopMu.Lock()
	defer ap.startStopMu.Unlock()

	select {
	case ap.triggerChan <- struct{}{}:
		return true
	default:
		return false
	}
}

func (ap *Autopilot) isStopped() bool {
	select {
	case <-ap.stopChan:
		return true
	default:
		return false
	}
}

func (ap *Autopilot) actionsHandler(jc jape.Context) {
	var since time.Time
	max := -1
	if jc.DecodeForm("since", (*api.ParamTime)(&since)) != nil || jc.DecodeForm("max", &max) != nil {
		return
	}
	jc.Encode(ap.Actions(since, max))
}

func (ap *Autopilot) configHandlerGET(jc jape.Context) {
	jc.Encode(ap.Config())
}

func (ap *Autopilot) configHandlerPUT(jc jape.Context) {
	var c api.AutopilotConfig
	if jc.Decode(&c) != nil {
		return
	}
	if jc.Check("failed to set config", ap.SetConfig(c)) != nil {
		return
	}
	ap.Trigger() // trigger the autopilot loop
}

func (ap *Autopilot) statusHandlerGET(jc jape.Context) {
	jc.Encode(api.AutopilotStatusResponseGET{
		CurrentPeriod: ap.c.currentPeriod(),
	})
}

func (ap *Autopilot) triggerHandlerPOST(jc jape.Context) {
	jc.Encode(fmt.Sprintf("triggered: %t", ap.Trigger()))
}

// New initializes an Autopilot.
func New(store Store, bus Bus, worker Worker, logger *zap.Logger, heartbeat time.Duration, scannerScanInterval time.Duration, scannerBatchSize, scannerNumThreads uint64, migrationHealthCutoff float64, accountsRefillInterval time.Duration) (*Autopilot, error) {
	ap := &Autopilot{
		bus:    bus,
		logger: logger.Sugar().Named("autopilot"),
		store:  store,
		worker: worker,

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

	ap.a = newAccounts(ap.logger, ap.bus, ap.worker, accountsRefillInterval)
	ap.s = scanner
	ap.c = newContractor(ap)
	ap.m = newMigrator(ap, migrationHealthCutoff)

	return ap, nil
}

// Handler returns an HTTP handler that serves the autopilot api.
func (ap *Autopilot) Handler() http.Handler {
	return jape.Mux(tracing.TracedRoutes("autopilot", map[string]jape.Handler{
		"GET    /actions": ap.actionsHandler,
		"GET    /config":  ap.configHandlerGET,
		"PUT    /config":  ap.configHandlerPUT,
		"GET    /status":  ap.statusHandlerGET,

		"POST    /debug/trigger": ap.triggerHandlerPOST,
	}))
}
