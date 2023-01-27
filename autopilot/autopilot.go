package autopilot

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/jape"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/object"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"go.sia.tech/renterd/wallet"
	"go.uber.org/zap"
)

type Store interface {
	Config() api.AutopilotConfig
	SetConfig(c api.AutopilotConfig) error
}

type Bus interface {
	// wallet
	WalletAddress() (types.Address, error)
	WalletBalance() (types.Currency, error)
	WalletDiscard(txn types.Transaction) error
	WalletFund(txn *types.Transaction, amount types.Currency) ([]types.Hash256, []types.Transaction, error)
	WalletOutputs() (resp []wallet.SiacoinElement, err error)
	WalletPending() (resp []types.Transaction, err error)
	WalletPrepareForm(renterKey types.PrivateKey, hostKey types.PublicKey, renterFunds types.Currency, renterAddress types.Address, hostCollateral types.Currency, endHeight uint64, hostSettings rhpv2.HostSettings) (txns []types.Transaction, err error)
	WalletPrepareRenew(contract types.FileContractRevision, renterKey types.PrivateKey, hostKey types.PublicKey, renterFunds types.Currency, renterAddress types.Address, endHeight uint64, hostSettings rhpv2.HostSettings) ([]types.Transaction, types.Currency, error)
	WalletRedistribute(outputs int, amount types.Currency) (id types.TransactionID, err error)
	WalletSign(txn *types.Transaction, toSign []types.Hash256, cf types.CoveredFields) error

	// hostdb
	Host(hostKey types.PublicKey) (hostdb.Host, error)
	Hosts(offset, limit int) ([]hostdb.Host, error)
	HostsForScanning(maxLastScan time.Time, offset, limit int) ([]hostdb.HostAddress, error)
	RecordInteractions(interactions []hostdb.Interaction) error

	// contracts
	ActiveContracts() (contracts []api.ContractMetadata, err error)
	AddContract(c rhpv2.ContractRevision, totalCost types.Currency, startHeight uint64) (api.ContractMetadata, error)
	AddRenewedContract(c rhpv2.ContractRevision, totalCost types.Currency, startHeight uint64, renewedFrom types.FileContractID) (api.ContractMetadata, error)
	AncestorContracts(id types.FileContractID, minStartHeight uint64) ([]api.ArchivedContract, error)
	Contracts(set string) ([]api.ContractMetadata, error)
	DeleteContracts(ids []types.FileContractID) error
	SetContractSet(set string, contracts []types.FileContractID) error

	// txpool
	RecommendedFee() (types.Currency, error)
	TransactionPool() (txns []types.Transaction, err error)

	// consensus
	ConsensusState() (api.ConsensusState, error)

	// objects
	PrepareSlabsForMigration(set string) error
	SlabsForMigration(offset, limit int) ([]object.Slab, error)

	// settings
	GougingSettings() (gs api.GougingSettings, err error)
	RedundancySettings() (rs api.RedundancySettings, err error)
}

type Worker interface {
	ActiveContracts(hostTimeout time.Duration) (api.ContractsResponse, error)
	MigrateSlab(s object.Slab) error
	RHPForm(endHeight uint64, hk types.PublicKey, hostIP string, renterAddress types.Address, renterFunds types.Currency, hostCollateral types.Currency) (rhpv2.ContractRevision, []types.Transaction, error)
	RHPRenew(fcid types.FileContractID, endHeight uint64, hk types.PublicKey, hostIP string, renterAddress types.Address, renterFunds types.Currency) (rhpv2.ContractRevision, []types.Transaction, error)
	RHPScan(hostKey types.PublicKey, hostIP string, timeout time.Duration) (api.RHPScanResponse, error)
}

type Autopilot struct {
	bus    Bus
	logger *zap.SugaredLogger
	store  Store
	worker Worker

	c *contractor
	m *migrator
	s *scanner

	ticker         *time.Ticker
	tickerDuration time.Duration
	stopChan       chan struct{}
	triggerChan    chan struct{}
	wg             sync.WaitGroup
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
	ap.ticker = time.NewTicker(ap.tickerDuration)

	ap.wg.Add(1)
	defer ap.wg.Done()
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

			// initiate a host scan
			ap.s.tryUpdateTimeout()
			ap.s.tryPerformHostScan()

			// fetch consensus state
			cs, err := ap.bus.ConsensusState()
			if err != nil {
				ap.logger.Errorf("iteration interrupted, could not fetch consensus state, err: %v", err)
				return
			}

			// do not continue if we are not synced
			if !cs.Synced {
				ap.logger.Debug("iteration interrupted, consensus not synced")
				return
			}

			// fetch config to ensure its not updated during maintenance
			cfg := ap.store.Config()

			// perform wallet maintenance
			err = ap.c.performWalletMaintenance(cfg, cs)
			if err != nil {
				ap.logger.Errorf("wallet maintenance failed, err: %v", err)
			}

			// perform maintenance
			err = ap.c.performContractMaintenance(cfg, cs)
			if err != nil {
				ap.logger.Errorf("contract maintenance failed, err: %v", err)
			}

			// migration
			ap.m.TryPerformMigrations()
		}()
	}
}

func (ap *Autopilot) Stop() error {
	if ap.ticker != nil {
		ap.ticker.Stop()
	}
	close(ap.stopChan)
	close(ap.triggerChan)
	ap.wg.Wait()
	return nil
}

func (ap *Autopilot) Trigger() bool {
	select {
	case ap.triggerChan <- struct{}{}:
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

// NewServer returns an HTTP handler that serves the renterd autopilot api.
func NewServer(ap *Autopilot) http.Handler {
	return jape.Mux(map[string]jape.Handler{
		"GET    /actions": ap.actionsHandler,
		"GET    /config":  ap.configHandlerGET,
		"PUT    /config":  ap.configHandlerPUT,
		"GET    /status":  ap.statusHandlerGET,

		"POST    /debug/trigger": ap.triggerHandlerPOST,
	})
}

// New initializes an Autopilot.
func New(store Store, bus Bus, worker Worker, logger *zap.Logger, heartbeat time.Duration, scannerScanInterval time.Duration, scannerBatchSize, scannerNumThreads uint64) (*Autopilot, error) {
	ap := &Autopilot{
		bus:    bus,
		logger: logger.Sugar().Named("autopilot"),
		store:  store,
		worker: worker,

		tickerDuration: heartbeat,
		stopChan:       make(chan struct{}),
		triggerChan:    make(chan struct{}),
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
	ap.c = newContractor(ap)
	ap.m = newMigrator(ap)

	return ap, nil
}
