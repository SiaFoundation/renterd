package autopilot

import (
	"net/http"
	"sync"
	"time"

	"go.sia.tech/jape"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/renterd/object"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"go.sia.tech/siad/types"
	"go.uber.org/zap"
)

type Store interface {
	Config() api.AutopilotConfig
	SetConfig(c api.AutopilotConfig) error
}

type Bus interface {
	// wallet
	WalletAddress() (types.UnlockHash, error)
	WalletDiscard(txn types.Transaction) error
	WalletFund(txn *types.Transaction, amount types.Currency) ([]types.OutputID, []types.Transaction, error)
	WalletPrepareForm(renterKey consensus.PrivateKey, hostKey consensus.PublicKey, renterFunds types.Currency, renterAddress types.UnlockHash, hostCollateral types.Currency, endHeight uint64, hostSettings rhpv2.HostSettings) (txns []types.Transaction, err error)
	WalletPrepareRenew(contract types.FileContractRevision, renterKey consensus.PrivateKey, hostKey consensus.PublicKey, renterFunds types.Currency, renterAddress types.UnlockHash, endHeight uint64, hostSettings rhpv2.HostSettings) ([]types.Transaction, types.Currency, error)
	WalletSign(txn *types.Transaction, toSign []types.OutputID, cf types.CoveredFields) error

	// hostdb
	Host(hostKey consensus.PublicKey) (hostdb.Host, error)
	Hosts(offset, limit int) ([]hostdb.Host, error)
	HostsForScanning(maxLastScan time.Time, offset, limit int) ([]hostdb.HostAddress, error)
	RecordInteractions(interactions []hostdb.Interaction) error

	// contracts
	AcquireContract(id types.FileContractID, d time.Duration) (bool, error)
	ActiveContracts() (contracts []api.ContractMetadata, err error)
	AddContract(c rhpv2.ContractRevision, totalCost types.Currency, startHeight uint64) (api.ContractMetadata, error)
	AddRenewedContract(c rhpv2.ContractRevision, totalCost types.Currency, startHeight uint64, renewedFrom types.FileContractID) (api.ContractMetadata, error)
	AncestorContracts(id types.FileContractID, minStartHeight uint64) ([]api.ArchivedContract, error)
	Contract(id types.FileContractID) (contract api.ContractMetadata, err error)
	Contracts(set string) ([]api.ContractMetadata, error)
	DeleteContracts(ids []types.FileContractID) error
	ReleaseContract(id types.FileContractID) error
	SetContractSet(set string, contracts []types.FileContractID) error

	// txpool
	RecommendedFee() (types.Currency, error)

	// consensus
	ConsensusState() (api.ConsensusState, error)

	// objects
	SlabsForMigration(n int, failureCutoff time.Time, goodContracts []types.FileContractID) ([]object.Slab, error)

	// settings
	GougingSettings() (gs api.GougingSettings, err error)
	RedundancySettings() (rs api.RedundancySettings, err error)
}

type Worker interface {
	ActiveContracts(timeout time.Duration) (api.ContractsResponse, error)
	MigrateSlab(s object.Slab) error
	RHPForm(endHeight uint64, hk consensus.PublicKey, hostIP string, renterAddress types.UnlockHash, renterFunds types.Currency, hostCollateral types.Currency) (rhpv2.ContractRevision, []types.Transaction, error)
	RHPRenew(fcid types.FileContractID, endHeight uint64, hk consensus.PublicKey, hostIP string, renterAddress types.UnlockHash, renterFunds types.Currency) (rhpv2.ContractRevision, []types.Transaction, error)
	RHPScan(hostKey consensus.PublicKey, hostIP string, timeout time.Duration) (api.RHPScanResponse, error)
}

type Autopilot struct {
	bus    Bus
	logger *zap.SugaredLogger
	store  Store
	worker Worker

	c *contractor
	m *migrator
	s *scanner

	ticker   *time.Ticker
	stopChan chan struct{}
	wg       sync.WaitGroup
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
	ap.wg.Add(1)
	defer ap.wg.Done()
	for {
		select {
		case <-ap.stopChan:
			return nil
		case <-ap.ticker.C:
		}
		ap.logger.Info("autopilot loop starting")

		// initiate a host scan
		ap.s.tryUpdateTimeout()
		ap.s.tryPerformHostScan()

		// fetch consensus state
		cs, err := ap.bus.ConsensusState()
		if err != nil {
			ap.logger.Errorf("loop interrupted, could not fetch consensus state, err: %v", err)
			continue
		}

		// do not continue if we are not synced
		if !cs.Synced {
			continue
		}

		// fetch config to ensure its not updated during maintenance
		cfg := ap.store.Config()

		// perform maintenance
		err = ap.c.performContractMaintenance(cfg, cs)
		if err != nil {
			ap.logger.Errorf("contract maintenance failed, err: %v", err)
			continue
		}

		// migration
		err = ap.m.UpdateContracts()
		if err != nil {
			ap.logger.Errorf("update contracts failed, err: %v", err)
		}
		ap.m.TryPerformMigrations()
	}
}

func (ap *Autopilot) Stop() error {
	ap.ticker.Stop()
	close(ap.stopChan)
	ap.wg.Wait()
	return nil
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
}

func (ap *Autopilot) statusHandlerGET(jc jape.Context) {
	jc.Encode(api.AutopilotStatusResponseGET{
		CurrentPeriod: ap.c.currentPeriod(),
	})
}

// NewServer returns an HTTP handler that serves the renterd autopilot api.
func NewServer(ap *Autopilot) http.Handler {
	return jape.Mux(map[string]jape.Handler{
		"GET    /actions": ap.actionsHandler,
		"GET    /config":  ap.configHandlerGET,
		"PUT    /config":  ap.configHandlerPUT,
		"GET    /status":  ap.statusHandlerGET,
	})
}

// New initializes an Autopilot.
func New(store Store, bus Bus, worker Worker, logger *zap.Logger, heartbeat time.Duration, scannerScanInterval time.Duration, scannerBatchSize, scannerNumThreads uint64) (*Autopilot, error) {
	ap := &Autopilot{
		bus:    bus,
		logger: logger.Sugar().Named("autopilot"),
		store:  store,
		worker: worker,

		ticker:   time.NewTicker(heartbeat),
		stopChan: make(chan struct{}),
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
