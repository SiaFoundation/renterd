package autopilot

import (
	"net/http"
	"time"

	"go.sia.tech/jape"
	"go.sia.tech/renterd/bus"
	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/renterd/object"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"go.sia.tech/renterd/types"
	"go.sia.tech/renterd/worker"
	siatypes "go.sia.tech/siad/types"
	"go.uber.org/zap"
)

type Store interface {
	Config() Config
	SetConfig(c Config) error
}

type Bus interface {
	// wallet
	WalletAddress() (siatypes.UnlockHash, error)
	WalletFund(txn *siatypes.Transaction, amount siatypes.Currency) ([]siatypes.OutputID, []siatypes.Transaction, error)
	WalletDiscard(txn siatypes.Transaction) error
	WalletSign(txn *siatypes.Transaction, toSign []siatypes.OutputID, cf siatypes.CoveredFields) error

	// hostdb
	AllHosts() ([]hostdb.Host, error)
	Hosts(notSince time.Time, max int) ([]hostdb.Host, error)
	Host(hostKey consensus.PublicKey) (hostdb.Host, error)
	RecordHostInteraction(hostKey consensus.PublicKey, hi hostdb.Interaction) error

	// contracts
	AddContract(c rhpv2.Contract, totalCost siatypes.Currency) error
	AddRenewedContract(c rhpv2.Contract, totalCost siatypes.Currency, renewedFrom siatypes.FileContractID) error
	DeleteContracts(ids []siatypes.FileContractID) error

	Contract(id siatypes.FileContractID) (contract types.Contract, err error)
	Contracts() ([]types.Contract, error)
	ContractMetadata(id siatypes.FileContractID) (types.ContractMetadata, error)
	UpdateContractMetadata(id siatypes.FileContractID, metadata types.ContractMetadata) error

	SpendingHistory(id siatypes.FileContractID, currentPeriod uint64) ([]types.ContractSpending, error)

	AcquireContract(id siatypes.FileContractID, d time.Duration) (siatypes.FileContractRevision, error)
	ReleaseContract(id siatypes.FileContractID) error

	// contractsets
	SetContractSet(name string, contracts []siatypes.FileContractID) error
	SetContracts(name string) ([]types.Contract, error)

	// txpool
	RecommendedFee() (siatypes.Currency, error)

	// consensus
	ConsensusState() (bus.ConsensusState, error)

	// objects
	MarkSlabsMigrationFailure(slabIDs []bus.SlabID) (int, error)
	SlabsForMigration(n int, failureCutoff time.Time, goodContracts []siatypes.FileContractID) ([]bus.SlabID, error)
	SlabForMigration(slabID bus.SlabID) (object.Slab, []types.Contract, error)
}

type Worker interface {
	MigrateSlab(s *object.Slab, from, to []worker.Contract, currentHeight uint64) error
	RHPScan(hostKey consensus.PublicKey, hostIP string, timeout time.Duration) (worker.RHPScanResponse, error)
	RHPPrepareForm(renterKey consensus.PrivateKey, hostKey consensus.PublicKey, renterFunds siatypes.Currency, renterAddress siatypes.UnlockHash, hostCollateral siatypes.Currency, endHeight uint64, hostSettings rhpv2.HostSettings) (siatypes.FileContract, siatypes.Currency, error)
	RHPPrepareRenew(contract siatypes.FileContractRevision, renterKey consensus.PrivateKey, hostKey consensus.PublicKey, renterFunds siatypes.Currency, renterAddress siatypes.UnlockHash, hostCollateral siatypes.Currency, endHeight uint64, hostSettings rhpv2.HostSettings) (siatypes.FileContract, siatypes.Currency, siatypes.Currency, error)
	RHPForm(renterKey consensus.PrivateKey, hostKey consensus.PublicKey, hostIP string, transactionSet []siatypes.Transaction) (rhpv2.Contract, []siatypes.Transaction, error)
	RHPRenew(renterKey consensus.PrivateKey, hostKey consensus.PublicKey, hostIP string, contractID siatypes.FileContractID, transactionSet []siatypes.Transaction, finalPayment siatypes.Currency) (rhpv2.Contract, []siatypes.Transaction, error)
}

type Autopilot struct {
	bus    Bus
	logger *zap.SugaredLogger
	store  Store
	worker Worker

	c *contractor
	m *migrator
	s *scanner

	masterKey [32]byte

	ticker   *time.Ticker
	stopChan chan struct{}
}

// Actions returns the autopilot actions that have occurred since the given time.
func (ap *Autopilot) Actions(since time.Time, max int) []Action {
	panic("unimplemented")
}

// Config returns the autopilot's current configuration.
func (ap *Autopilot) Config() Config {
	return ap.store.Config()
}

// SetConfig updates the autopilot's configuration.
func (ap *Autopilot) SetConfig(c Config) error {
	return ap.store.SetConfig(c)
}

func (ap *Autopilot) Run() error {
	for {
		select {
		case <-ap.stopChan:
			return nil
		case <-ap.ticker.C:
		}

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

		// update contractor's internal state of consensus
		ap.c.applyConsensusState(cfg, cs)

		// perform maintenance
		err = ap.c.performContractMaintenance(cfg)
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
	return nil
}

func (ap *Autopilot) actionsHandler(jc jape.Context) {
	var since time.Time
	max := -1
	if jc.DecodeForm("since", (*paramTime)(&since)) != nil || jc.DecodeForm("max", &max) != nil {
		return
	}
	jc.Encode(ap.Actions(since, max))
}

func (ap *Autopilot) configHandlerGET(jc jape.Context) {
	jc.Encode(ap.Config())
}

func (ap *Autopilot) configHandlerPUT(jc jape.Context) {
	var c Config
	if jc.Decode(&c) != nil {
		return
	}
	if jc.Check("failed to set config", ap.SetConfig(c)) != nil {
		return
	}
}

// New initializes an Autopilot.
func New(store Store, bus Bus, worker Worker, logger *zap.Logger, heartbeat time.Duration) (_ http.Handler, run, cleanup func() error) {
	ap := &Autopilot{
		bus:    bus,
		logger: logger.Sugar(),
		store:  store,
		worker: worker,

		ticker:   time.NewTicker(heartbeat),
		stopChan: make(chan struct{}),
	}

	ap.c = newContractor(ap)
	ap.m = newMigrator(ap)
	ap.s = newScanner(
		ap,
		scannerNumThreads,
		scannerScanInterval,
		scannerTimeoutInterval,
	)

	return jape.Mux(map[string]jape.Handler{
		"GET    /actions": ap.actionsHandler,
		"GET    /config":  ap.configHandlerGET,
		"PUT    /config":  ap.configHandlerPUT,
	}), ap.Run, ap.Stop
}
