package autopilot

import (
	"sync"
	"time"

	"go.sia.tech/renterd/bus"
	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/renterd/object"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"go.sia.tech/renterd/worker"
	"go.sia.tech/siad/types"
	"go.uber.org/zap"
)

type Store interface {
	Config() Config
	SetConfig(c Config) error
}

type Bus interface {
	// wallet
	WalletAddress() (types.UnlockHash, error)
	WalletFund(txn *types.Transaction, amount types.Currency) ([]types.OutputID, []types.Transaction, error)
	WalletDiscard(txn types.Transaction) error
	WalletSign(txn *types.Transaction, toSign []types.OutputID, cf types.CoveredFields) error

	// hostdb
	AllHosts() ([]hostdb.Host, error)
	Hosts(notSince time.Time, max int) ([]hostdb.Host, error)
	Host(hostKey consensus.PublicKey) (hostdb.Host, error)
	RecordHostInteraction(hostKey consensus.PublicKey, hi hostdb.Interaction) error

	// contracts
	AddContract(c rhpv2.ContractRevision, totalCost types.Currency, startHeight uint64) error
	AddRenewedContract(c rhpv2.ContractRevision, totalCost types.Currency, startHeight uint64, renewedFrom types.FileContractID) error
	DeleteContracts(ids []types.FileContractID) error

	Contract(id types.FileContractID) (contract bus.Contract, err error)
	Contracts() ([]bus.Contract, error)

	AcquireContract(id types.FileContractID, d time.Duration) (types.FileContractRevision, error)
	ReleaseContract(id types.FileContractID) error

	// contractsets
	SetContractSet(name string, contracts []types.FileContractID) error
	ContractSetContracts(name string) ([]bus.Contract, error)

	// txpool
	RecommendedFee() (types.Currency, error)

	// consensus
	ConsensusState() (bus.ConsensusState, error)

	// objects
	SlabsForMigration(n int, failureCutoff time.Time, goodContracts []types.FileContractID) ([]object.Slab, error)
}

type Worker interface {
	RHPScan(hostKey consensus.PublicKey, hostIP string, timeout time.Duration) (worker.RHPScanResponse, error)
	RHPPrepareForm(renterKey consensus.PrivateKey, hostKey consensus.PublicKey, renterFunds types.Currency, renterAddress types.UnlockHash, hostCollateral types.Currency, endHeight uint64, hostSettings rhpv2.HostSettings) (types.FileContract, types.Currency, error)
	RHPPrepareRenew(contract types.FileContractRevision, renterKey consensus.PrivateKey, hostKey consensus.PublicKey, renterFunds types.Currency, renterAddress types.UnlockHash, endHeight uint64, hostSettings rhpv2.HostSettings) (types.FileContract, types.Currency, types.Currency, error)
	RHPForm(renterKey consensus.PrivateKey, hostKey consensus.PublicKey, hostIP string, transactionSet []types.Transaction) (rhpv2.ContractRevision, []types.Transaction, error)
	RHPRenew(renterKey consensus.PrivateKey, hostKey consensus.PublicKey, hostIP string, contractID types.FileContractID, transactionSet []types.Transaction, finalPayment types.Currency) (rhpv2.ContractRevision, []types.Transaction, error)
	MigrateSlab(s object.Slab) error
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
	wg       sync.WaitGroup
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
	ap.wg.Add(1)
	defer ap.wg.Done()
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
	ap.wg.Wait()
	return nil
}

// New initializes an Autopilot.
func New(store Store, bus Bus, worker Worker, logger *zap.Logger, heartbeat time.Duration, scanInterval time.Duration) *Autopilot {
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
		scanInterval,
		scannerTimeoutInterval,
	)

	return ap
}
