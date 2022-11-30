package autopilot

import (
	"time"

	"go.sia.tech/renterd/bus"
	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/renterd/object"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"go.sia.tech/renterd/worker"
	"go.sia.tech/siad/types"
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
	AddContract(c rhpv2.Contract) error
	AllContracts() ([]bus.Contract, error)
	ActiveContracts() ([]bus.Contract, error)
	DeleteContracts(ids []types.FileContractID) error

	Contract(id types.FileContractID) (contract rhpv2.Contract, err error)
	ContractMetadata(id types.FileContractID) (bus.ContractMetadata, error)
	UpdateContractMetadata(id types.FileContractID, metadata bus.ContractMetadata) error

	SpendingHistory(id types.FileContractID, currentPeriod uint64) ([]bus.ContractSpending, error)

	AcquireContractLock(id types.FileContractID) (types.FileContractRevision, error)
	ReleaseContractLock(id types.FileContractID) error

	// contractsets
	SetHostSet(name string, hosts []consensus.PublicKey) error
	HostSetContracts(name string) ([]bus.Contract, error)

	// txpool
	RecommendedFee() (types.Currency, error)

	// consensus
	ConsensusState() (bus.ConsensusState, error)

	// objects
	MarkSlabsMigrationFailure(slabIDs []bus.SlabID) (int, error)
	SlabsForMigration(n int, failureCutoff time.Time, goodContracts []types.FileContractID) ([]bus.SlabID, error)
	SlabForMigration(slabID bus.SlabID) (object.Slab, []bus.MigrationContract, error)
}

type Worker interface {
	MigrateSlab(s *object.Slab, from, to []worker.Contract, currentHeight uint64) error
	RHPScan(hostKey consensus.PublicKey, hostIP string) (worker.RHPScanResponse, error)
	RHPPrepareForm(renterKey consensus.PrivateKey, hostKey consensus.PublicKey, renterFunds types.Currency, renterAddress types.UnlockHash, hostCollateral types.Currency, endHeight uint64, hostSettings rhpv2.HostSettings) (types.FileContract, types.Currency, error)
	RHPPrepareRenew(contract types.FileContractRevision, renterKey consensus.PrivateKey, hostKey consensus.PublicKey, renterFunds types.Currency, renterAddress types.UnlockHash, hostCollateral types.Currency, endHeight uint64, hostSettings rhpv2.HostSettings) (types.FileContract, types.Currency, types.Currency, error)
	RHPForm(renterKey consensus.PrivateKey, hostKey consensus.PublicKey, hostIP string, transactionSet []types.Transaction) (rhpv2.Contract, []types.Transaction, error)
	RHPRenew(renterKey consensus.PrivateKey, hostKey consensus.PublicKey, hostIP string, contractID types.FileContractID, transactionSet []types.Transaction, finalPayment types.Currency) (rhpv2.Contract, []types.Transaction, error)
}

type Autopilot struct {
	store  Store
	bus    Bus
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

		ap.s.tryPerformHostScan()

		// fetch consensus state
		cs, err := ap.bus.ConsensusState()
		if err != nil {
			// TODO: log error
			continue
		}

		// do not continue if we are not synced
		if !cs.Synced {
			// TODO: log occurrence
			continue
		}

		// fetch config to ensure its not updated during maintenance
		cfg := ap.store.Config()

		// update contractor's internal state of consensus
		ap.c.applyConsensusState(cfg, cs)

		// perform maintenance
		_ = ap.c.performContractMaintenance(cfg) // TODO: handle error

		// migration
		_ = ap.m.UpdateContracts() // TODO: handle error
		ap.m.TryPerformMigrations()
	}
}

func (ap *Autopilot) Stop() {
	ap.ticker.Stop()
	close(ap.stopChan)
}

// New initializes an Autopilot.
func New(store Store, bus Bus, worker Worker, tick time.Duration) (*Autopilot, error) {
	ap := &Autopilot{
		store:  store,
		bus:    bus,
		worker: worker,

		ticker:   time.NewTicker(tick),
		stopChan: make(chan struct{}),
	}
	ap.c = newContractor(ap)
	ap.m = newMigrator(ap)
	ap.s = newScanner(ap)
	return ap, nil
}
