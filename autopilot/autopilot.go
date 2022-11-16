package autopilot

import (
	"net/http"
	"time"

	"go.sia.tech/jape"
	"go.sia.tech/renterd/bus"
	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/internal/consensus"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"go.sia.tech/renterd/worker"
	"go.sia.tech/siad/types"
	"golang.org/x/crypto/blake2b"
)

type Store interface {
	Config() Config
	SetConfig(c Config) error

	State() State
	SetState(s State) error
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
	AllContracts(currentPeriod uint64) ([]bus.Contract, error)
	ActiveContracts(maxEndHeight uint64) ([]bus.Contract, error)

	ContractData(cID types.FileContractID) (rhpv2.Contract, error)
	ContractHistory(cID types.FileContractID, currentPeriod uint64) ([]bus.Contract, error)
	UpdateContractMetadata(cID types.FileContractID, metadata bus.ContractMetadata) error

	AcquireContractLock(cID types.FileContractID) (types.FileContractRevision, error)
	ReleaseContractLock(cID types.FileContractID) error

	// contractsets
	SetHostSet(name string, hosts []consensus.PublicKey) error
	HostSetContracts(name string) ([]bus.Contract, error)

	// txpool
	RecommendedFee() (types.Currency, error)
}

type Worker interface {
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
	s *scanner

	masterKey [32]byte

	ticker   *time.Ticker
	stopChan chan struct{}
}

// Config returns the autopilot's current configuration.
func (ap *Autopilot) Config() Config {
	return ap.store.Config()
}

// SetConfig updates the autopilot's configuration.
func (ap *Autopilot) SetConfig(c Config) error {
	return ap.store.SetConfig(c)
}

// Actions returns the autopilot actions that have occurred since the given time.
func (ap *Autopilot) Actions(since time.Time, max int) []Action {
	panic("unimplemented")
}

func (ap *Autopilot) Run() error {
	if err := ap.load(); err != nil {
		return err
	}

	for {
		select {
		case <-ap.stopChan:
			return nil
		case <-ap.ticker.C:
		}

		// run host scan in separate goroutine, if a host scan is already
		// running this is a no-op
		go ap.s.tryPerformHostScan()

		// perform contract maintenance in a blocking fashion
		_ = ap.c.performContractMaintenance()
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
	ap.s = newScanner(ap)
	return ap, nil
}

// TODO: deriving the renter key from the host key using the master key only
// works if we persist a hash of the renter's master key in the database and
// compare it on startup, otherwise there's no way of knowing the derived key is
// usuable
//
// TODO: instead of deriving a renter key use a randomly generated salt so we're
// not limited to one key per host
func (ap *Autopilot) deriveRenterKey(hostKey consensus.PublicKey) consensus.PrivateKey {
	seed := blake2b.Sum256(append(ap.masterKey[:], hostKey[:]...))
	pk := consensus.NewPrivateKeyFromSeed(seed[:])
	for i := range seed {
		seed[i] = 0
	}
	return pk
}

func (ap *Autopilot) load() error {
	// set the current period
	state := ap.store.State()
	if state.CurrentPeriod == 0 {
		contracts := ap.store.Config().Contracts
		state.CurrentPeriod = state.BlockHeight
		if contracts.Period > contracts.RenewWindow {
			state.CurrentPeriod -= contracts.RenewWindow
		}

		if err := ap.store.SetState(state); err != nil {
			return err
		}
	}

	return nil
}

// NewServer returns an HTTP handler that serves the renterd autopilot API.
func NewServer(ap *Autopilot) http.Handler {
	return jape.Mux(map[string]jape.Handler{
		"GET    /actions": ap.actionsHandler,
		"GET    /config":  ap.configHandlerGET,
		"PUT    /config":  ap.configHandlerPUT,
	})
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
	if jc.Decode(&c) == nil {
		ap.SetConfig(c)
	}
}
