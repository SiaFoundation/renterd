package autopilot

import (
	"net/http"
	"time"

	"go.sia.tech/jape"
	"go.sia.tech/renterd/bus"
	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/internal/consensus"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
)

type Store interface {
	Tip() consensus.ChainIndex
	Config() Config
	SetConfig(c Config) error
}

type Bus interface {
	AllHosts() ([]hostdb.Host, error)
	Hosts(notSince time.Time, max int) ([]hostdb.Host, error)
	RecordHostInteraction(hostKey consensus.PublicKey, hi hostdb.Interaction) error
	HostSetContracts(name string) ([]bus.Contract, error)
}

type Worker interface {
	RHPScan(hostKey consensus.PublicKey, hostIP string) (resp rhpv2.HostSettings, err error)
}

type Autopilot struct {
	store     Store
	bus       Bus
	worker    Worker
	masterKey [32]byte

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
	go ap.hostScanLoop()
	<-ap.stopChan
	return nil // TODO
}

func (ap *Autopilot) Stop() {
	close(ap.stopChan)
}

// New initializes an Autopilot.
func New(store Store, bus Bus, worker Worker) (*Autopilot, error) {
	return &Autopilot{
		store:  store,
		bus:    bus,
		worker: worker,
	}, nil
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

func (ap *Autopilot) actionsHandler(jc jape.Context) {
	var since time.Time
	max := -1
	if jc.DecodeForm("since", (*paramTime)(&since)) != nil || jc.DecodeForm("max", &max) != nil {
		return
	}
	jc.Encode(ap.Actions(since, max))
}

// NewServer returns an HTTP handler that serves the renterd autopilot API.
func NewServer(ap *Autopilot) http.Handler {
	return jape.Mux(map[string]jape.Handler{
		"GET    /config":  ap.configHandlerGET,
		"PUT    /config":  ap.configHandlerPUT,
		"GET    /actions": ap.actionsHandler,
	})
}
