package autopilot

import (
	"net/http"
	"time"

	"go.sia.tech/jape"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/consensus"
)

type Store interface {
	Tip() consensus.ChainIndex
	Config() Config
	SetConfig(c Config) error
}

type Autopilot struct {
	store     Store
	renter    *api.Client
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
func New(store Store, renter *api.Client, masterKey [32]byte) *Autopilot {
	return &Autopilot{
		store:     store,
		renter:    renter,
		masterKey: masterKey,
	}
}

type server struct {
	ap *Autopilot
}

func (s *server) configHandlerGET(jc jape.Context) {
	jc.Encode(s.ap.Config())
}

func (s *server) configHandlerPUT(jc jape.Context) {
	var c Config
	if jc.Decode(&c) == nil {
		s.ap.SetConfig(c)
	}
}

func (s *server) actionsHandler(jc jape.Context) {
	var since time.Time
	max := -1
	if jc.DecodeForm("since", (*paramTime)(&since)) != nil || jc.DecodeForm("max", &max) != nil {
		return
	}
	jc.Encode(s.ap.Actions(since, max))
}

func (s *server) objectsHandlerGET(jc jape.Context) {
	panic("unimplemented")
}

func (s *server) objectsHandlerPUT(jc jape.Context) {
	panic("unimplemented")
}

// NewServer returns an HTTP handler that serves the autopilot API.
func NewServer(ap *Autopilot) http.Handler {
	srv := server{ap}
	return jape.Mux(map[string]jape.Handler{
		"GET    /config":       srv.configHandlerGET,
		"PUT    /config":       srv.configHandlerPUT,
		"GET    /actions":      srv.actionsHandler,
		"GET    /objects/*key": srv.objectsHandlerGET,
		"PUT    /objects/*key": srv.objectsHandlerPUT,
	})
}
