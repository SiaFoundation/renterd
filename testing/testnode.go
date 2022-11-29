package testing

import (
	"context"
	"encoding/hex"
	"net/http"
	"path/filepath"
	"time"

	"go.sia.tech/renterd/bus"
	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/renterd/node"
	"go.sia.tech/renterd/worker"
	"lukechampine.com/frand"
)

type noopHandler struct{}

func (h *noopHandler) ServeHTTP(http.ResponseWriter, *http.Request) {
}

// TestCluster is a helper type that allows for easily creating a number of
// nodes connected to each other and ready for testing.
type TestCluster struct {
	ap      *node.Node
	bus     *node.Node
	workers []*node.Node

	Cleanup []func(context.Context) error
}

func randomPassword() string {
	return hex.EncodeToString(frand.Bytes(32))
}

func newTestCluster(dir string) (*TestCluster, error) {
	// Use shared wallet key.
	wk := consensus.GeneratePrivateKey()

	// Prepare individual dirs.
	busDir := filepath.Join(dir, "bus")
	workerDir := filepath.Join(dir, "worker")
	autopilotDir := filepath.Join(dir, "autopilot")

	// Generate API passwords.
	busPassword := randomPassword()
	workerPassword := randomPassword()
	autopilotPassword := randomPassword()

	// Create bus.
	bus, err := node.NewNode("127.0.0.1:0", busPassword, busDir, &noopHandler{}, wk)
	if err != nil {
		return nil, err
	}
	err = bus.CreateBus(true, "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	busAPIAddr, _ := bus.APICredentials()

	// Create worker.
	worker, err := node.NewNode("127.0.0.1:0", workerPassword, workerDir, &noopHandler{}, wk)
	if err != nil {
		return nil, err
	}
	err = worker.AddBus(busAPIAddr, busPassword)
	if err != nil {
		return nil, err
	}
	if err := worker.CreateWorker(); err != nil {
		return nil, err
	}
	workerAPIAddr, _ := worker.APICredentials()

	// Create autopilot.
	autopilot, err := node.NewNode("127.0.0.1:0", autopilotPassword, autopilotDir, &noopHandler{}, wk)
	if err != nil {
		return nil, err
	}
	err = autopilot.AddBus(busAPIAddr, busPassword)
	if err != nil {
		return nil, err
	}
	if err := autopilot.AddWorker(workerAPIAddr, workerPassword); err != nil {
		return nil, err
	}
	if err := autopilot.CreateAutopilot(3 * time.Second); err != nil {
		return nil, err
	}

	go bus.Serve()
	go worker.Serve()
	go autopilot.Serve()

	return &TestCluster{
		ap:      autopilot,
		bus:     bus,
		workers: []*node.Node{worker},
		Cleanup: []func(ctx context.Context) error{
			autopilot.Shutdown,
			worker.Shutdown,
			bus.Shutdown,
		},
	}, nil
}

func (c *TestCluster) AP() {
	panic("not implemented")
}

func (c *TestCluster) Bus() *bus.Client {
	apiAddr, apiPW := c.bus.APICredentials()
	return bus.NewClient(apiAddr, apiPW)
}

func (c *TestCluster) Workers() []*worker.Client {
	workers := make([]*worker.Client, len(c.workers))
	for i, w := range c.workers {
		apiAddr, apiPW := w.APICredentials()
		workers[i] = worker.NewClient(apiAddr, apiPW)
	}
	return workers
}

// Close closes all nodes within the cluster.
func (c *TestCluster) Shutdown(ctx context.Context) error {
	for _, f := range c.Cleanup {
		if err := f(ctx); err != nil {
			return err
		}
	}
	return nil
}
