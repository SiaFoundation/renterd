package testing

import (
	"encoding/hex"
	"net/http"
	"path/filepath"
	"time"

	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/renterd/node"
	"lukechampine.com/frand"
)

type noopHandler struct{}

func (h *noopHandler) ServeHTTP(http.ResponseWriter, *http.Request) {
}

type TestCluster struct {
	Autopilot *node.Node
	Bus       *node.Node
	Workers   []*node.Node

	Cleanup []func() error
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
	bus := node.NewNode("127.0.0.1:0", busPassword, busDir, &noopHandler{}, wk)
	err := bus.CreateBus(true, "127.0.0.1:0")
	if err != nil {
		return nil, err
	}

	// Create worker.
	worker := node.NewNode("127.0.0.1:0", workerPassword, workerDir, &noopHandler{}, wk)
	err = worker.AddBus(bus.APIAddress(), busPassword)
	if err != nil {
		return nil, err
	}
	if err := worker.CreateWorker(); err != nil {
		return nil, err
	}

	// Create autopilot.
	autopilot := node.NewNode("127.0.0.1:0", autopilotPassword, autopilotDir, &noopHandler{}, wk)
	err = autopilot.AddBus(bus.APIAddress(), busPassword)
	if err != nil {
		return nil, err
	}
	if err := autopilot.AddWorker(worker.APIAddress(), workerPassword); err != nil {
		return nil, err
	}
	if err := autopilot.CreateAutopilot(3 * time.Second); err != nil {
		return nil, err
	}

	return &TestCluster{
		Autopilot: autopilot,
		Bus:       bus,
		Workers:   []*node.Node{worker},
		Cleanup: []func() error{
			autopilot.Close,
			worker.Close,
			bus.Close,
		},
	}, nil
}

func (c *TestCluster) Close() error {
	for _, f := range c.Cleanup {
		if err := f(); err != nil {
			return err
		}
	}
	return nil
}
