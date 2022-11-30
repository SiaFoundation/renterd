package testing

import (
	"context"
	"encoding/hex"
	"fmt"
	"net/http"
	"path/filepath"
	"time"

	"go.sia.tech/renterd/bus"
	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/renterd/node"
	"go.sia.tech/renterd/worker"
	"go.sia.tech/siad/modules"
	sianode "go.sia.tech/siad/node"
	"go.sia.tech/siad/siatest"
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

	dir   string
	hosts []*siatest.TestNode

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

	// TODO: Mine a few blocks for funding.

	return &TestCluster{
		ap:      autopilot,
		bus:     bus,
		dir:     dir,
		workers: []*node.Node{worker},
		Cleanup: []func(ctx context.Context) error{
			autopilot.Shutdown,
			worker.Shutdown,
			bus.Shutdown,
		},
	}, nil
}

// AddHosts adds n hosts to the cluster. These hosts will be funded and announce
// themselves on the network, ready to form contracts.
func (c *TestCluster) AddHosts(n int) error {
	apiAddr, _ := c.bus.APICredentials()
	for i := 0; i < n; i++ {
		hostDir := filepath.Join(c.dir, "hosts", fmt.Sprint(len(c.hosts)+1))
		n, err := siatest.NewCleanNode(sianode.Host(hostDir))
		if err != nil {
			return err
		}
		c.hosts = append(c.hosts, n)

		// Connect to bus.
		if err := n.GatewayConnectPost(modules.NetAddress(apiAddr)); err != nil {
			return err
		}

		// TODO: fund host from bus.
	}
	// TODO: wait for hosts to be synced with consensus.

	// TODO: announce hosts.

	// TODO: wait for hosts to show up in hostdb.

	return nil
}

// AP returns a client configured to talk to the cluster's autopilot.
func (c *TestCluster) AP() {
	panic("not implemented")
}

// Bus returns a client configured to talk to the cluster's bus.
func (c *TestCluster) Bus() *bus.Client {
	apiAddr, apiPW := c.bus.APICredentials()
	return bus.NewClient(apiAddr, apiPW)
}

// Workers returns clients configured to talk to the cluster's workers.
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
