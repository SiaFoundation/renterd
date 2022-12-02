package testing

import (
	"context"
	"encoding/hex"
	"net"
	"net/http"
	"path/filepath"
	"sync"
	"time"

	"go.sia.tech/jape"
	"go.sia.tech/renterd/autopilot"
	"go.sia.tech/renterd/bus"
	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/renterd/internal/node"
	"go.sia.tech/renterd/worker"
	"go.sia.tech/siad/siatest"
	"lukechampine.com/frand"
)

// TestCluster is a helper type that allows for easily creating a number of
// nodes connected to each other and ready for testing.
type TestCluster struct {
	hosts []*siatest.TestNode

	Bus    *bus.Client
	Worker *worker.Client
	// Autopilot *autopilot.Client // TODO: add once available

	cleanups  []func() error
	listeners []net.Listener
	shutdowns []func(context.Context) error

	dir string
	wg  sync.WaitGroup
}

func randomPassword() string {
	return hex.EncodeToString(frand.Bytes(32))
}

func newTestCluster(dir string) (*TestCluster, error) {
	// Use shared wallet key.
	wk := consensus.GeneratePrivateKey()

	// Prepare individual dirs.
	busDir := filepath.Join(dir, "bus")
	autopilotDir := filepath.Join(dir, "autopilot")
	// Generate API passwords.
	busPassword := randomPassword()
	workerPassword := randomPassword()
	autopilotPassword := randomPassword()

	busListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	workerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	autopilotListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	busAddr := "http://" + busListener.Addr().String()
	workerAddr := "http://" + workerListener.Addr().String()

	// Create bus.
	var cleanups []func() error
	b, cleanup, err := node.NewBus(node.BusConfig{
		Enabled:     true,
		Bootstrap:   true,
		GatewayAddr: "127.0.0.1:0",
	}, busDir, wk)
	if err != nil {
		return nil, err
	}
	cleanups = append(cleanups, cleanup)
	busAuth := jape.BasicAuth(busPassword)
	busServer := http.Server{
		Handler: busAuth(bus.NewServer(b)),
	}

	// Create worker.
	w, cleanup, err := node.NewWorker(node.WorkerConfig{
		Enabled:     true,
		BusAddr:     busAddr,
		BusPassword: busPassword,
	}, wk)
	if err != nil {
		return nil, err
	}
	cleanups = append(cleanups, cleanup)
	workerAuth := jape.BasicAuth(workerPassword)
	workerServer := http.Server{
		Handler: workerAuth(worker.NewServer(w)),
	}

	// Create autopilot.
	ap, cleanup, err := node.NewAutopilot(node.AutopilotConfig{
		Enabled:        true,
		BusAddr:        busAddr,
		BusPassword:    busPassword,
		WorkerAddr:     workerAddr,
		WorkerPassword: workerPassword,
		Heartbeat:      time.Minute,
	}, autopilotDir)
	if err != nil {
		return nil, err
	}
	cleanups = append(cleanups, cleanup)
	autopilotAuth := jape.BasicAuth(autopilotPassword)
	autopilotServer := http.Server{
		Handler: autopilotAuth(autopilot.NewServer(ap)),
	}

	cluster := &TestCluster{
		dir: dir,

		// Autopilot: autopilot.NewClient(autopilotAddr, autopilotPassword), // TODO
		Bus:    bus.NewClient(busAddr, busPassword),
		Worker: worker.NewClient(workerAddr, workerPassword),

		cleanups:  cleanups,
		shutdowns: []func(context.Context) error{busServer.Shutdown, workerServer.Shutdown, autopilotServer.Shutdown},
	}

	// Spin up the servers.
	cluster.wg.Add(1)
	go func() {
		_ = busServer.Serve(busListener)
		cluster.wg.Done()
	}()
	cluster.wg.Add(1)
	go func() {
		_ = workerServer.Serve(workerListener)
		cluster.wg.Done()
	}()
	cluster.wg.Add(1)
	go func() {
		_ = autopilotServer.Serve(autopilotListener)
		cluster.wg.Done()
	}()
	return cluster, nil
}

// AddHosts adds n hosts to the cluster. These hosts will be funded and announce
// themselves on the network, ready to form contracts.
func (c *TestCluster) AddHosts(n int) error {
	panic("not implemented")
}

// Close performs the cleanup on all servers of the cluster.
func (c *TestCluster) Close() error {
	for _, c := range c.cleanups {
		if err := c(); err != nil {
			return err
		}
	}
	return nil
}

// Shutdown calls Shutdown on all http servers of the cluster.
func (c *TestCluster) Shutdown(ctx context.Context) error {
	for _, s := range c.shutdowns {
		if err := s(ctx); err != nil {
			return err
		}
	}
	c.wg.Wait() // wait for servers to shut down
	return nil
}
