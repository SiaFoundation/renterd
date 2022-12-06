package testing

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
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
	"go.sia.tech/siad/modules"
	sianode "go.sia.tech/siad/node"
	"go.sia.tech/siad/node/api/client"
	"go.sia.tech/siad/types"

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
	shutdowns []func(context.Context) error

	dir         string
	gatewayAddr string
	wg          sync.WaitGroup
}

// randomPassword creates a random 32 byte password encoded as a string.
func randomPassword() string {
	return hex.EncodeToString(frand.Bytes(32))
}

// newTestCluster creates a new cluster without hosts with a funded bus.
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
		Bootstrap:   false,
		GatewayAddr: "127.0.0.1:0",
		Miner:       true,
	}, busDir, wk)
	if err != nil {
		return nil, err
	}
	cleanups = append(cleanups, cleanup)
	busAuth := jape.BasicAuth(busPassword)
	busServer := http.Server{
		Handler: busAuth(b),
	}
	busClient := bus.NewClient(busAddr, busPassword)
	gatewayAddr, err := busClient.SyncerAddress()
	if err != nil {
		return nil, err
	}

	// Create worker.
	w, cleanup, err := node.NewWorker(node.WorkerConfig{}, busClient, wk)
	if err != nil {
		return nil, err
	}
	cleanups = append(cleanups, cleanup)
	workerAuth := jape.BasicAuth(workerPassword)
	workerServer := http.Server{
		Handler: workerAuth(w),
	}
	workerClient := worker.NewClient(workerAddr, workerPassword)

	// Create autopilot.
	ap, cleanup, err := node.NewAutopilot(node.AutopilotConfig{
		Heartbeat: time.Minute,
	}, busClient, workerClient, autopilotDir)
	if err != nil {
		return nil, err
	}
	cleanups = append(cleanups, cleanup)
	autopilotAuth := jape.BasicAuth(autopilotPassword)
	autopilotServer := http.Server{
		Handler: autopilotAuth(autopilot.NewServer(ap)),
	}

	cluster := &TestCluster{
		dir:         dir,
		gatewayAddr: gatewayAddr,

		//Autopilot: autopilot.NewClient(autopilotAddr, autopilotPassword), // TODO
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

	// Fund the bus by mining beyond the foundation hardfork height.
	if err := cluster.MineBlocks(10 + int(types.FoundationHardforkHeight)); err != nil {
		return nil, err
	}
	return cluster, nil
}

// addStorageFolderToHosts adds a single storage folder to each host.
func addStorageFolderToHost(hosts []*siatest.TestNode) error {
	// The following api call is very slow. Using multiple threads speeds that
	// process up a lot.
	for _, host := range hosts {
		storage := 512 * modules.SectorSize
		if err := host.HostStorageFoldersAddPost(host.Dir, storage); err != nil {
			return err
		}
	}
	return nil
}

// announceHosts adds storage and a registry to each host and announces them to
// the group
func announceHosts(hosts []*siatest.TestNode) error {
	for _, host := range hosts {
		if err := host.HostModifySettingPost(client.HostParamAcceptingContracts, true); err != nil {
			return err
		}
		if err := host.HostModifySettingPost(client.HostParamRegistrySize, 1<<18); err != nil {
			return err
		}
		if err := host.HostAnnouncePost(); err != nil {
			return err
		}
	}
	return nil
}

func (c *TestCluster) sync(hosts []*siatest.TestNode) error {
	for i := 0; i < 100; i++ {
		synced, err := c.synced(hosts)
		if err != nil {
			return err
		}
		if synced {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return errors.New("cluster was unable to sync in time")
}

// synced returns true if bus and hosts are at the same blockheight.
func (c *TestCluster) synced(hosts []*siatest.TestNode) (bool, error) {
	cs, err := c.Bus.ConsensusState()
	if err != nil {
		return false, err
	}
	for _, h := range hosts {
		bh, err := h.BlockHeight()
		if err != nil {
			return false, err
		}
		if cs.BlockHeight != uint64(bh) {
			return false, nil
		}
	}
	return true, nil
}

// MineBlocks uses the bus' miner to mine n blocks.
func (c *TestCluster) MineBlocks(n int) error {
	addr, err := c.Bus.WalletAddress()
	if err != nil {
		return err
	}
	return c.Bus.MineBlocks(addr, n)
}

// AddHosts adds n hosts to the cluster. These hosts will be funded and announce
// themselves on the network, ready to form contracts.
func (c *TestCluster) AddHosts(n int) error {
	// Create hosts.
	var newHosts []*siatest.TestNode
	for i := 0; i < n; i++ {
		hostDir := filepath.Join(c.dir, "hosts", fmt.Sprint(len(c.hosts)+1))
		n, err := siatest.NewCleanNodeAsync(sianode.Host(hostDir))
		if err != nil {
			return err
		}
		c.hosts = append(c.hosts, n)
		newHosts = append(newHosts, n)

		// Connect gateways.
		if err := c.Bus.SyncerConnect(string(n.GatewayAddress())); err != nil {
			return err
		}
	}

	// Fund host from bus.
	balance, err := c.Bus.WalletBalance()
	if err != nil {
		return err
	}
	fundAmt := balance.Div64(2).Div64(uint64(len(newHosts))) // 50% of bus balance
	var scos []types.SiacoinOutput
	for _, h := range newHosts {
		wag, err := h.WalletAddressGet()
		if err != nil {
			return err
		}
		scos = append(scos, types.SiacoinOutput{
			Value:      fundAmt,
			UnlockHash: wag.Address,
		})
	}
	if err := c.Bus.SendSiacoins(scos); err != nil {
		return err
	}

	// Mine transaction.
	if err := c.MineBlocks(1); err != nil {
		return err
	}

	// Wait for hosts to sync up with consensus.
	if err := c.sync(newHosts); err != nil {
		return err
	}

	// Announce hosts.
	if err := addStorageFolderToHost(newHosts); err != nil {
		return err
	}
	if err := announceHosts(newHosts); err != nil {
		return err
	}

	// Mine a few more blocks to mine the announcements and sync the
	// cluster.
	if err := c.MineBlocks(5); err != nil {
		return err
	}

	// Sync cluster.
	if err := c.Sync(); err != nil {
		return err
	}

	// Hosts should show up in hostdb.
	for _, h := range newHosts {
		hpk, err := h.HostPublicKey()
		if err != nil {
			return err
		}
		_, err = c.Bus.Host(consensus.PublicKey(hpk.ToPublicKey()))
		if err != nil {
			return err
		}
	}
	return nil
}

// Close performs the cleanup on all servers of the cluster.
func (c *TestCluster) Close() error {
	for _, c := range c.cleanups {
		if err := c(); err != nil {
			return err
		}
	}
	for _, h := range c.hosts {
		if err := h.Close(); err != nil {
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

// Sync blocks until the whole cluster has reached the same block height.
func (c *TestCluster) Sync() error {
	return c.sync(c.hosts)
}
