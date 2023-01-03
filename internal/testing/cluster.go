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
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/autopilot"
	"go.sia.tech/renterd/bus"
	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/renterd/internal/node"
	"go.sia.tech/renterd/internal/stores"
	"go.sia.tech/siad/modules"
	sianode "go.sia.tech/siad/node"
	"go.sia.tech/siad/node/api/client"
	"go.sia.tech/siad/types"
	"go.uber.org/zap"

	"go.sia.tech/renterd/worker"
	"go.sia.tech/siad/siatest"
	"lukechampine.com/frand"
)

var (
	// defaultAutopilotConfig is the autopilot used for testing unless a
	// different one is explicitly set.
	defaultAutopilotConfig = api.AutopilotConfig{
		Contracts: api.ContractsConfig{
			Allowance:   types.SiacoinPrecision.Mul64(1e3),
			Hosts:       5,
			Period:      50,
			RenewWindow: 24,

			Download: modules.SectorSize * 500,
			Upload:   modules.SectorSize * 500,
			Storage:  modules.SectorSize * 5e3,
		},
		Hosts: api.HostsConfig{
			IgnoreRedundantIPs: true, // ignore for integration tests by default // TODO: add test for IP filter.
		},
	}

	defaultRedundancy = api.RedundancySettings{
		MinShards:   2,
		TotalShards: 5,
	}
)

// TestCluster is a helper type that allows for easily creating a number of
// nodes connected to each other and ready for testing.
type TestCluster struct {
	hosts []*siatest.TestNode

	Autopilot *autopilot.Client
	Bus       *bus.Client
	Worker    *worker.Client

	cleanups []func(context.Context) error

	miner *node.Miner
	dir   string
	wg    sync.WaitGroup
}

// randomPassword creates a random 32 byte password encoded as a string.
func randomPassword() string {
	return hex.EncodeToString(frand.Bytes(32))
}

// Retry will call 'fn' 'tries' times, waiting 'durationBetweenAttempts'
// between each attempt, returning 'nil' the first time that 'fn' returns nil.
// If 'nil' is never returned, then the final error returned by 'fn' is
// returned.
func Retry(tries int, durationBetweenAttempts time.Duration, fn func() error) (err error) {
	for i := 1; i < tries; i++ {
		err = fn()
		if err == nil {
			return nil
		}
		time.Sleep(durationBetweenAttempts)
	}
	return fn()
}

func withCtx(f func() error) func(context.Context) error {
	return func(context.Context) error {
		return f()
	}
}

// newTestCluster creates a new cluster without hosts with a funded bus.
func newTestCluster(dir string, logger *zap.Logger) (*TestCluster, error) {
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
	autopilotAddr := "http://" + autopilotListener.Addr().String()

	// Create clients.
	autopilotClient := autopilot.NewClient(autopilotAddr, autopilotPassword)
	busClient := bus.NewClient(busAddr, busPassword)
	workerClient := worker.NewClient(workerAddr, workerPassword)

	// Create miner.
	miner := node.NewMiner(busClient)

	// Create bus.
	var cleanups []func(context.Context) error
	b, bCleanup, err := node.NewBus(node.BusConfig{
		Bootstrap:          false,
		GatewayAddr:        "127.0.0.1:0",
		Miner:              miner,
		RedundancySettings: defaultRedundancy,
	}, busDir, wk)
	if err != nil {
		return nil, err
	}
	busAuth := jape.BasicAuth(busPassword)
	busServer := http.Server{
		Handler: busAuth(b),
	}
	cleanups = append(cleanups, withCtx(bCleanup))
	cleanups = append(cleanups, busServer.Shutdown)

	// Create worker.
	w, wCleanup, err := node.NewWorker(node.WorkerConfig{}, busClient, wk)
	if err != nil {
		return nil, err
	}
	workerAuth := jape.BasicAuth(workerPassword)
	workerServer := http.Server{
		Handler: workerAuth(w),
	}
	cleanups = append(cleanups, withCtx(wCleanup))
	cleanups = append(cleanups, workerServer.Shutdown)

	// Create autopilot store.
	autopilotStore, err := stores.NewJSONAutopilotStore(autopilotDir)
	if err != nil {
		return nil, err
	}

	// Create autopilot.
	ap, aCleanup, err := node.NewAutopilot(node.AutopilotConfig{
		Heartbeat:       time.Second,
		ScannerInterval: 2 * time.Second,
	}, autopilotStore, busClient, workerClient, logger)
	if err != nil {
		return nil, err
	}
	autopilotAuth := jape.BasicAuth(autopilotPassword)
	autopilotServer := http.Server{
		Handler: autopilotAuth(autopilot.NewServer(ap)),
	}
	cleanups = append(cleanups, withCtx(aCleanup))
	cleanups = append(cleanups, autopilotServer.Shutdown)

	cluster := &TestCluster{
		dir:   dir,
		miner: miner,

		Autopilot: autopilotClient,
		Bus:       busClient,
		Worker:    workerClient,

		cleanups: cleanups,
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
	cluster.wg.Add(1)
	go func() {
		_ = ap.Run()
		cluster.wg.Done()
	}()

	// Fund the bus by mining beyond the foundation hardfork height.
	if err := cluster.MineBlocks(10 + int(types.FoundationHardforkHeight)); err != nil {
		return nil, err
	}

	// Set autopilot config.
	err = autopilotClient.SetConfig(defaultAutopilotConfig)
	if err != nil {
		return nil, err
	}
	return cluster, nil
}

// addStorageFolderToHosts adds a single storage folder to each host.
func addStorageFolderToHost(hosts []*siatest.TestNode) error {
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

// MineToRenewWindow is a helper which mines enough blocks for the autopilot to
// reach its renew window.
func (c *TestCluster) MineToRenewWindow() error {
	cs, err := c.Bus.ConsensusState()
	if err != nil {
		return err
	}
	cfg, err := c.Autopilot.Config()
	if err != nil {
		return err
	}
	currentPeriod, err := c.Autopilot.Status()
	if err != nil {
		return err
	}
	renewWindowStart := currentPeriod + cfg.Contracts.Period
	if cs.BlockHeight >= renewWindowStart {
		return fmt.Errorf("already in renew window: bh: %v, currentPeriod: %v, periodLength: %v, renewWindow: %v", cs.BlockHeight, currentPeriod, cfg.Contracts.Period, renewWindowStart)
	}
	return c.MineBlocks(int(renewWindowStart - cs.BlockHeight))
}

// sync blocks until the cluster is synced.
func (c *TestCluster) sync(hosts []*siatest.TestNode) error {
	return Retry(100, 100*time.Millisecond, func() error {
		synced, err := c.synced(hosts)
		if err != nil {
			return err
		}
		if !synced {
			return errors.New("cluster was unable to sync in time")
		}
		return nil
	})
}

// synced returns true if bus and hosts are at the same blockheight.
func (c *TestCluster) synced(hosts []*siatest.TestNode) (bool, error) {
	cs, err := c.Bus.ConsensusState()
	if err != nil {
		return false, err
	}
	if !cs.Synced {
		return false, nil // can't be synced if bus itself isn't synced
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
	return c.miner.Mine(addr, n)
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

	//  Wait for the contracts to form.
	hostsWithContracts := make(map[string]struct{})
	return Retry(20, time.Second, func() error {
		contracts, err := c.Bus.Contracts()
		if err != nil {
			return err
		}
		for _, c := range contracts {
			hostsWithContracts[c.HostKey.String()] = struct{}{}
		}
		for _, h := range newHosts {
			hpk, err := h.HostPublicKey()
			if err != nil {
				return err
			}
			_, exists := hostsWithContracts[hpk.String()]
			if !exists {
				return fmt.Errorf("missing contract for host %v", hpk.String())
			}
		}
		return nil
	})
}

// Shutdown shuts down a TestCluster. Cleanups are performed in reverse order.
func (c *TestCluster) Shutdown(ctx context.Context) error {
	for i := len(c.cleanups) - 1; i >= 0; i-- {
		if err := c.cleanups[i](ctx); err != nil {
			return err
		}
	}
	for _, h := range c.hosts {
		if err := h.Close(); err != nil {
			return err
		}
	}
	c.wg.Wait()
	return nil
}

// Sync blocks until the whole cluster has reached the same block height.
func (c *TestCluster) Sync() error {
	return c.sync(c.hosts)
}
