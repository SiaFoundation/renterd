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

	"go.sia.tech/core/types"
	"go.sia.tech/jape"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/autopilot"
	"go.sia.tech/renterd/bus"
	"go.sia.tech/renterd/internal/node"
	"go.sia.tech/renterd/internal/stores"
	"go.sia.tech/siad/build"
	"go.sia.tech/siad/modules"
	sianode "go.sia.tech/siad/node"
	"go.sia.tech/siad/node/api/client"
	"go.uber.org/zap"
	"lukechampine.com/frand"

	"go.sia.tech/renterd/worker"
	"go.sia.tech/siad/siatest"
)

const (
	testInteractionsFlushInterval = 100 * time.Millisecond
	testPersistInterval           = 2 * time.Second
)

var (
	// defaultAutopilotConfig is the autopilot used for testing unless a
	// different one is explicitly set.
	defaultAutopilotConfig = api.AutopilotConfig{
		Contracts: api.ContractsConfig{
			Allowance:   types.Siacoins(1).Mul64(1e3),
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
		TotalShards: 3,
	}

	defaultGouging = api.GougingSettings{
		MaxRPCPrice:      types.Siacoins(1),
		MaxContractPrice: types.Siacoins(1),
		MaxDownloadPrice: types.Siacoins(1).Mul64(2500),
		MaxUploadPrice:   types.Siacoins(1).Mul64(2500),
		MaxStoragePrice:  types.Siacoins(1),
	}
)

type TestNode struct {
	*siatest.TestNode
}

func (n *TestNode) HostKey() (hk types.PublicKey) {
	spk, err := n.HostPublicKey()
	if err != nil {
		panic(err)
	}
	copy(hk[:], spk.Key)
	return
}

// TestCluster is a helper type that allows for easily creating a number of
// nodes connected to each other and ready for testing.
type TestCluster struct {
	hosts []*TestNode

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
	return newTestClusterWithFunding(dir, true, logger)
}

// newTestClusterWithFunding creates a new cluster without hosts that is funded
// by mining multiple blocks if 'funding' is set.
func newTestClusterWithFunding(dir string, funding bool, logger *zap.Logger) (*TestCluster, error) {
	// Use shared wallet key.
	wk := types.GeneratePrivateKey()

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
		Bootstrap:                false,
		InteractionFlushInterval: testInteractionsFlushInterval,
		GatewayAddr:              "127.0.0.1:0",
		Miner:                    miner,
		PersistInterval:          testPersistInterval,
		GougingSettings:          defaultGouging,
		RedundancySettings:       defaultRedundancy,
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
	w, wCleanup, err := node.NewWorker(node.WorkerConfig{
		SessionReconnectTimeout: 10 * time.Second,
		SessionTTL:              2 * time.Minute,
	}, busClient, wk)
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
		Heartbeat:         time.Second,
		ScannerInterval:   time.Second,
		ScannerBatchSize:  10,
		ScannerNumThreads: 1,
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

	// Fund the bus.
	if funding {
		if err := cluster.MineBlocks(20); err != nil {
			return nil, err
		}
	}

	// Set autopilot config.
	err = autopilotClient.SetConfig(defaultAutopilotConfig)
	if err != nil {
		return nil, err
	}
	return cluster, nil
}

// addStorageFolderToHosts adds a single storage folder to each host.
func addStorageFolderToHost(hosts []*TestNode) error {
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
func announceHosts(hosts []*TestNode) error {
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
func (c *TestCluster) sync(hosts []*TestNode) error {
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
func (c *TestCluster) synced(hosts []*TestNode) (bool, error) {
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

func (c *TestCluster) WaitForContracts() ([]api.Contract, error) {
	// build hosts map
	hostsmap := make(map[string]struct{})
	for _, host := range c.hosts {
		hostsmap[host.HostKey().String()] = struct{}{}
	}

	//  wait for the contracts to form
	if err := c.waitForHostContracts(hostsmap); err != nil {
		return nil, err
	}

	// fetch active contracts
	resp, err := c.Worker.ActiveContracts(time.Minute)
	if err != nil {
		return nil, err
	}
	return resp.Contracts, nil
}

func (c *TestCluster) RemoveHost(host *TestNode) error {
	if err := host.Close(); err != nil {
		return err
	}

	for i, h := range c.hosts {
		if h.HostKey().String() == host.HostKey().String() {
			c.hosts = append(c.hosts[:i], c.hosts[i+1:]...)
			break
		}
	}
	return nil
}

// AddHosts adds n hosts to the cluster. These hosts will be funded and announce
// themselves on the network, ready to form contracts.
func (c *TestCluster) AddHosts(n int) ([]*TestNode, error) {
	// Create hosts.
	var newHosts []*TestNode
	for i := 0; i < n; i++ {
		hostDir := filepath.Join(c.dir, "hosts", fmt.Sprint(len(c.hosts)+1))
		n, err := siatest.NewCleanNodeAsync(sianode.Host(hostDir))
		if err != nil {
			return nil, err
		}
		c.hosts = append(c.hosts, &TestNode{n})
		newHosts = append(newHosts, &TestNode{n})

		// Connect gateways.
		if err := c.Bus.SyncerConnect(string(n.GatewayAddress())); err != nil {
			return nil, err
		}
	}

	// Fund host from bus.
	balance, err := c.Bus.WalletBalance()
	if err != nil {
		return nil, err
	}
	fundAmt := balance.Div64(2).Div64(uint64(len(newHosts))) // 50% of bus balance
	var scos []types.SiacoinOutput
	for _, h := range newHosts {
		wag, err := h.WalletAddressGet()
		if err != nil {
			return nil, err
		}
		scos = append(scos, types.SiacoinOutput{
			Value:   fundAmt,
			Address: types.Address(wag.Address),
		})
	}
	if err := c.Bus.SendSiacoins(scos); err != nil {
		return nil, err
	}

	// Mine transaction.
	if err := c.MineBlocks(1); err != nil {
		return nil, err
	}

	// Wait for hosts to sync up with consensus.
	if err := c.sync(newHosts); err != nil {
		return nil, err
	}

	// Announce hosts.
	if err := addStorageFolderToHost(newHosts); err != nil {
		return nil, err
	}
	if err := announceHosts(newHosts); err != nil {
		return nil, err
	}

	// Mine a few blocks. The host should show up eventually.
	err = build.Retry(10, time.Second, func() error {
		if err := c.MineBlocks(1); err != nil {
			return err
		}

		for _, h := range newHosts {
			_, err = c.Bus.Host(h.HostKey())
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Wait for all hosts to be synced.
	if err := c.Sync(); err != nil {
		return nil, err
	}

	return newHosts, nil
}

func (c *TestCluster) AddHostsBlocking(n int) ([]*TestNode, error) {
	// add hosts
	hosts, err := c.AddHosts(n)
	if err != nil {
		return nil, err
	}

	// build hosts map
	hostsmap := make(map[string]struct{})
	for _, host := range hosts {
		hostsmap[host.HostKey().String()] = struct{}{}
	}

	// wait for contracts to form
	if err := c.waitForHostContracts(hostsmap); err != nil {
		return nil, err
	}

	return hosts, nil
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

// waitForHostContracts will fetch the active contracts from the bus and wait
// until we have a contract with every host in the given hosts map
func (c *TestCluster) waitForHostContracts(hosts map[string]struct{}) error {
	return Retry(30, time.Second, func() error {
		contracts, err := c.Bus.ActiveContracts()
		if err != nil {
			return err
		}

		existing := make(map[string]struct{})
		for _, c := range contracts {
			existing[c.HostKey.String()] = struct{}{}
		}

		for hpk := range hosts {
			if _, exists := existing[hpk]; !exists {
				c.MineBlocks(1)
				return fmt.Errorf("missing contract for host %v", hpk)
			}
		}
		return nil
	})
}
