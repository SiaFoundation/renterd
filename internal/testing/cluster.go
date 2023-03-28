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
	"gorm.io/gorm"
	"lukechampine.com/frand"

	"go.sia.tech/renterd/worker"
	"go.sia.tech/siad/siatest"
)

const (
	testBusFlushInterval = 100 * time.Millisecond
	testPersistInterval  = 2 * time.Second
	latestHardforkHeight = 50 // foundation hardfork height in testing
)

var (
	// testAutopilotConfig is the autopilot used for testing unless a different
	// one is explicitly set.
	testAutopilotConfig = api.AutopilotConfig{
		Contracts: api.ContractsConfig{
			Allowance:   types.Siacoins(1).Mul64(1e3),
			Amount:      5,
			Period:      50,
			RenewWindow: 24,

			Download: modules.SectorSize * 500,
			Upload:   modules.SectorSize * 500,
			Storage:  modules.SectorSize * 5e3,

			Set: "autopilot",
		},
		Hosts: api.HostsConfig{
			IgnoreRedundantIPs: true, // ignore for integration tests by default
		},
	}

	testRedundancySettings = api.RedundancySettings{
		MinShards:   2,
		TotalShards: 3,
	}

	testGougingSettings = api.GougingSettings{
		MinMaxCollateral: types.Siacoins(10),                   // at least up to 10 SC per contract
		MaxRPCPrice:      types.Siacoins(1).Div64(1000),        // 1mS per RPC
		MaxContractPrice: types.Siacoins(10),                   // 10 SC per contract
		MaxDownloadPrice: types.Siacoins(1).Mul64(1000),        // 1000 SC per 1 TiB
		MaxUploadPrice:   types.Siacoins(1).Mul64(1000),        // 1000 SC per 1 TiB
		MaxStoragePrice:  types.Siacoins(1000).Div64(144 * 30), // 1000 SC per month

		HostBlockHeightLeeway: 120, // amount of leeway given to host block height
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

	miner  *node.Miner
	dbName string
	dir    string
	logger *zap.Logger
	wk     types.PrivateKey
	wg     sync.WaitGroup
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

// Reboot simulates a reboot of the cluster by calling Shutdown and creating a
// new cluster using the same settings as the previous one.
// NOTE: Simulating a reboot means that the hosts stay active and are not
// restarted.
func (c *TestCluster) Reboot(ctx context.Context) (*TestCluster, error) {
	hosts := c.hosts
	c.hosts = nil
	if err := c.Shutdown(ctx); err != nil {
		return nil, err
	}

	newCluster, err := newTestClusterWithFunding(c.dir, c.dbName, false, c.wk, c.logger)
	if err != nil {
		return nil, err
	}
	newCluster.hosts = hosts
	return newCluster, nil
}

// newTestCluster creates a new cluster without hosts with a funded bus.
func newTestCluster(dir string, logger *zap.Logger) (*TestCluster, error) {
	wk := types.GeneratePrivateKey()
	return newTestClusterWithFunding(dir, "", true, wk, logger)
}

// newTestClusterWithFunding creates a new cluster without hosts that is funded
// by mining multiple blocks if 'funding' is set.
func newTestClusterWithFunding(dir, dbName string, funding bool, wk types.PrivateKey, logger *zap.Logger) (*TestCluster, error) {
	// Check if we are testing against an external database. If so, we create a
	// database with a random name first.
	var dialector gorm.Dialector
	uri, user, password, _ := stores.DBConfigFromEnv()
	if uri != "" {
		tmpDB, err := gorm.Open(stores.NewMySQLConnection(user, password, uri, ""))
		if err != nil {
			return nil, err
		}
		if dbName == "" {
			dbName = "db" + hex.EncodeToString(frand.Bytes(16))
		}
		if err := tmpDB.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s;", dbName)).Error; err != nil {
			return nil, err
		}
		dialector = stores.NewMySQLConnection(user, password, uri, dbName)
	}

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
	var shutdownFns []func(context.Context) error
	b, bStopFn, err := node.NewBus(node.BusConfig{
		DBDialector:     dialector,
		Bootstrap:       false,
		GatewayAddr:     "127.0.0.1:0",
		Miner:           miner,
		PersistInterval: testPersistInterval,
	}, busDir, wk, logger)
	if err != nil {
		return nil, err
	}
	busAuth := jape.BasicAuth(busPassword)
	busServer := http.Server{
		Handler: busAuth(b),
	}
	shutdownFns = append(shutdownFns, bStopFn)
	shutdownFns = append(shutdownFns, busServer.Shutdown)

	// Create worker.
	w, wStopFn, err := node.NewWorker(node.WorkerConfig{
		ID:                      "worker",
		BusFlushInterval:        testBusFlushInterval,
		SessionReconnectTimeout: 10 * time.Second,
		SessionTTL:              2 * time.Minute,
	}, busClient, wk, logger)
	if err != nil {
		return nil, err
	}
	workerAuth := jape.BasicAuth(workerPassword)
	workerServer := http.Server{
		Handler: workerAuth(w),
	}
	shutdownFns = append(shutdownFns, wStopFn)
	shutdownFns = append(shutdownFns, workerServer.Shutdown)

	// Create autopilot store.
	autopilotStore, err := stores.NewJSONAutopilotStore(autopilotDir)
	if err != nil {
		return nil, err
	}

	// Set autopilot config.
	err = autopilotStore.SetConfig(testAutopilotConfig)
	if err != nil {
		return nil, err
	}

	// Create autopilot.
	ap, aStartFn, aStopFn, err := node.NewAutopilot(node.AutopilotConfig{
		AccountsRefillInterval: time.Second,
		Heartbeat:              time.Second,
		MigrationHealthCutoff:  0.99,
		ScannerInterval:        time.Second,
		ScannerBatchSize:       10,
		ScannerNumThreads:      1,
	}, autopilotStore, busClient, []autopilot.Worker{workerClient}, logger)
	if err != nil {
		return nil, err
	}
	autopilotAuth := jape.BasicAuth(autopilotPassword)
	autopilotServer := http.Server{
		Handler: autopilotAuth(ap),
	}
	shutdownFns = append(shutdownFns, aStopFn)
	shutdownFns = append(shutdownFns, autopilotServer.Shutdown)

	cluster := &TestCluster{
		dir:    dir,
		dbName: dbName,
		logger: logger,
		miner:  miner,
		wk:     wk,

		Autopilot: autopilotClient,
		Bus:       busClient,
		Worker:    workerClient,

		cleanups: shutdownFns,
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
		_ = aStartFn()
		cluster.wg.Done()
	}()

	// Fund the bus.
	if funding {
		if err := cluster.MineBlocks(latestHardforkHeight); err != nil {
			return nil, err
		}
		err = Retry(1000, 100*time.Millisecond, func() error {
			resp, err := busClient.ConsensusState(context.Background())
			if err != nil {
				return err
			}
			if !resp.Synced || resp.BlockHeight < latestHardforkHeight {
				return fmt.Errorf("chain not synced: %v %v", resp.Synced, resp.BlockHeight < latestHardforkHeight)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	// Update the bus settings.
	err = busClient.UpdateGougingSettings(context.Background(), testGougingSettings)
	if err != nil {
		return nil, err
	}
	err = busClient.UpdateRedundancySettings(context.Background(), testRedundancySettings)
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
		if err := host.HostModifySettingPost(client.HostParamMaxCollateral, types.Siacoins(100).ExactString()); err != nil {
			return err
		}
		if err := host.HostModifySettingPost(client.HostParamCollateral, types.Siacoins(1).Div64(4096).Div64(testAutopilotConfig.Contracts.Period).ExactString()); err != nil {
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
	cs, err := c.Bus.ConsensusState(context.Background())
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
	cs, err := c.Bus.ConsensusState(context.Background())
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
	addr, err := c.Bus.WalletAddress(context.Background())
	if err != nil {
		return err
	}
	return c.miner.Mine(addr, n)
}

func (c *TestCluster) WaitForAccounts() ([]api.Account, error) {
	// build hosts map
	hostsMap := make(map[types.PublicKey]struct{})
	for _, host := range c.hosts {
		hostsMap[host.HostKey()] = struct{}{}
	}

	//  wait for accounts to be filled
	if err := c.waitForHostAccounts(hostsMap); err != nil {
		return nil, err
	}

	// fetch all accounts
	return c.Bus.Accounts(context.Background())
}

func (c *TestCluster) WaitForContracts() ([]api.Contract, error) {
	// build hosts map
	hostsMap := make(map[types.PublicKey]struct{})
	for _, host := range c.hosts {
		hostsMap[host.HostKey()] = struct{}{}
	}

	//  wait for the contracts to form
	if err := c.waitForHostContracts(hostsMap); err != nil {
		return nil, err
	}

	// fetch active contracts
	resp, err := c.Worker.ActiveContracts(context.Background(), time.Minute)
	if err != nil {
		return nil, err
	}
	if resp.Error != "" {
		return nil, errors.New(resp.Error)
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
		if err := c.Bus.SyncerConnect(context.Background(), string(n.GatewayAddress())); err != nil {
			return nil, err
		}
	}

	// Fund host from bus.
	balance, err := c.Bus.WalletBalance(context.Background())
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
	if err := c.Bus.SendSiacoins(context.Background(), scos); err != nil {
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
			_, err = c.Bus.Host(context.Background(), h.HostKey())
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
	hostsmap := make(map[types.PublicKey]struct{})
	for _, host := range hosts {
		hostsmap[host.HostKey()] = struct{}{}
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

// waitForHostAccounts will fetch the accounts from the worker and wait until
// they have money in them
func (c *TestCluster) waitForHostAccounts(hosts map[types.PublicKey]struct{}) error {
	return Retry(30, time.Second, func() error {
		accounts, err := c.Bus.Accounts(context.Background())
		if err != nil {
			return err
		}

		funded := make(map[types.PublicKey]struct{})
		for _, a := range accounts {
			if a.Balance.Uint64() > 0 {
				funded[a.Host] = struct{}{}
			}
		}

		for hpk := range hosts {
			if _, exists := funded[hpk]; !exists {
				return fmt.Errorf("missing funded account for host %v", hpk)
			}
		}
		return nil
	})
}

// waitForHostContracts will fetch the active contracts from the bus and wait
// until we have a contract with every host in the given hosts map
func (c *TestCluster) waitForHostContracts(hosts map[types.PublicKey]struct{}) error {
	return Retry(30, time.Second, func() error {
		contracts, err := c.Bus.ActiveContracts(context.Background())
		if err != nil {
			return err
		}

		existing := make(map[types.PublicKey]struct{})
		for _, c := range contracts {
			existing[c.HostKey] = struct{}{}
		}

		for hpk := range hosts {
			if _, exists := existing[hpk]; !exists {
				return fmt.Errorf("missing contract for host %v", hpk)
			}
		}
		return nil
	})
}
