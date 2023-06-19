package testing

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.sia.tech/core/consensus"
	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/jape"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/autopilot"
	"go.sia.tech/renterd/bus"
	"go.sia.tech/renterd/internal/node"
	"go.sia.tech/renterd/stores"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"lukechampine.com/frand"

	"go.sia.tech/renterd/worker"
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
			Amount:      3,
			Period:      144,
			RenewWindow: 72,

			Download: rhpv2.SectorSize * 500,
			Upload:   rhpv2.SectorSize * 500,
			Storage:  rhpv2.SectorSize * 5e3,

			Set: "autopilot",
		},
		Hosts: api.HostsConfig{
			MaxDowntimeHours:  10,
			AllowRedundantIPs: true, // allow for integration tests by default
		},
	}

	testGougingSettings = api.GougingSettings{
		MinMaxCollateral: types.Siacoins(10),                   // at least up to 10 SC per contract
		MaxRPCPrice:      types.Siacoins(1).Div64(1000),        // 1mS per RPC
		MaxContractPrice: types.Siacoins(10),                   // 10 SC per contract
		MaxDownloadPrice: types.Siacoins(1).Mul64(1000),        // 1000 SC per 1 TiB
		MaxUploadPrice:   types.Siacoins(1).Mul64(1000),        // 1000 SC per 1 TiB
		MaxStoragePrice:  types.Siacoins(1000).Div64(144 * 30), // 1000 SC per month

		HostBlockHeightLeeway: 240, // amount of leeway given to host block height

		MinPriceTableValidity:         10 * time.Second,  // minimum value for price table validity
		MinAccountExpiry:              time.Hour,         // minimum value for account expiry
		MinMaxEphemeralAccountBalance: types.Siacoins(1), // 1SC
	}

	testRedundancySettings = api.RedundancySettings{
		MinShards:   2,
		TotalShards: 3,
	}
)

// TestCluster is a helper type that allows for easily creating a number of
// nodes connected to each other and ready for testing.
type TestCluster struct {
	hosts []*Host

	Autopilot *autopilot.Client
	Bus       *bus.Client
	Worker    *worker.Client

	workerShutdownFns    []func(context.Context) error
	busShutdownFns       []func(context.Context) error
	autopilotShutdownFns []func(context.Context) error

	miner  *node.Miner
	apID   string
	dbName string
	dir    string
	logger *zap.Logger
	wk     types.PrivateKey
	wg     sync.WaitGroup
}

func (tc *TestCluster) ShutdownAutopilot(ctx context.Context) error {
	for _, fn := range tc.autopilotShutdownFns {
		if err := fn(ctx); err != nil {
			return err
		}
	}
	tc.autopilotShutdownFns = nil
	return nil
}

func (tc *TestCluster) ShutdownWorker(ctx context.Context) error {
	for _, fn := range tc.workerShutdownFns {
		if err := fn(ctx); err != nil {
			return err
		}
	}
	tc.workerShutdownFns = nil
	return nil
}

func (tc *TestCluster) ShutdownBus(ctx context.Context) error {
	for _, fn := range tc.busShutdownFns {
		if err := fn(ctx); err != nil {
			return err
		}
	}
	tc.busShutdownFns = nil
	return nil
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

// AutopilotConfig returns the autopilot's config and current period.
func (c *TestCluster) AutopilotConfig(ctx context.Context) (api.AutopilotConfig, uint64, error) {
	ap, err := c.Bus.Autopilot(context.Background(), c.apID)
	if err != nil {
		return api.AutopilotConfig{}, 0, err
	}
	return ap.Config, ap.CurrentPeriod, nil
}

// UpdateAutopilotConfig updates the cluster's autopilot with given config.
func (c *TestCluster) UpdateAutopilotConfig(ctx context.Context, cfg api.AutopilotConfig) error {
	return c.Bus.UpdateAutopilot(context.Background(), api.Autopilot{
		ID:     c.apID,
		Config: cfg,
	})
}

// newTestCluster creates a new cluster without hosts with a funded bus.
func newTestCluster(dir string, logger *zap.Logger) (*TestCluster, error) {
	wk := types.GeneratePrivateKey()
	return newTestClusterWithFunding(dir, "", true, wk, logger)
}

// newTestClusterWithFunding creates a new cluster without hosts that is funded
// by mining multiple blocks if 'funding' is set.
func newTestClusterWithFunding(dir, dbName string, funding bool, wk types.PrivateKey, logger *zap.Logger) (*TestCluster, error) {
	return newTestClusterCustom(dir, dbName, funding, wk, testBusCfg(), testWorkerCfg(), testApCfg(), logger)
}

// newTestClusterCustom creates a customisable cluster.
func newTestClusterCustom(dir, dbName string, funding bool, wk types.PrivateKey, busCfg node.BusConfig, workerCfg node.WorkerConfig, apCfg node.AutopilotConfig, logger *zap.Logger) (*TestCluster, error) {
	// Check if we are testing against an external database. If so, we create a
	// database with a random name first.
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
		busCfg.DBDialector = stores.NewMySQLConnection(user, password, uri, dbName)
	}

	// Prepare individual dirs.
	apDir := filepath.Join(dir, "autopilot")
	busDir := filepath.Join(dir, "bus")

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
	busCfg.Miner = node.NewMiner(busClient)

	// Create bus.
	b, bStopFn, err := node.NewBus(busCfg, busDir, wk, logger)
	if err != nil {
		return nil, err
	}
	busAuth := jape.BasicAuth(busPassword)
	busServer := http.Server{
		Handler: busAuth(b),
	}

	var busShutdownFns []func(context.Context) error
	busShutdownFns = append(busShutdownFns, busServer.Shutdown)
	busShutdownFns = append(busShutdownFns, bStopFn)

	// Create worker.
	w, wStopFn, err := node.NewWorker(workerCfg, busClient, wk, logger)
	if err != nil {
		return nil, err
	}
	workerAuth := jape.BasicAuth(workerPassword)
	workerServer := http.Server{
		Handler: workerAuth(w),
	}

	var workerShutdownFns []func(context.Context) error
	workerShutdownFns = append(workerShutdownFns, workerServer.Shutdown)
	workerShutdownFns = append(workerShutdownFns, wStopFn)

	// Create autopilot.
	ap, aStartFn, _, aStopFn, err := node.NewAutopilot(apCfg, busClient, []autopilot.Worker{workerClient}, apDir, logger)
	if err != nil {
		return nil, err
	}
	autopilotAuth := jape.BasicAuth(autopilotPassword)
	autopilotServer := http.Server{
		Handler: autopilotAuth(ap),
	}

	var autopilotShutdownFns []func(context.Context) error
	autopilotShutdownFns = append(autopilotShutdownFns, autopilotServer.Shutdown)
	autopilotShutdownFns = append(autopilotShutdownFns, aStopFn)

	cluster := &TestCluster{
		apID:   apCfg.ID,
		dir:    dir,
		dbName: dbName,
		logger: logger,
		miner:  busCfg.Miner,
		wk:     wk,

		Autopilot: autopilotClient,
		Bus:       busClient,
		Worker:    workerClient,

		workerShutdownFns:    workerShutdownFns,
		busShutdownFns:       busShutdownFns,
		autopilotShutdownFns: autopilotShutdownFns,
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
			balance, err := cluster.Bus.WalletBalance(context.Background())
			if err != nil {
				return err
			}
			if balance.IsZero() {
				return errors.New("wallet not funded")
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	// Update the bus settings.
	err = busClient.UpdateSetting(context.Background(), api.SettingGouging, testGougingSettings)
	if err != nil {
		return nil, err
	}
	err = busClient.UpdateSetting(context.Background(), api.SettingRedundancy, testRedundancySettings)
	if err != nil {
		return nil, err
	}

	// Update the autopilot to use test settings
	err = busClient.UpdateAutopilot(context.Background(), api.Autopilot{
		ID:     apCfg.ID,
		Config: testAutopilotConfig,
	})
	if err != nil {
		return nil, err
	}

	return cluster, nil
}

// addStorageFolderToHosts adds a single storage folder to each host.
func addStorageFolderToHost(hosts []*Host) error {
	for _, host := range hosts {
		sectors := uint64(10)
		volumeDir := filepath.Join(host.dir, "volumes")
		if err := os.MkdirAll(volumeDir, 0777); err != nil {
			return err
		}
		if err := host.AddVolume(filepath.Join(volumeDir, "volume.dat"), sectors); err != nil {
			return err
		}
	}
	return nil
}

// announceHosts adds storage and a registry to each host and announces them to
// the group
func announceHosts(hosts []*Host) error {
	for _, host := range hosts {
		settings := defaultHostSettings
		settings.NetAddress = host.RHPv2Addr()
		settings.MaxRegistryEntries = 1 << 18
		if err := host.settings.UpdateSettings(settings); err != nil {
			return err
		}
		if err := host.settings.Announce(); err != nil {
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
	ap, err := c.Bus.Autopilot(context.Background(), c.apID)
	if err != nil {
		return err
	}
	renewWindowStart := ap.CurrentPeriod + ap.Config.Contracts.Period
	if cs.BlockHeight >= renewWindowStart {
		return fmt.Errorf("already in renew window: bh: %v, currentPeriod: %v, periodLength: %v, renewWindow: %v", cs.BlockHeight, ap.CurrentPeriod, ap.Config.Contracts.Period, renewWindowStart)
	}
	err = c.MineBlocks(int(renewWindowStart - cs.BlockHeight))
	if err != nil {
		return err
	}
	return c.Sync()
}

// sync blocks until the cluster is synced.
func (c *TestCluster) sync(hosts []*Host) error {
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
func (c *TestCluster) synced(hosts []*Host) (bool, error) {
	cs, err := c.Bus.ConsensusState(context.Background())
	if err != nil {
		return false, err
	}
	if !cs.Synced {
		return false, nil // can't be synced if bus itself isn't synced
	}
	for _, h := range hosts {
		bh := h.cs.Height()
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
	// If we don't have any hosts in the cluster mine all blocks right away.
	if len(c.hosts) == 0 {
		if err := c.miner.Mine(addr, n); err != nil {
			return err
		}
		return c.Sync()
	}
	// Otherwise mine blocks in batches of 3 to avoid going out of sync with
	// hosts by too many blocks.
	for mined := 0; mined < n; {
		toMine := n - mined
		if toMine > 10 {
			toMine = 10
		}
		if err := c.miner.Mine(addr, toMine); err != nil {
			return err
		}
		if err := c.Sync(); err != nil {
			return err
		}
		mined += toMine
	}
	return nil
}

func (c *TestCluster) WaitForAccounts() ([]api.Account, error) {
	// build hosts map
	hostsMap := make(map[types.PublicKey]struct{})
	for _, host := range c.hosts {
		hostsMap[host.PublicKey()] = struct{}{}
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
		hostsMap[host.PublicKey()] = struct{}{}
	}

	//  wait for the contracts to form
	if err := c.waitForHostContracts(hostsMap); err != nil {
		return nil, err
	}

	// fetch all contracts
	resp, err := c.Worker.Contracts(context.Background(), time.Minute)
	if err != nil {
		return nil, err
	}
	if resp.Error != "" {
		return nil, errors.New(resp.Error)
	}
	return resp.Contracts, nil
}

func (c *TestCluster) RemoveHost(host *Host) error {
	if err := host.Close(); err != nil {
		return err
	}

	for i, h := range c.hosts {
		if h.PublicKey().String() == host.PublicKey().String() {
			c.hosts = append(c.hosts[:i], c.hosts[i+1:]...)
			break
		}
	}
	return nil
}

func (c *TestCluster) NewHost() (*Host, error) {
	// Create host.
	hostDir := filepath.Join(c.dir, "hosts", fmt.Sprint(len(c.hosts)+1))
	h, err := NewHost(types.GeneratePrivateKey(), hostDir, false)
	if err != nil {
		return nil, err
	}

	// Connect gateways.
	if err := c.Bus.SyncerConnect(context.Background(), h.GatewayAddr()); err != nil {
		return nil, err
	}

	return h, nil
}

func (c *TestCluster) AddHost(h *Host) error {
	// Add the host
	c.hosts = append(c.hosts, h)

	// Fund host from bus.
	balance, err := c.Bus.WalletBalance(context.Background())
	if err != nil {
		return err
	}
	fundAmt := balance.Div64(2).Div64(uint64(len(c.hosts))) // 50% of bus balance
	var scos []types.SiacoinOutput
	for i := 0; i < 10; i++ {
		scos = append(scos, types.SiacoinOutput{
			Value:   fundAmt.Div64(10),
			Address: h.WalletAddress(),
		})
	}
	if err := c.Bus.SendSiacoins(context.Background(), scos); err != nil {
		return err
	}

	// Mine transaction.
	if err := c.MineBlocks(1); err != nil {
		return err
	}

	// Wait for hosts to sync up with consensus.
	hosts := []*Host{h}
	if err := c.sync(hosts); err != nil {
		return err
	}

	// Announce hosts.
	if err := addStorageFolderToHost(hosts); err != nil {
		return err
	}
	if err := announceHosts(hosts); err != nil {
		return err
	}

	// Mine a few blocks. The host should show up eventually.
	err = Retry(10, time.Second, func() error {
		if err := c.MineBlocks(1); err != nil {
			return err
		}

		_, err = c.Bus.Host(context.Background(), h.PublicKey())
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return err
	}

	// Wait for host to be synced.
	if err := c.Sync(); err != nil {
		return err
	}

	return nil
}

// AddHosts adds n hosts to the cluster. These hosts will be funded and announce
// themselves on the network, ready to form contracts.
func (c *TestCluster) AddHosts(n int) ([]*Host, error) {
	var newHosts []*Host
	for i := 0; i < n; i++ {
		h, err := c.NewHost()
		if err != nil {
			return nil, err
		}
		err = c.AddHost(h)
		if err != nil {
			return nil, err
		}
		newHosts = append(newHosts, h)
	}
	return newHosts, nil
}

func (c *TestCluster) AddHostsBlocking(n int) ([]*Host, error) {
	// add hosts
	hosts, err := c.AddHosts(n)
	if err != nil {
		return nil, err
	}

	// build hosts map
	hostsmap := make(map[types.PublicKey]struct{})
	for _, host := range hosts {
		hostsmap[host.PublicKey()] = struct{}{}
	}

	// wait for contracts to form
	if err := c.waitForHostContracts(hostsmap); err != nil {
		return nil, err
	}

	return hosts, nil
}

// Shutdown shuts down a TestCluster.
func (c *TestCluster) Shutdown(ctx context.Context) error {
	if err := c.ShutdownAutopilot(ctx); err != nil {
		return err
	}
	if err := c.ShutdownWorker(ctx); err != nil {
		return err
	}
	if err := c.ShutdownBus(ctx); err != nil {
		return err
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
				funded[a.HostKey] = struct{}{}
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

// waitForHostContracts will fetch the contracts from the bus and wait until we
// have a contract with every host in the given hosts map
func (c *TestCluster) waitForHostContracts(hosts map[types.PublicKey]struct{}) error {
	return Retry(30, time.Second, func() error {
		contracts, err := c.Bus.Contracts(context.Background())
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

// testNetwork returns a custom network for testing which matches the
// configuration of siad consensus in testing.
func testNetwork() *consensus.Network {
	n := &consensus.Network{
		InitialCoinbase: types.Siacoins(300000),
		MinimumCoinbase: types.Siacoins(299990),
		InitialTarget:   types.BlockID{4: 32},
	}

	n.HardforkDevAddr.Height = 3
	n.HardforkDevAddr.OldAddress = types.Address{}
	n.HardforkDevAddr.NewAddress = types.Address{}

	n.HardforkTax.Height = 10

	n.HardforkStorageProof.Height = 10

	n.HardforkOak.Height = 20
	n.HardforkOak.FixHeight = 23
	n.HardforkOak.GenesisTimestamp = time.Now().Add(-1e6 * time.Second)

	n.HardforkASIC.Height = 5
	n.HardforkASIC.OakTime = 10000 * time.Second
	n.HardforkASIC.OakTarget = types.BlockID{255, 255}

	n.HardforkFoundation.Height = 50
	n.HardforkFoundation.PrimaryAddress = types.GeneratePrivateKey().PublicKey().StandardAddress()
	n.HardforkFoundation.FailsafeAddress = types.GeneratePrivateKey().PublicKey().StandardAddress()

	return n
}

func testBusCfg() node.BusConfig {
	return node.BusConfig{
		Bootstrap:       false,
		GatewayAddr:     "127.0.0.1:0",
		Network:         testNetwork(),
		PersistInterval: testPersistInterval,
		UsedUTXOExpiry:  time.Minute,
	}
}

func testWorkerCfg() node.WorkerConfig {
	return node.WorkerConfig{
		AllowPrivateIPs:        true,
		ContractLockTimeout:    5 * time.Second,
		ID:                     "worker",
		BusFlushInterval:       testBusFlushInterval,
		DownloadSectorTimeout:  500 * time.Millisecond,
		UploadOverdriveTimeout: 500 * time.Millisecond,
		UploadMaxOverdrive:     5,
	}
}

func testApCfg() node.AutopilotConfig {
	return node.AutopilotConfig{
		ID:                       "autopilot",
		AccountsRefillInterval:   time.Second,
		Heartbeat:                time.Second,
		MigrationHealthCutoff:    0.99,
		ScannerInterval:          time.Second,
		ScannerBatchSize:         10,
		ScannerNumThreads:        1,
		ScannerMinRecentFailures: 5,
	}
}
