package e2e

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
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"go.sia.tech/jape"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/autopilot"
	"go.sia.tech/renterd/build"
	"go.sia.tech/renterd/bus"
	"go.sia.tech/renterd/config"
	"go.sia.tech/renterd/internal/node"
	"go.sia.tech/renterd/internal/test"
	"go.sia.tech/renterd/stores"
	"go.sia.tech/renterd/worker/s3"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gorm.io/gorm"
	"lukechampine.com/frand"

	"go.sia.tech/renterd/worker"
	gormlogger "gorm.io/gorm/logger"
	"moul.io/zapgorm2"
)

const (
	testBusFlushInterval   = 100 * time.Millisecond
	testBusPersistInterval = 2 * time.Second
	latestHardforkHeight   = 50 // foundation hardfork height in testing
)

var (
	clusterOptsDefault  = testClusterOptions{}
	clusterOptNoFunding = false
)

// TestCluster is a helper type that allows for easily creating a number of
// nodes connected to each other and ready for testing.
type TestCluster struct {
	hosts []*Host

	Autopilot *autopilot.Client
	Bus       *bus.Client
	Worker    *worker.Client
	S3        *minio.Client
	S3Core    *minio.Core

	workerShutdownFns    []func(context.Context) error
	busShutdownFns       []func(context.Context) error
	autopilotShutdownFns []func(context.Context) error
	s3ShutdownFns        []func(context.Context) error

	network *consensus.Network
	miner   *node.Miner
	apID    string
	dbName  string
	dir     string
	logger  *zap.Logger
	tt      test.TT
	wk      types.PrivateKey
	wg      sync.WaitGroup
}

func (tc *TestCluster) ShutdownAutopilot(ctx context.Context) {
	tc.tt.Helper()
	for _, fn := range tc.autopilotShutdownFns {
		tc.tt.OK(fn(ctx))
	}
	tc.autopilotShutdownFns = nil
}

func (tc *TestCluster) ShutdownWorker(ctx context.Context) {
	tc.tt.Helper()
	for _, fn := range tc.workerShutdownFns {
		tc.tt.OK(fn(ctx))
	}
	tc.workerShutdownFns = nil
}

func (tc *TestCluster) ShutdownBus(ctx context.Context) {
	tc.tt.Helper()
	for _, fn := range tc.busShutdownFns {
		tc.tt.OK(fn(ctx))
	}
	tc.busShutdownFns = nil
}

func (tc *TestCluster) ShutdownS3(ctx context.Context) {
	tc.tt.Helper()
	for _, fn := range tc.s3ShutdownFns {
		tc.tt.OK(fn(ctx))
	}
	tc.s3ShutdownFns = nil
}

// randomPassword creates a random 32 byte password encoded as a string.
func randomPassword() string {
	return hex.EncodeToString(frand.Bytes(32))
}

// Reboot simulates a reboot of the cluster by calling Shutdown and creating a
// new cluster using the same settings as the previous one.
// NOTE: Simulating a reboot means that the hosts stay active and are not
// restarted.
func (c *TestCluster) Reboot(t *testing.T) *TestCluster {
	c.tt.Helper()
	hosts := c.hosts
	c.hosts = nil
	c.Shutdown()

	newCluster := newTestCluster(t, testClusterOptions{
		dir:       c.dir,
		dbName:    c.dbName,
		logger:    c.logger,
		funding:   &clusterOptNoFunding,
		walletKey: &c.wk,
	})
	newCluster.hosts = hosts
	return newCluster
}

// AutopilotConfig returns the autopilot's config and current period.
func (c *TestCluster) AutopilotConfig(ctx context.Context) (api.AutopilotConfig, uint64) {
	c.tt.Helper()
	ap, err := c.Bus.Autopilot(ctx, c.apID)
	c.tt.OK(err)
	return ap.Config, ap.CurrentPeriod
}

// UpdateAutopilotConfig updates the cluster's autopilot with given config.
func (c *TestCluster) UpdateAutopilotConfig(ctx context.Context, cfg api.AutopilotConfig) {
	c.tt.Helper()
	c.tt.OK(c.Bus.UpdateAutopilot(ctx, api.Autopilot{
		ID:     c.apID,
		Config: cfg,
	}))
}

type testClusterOptions struct {
	dbName               string
	dir                  string
	funding              *bool
	hosts                int
	logger               *zap.Logger
	uploadPacking        bool
	skipSettingAutopilot bool
	skipRunningAutopilot bool
	walletKey            *types.PrivateKey

	autopilotCfg      *node.AutopilotConfig
	autopilotSettings *api.AutopilotConfig
	busCfg            *node.BusConfig
	workerCfg         *config.Worker
}

// newTestLogger creates a console logger used for testing.
func newTestLogger() *zap.Logger {
	return newTestLoggerCustom(zapcore.DebugLevel)
}

// newTestLoggerCustom creates a console logger used for testing and allows
// passing in the desired log level.
func newTestLoggerCustom(level zapcore.Level) *zap.Logger {
	config := zap.NewProductionEncoderConfig()
	config.EncodeTime = zapcore.RFC3339TimeEncoder
	config.EncodeLevel = zapcore.CapitalColorLevelEncoder
	config.StacktraceKey = ""
	consoleEncoder := zapcore.NewConsoleEncoder(config)

	return zap.New(
		zapcore.NewCore(consoleEncoder, zapcore.AddSync(os.Stdout), level),
		zap.AddCaller(),
		zap.AddStacktrace(level),
	)
}

// newTestCluster creates a new cluster without hosts with a funded bus.
func newTestCluster(t *testing.T, opts testClusterOptions) *TestCluster {
	t.Helper()

	// Skip any test that requires a cluster when running short tests.
	if testing.Short() {
		t.SkipNow()
	}
	tt := test.NewTT(t)

	// Ensure we don't hang
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	// Apply options.
	dbName := opts.dbName
	dir := t.TempDir()
	if opts.dir != "" {
		dir = opts.dir
	}
	logger := newTestLogger()
	if opts.logger != nil {
		logger = opts.logger
	}
	wk := types.GeneratePrivateKey()
	if opts.walletKey != nil {
		wk = *opts.walletKey
	}
	busCfg, workerCfg, apCfg := testBusCfg(), testWorkerCfg(), testApCfg()
	if opts.busCfg != nil {
		busCfg = *opts.busCfg
	}
	if opts.workerCfg != nil {
		workerCfg = *opts.workerCfg
	}
	if opts.autopilotCfg != nil {
		apCfg = *opts.autopilotCfg
	}
	funding := true
	if opts.funding != nil {
		funding = *opts.funding
	}
	nHosts := 0
	if opts.hosts != 0 {
		nHosts = opts.hosts
	}
	enableUploadPacking := false
	if opts.uploadPacking {
		enableUploadPacking = opts.uploadPacking
	}
	apSettings := test.AutopilotConfig
	if opts.autopilotSettings != nil {
		apSettings = *opts.autopilotSettings
	}

	// default database logger
	if busCfg.DBLogger == nil {
		busCfg.DBLogger = zapgorm2.Logger{
			ZapLogger:                 logger.Named("SQL"),
			LogLevel:                  gormlogger.Warn,
			SlowThreshold:             100 * time.Millisecond,
			SkipCallerLookup:          false,
			IgnoreRecordNotFoundError: true,
			Context:                   nil,
		}
	}

	// Check if we are testing against an external database. If so, we create a
	// database with a random name first.
	uri, user, password, _ := stores.DBConfigFromEnv()
	if uri != "" {
		tmpDB, err := gorm.Open(stores.NewMySQLConnection(user, password, uri, ""))
		tt.OK(err)

		if dbName == "" {
			dbName = "db" + hex.EncodeToString(frand.Bytes(16))
		}
		dbMetricsName := "db" + hex.EncodeToString(frand.Bytes(16))
		tt.OK(tmpDB.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s;", dbName)).Error)
		tt.OK(tmpDB.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s;", dbMetricsName)).Error)

		busCfg.DBDialector = stores.NewMySQLConnection(user, password, uri, dbName)
		busCfg.DBMetricsDialector = stores.NewMySQLConnection(user, password, uri, dbMetricsName)
	}

	// Prepare individual dirs.
	busDir := filepath.Join(dir, "bus")

	// Generate API passwords.
	busPassword := randomPassword()
	workerPassword := randomPassword()
	autopilotPassword := randomPassword()

	busListener, err := net.Listen("tcp", "127.0.0.1:0")
	tt.OK(err)

	workerListener, err := net.Listen("tcp", "127.0.0.1:0")
	tt.OK(err)

	s3Listener, err := net.Listen("tcp", "127.0.0.1:0")
	tt.OK(err)

	autopilotListener, err := net.Listen("tcp", "127.0.0.1:0")
	tt.OK(err)

	busAddr := "http://" + busListener.Addr().String()
	workerAddr := "http://" + workerListener.Addr().String()
	s3Addr := s3Listener.Addr().String() // not fully qualified path
	autopilotAddr := "http://" + autopilotListener.Addr().String()

	// Create clients.
	autopilotClient := autopilot.NewClient(autopilotAddr, autopilotPassword)
	busClient := bus.NewClient(busAddr, busPassword)
	workerClient := worker.NewClient(workerAddr, workerPassword)
	s3Client, err := minio.New(s3Addr, &minio.Options{
		Creds:  test.S3Credentials,
		Secure: false,
	})
	tt.OK(err)

	url := s3Client.EndpointURL()
	s3Core, err := minio.NewCore(url.Host+url.Path, &minio.Options{
		Creds: test.S3Credentials,
	})
	tt.OK(err)

	// Create miner.
	busCfg.Miner = node.NewMiner(busClient)

	// Create bus.
	b, bStopFn, err := node.NewBus(busCfg, busDir, wk, logger)
	tt.OK(err)

	busAuth := jape.BasicAuth(busPassword)
	busServer := http.Server{
		Handler: busAuth(b),
	}

	var busShutdownFns []func(context.Context) error
	busShutdownFns = append(busShutdownFns, busServer.Shutdown)
	busShutdownFns = append(busShutdownFns, bStopFn)

	// Create worker.
	w, s3Handler, wShutdownFn, err := node.NewWorker(workerCfg, s3.Opts{}, busClient, wk, logger)
	tt.OK(err)

	workerAuth := jape.BasicAuth(workerPassword)
	workerServer := http.Server{
		Handler: workerAuth(w),
	}

	var workerShutdownFns []func(context.Context) error
	workerShutdownFns = append(workerShutdownFns, workerServer.Shutdown)
	workerShutdownFns = append(workerShutdownFns, wShutdownFn)

	// Create S3 API.
	s3Server := http.Server{
		Handler: s3Handler,
	}

	var s3ShutdownFns []func(context.Context) error
	s3ShutdownFns = append(s3ShutdownFns, s3Server.Shutdown)

	// Create autopilot.
	ap, aStartFn, aStopFn, err := node.NewAutopilot(apCfg, busClient, []autopilot.Worker{workerClient}, logger)
	tt.OK(err)

	autopilotAuth := jape.BasicAuth(autopilotPassword)
	autopilotServer := http.Server{
		Handler: autopilotAuth(ap),
	}

	var autopilotShutdownFns []func(context.Context) error
	autopilotShutdownFns = append(autopilotShutdownFns, autopilotServer.Shutdown)
	autopilotShutdownFns = append(autopilotShutdownFns, aStopFn)

	cluster := &TestCluster{
		apID:    apCfg.ID,
		dir:     dir,
		dbName:  dbName,
		logger:  logger,
		network: busCfg.Network,
		miner:   busCfg.Miner,
		tt:      tt,
		wk:      wk,

		Autopilot: autopilotClient,
		Bus:       busClient,
		Worker:    workerClient,
		S3:        s3Client,
		S3Core:    s3Core,

		workerShutdownFns:    workerShutdownFns,
		busShutdownFns:       busShutdownFns,
		autopilotShutdownFns: autopilotShutdownFns,
		s3ShutdownFns:        s3ShutdownFns,
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
		_ = s3Server.Serve(s3Listener)
		cluster.wg.Done()
	}()
	cluster.wg.Add(1)
	go func() {
		_ = autopilotServer.Serve(autopilotListener)
		cluster.wg.Done()
	}()
	if !opts.skipRunningAutopilot {
		cluster.wg.Add(1)
		go func() {
			_ = aStartFn()
			cluster.wg.Done()
		}()
	}

	// Set the test contract set to make sure we can add objects at the
	// beginning of a test right away.
	tt.OK(busClient.SetContractSet(ctx, test.ContractSet, []types.FileContractID{}))

	// Update the autopilot to use test settings
	if !opts.skipSettingAutopilot {
		tt.OK(busClient.UpdateAutopilot(ctx, api.Autopilot{
			ID:     apCfg.ID,
			Config: apSettings,
		}))
	}

	// Update the bus settings.
	tt.OK(busClient.UpdateSetting(ctx, api.SettingGouging, test.GougingSettings))
	tt.OK(busClient.UpdateSetting(ctx, api.SettingRedundancy, test.RedundancySettings))
	tt.OK(busClient.UpdateSetting(ctx, api.SettingContractSet, test.ContractSetSettings))
	tt.OK(busClient.UpdateSetting(ctx, api.SettingS3Authentication, api.S3AuthenticationSettings{
		V4Keypairs: map[string]string{test.S3AccessKeyID: test.S3SecretAccessKey},
	}))
	tt.OK(busClient.UpdateSetting(ctx, api.SettingUploadPacking, api.UploadPackingSettings{
		Enabled:               enableUploadPacking,
		SlabBufferMaxSizeSoft: build.DefaultUploadPackingSettings.SlabBufferMaxSizeSoft,
	}))

	// Fund the bus.
	if funding {
		cluster.MineBlocks(latestHardforkHeight)
		tt.Retry(1000, 100*time.Millisecond, func() error {
			resp, err := busClient.ConsensusState(ctx)
			if err != nil {
				return err
			}

			if !resp.Synced || resp.BlockHeight < latestHardforkHeight {
				return fmt.Errorf("chain not synced: %v %v", resp.Synced, resp.BlockHeight < latestHardforkHeight)
			}
			res, err := cluster.Bus.Wallet(ctx)
			if err != nil {
				return err
			}

			if res.Confirmed.IsZero() {
				tt.Fatal("wallet not funded")
			}
			return nil
		})
	}

	if nHosts > 0 {
		cluster.AddHostsBlocking(nHosts)
		cluster.WaitForContracts()
		cluster.WaitForContractSet(test.ContractSet, nHosts)
		_ = cluster.WaitForAccounts()
	}

	return cluster
}

// addStorageFolderToHosts adds a single storage folder to each host.
func addStorageFolderToHost(ctx context.Context, hosts []*Host) error {
	for _, host := range hosts {
		sectors := uint64(10)
		volumeDir := filepath.Join(host.dir, "volumes")
		if err := os.MkdirAll(volumeDir, 0777); err != nil {
			return err
		}
		if err := host.AddVolume(ctx, filepath.Join(volumeDir, "volume.dat"), sectors); err != nil {
			return err
		}
	}
	return nil
}

// announceHosts configures hosts with default settings and announces them to
// the group
func announceHosts(hosts []*Host) error {
	for _, host := range hosts {
		settings := defaultHostSettings
		settings.NetAddress = host.RHPv2Addr()
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
func (c *TestCluster) MineToRenewWindow() {
	c.tt.Helper()
	cs, err := c.Bus.ConsensusState(context.Background())
	c.tt.OK(err)

	ap, err := c.Bus.Autopilot(context.Background(), c.apID)
	c.tt.OK(err)

	renewWindowStart := ap.CurrentPeriod + ap.Config.Contracts.Period
	if cs.BlockHeight >= renewWindowStart {
		c.tt.Fatalf("already in renew window: bh: %v, currentPeriod: %v, periodLength: %v, renewWindow: %v", cs.BlockHeight, ap.CurrentPeriod, ap.Config.Contracts.Period, renewWindowStart)
	}
	c.MineBlocks(int(renewWindowStart - cs.BlockHeight))
	c.Sync()
}

// sync blocks until the cluster is synced.
func (c *TestCluster) sync(hosts []*Host) {
	c.tt.Helper()
	c.tt.Retry(100, 100*time.Millisecond, func() error {
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
	c.tt.Helper()
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
func (c *TestCluster) MineBlocks(n int) {
	c.tt.Helper()
	wallet, err := c.Bus.Wallet(context.Background())
	c.tt.OK(err)

	// If we don't have any hosts in the cluster mine all blocks right away.
	if len(c.hosts) == 0 {
		c.tt.OK(c.miner.Mine(wallet.Address, n))
		c.Sync()
	}
	// Otherwise mine blocks in batches of 3 to avoid going out of sync with
	// hosts by too many blocks.
	for mined := 0; mined < n; {
		toMine := n - mined
		if toMine > 10 {
			toMine = 10
		}
		c.tt.OK(c.miner.Mine(wallet.Address, toMine))
		c.Sync()
		mined += toMine
	}
}

func (c *TestCluster) WaitForAccounts() []api.Account {
	c.tt.Helper()

	// build hosts map
	hostsMap := make(map[types.PublicKey]struct{})
	for _, host := range c.hosts {
		hostsMap[host.PublicKey()] = struct{}{}
	}

	//  wait for accounts to be filled
	c.waitForHostAccounts(hostsMap)

	// fetch all accounts
	accounts, err := c.Bus.Accounts(context.Background())
	c.tt.OK(err)
	return accounts
}

func (c *TestCluster) WaitForContracts() []api.Contract {
	c.tt.Helper()
	// build hosts map
	hostsMap := make(map[types.PublicKey]struct{})
	for _, host := range c.hosts {
		hostsMap[host.PublicKey()] = struct{}{}
	}

	//  wait for the contracts to form
	c.waitForHostContracts(hostsMap)

	// fetch all contracts
	resp, err := c.Worker.Contracts(context.Background(), time.Minute)
	c.tt.OK(err)
	if resp.Error != "" {
		c.tt.Fatal(resp.Error)
	}
	return resp.Contracts
}

func (c *TestCluster) WaitForContractSetContracts(set string, n int) {
	c.tt.Helper()

	// limit n to number of hosts we have
	if n > len(c.hosts) {
		n = len(c.hosts)
	}

	c.tt.Retry(300, 100*time.Millisecond, func() error {
		sets, err := c.Bus.ContractSets(context.Background())
		if err != nil {
			return err
		}

		// check if set exists
		if len(sets) == 0 {
			return errors.New("no contract sets found")
		} else {
			var found bool
			for _, s := range sets {
				if s == set {
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("set '%v' not found, sets: %v", set, sets)
			}
		}

		// check if it contains the desired number of contracts
		csc, err := c.Bus.Contracts(context.Background(), api.ContractsOpts{ContractSet: set})
		if err != nil {
			return err
		}
		if len(csc) != n {
			return fmt.Errorf("contract set does not contain the desired number of contracts, %v!=%v", len(csc), n)
		}
		return nil
	})
}

func (c *TestCluster) RemoveHost(host *Host) {
	c.tt.Helper()
	c.tt.OK(host.Close())

	for i, h := range c.hosts {
		if h.PublicKey().String() == host.PublicKey().String() {
			c.hosts = append(c.hosts[:i], c.hosts[i+1:]...)
			break
		}
	}
}

func (c *TestCluster) NewHost() *Host {
	c.tt.Helper()
	// Create host.
	hostDir := filepath.Join(c.dir, "hosts", fmt.Sprint(len(c.hosts)+1))
	h, err := NewHost(types.GeneratePrivateKey(), hostDir, c.network, false)
	c.tt.OK(err)

	// Connect gateways.
	c.tt.OK(c.Bus.SyncerConnect(context.Background(), h.GatewayAddr()))
	return h
}

func (c *TestCluster) AddHost(h *Host) {
	c.tt.Helper()
	// Add the host
	c.hosts = append(c.hosts, h)

	// Fund host from bus.
	fundAmt := types.Siacoins(100e3)
	var scos []types.SiacoinOutput
	for i := 0; i < 10; i++ {
		scos = append(scos, types.SiacoinOutput{
			Value:   fundAmt,
			Address: h.WalletAddress(),
		})
	}
	c.tt.OK(c.Bus.SendSiacoins(context.Background(), scos, false))

	// Mine transaction.
	c.MineBlocks(1)

	// Wait for hosts to sync up with consensus.
	hosts := []*Host{h}
	c.sync(hosts)

	// Announce hosts.
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	c.tt.OK(addStorageFolderToHost(ctx, hosts))
	c.tt.OK(announceHosts(hosts))

	// Mine a few blocks. The host should show up eventually.
	c.tt.Retry(10, time.Second, func() error {
		c.tt.Helper()

		c.MineBlocks(1)
		_, err := c.Bus.Host(context.Background(), h.PublicKey())
		if err != nil {
			return err
		}
		return nil
	})

	// Wait for host to be synced.
	c.Sync()
}

// AddHosts adds n hosts to the cluster. These hosts will be funded and announce
// themselves on the network, ready to form contracts.
func (c *TestCluster) AddHosts(n int) []*Host {
	c.tt.Helper()
	var newHosts []*Host
	for i := 0; i < n; i++ {
		h := c.NewHost()
		c.AddHost(h)
		newHosts = append(newHosts, h)
	}
	return newHosts
}

func (c *TestCluster) AddHostsBlocking(n int) []*Host {
	c.tt.Helper()

	// add hosts
	hosts := c.AddHosts(n)

	// build hosts map
	hostsmap := make(map[types.PublicKey]struct{})
	for _, host := range hosts {
		hostsmap[host.PublicKey()] = struct{}{}
	}

	// wait for contracts to form
	c.waitForHostContracts(hostsmap)
	return hosts
}

// Shutdown shuts down a TestCluster.
func (c *TestCluster) Shutdown() {
	c.tt.Helper()
	ctx := context.Background()
	c.ShutdownAutopilot(ctx)
	c.ShutdownS3(ctx)
	c.ShutdownWorker(ctx)
	c.ShutdownBus(ctx)
	for _, h := range c.hosts {
		c.tt.OK(h.Close())
	}
	c.wg.Wait()
}

// Sync blocks until the whole cluster has reached the same block height.
func (c *TestCluster) Sync() {
	c.tt.Helper()
	c.sync(c.hosts)
}

// waitForHostAccounts will fetch the accounts from the worker and wait until
// they have money in them
func (c *TestCluster) waitForHostAccounts(hosts map[types.PublicKey]struct{}) {
	c.tt.Helper()
	c.tt.Retry(300, 100*time.Millisecond, func() error {
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

func (c *TestCluster) WaitForContractSet(set string, n int) {
	c.tt.Helper()
	c.tt.Retry(300, 100*time.Millisecond, func() error {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		contracts, err := c.Bus.Contracts(ctx, api.ContractsOpts{ContractSet: set})
		if err != nil {
			return err
		}
		if len(contracts) != n {
			return fmt.Errorf("contract set not ready yet, %v!=%v", len(contracts), n)
		}
		return nil
	})
}

// waitForHostContracts will fetch the contracts from the bus and wait until we
// have a contract with every host in the given hosts map
func (c *TestCluster) waitForHostContracts(hosts map[types.PublicKey]struct{}) {
	c.tt.Helper()
	c.tt.Retry(300, 100*time.Millisecond, func() error {
		contracts, err := c.Bus.Contracts(context.Background(), api.ContractsOpts{})
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
	n.HardforkFoundation.PrimaryAddress = types.StandardUnlockHash(types.GeneratePrivateKey().PublicKey())
	n.HardforkFoundation.FailsafeAddress = types.StandardUnlockHash(types.GeneratePrivateKey().PublicKey())

	// make it difficult to reach v2 in most tests
	n.HardforkV2.AllowHeight = 1000
	n.HardforkV2.RequireHeight = 1020

	return n
}

func testBusCfg() node.BusConfig {
	return node.BusConfig{
		Bus: config.Bus{
			AnnouncementMaxAgeHours:       24 * 7 * 52, // 1 year
			Bootstrap:                     false,
			GatewayAddr:                   "127.0.0.1:0",
			PersistInterval:               testBusPersistInterval,
			UsedUTXOExpiry:                time.Minute,
			SlabBufferCompletionThreshold: 0,
		},
		Network:             testNetwork(),
		SlabPruningInterval: time.Second,
	}
}

func testWorkerCfg() config.Worker {
	return config.Worker{
		AllowPrivateIPs:          true,
		ContractLockTimeout:      5 * time.Second,
		ID:                       "worker",
		BusFlushInterval:         testBusFlushInterval,
		DownloadOverdriveTimeout: 500 * time.Millisecond,
		UploadOverdriveTimeout:   500 * time.Millisecond,
		DownloadMaxMemory:        1 << 28, // 256 MiB
		UploadMaxMemory:          1 << 28, // 256 MiB
		UploadMaxOverdrive:       5,
	}
}

func testApCfg() node.AutopilotConfig {
	return node.AutopilotConfig{
		ID: api.DefaultAutopilotID,
		Autopilot: config.Autopilot{
			AccountsRefillInterval:         time.Second,
			Heartbeat:                      time.Second,
			MigrationHealthCutoff:          0.99,
			MigratorParallelSlabsPerWorker: 1,
			RevisionSubmissionBuffer:       0,
			ScannerInterval:                time.Second,
			ScannerBatchSize:               10,
			ScannerNumThreads:              1,
		},
	}
}
