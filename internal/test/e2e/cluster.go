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
	"go.sia.tech/coreutils"
	"go.sia.tech/coreutils/syncer"
	"go.sia.tech/jape"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/autopilot"
	"go.sia.tech/renterd/bus"
	"go.sia.tech/renterd/config"
	"go.sia.tech/renterd/internal/chain"
	"go.sia.tech/renterd/internal/node"
	"go.sia.tech/renterd/internal/test"
	"go.sia.tech/renterd/internal/utils"
	iworker "go.sia.tech/renterd/internal/worker"
	"go.sia.tech/renterd/stores"
	"go.sia.tech/renterd/worker/s3"
	"go.sia.tech/web/renterd"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gorm.io/gorm"
	"lukechampine.com/frand"

	"go.sia.tech/renterd/worker"
)

const (
	testBusFlushInterval = 100 * time.Millisecond
)

var (
	clusterOptsDefault  = testClusterOptions{}
	clusterOptNoFunding = false
)

// TestCluster is a helper type that allows for easily creating a number of
// nodes connected to each other and ready for testing.
type TestCluster struct {
	dir string

	hosts []*Host

	apCfg  node.AutopilotConfig
	busCfg node.BusConfig

	Autopilot *autopilot.Client
	Bus       *bus.Client
	Worker    *worker.Client
	S3        *minio.Client
	S3Core    *minio.Core

	workerShutdownFns    []func(context.Context) error
	busShutdownFns       []func(context.Context) error
	autopilotShutdownFns []func(context.Context) error
	s3ShutdownFns        []func(context.Context) error

	cm     *chain.Manager
	cs     *chain.ChainSubscriber
	logger *zap.Logger
	tt     test.TT
	wk     types.PrivateKey
	wg     sync.WaitGroup
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
		dir:          c.dir,
		autopilotCfg: &c.apCfg,
		busCfg:       &c.busCfg,
		logger:       c.logger,
		funding:      &clusterOptNoFunding,
		walletKey:    &c.wk,
	})
	newCluster.hosts = hosts
	return newCluster
}

// AutopilotConfig returns the autopilot's config and current period.
func (c *TestCluster) AutopilotConfig(ctx context.Context) (api.AutopilotConfig, uint64) {
	c.tt.Helper()
	ap, err := c.Bus.Autopilot(ctx, c.apCfg.ID)
	c.tt.OK(err)
	return ap.Config, ap.CurrentPeriod
}

// UpdateAutopilotConfig updates the cluster's autopilot with given config.
func (c *TestCluster) UpdateAutopilotConfig(ctx context.Context, cfg api.AutopilotConfig) {
	c.tt.Helper()
	c.tt.OK(c.Bus.UpdateAutopilot(ctx, api.Autopilot{
		ID:     c.apCfg.ID,
		Config: cfg,
	}))
}

type testClusterOptions struct {
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
	return newTestLoggerCustom(zapcore.WarnLevel)
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
	// Skip any test that requires a cluster when running short tests.
	if testing.Short() {
		t.SkipNow()
	}
	tt := test.NewTT(t)

	// Ensure we don't hang
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	// Apply options.
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
	if busCfg.Logger == nil {
		busCfg.Logger = logger
	}

	// Check if we are testing against an external database. If so, we create a
	// database with a random name first.
	if mysql := config.MySQLConfigFromEnv(); mysql.URI != "" {
		// generate a random database name if none are set
		if busCfg.Database.MySQL.Database == "" {
			busCfg.Database.MySQL.Database = "db" + hex.EncodeToString(frand.Bytes(16))
		}
		if busCfg.Database.MySQL.MetricsDatabase == "" {
			busCfg.Database.MySQL.MetricsDatabase = "db" + hex.EncodeToString(frand.Bytes(16))
		}

		tmpDB, err := gorm.Open(stores.NewMySQLConnection(mysql.User, mysql.Password, mysql.URI, ""))
		tt.OK(err)
		tt.OK(tmpDB.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s;", busCfg.Database.MySQL.Database)).Error)
		tt.OK(tmpDB.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s;", busCfg.Database.MySQL.MetricsDatabase)).Error)
		tmpDBB, err := tmpDB.DB()
		tt.OK(err)
		tt.OK(tmpDBB.Close())
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

	busAddr := fmt.Sprintf("http://%s/bus", busListener.Addr().String())
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

	// Create bus.
	b, bSetupFn, bShutdownFn, cm, cs, err := node.NewBus(busCfg, busDir, wk, logger)
	tt.OK(err)

	busAuth := jape.BasicAuth(busPassword)
	busServer := &http.Server{
		Handler: utils.TreeMux{
			Handler: renterd.Handler(), // ui
			Sub: map[string]utils.TreeMux{
				"/bus": {
					Handler: busAuth(b),
				},
			},
		},
	}

	var busShutdownFns []func(context.Context) error
	busShutdownFns = append(busShutdownFns, busServer.Shutdown)
	busShutdownFns = append(busShutdownFns, bShutdownFn)

	// Create worker.
	w, s3Handler, wSetupFn, wShutdownFn, err := node.NewWorker(workerCfg, s3.Opts{}, busClient, wk, logger)
	tt.OK(err)
	workerServer := http.Server{
		Handler: iworker.Auth(workerPassword, false)(w),
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
		dir: dir,

		apCfg:  apCfg,
		busCfg: busCfg,

		logger: logger,
		cm:     cm,
		cs:     cs,
		tt:     tt,
		wk:     wk,

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

	// Finish bus setup.
	bSetupFn()

	// Finish worker setup.
	if err := wSetupFn(ctx, workerAddr, workerPassword); err != nil {
		tt.Fatalf("failed to setup worker, err: %v", err)
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
	tt.OK(busClient.UpdateSetting(ctx, api.SettingContractSet, test.ContractSetSettings))
	tt.OK(busClient.UpdateSetting(ctx, api.SettingPricePinning, test.PricePinSettings))
	tt.OK(busClient.UpdateSetting(ctx, api.SettingRedundancy, test.RedundancySettings))
	tt.OK(busClient.UpdateSetting(ctx, api.SettingS3Authentication, api.S3AuthenticationSettings{
		V4Keypairs: map[string]string{test.S3AccessKeyID: test.S3SecretAccessKey},
	}))
	tt.OK(busClient.UpdateSetting(ctx, api.SettingUploadPacking, api.UploadPackingSettings{
		Enabled:               enableUploadPacking,
		SlabBufferMaxSizeSoft: api.DefaultUploadPackingSettings.SlabBufferMaxSizeSoft,
	}))

	// Fund the bus.
	if funding {
		cluster.MineBlocks(busCfg.Network.HardforkFoundation.Height + blocksPerDay) // mine until the first block reward matures
		tt.Retry(100, 100*time.Millisecond, func() error {
			if cs, err := busClient.ConsensusState(ctx); err != nil {
				return err
			} else if !cs.Synced {
				return fmt.Errorf("chain not synced: %v", cs.Synced)
			}

			if res, err := cluster.Bus.Wallet(ctx); err != nil {
				return err
			} else if res.Confirmed.IsZero() {
				return fmt.Errorf("wallet not funded: %+v", res)
			} else {
				return nil
			}
		})
	}

	if nHosts > 0 {
		cluster.AddHostsBlocking(nHosts)
		cluster.WaitForPeers()
		cluster.WaitForContracts()
		cluster.WaitForContractSet(test.ContractSet, nHosts)
		cluster.WaitForAccounts()
	}

	// Ping the UI
	resp, err := http.DefaultClient.Get(fmt.Sprintf("http://%v", busListener.Addr()))
	tt.OK(err)
	if resp.StatusCode != http.StatusOK {
		tt.Fatalf("unexpected status code: %v", resp.StatusCode)
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

	ap, err := c.Bus.Autopilot(context.Background(), c.apCfg.ID)
	c.tt.OK(err)

	renewWindowStart := ap.CurrentPeriod + ap.Config.Contracts.Period
	if cs.BlockHeight >= renewWindowStart {
		c.tt.Fatalf("already in renew window: bh: %v, currentPeriod: %v, periodLength: %v, renewWindow: %v", cs.BlockHeight, ap.CurrentPeriod, ap.Config.Contracts.Period, renewWindowStart)
	}
	c.MineBlocks(renewWindowStart - cs.BlockHeight)
}

// MineBlocks mines n blocks
func (c *TestCluster) MineBlocks(n uint64) {
	c.tt.Helper()
	wallet, err := c.Bus.Wallet(context.Background())
	c.tt.OK(err)

	// If we don't have any hosts in the cluster mine all blocks right away.
	if len(c.hosts) == 0 {
		c.tt.OK(c.mineBlocks(wallet.Address, n))
		c.sync()
		return
	}

	// Otherwise mine blocks in batches of 10 blocks to avoid going out of sync
	// with hosts by too many blocks.
	for mined := uint64(0); mined < n; {
		toMine := n - mined
		if toMine > 10 {
			toMine = 10
		}
		c.tt.OK(c.mineBlocks(wallet.Address, toMine))
		mined += toMine
		c.sync()
	}
	c.sync()
}

func (c *TestCluster) sync() {
	tip := c.cm.Tip()
	c.tt.Retry(300, 100*time.Millisecond, func() error {
		cs, err := c.Bus.ConsensusState(context.Background())
		if err != nil {
			return err
		} else if !cs.Synced {
			return errors.New("bus is not synced")
		} else if cs.BlockHeight < tip.Height {
			return fmt.Errorf("subscriber hasn't caught up, %d < %d", cs.BlockHeight, tip.Height)
		}

		for _, h := range c.hosts {
			if hh := h.cm.Tip().Height; hh < tip.Height {
				return fmt.Errorf("host %v is not synced, %v < %v", h.PublicKey(), hh, cs.BlockHeight)
			}
		}
		return nil
	})
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

func (c *TestCluster) WaitForPeers() {
	c.tt.Helper()
	c.tt.Retry(300, 100*time.Millisecond, func() error {
		peers, err := c.Bus.SyncerPeers(context.Background())
		if err != nil {
			return err
		} else if len(peers) == 0 {
			return errors.New("no peers found")
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

	pk := types.GeneratePrivateKey()

	// Prepare syncer options
	var opts []syncer.Option
	opts = append(opts,
		syncer.WithLogger(c.logger.Named(pk.PublicKey().String()).Named("syncer")),
		syncer.WithSendBlocksTimeout(time.Minute),
	)
	if c.busCfg.SyncerPeerDiscoveryInterval > 0 {
		opts = append(opts, syncer.WithPeerDiscoveryInterval(c.busCfg.SyncerPeerDiscoveryInterval))
	}
	if c.busCfg.SyncerSyncInterval > 0 {
		opts = append(opts, syncer.WithSyncInterval(c.busCfg.SyncerSyncInterval))
	}

	// Create host.
	hostDir := filepath.Join(c.dir, "hosts", fmt.Sprint(len(c.hosts)+1))
	h, err := NewHost(pk, hostDir, c.busCfg.Network, c.busCfg.Genesis, opts...)
	c.tt.OK(err)

	// Connect gateways.
	c.tt.OK(c.Bus.SyncerConnect(context.Background(), h.SyncerAddr()))
	return h
}

func (c *TestCluster) AddHost(h *Host) {
	c.tt.Helper()
	// Add the host
	c.hosts = append(c.hosts, h)

	// Fund host from bus.
	fundAmt := types.Siacoins(25e3)
	var scos []types.SiacoinOutput
	for i := 0; i < 10; i++ {
		scos = append(scos, types.SiacoinOutput{
			Value:   fundAmt.Div64(10),
			Address: h.WalletAddress(),
		})
	}
	c.tt.OK(c.Bus.SendSiacoins(context.Background(), scos, true))

	// Mine transaction.
	c.MineBlocks(1)

	// Wait for host's wallet to be funded
	c.tt.Retry(100, 100*time.Millisecond, func() error {
		balance, err := h.wallet.Balance()
		c.tt.OK(err)
		if balance.Confirmed.IsZero() {
			return errors.New("host wallet not funded")
		}
		return nil
	})

	// Announce hosts.
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	c.tt.OK(addStorageFolderToHost(ctx, []*Host{h}))
	c.tt.OK(announceHosts([]*Host{h}))

	// Mine a few blocks. The host should show up eventually.
	c.tt.Retry(10, time.Second, func() error {
		c.tt.Helper()

		_, err := c.Bus.Host(context.Background(), h.PublicKey())
		if err != nil {
			c.MineBlocks(1)
			return err
		}
		return nil
	})
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

func (c *TestCluster) mineBlocks(addr types.Address, n uint64) error {
	for i := uint64(0); i < n; i++ {
		if block, found := coreutils.MineBlock(c.cm, addr, 5*time.Second); !found {
			c.tt.Fatal("failed to mine block")
		} else if err := c.Bus.AcceptBlock(context.Background(), block); err != nil {
			return err
		}
	}
	return nil
}

// testNetwork returns a modified version of Zen used for testing
func testNetwork() (*consensus.Network, types.Block) {
	// use a modified version of Zen
	n, genesis := chain.TestnetZen()

	// we have to set the initial target to 128 to ensure blocks we mine match
	// the PoW testnet in siad testnet consensu
	n.InitialTarget = types.BlockID{0x80}

	// we have to make minimum coinbase get hit after 10 blocks to ensure we
	// match the siad test network settings, otherwise the blocksubsidy is
	// considered invalid after 10 blocks
	n.MinimumCoinbase = types.Siacoins(299990)
	n.HardforkDevAddr.Height = 1
	n.HardforkTax.Height = 1
	n.HardforkStorageProof.Height = 1
	n.HardforkOak.Height = 1
	n.HardforkASIC.Height = 1
	n.HardforkFoundation.Height = 1
	n.HardforkV2.AllowHeight = 1000
	n.HardforkV2.RequireHeight = 1020

	return n, genesis
}

func testBusCfg() node.BusConfig {
	network, genesis := testNetwork()

	return node.BusConfig{
		Bus: config.Bus{
			AnnouncementMaxAgeHours:       24 * 7 * 52, // 1 year
			Bootstrap:                     false,
			GatewayAddr:                   "127.0.0.1:0",
			UsedUTXOExpiry:                time.Minute,
			SlabBufferCompletionThreshold: 0,
		},
		Database: config.Database{
			MySQL: config.MySQLConfigFromEnv(),
		},
		DatabaseLog: config.DatabaseLog{
			Enabled:                   true,
			IgnoreRecordNotFoundError: true,
			SlowThreshold:             100 * time.Millisecond,
		},
		Network:                     network,
		Genesis:                     genesis,
		SyncerSyncInterval:          100 * time.Millisecond,
		SyncerPeerDiscoveryInterval: 100 * time.Millisecond,
		RetryTxIntervals: []time.Duration{
			50 * time.Millisecond,
			100 * time.Millisecond,
			200 * time.Millisecond,
			500 * time.Millisecond,
			time.Second,
			5 * time.Second,
		},
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
