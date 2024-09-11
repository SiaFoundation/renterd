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

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	s3aws "github.com/aws/aws-sdk-go/service/s3"
	"github.com/minio/minio-go/v7"
	"go.sia.tech/core/consensus"
	"go.sia.tech/core/gateway"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/syncer"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/jape"
	"go.sia.tech/renterd/alerts"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/autopilot"
	"go.sia.tech/renterd/bus"
	"go.sia.tech/renterd/config"
	"go.sia.tech/renterd/internal/test"
	"go.sia.tech/renterd/internal/utils"
	"go.sia.tech/renterd/stores"
	"go.sia.tech/renterd/stores/sql"
	"go.sia.tech/renterd/stores/sql/mysql"
	"go.sia.tech/renterd/stores/sql/sqlite"
	"go.sia.tech/renterd/webhooks"
	"go.sia.tech/renterd/worker/s3"
	"go.sia.tech/web/renterd"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/crypto/blake2b"
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
	hosts []*Host

	Autopilot *autopilot.Client
	Bus       *bus.Client
	Worker    *worker.Client
	S3Aws     *s3aws.S3
	S3        *minio.Client
	S3Core    *minio.Core

	workerShutdownFns    []func(context.Context) error
	busShutdownFns       []func(context.Context) error
	autopilotShutdownFns []func(context.Context) error
	s3ShutdownFns        []func(context.Context) error

	network      *consensus.Network
	genesisBlock types.Block
	bs           bus.Store
	cm           *chain.Manager
	apID         string
	dbName       string
	dir          string
	logger       *zap.Logger
	tt           test.TT
	wk           types.PrivateKey
	wg           sync.WaitGroup
}

type dbConfig struct {
	Database         config.Database
	DatabaseLog      config.DatabaseLog
	RetryTxIntervals []time.Duration
}

func (tc *TestCluster) Accounts() []api.Account {
	tc.tt.Helper()
	accounts, err := tc.Worker.Accounts(context.Background())
	tc.tt.OK(err)
	return accounts
}

func (tc *TestCluster) ContractRoots(ctx context.Context, fcid types.FileContractID) ([]types.Hash256, error) {
	tc.tt.Helper()

	c, err := tc.Bus.Contract(ctx, fcid)
	if err != nil {
		return nil, err
	}

	var h *Host
	for _, host := range tc.hosts {
		if host.PublicKey() == c.HostKey {
			h = host
			break
		}
	}
	if h == nil {
		return nil, fmt.Errorf("no host found for contract %v", c)
	}

	roots, err := h.store.SectorRoots()
	if err != nil {
		return nil, err
	}
	return roots[c.ID], nil
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

	autopilotCfg      *config.Autopilot
	autopilotSettings *api.AutopilotConfig
	busCfg            *config.Bus
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
	busCfg, workerCfg, apCfg, dbCfg := testBusCfg(), testWorkerCfg(), testApCfg(), testDBCfg()
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
	if opts.dbName != "" {
		dbCfg.Database.MySQL.Database = opts.dbName
	}

	// Check if we are testing against an external database. If so, we create a
	// database with a random name first.
	if mysqlCfg := config.MySQLConfigFromEnv(); mysqlCfg.URI != "" {
		// generate a random database name if none are set
		if dbCfg.Database.MySQL.Database == "" {
			dbCfg.Database.MySQL.Database = "db" + hex.EncodeToString(frand.Bytes(16))
		}
		if dbCfg.Database.MySQL.MetricsDatabase == "" {
			dbCfg.Database.MySQL.MetricsDatabase = "db" + hex.EncodeToString(frand.Bytes(16))
		}

		tmpDB, err := mysql.Open(mysqlCfg.User, mysqlCfg.Password, mysqlCfg.URI, "")
		tt.OK(err)
		tt.OKAll(tmpDB.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s;", dbCfg.Database.MySQL.Database)))
		tt.OKAll(tmpDB.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s;", dbCfg.Database.MySQL.MetricsDatabase)))
		tt.OK(tmpDB.Close())
	}

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

	mySession := session.Must(session.NewSession())
	s3AWSClient := s3aws.New(mySession, aws.NewConfig().
		WithEndpoint(s3Client.EndpointURL().String()).
		WithRegion("dummy").
		WithS3ForcePathStyle(true).
		WithCredentials(credentials.NewCredentials(&credentials.StaticProvider{
			Value: credentials.Value{
				AccessKeyID:     test.S3AccessKeyID,
				SecretAccessKey: test.S3SecretAccessKey,
			},
		})))

	// Create bus.
	busDir := filepath.Join(dir, "bus")
	b, bShutdownFn, cm, bs, err := newTestBus(ctx, busDir, busCfg, dbCfg, wk, logger)
	tt.OK(err)

	busAuth := jape.BasicAuth(busPassword)
	busServer := &http.Server{
		Handler: utils.TreeMux{
			Handler: renterd.Handler(), // ui
			Sub: map[string]utils.TreeMux{
				"/bus": {
					Handler: busAuth(b.Handler()),
				},
			},
		},
	}

	var busShutdownFns []func(context.Context) error
	busShutdownFns = append(busShutdownFns, busServer.Shutdown)
	busShutdownFns = append(busShutdownFns, bShutdownFn)

	// Create worker.
	workerKey := blake2b.Sum256(append([]byte("worker"), wk...))
	w, err := worker.New(workerCfg, workerKey, busClient, logger)
	tt.OK(err)

	workerServer := http.Server{Handler: utils.Auth(workerPassword, false)(w.Handler())}
	var workerShutdownFns []func(context.Context) error
	workerShutdownFns = append(workerShutdownFns, workerServer.Shutdown)
	workerShutdownFns = append(workerShutdownFns, w.Shutdown)

	// Create S3 API.
	s3Handler, err := s3.New(busClient, w, logger, s3.Opts{})
	tt.OK(err)

	s3Server := http.Server{Handler: s3Handler}
	var s3ShutdownFns []func(context.Context) error
	s3ShutdownFns = append(s3ShutdownFns, s3Server.Shutdown)

	// Create autopilot.
	ap, err := autopilot.New(apCfg, busClient, []autopilot.Worker{workerClient}, logger)
	tt.OK(err)

	autopilotAuth := jape.BasicAuth(autopilotPassword)
	autopilotServer := http.Server{
		Handler: autopilotAuth(ap.Handler()),
	}

	var autopilotShutdownFns []func(context.Context) error
	autopilotShutdownFns = append(autopilotShutdownFns, autopilotServer.Shutdown)
	autopilotShutdownFns = append(autopilotShutdownFns, ap.Shutdown)

	network, genesis := testNetwork()
	cluster := &TestCluster{
		apID:         apCfg.ID,
		dir:          dir,
		dbName:       dbCfg.Database.MySQL.Database,
		logger:       logger,
		network:      network,
		genesisBlock: genesis,
		bs:           bs,
		cm:           cm,
		tt:           tt,
		wk:           wk,

		Autopilot: autopilotClient,
		Bus:       busClient,
		Worker:    workerClient,
		S3:        s3Client,
		S3Aws:     s3AWSClient,
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
			ap.Run()
			cluster.wg.Done()
		}()
	}

	// Finish worker setup.
	if err := w.Setup(ctx, workerAddr, workerPassword); err != nil {
		tt.Fatalf("failed to setup worker, err: %v", err)
	}

	// Set the test contract set to make sure we can add objects at the
	// beginning of a test right away.
	tt.OK(busClient.UpdateContractSet(ctx, test.ContractSet, nil, nil))

	// Update the autopilot to use test settings
	if !opts.skipSettingAutopilot {
		tt.OK(busClient.UpdateAutopilot(ctx, api.Autopilot{
			ID:     apCfg.ID,
			Config: apSettings,
		}))
	}

	// Build upload settings.
	us := test.UploadSettings
	us.Packing = api.UploadPackingSettings{
		Enabled:               enableUploadPacking,
		SlabBufferMaxSizeSoft: 1 << 32, // 4 GiB,
	}

	// Build S3 settings.
	s3 := api.S3Settings{
		Authentication: api.S3AuthenticationSettings{
			V4Keypairs: map[string]string{test.S3AccessKeyID: test.S3SecretAccessKey},
		},
	}

	// Update the bus settings.
	tt.OK(busClient.UpdateGougingSettings(ctx, test.GougingSettings))
	tt.OK(busClient.UpdatePinnedSettings(ctx, test.PricePinSettings))
	tt.OK(busClient.UpdateUploadSettings(ctx, us))
	tt.OK(busClient.UpdateS3Settings(ctx, s3))

	// Fund the bus.
	if funding {
		cluster.MineBlocks(network.HardforkFoundation.Height + blocksPerDay) // mine until the first block reward matures
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

func newTestBus(ctx context.Context, dir string, cfg config.Bus, cfgDb dbConfig, pk types.PrivateKey, logger *zap.Logger) (*bus.Bus, func(ctx context.Context) error, *chain.Manager, bus.Store, error) {
	// create store config
	alertsMgr := alerts.NewManager()
	storeCfg, err := buildStoreConfig(alertsMgr, dir, cfg.SlabBufferCompletionThreshold, cfgDb, pk, logger)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	// create store
	sqlStore, err := stores.NewSQLStore(storeCfg)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	// create webhooks manager
	wh, err := webhooks.NewManager(sqlStore, logger)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	// hookup webhooks <-> alerts
	alertsMgr.RegisterWebhookBroadcaster(wh)

	// create consensus directory
	consensusDir := filepath.Join(dir, "consensus")
	if err := os.MkdirAll(consensusDir, 0700); err != nil {
		return nil, nil, nil, nil, err
	}

	// create chain database
	chainPath := filepath.Join(consensusDir, "blockchain.db")
	bdb, err := coreutils.OpenBoltChainDB(chainPath)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	// create chain manager
	network, genesis := testNetwork()
	store, state, err := chain.NewDBStore(bdb, network, genesis)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	cm := chain.NewManager(store, state)

	// create wallet
	w, err := wallet.NewSingleAddressWallet(pk, cm, sqlStore, wallet.WithReservationDuration(cfg.UsedUTXOExpiry))
	if err != nil {
		return nil, nil, nil, nil, err
	}

	// create syncer, peers will reject us if our hostname is empty or
	// unspecified, so use loopback
	l, err := net.Listen("tcp", cfg.GatewayAddr)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	syncerAddr := l.Addr().String()
	host, port, _ := net.SplitHostPort(syncerAddr)
	if ip := net.ParseIP(host); ip == nil || ip.IsUnspecified() {
		syncerAddr = net.JoinHostPort("127.0.0.1", port)
	}

	// create header
	header := gateway.Header{
		GenesisID:  genesis.ID(),
		UniqueID:   gateway.GenerateUniqueID(),
		NetAddress: syncerAddr,
	}

	// create the syncer
	s := syncer.New(l, cm, sqlStore, header, syncer.WithLogger(logger.Named("syncer")), syncer.WithSendBlocksTimeout(time.Minute))

	// start syncer
	errChan := make(chan error, 1)
	go func() {
		errChan <- s.Run(context.Background())
		close(errChan)
	}()

	// create a helper function to wait for syncer to wind down on shutdown
	syncerShutdown := func(ctx context.Context) error {
		select {
		case err := <-errChan:
			return err
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}

	// create master key - we currently derive the same key used by the workers
	// to ensure contracts formed by the bus can be renewed by the autopilot
	masterKey := blake2b.Sum256(append([]byte("worker"), pk...))

	// create bus
	announcementMaxAgeHours := time.Duration(cfg.AnnouncementMaxAgeHours) * time.Hour
	b, err := bus.New(ctx, masterKey, alertsMgr, wh, cm, s, w, sqlStore, announcementMaxAgeHours, "", network, logger)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	shutdownFn := func(ctx context.Context) error {
		return errors.Join(
			s.Close(),
			w.Close(),
			b.Shutdown(ctx),
			sqlStore.Close(),
			bdb.Close(),
			syncerShutdown(ctx),
		)
	}
	return b, shutdownFn, cm, sqlStore, nil
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
	accounts, err := c.Worker.Accounts(context.Background())
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
	if len(resp.Errors) > 0 {
		c.tt.Fatal(resp.Errors)
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
	// Create host.
	hostDir := filepath.Join(c.dir, "hosts", fmt.Sprint(len(c.hosts)+1))
	h, err := NewHost(types.GeneratePrivateKey(), hostDir, c.network, c.genesisBlock)
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
	fundAmt := types.Siacoins(5e3)
	for i := 0; i < 5; i++ {
		c.tt.OKAll(c.Bus.SendSiacoins(context.Background(), h.WalletAddress(), fundAmt, true))
	}

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

	// Mine until the host shows up.
	c.tt.Retry(100, 100*time.Millisecond, func() error {
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
		accounts, err := c.Worker.Accounts(context.Background())
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

func testBusCfg() config.Bus {
	return config.Bus{
		AnnouncementMaxAgeHours:       24 * 7 * 52, // 1 year
		Bootstrap:                     false,
		GatewayAddr:                   "127.0.0.1:0",
		UsedUTXOExpiry:                time.Minute,
		SlabBufferCompletionThreshold: 0,
	}
}

func testDBCfg() dbConfig {
	return dbConfig{
		Database: config.Database{
			MySQL: config.MySQLConfigFromEnv(),
		},
		DatabaseLog: config.DatabaseLog{
			Enabled:                   true,
			IgnoreRecordNotFoundError: true,
			SlowThreshold:             100 * time.Millisecond,
		},
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
		AccountsRefillInterval:   time.Second,
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

func testApCfg() config.Autopilot {
	return config.Autopilot{
		Heartbeat:                      time.Second,
		ID:                             api.DefaultAutopilotID,
		MigrationHealthCutoff:          0.99,
		MigratorParallelSlabsPerWorker: 1,
		RevisionSubmissionBuffer:       0,
		ScannerInterval:                time.Second,
		ScannerBatchSize:               10,
		ScannerNumThreads:              1,
	}
}

func buildStoreConfig(am alerts.Alerter, dir string, slabBufferCompletionThreshold int64, cfg dbConfig, pk types.PrivateKey, logger *zap.Logger) (stores.Config, error) {
	// create database connections
	var dbMain sql.Database
	var dbMetrics sql.MetricsDatabase
	if cfg.Database.MySQL.URI != "" {
		// create MySQL connections
		connMain, err := mysql.Open(
			cfg.Database.MySQL.User,
			cfg.Database.MySQL.Password,
			cfg.Database.MySQL.URI,
			cfg.Database.MySQL.Database,
		)
		if err != nil {
			return stores.Config{}, fmt.Errorf("failed to open MySQL main database: %w", err)
		}
		connMetrics, err := mysql.Open(
			cfg.Database.MySQL.User,
			cfg.Database.MySQL.Password,
			cfg.Database.MySQL.URI,
			cfg.Database.MySQL.MetricsDatabase,
		)
		if err != nil {
			return stores.Config{}, fmt.Errorf("failed to open MySQL metrics database: %w", err)
		}
		dbMain, err = mysql.NewMainDatabase(connMain, logger, cfg.DatabaseLog.SlowThreshold, cfg.DatabaseLog.SlowThreshold)
		if err != nil {
			return stores.Config{}, fmt.Errorf("failed to create MySQL main database: %w", err)
		}
		dbMetrics, err = mysql.NewMetricsDatabase(connMetrics, logger, cfg.DatabaseLog.SlowThreshold, cfg.DatabaseLog.SlowThreshold)
		if err != nil {
			return stores.Config{}, fmt.Errorf("failed to create MySQL metrics database: %w", err)
		}
	} else {
		// create database directory
		dbDir := filepath.Join(dir, "db")
		if err := os.MkdirAll(dbDir, 0700); err != nil {
			return stores.Config{}, err
		}

		// create SQLite connections
		db, err := sqlite.Open(filepath.Join(dbDir, "db.sqlite"))
		if err != nil {
			return stores.Config{}, fmt.Errorf("failed to open SQLite main database: %w", err)
		}
		dbMain, err = sqlite.NewMainDatabase(db, logger, cfg.DatabaseLog.SlowThreshold, cfg.DatabaseLog.SlowThreshold)
		if err != nil {
			return stores.Config{}, fmt.Errorf("failed to create SQLite main database: %w", err)
		}

		dbm, err := sqlite.Open(filepath.Join(dbDir, "metrics.sqlite"))
		if err != nil {
			return stores.Config{}, fmt.Errorf("failed to open SQLite metrics database: %w", err)
		}
		dbMetrics, err = sqlite.NewMetricsDatabase(dbm, logger, cfg.DatabaseLog.SlowThreshold, cfg.DatabaseLog.SlowThreshold)
		if err != nil {
			return stores.Config{}, fmt.Errorf("failed to create SQLite metrics database: %w", err)
		}
	}

	return stores.Config{
		Alerts:                        alerts.WithOrigin(am, "bus"),
		DB:                            dbMain,
		DBMetrics:                     dbMetrics,
		PartialSlabDir:                filepath.Join(dir, "partial_slabs"),
		Migrate:                       true,
		SlabBufferCompletionThreshold: slabBufferCompletionThreshold,
		Logger:                        logger,
		WalletAddress:                 types.StandardUnlockHash(pk.PublicKey()),

		RetryTransactionIntervals: cfg.RetryTxIntervals,
		LongQueryDuration:         cfg.DatabaseLog.SlowThreshold,
		LongTxDuration:            cfg.DatabaseLog.SlowThreshold,
	}, nil
}
