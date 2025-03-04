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
	"go.sia.tech/core/consensus"
	"go.sia.tech/core/gateway"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/syncer"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/renterd/alerts"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/autopilot"
	"go.sia.tech/renterd/autopilot/contractor"
	"go.sia.tech/renterd/autopilot/migrator"
	"go.sia.tech/renterd/autopilot/pruner"
	"go.sia.tech/renterd/autopilot/scanner"
	"go.sia.tech/renterd/autopilot/walletmaintainer"
	"go.sia.tech/renterd/bus"
	"go.sia.tech/renterd/bus/client"
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
	testBucket           = "testbucket"
	testBusFlushInterval = 100 * time.Millisecond
)

var (
	clusterOptsDefault  = testClusterOptions{}
	clusterOptNoFunding = false
)

type Autopilot interface {
	Handler() http.Handler
	Run()
	Shutdown(context.Context) error
}

// TestCluster is a helper type that allows for easily creating a number of
// nodes connected to each other and ready for testing.
type TestCluster struct {
	hosts []*Host

	Autopilot *autopilot.Client
	Bus       *bus.Client
	Worker    *worker.Client
	S3        *s3TestClient

	workerShutdownFns    []func(context.Context) error
	busShutdownFns       []func(context.Context) error
	autopilotShutdownFns []func(context.Context) error
	s3ShutdownFns        []func(context.Context) error
	listenerShutdownFns  []func() error

	network      *consensus.Network
	genesisBlock types.Block
	bs           bus.Store
	cm           *chain.Manager
	dbName       string
	dir          string
	logger       *zap.Logger
	tt           test.TT
	wk           types.PrivateKey
	wg           sync.WaitGroup
}

type dbConfig struct {
	Database    config.Database
	DatabaseLog config.DatabaseLog
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
	var roots []types.Hash256
	state, unlock, err := h.contractsV2.LockV2Contract(fcid)
	if err == nil {
		roots = append(roots, state.Roots...)
		defer unlock()
	}
	return append(roots, h.contracts.SectorRoots(fcid)...), nil
}

func (tc *TestCluster) IsPassedV2AllowHeight() bool {
	return tc.cm.Tip().Height >= tc.network.HardforkV2.AllowHeight
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

type testClusterOptions struct {
	dbName               string
	dir                  string
	funding              *bool
	hosts                int
	logger               *zap.Logger
	uploadPacking        bool
	skipRunningAutopilot bool
	skipSettingAutopilot bool
	skipUpdatingSettings bool
	walletKey            *types.PrivateKey

	autopilotCfg    *config.Autopilot
	autopilotConfig *api.AutopilotConfig
	cm              *chain.Manager
	busCfg          *config.Bus
	workerCfg       *config.Worker
}

// newTestLogger creates a console logger used for testing.
func newTestLogger(enable bool) *zap.Logger {
	if !enable {
		return zap.NewNop()
	}
	config := zap.NewProductionEncoderConfig()
	config.EncodeTime = zapcore.RFC3339TimeEncoder
	config.EncodeLevel = zapcore.CapitalColorLevelEncoder
	config.StacktraceKey = ""
	consoleEncoder := zapcore.NewConsoleEncoder(config)

	return zap.New(
		zapcore.NewCore(consoleEncoder, zapcore.AddSync(os.Stdout), zap.DebugLevel),
		zap.AddCaller(),
		zap.AddStacktrace(zap.DebugLevel),
	)
}

// newTestCluster creates a new cluster without hosts with a funded bus.
func newTestCluster(t *testing.T, opts testClusterOptions) *TestCluster {
	t.Helper()

	// Skip any test that requires a cluster when running short tests.
	tt := test.NewTT(t)

	// Ensure we don't hang
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	// Apply options.
	dir := t.TempDir()
	if opts.dir != "" {
		dir = opts.dir
	}
	logger := newTestLogger(false) // enable logging here if needed
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
	apConfig := test.AutopilotConfig
	if opts.autopilotConfig != nil {
		apConfig = *opts.autopilotConfig
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
	s3Addr := "http://" + s3Listener.Addr().String() // not fully qualified path
	autopilotAddr := "http://" + autopilotListener.Addr().String()

	// Create clients.
	autopilotClient := autopilot.NewClient(autopilotAddr, autopilotPassword)
	busClient := bus.NewClient(busAddr, busPassword)
	workerClient := worker.NewClient(workerAddr, workerPassword)

	mySession := session.Must(session.NewSession())
	s3AWSClient := s3aws.New(mySession, aws.NewConfig().
		WithEndpoint(s3Addr).
		WithRegion("dummy").
		WithS3ForcePathStyle(true).
		WithCredentials(credentials.NewCredentials(&credentials.StaticProvider{
			Value: credentials.Value{
				AccessKeyID:     test.S3AccessKeyID,
				SecretAccessKey: test.S3SecretAccessKey,
			},
		})))

	cm := opts.cm
	if cm == nil {
		// create chain manager
		network, genesis := testNetwork()
		store, state, err := chain.NewDBStore(chain.NewMemDB(), network, genesis)
		tt.OK(err)
		cm = chain.NewManager(store, state)
	}
	network := cm.TipState().Network
	genesis := types.Block{Timestamp: network.HardforkOak.GenesisTimestamp}

	// Create bus.
	busDir := filepath.Join(dir, "bus")
	b, bShutdownFn, cm, bs, err := newTestBus(ctx, cm, genesis, busDir, busCfg, dbCfg, wk, logger)
	tt.OK(err)

	tokens := api.NewTokenStore()
	busAuth := api.Auth(tokens, busPassword)
	busServer := &http.Server{
		Handler: api.TreeMux{
			Handler: renterd.Handler(), // ui
			Sub: map[string]api.TreeMux{
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

	workerServer := http.Server{Handler: api.WorkerAuth(tokens, workerPassword, false)(w.Handler())}
	var workerShutdownFns []func(context.Context) error
	workerShutdownFns = append(workerShutdownFns, workerServer.Shutdown)
	workerShutdownFns = append(workerShutdownFns, w.Shutdown)

	// Create S3 API.
	s3Handler, err := s3.New(busClient, w, logger, s3.Opts{})
	tt.OK(err)

	s3Server := http.Server{Handler: s3Handler}
	var s3ShutdownFns []func(context.Context) error
	s3ShutdownFns = append(s3ShutdownFns, s3Server.Shutdown)

	ap, err := newTestAutopilot(workerKey, apCfg, busClient, logger)
	tt.OK(err)

	autopilotAuth := api.Auth(tokens, autopilotPassword)
	autopilotServer := http.Server{
		Handler: autopilotAuth(ap.Handler()),
	}

	var autopilotShutdownFns []func(context.Context) error
	autopilotShutdownFns = append(autopilotShutdownFns, autopilotServer.Shutdown)
	autopilotShutdownFns = append(autopilotShutdownFns, ap.Shutdown)

	cluster := &TestCluster{
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
		S3:        &s3TestClient{s3AWSClient},

		workerShutdownFns:    workerShutdownFns,
		busShutdownFns:       busShutdownFns,
		autopilotShutdownFns: autopilotShutdownFns,
		s3ShutdownFns:        s3ShutdownFns,
		listenerShutdownFns: []func() error{
			autopilotListener.Close,
			s3Listener.Close,
			workerListener.Close,
			busListener.Close,
		},
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

	// Update the autopilot to use test settings
	if !opts.skipSettingAutopilot {
		tt.OKAll(
			busClient.UpdateAutopilotConfig(ctx,
				client.WithContractsConfig(apConfig.Contracts),
				client.WithHostsConfig(apConfig.Hosts),
				client.WithAutopilotEnabled(true),
			),
		)
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
	if !opts.skipUpdatingSettings {
		tt.OK(busClient.UpdateGougingSettings(ctx, test.GougingSettings))
		tt.OK(busClient.UpdateUploadSettings(ctx, us))
		tt.OK(busClient.UpdateS3Settings(ctx, s3))
	}

	// Fund the bus.
	if funding {
		cluster.MineBlocks(network.HardforkFoundation.Height + network.MaturityDelay + 10) // mine until the first block reward matures plus a few more mature
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

	// Add test bucket
	err = cluster.Bus.CreateBucket(ctx, testBucket, api.CreateBucketOptions{})
	if err != nil && !utils.IsErr(err, api.ErrBucketExists) {
		tt.Fatalf("failed to create bucket: %v", err)
	}

	if nHosts > 0 {
		cluster.AddHostsBlocking(nHosts)
		cluster.WaitForPeers()
		cluster.WaitForContracts()
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

func newTestAutopilot(masterKey utils.MasterKey, cfg config.Autopilot, bus *bus.Client, l *zap.Logger) (Autopilot, error) {
	a := alerts.WithOrigin(bus, "autopilot")
	l = l.Named("autopilot")

	ctx, cancel := context.WithCancelCause(context.Background())
	m, err := migrator.New(ctx, masterKey, a, bus, bus, cfg.MigratorHealthCutoff, cfg.MigratorNumThreads, cfg.MigratorDownloadMaxOverdrive, cfg.MigratorUploadMaxOverdrive, cfg.MigratorDownloadOverdriveTimeout, cfg.MigratorUploadOverdriveTimeout, cfg.MigratorAccountsRefillInterval, l)
	if err != nil {
		cancel(nil)
		return nil, err
	}

	s, err := scanner.New(bus, cfg.ScannerBatchSize, cfg.ScannerNumThreads, cfg.ScannerInterval, l)
	if err != nil {
		cancel(nil)
		return nil, err
	}

	c := contractor.New(bus, bus, bus, bus, bus, cfg.RevisionSubmissionBuffer, cfg.RevisionBroadcastInterval, cfg.AllowRedundantHostIPs, l)
	p := pruner.New(bus, l)
	w := walletmaintainer.New(a, bus, 5, 5, l)

	return autopilot.New(ctx, cancel, bus, c, m, p, s, w, cfg.Heartbeat, l), nil
}

func newTestBus(ctx context.Context, cm *chain.Manager, genesisBlock types.Block, dir string, cfg config.Bus, cfgDb dbConfig, pk types.PrivateKey, logger *zap.Logger) (*bus.Bus, func(ctx context.Context) error, *chain.Manager, bus.Store, error) {
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
		GenesisID:  genesisBlock.ID(),
		UniqueID:   gateway.GenerateUniqueID(),
		NetAddress: syncerAddr,
	}

	// create the syncer
	s := syncer.New(l, cm, sqlStore, header, syncer.WithLogger(logger.Named("syncer")),
		syncer.WithSendBlocksTimeout(2*time.Second),
		syncer.WithRPCTimeout(2*time.Second),
	)

	// start syncer
	errChan := make(chan error, 1)
	go func() {
		errChan <- s.Run()
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
	b, err := bus.New(ctx, cfg, masterKey, alertsMgr, wh, cm, s, w, sqlStore, "", logger)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	shutdownFn := func(ctx context.Context) error {
		return errors.Join(
			s.Close(),
			w.Close(),
			b.Shutdown(ctx),
			sqlStore.Close(),
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
		settings := host.settings.Settings()
		settings.NetAddress = host.rhp4Listener.Addr().(*net.TCPAddr).IP.String()
		if err := host.UpdateSettings(settings); err != nil {
			return err
		}
		if err := host.settings.Announce(); err != nil {
			return err
		}
	}
	return nil
}

// MineToRenewWindow is a helper which mines to the height where all existing
// contracts entered their respective renew window.
func (c *TestCluster) MineToRenewWindow() {
	c.tt.Helper()

	// fetch autopilot config
	cfg, err := c.Bus.AutopilotConfig(context.Background())
	c.tt.OK(err)

	// fetch consensus state
	cs, err := c.Bus.ConsensusState(context.Background())
	c.tt.OK(err)

	// fetch all contracts
	contracts, err := c.Bus.Contracts(context.Background(), api.ContractsOpts{FilterMode: api.ContractFilterModeActive})
	c.tt.OK(err)

	// find max window start
	var maxEndHeight uint64
	for _, contract := range contracts {
		if contract.EndHeight() > maxEndHeight {
			maxEndHeight = contract.EndHeight()
		}
	}
	if maxEndHeight < cfg.Contracts.RenewWindow {
		c.tt.Fatal("contract was already in renew window")
	} else if targetBH := maxEndHeight - cfg.Contracts.RenewWindow; cs.BlockHeight >= targetBH {
		return // already in renew window
	} else {
		c.MineBlocks(targetBH - cs.BlockHeight)
	}
}

// MineBlocks mines n blocks
func (c *TestCluster) MineBlocks(n uint64) {
	c.tt.Helper()
	wallet, err := c.Bus.Wallet(context.Background())
	c.tt.OK(err)

	c.tt.OK(c.mineBlocks(wallet.Address, n))
	c.sync()
}

func (c *TestCluster) sync() {
	tip := c.cm.Tip()
	c.tt.Retry(10000, time.Millisecond, func() error {
		cs, err := c.Bus.ConsensusState(context.Background())
		if err != nil {
			return err
		} else if !cs.Synced {
			return errors.New("bus is not synced")
		} else if cs.BlockHeight < tip.Height {
			return fmt.Errorf("subscriber hasn't caught up, %d < %d", cs.BlockHeight, tip.Height)
		}

		wallet, err := c.Bus.Wallet(context.Background())
		if err != nil {
			return err
		} else if wallet.ScanHeight < tip.Height {
			return fmt.Errorf("wallet hasn't caught up, %d < %d", wallet.ScanHeight, tip.Height)
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

func (c *TestCluster) WaitForContracts() []api.ContractMetadata {
	c.tt.Helper()

	// build hosts map
	hostsMap := make(map[types.PublicKey]struct{})
	for _, host := range c.hosts {
		hostsMap[host.PublicKey()] = struct{}{}
	}

	//  wait for the contracts to form
	c.waitForHostContracts(hostsMap)

	// fetch all contracts
	resp, err := c.Bus.Contracts(context.Background(), api.ContractsOpts{FilterMode: api.ContractFilterModeGood})
	c.tt.OK(err)
	return resp
}

func (c *TestCluster) WaitForPeers() {
	c.tt.Helper()
	c.tt.Retry(3000, time.Millisecond, func() error {
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
	h, err := NewHost(types.GeneratePrivateKey(), c.cm, hostDir, c.network, c.genesisBlock)
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
	c.tt.Retry(1000, time.Millisecond, func() error {
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

	// Mine a block and wait until the host shows up.
	c.MineBlocks(1)
	c.tt.Retry(1000, time.Millisecond, func() error {
		c.tt.Helper()

		_, err := c.Bus.Host(context.Background(), h.PublicKey())
		if err != nil {
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

// MineTransactions tries to mine the transactions in the transaction pool until
// it is empty.
func (c *TestCluster) MineTransactions(ctx context.Context) error {
	return test.Retry(1000, 10*time.Millisecond, func() error {
		txns, err := c.Bus.TransactionPool(ctx)
		if err != nil {
			return err
		} else if len(txns) > 0 {
			c.MineBlocks(1)
		}
		return nil
	})
}

// Shutdown shuts down a TestCluster.
func (c *TestCluster) Shutdown() {
	c.tt.Helper()
	ctx := context.Background()
	c.ShutdownAutopilot(ctx)
	c.ShutdownS3(ctx)
	c.ShutdownWorker(ctx)
	c.ShutdownBus(ctx)
	for _, fn := range c.listenerShutdownFns {
		fn()
	}
	for _, h := range c.hosts {
		c.wg.Add(1)
		go func() {
			c.tt.OK(h.Close())
			c.wg.Done()
		}()
	}
	c.wg.Wait()
}

// waitForHostAccounts will fetch the accounts from the worker and wait until
// they have money in them
func (c *TestCluster) waitForHostAccounts(hosts map[types.PublicKey]struct{}) {
	c.tt.Helper()
	c.tt.Retry(3000, time.Millisecond, func() error {
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

// waitForHostContracts will fetch the contracts from the bus and wait until we
// have a contract with every host in the given hosts map
func (c *TestCluster) waitForHostContracts(hosts map[types.PublicKey]struct{}) {
	c.tt.Helper()
	c.tt.Retry(3000, time.Millisecond, func() error {
		contracts, err := c.Bus.Contracts(context.Background(), api.ContractsOpts{FilterMode: api.ContractFilterModeGood})
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
	n.HardforkV2.AllowHeight = HardforkV2AllowHeight
	n.HardforkV2.RequireHeight = HardforkV2RequireHeight
	n.MaturityDelay = 1
	n.BlockInterval = 10 * time.Millisecond

	return n, genesis
}

func testBusCfg() config.Bus {
	return config.Bus{
		AllowPrivateIPs:               true,
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
	}
}

func testWorkerCfg() config.Worker {
	return config.Worker{
		AccountsRefillInterval:   10 * time.Millisecond,
		CacheExpiry:              100 * time.Millisecond,
		ID:                       "worker",
		BusFlushInterval:         testBusFlushInterval,
		DownloadOverdriveTimeout: 500 * time.Millisecond,
		UploadOverdriveTimeout:   500 * time.Millisecond,
		DownloadMaxMemory:        1 << 28, // 256 MiB
		UploadMaxMemory:          1 << 28, // 256 MiB
		DownloadMaxOverdrive:     5,       // TODO: added b/c I think this was overlooked but not sure
		UploadMaxOverdrive:       5,
	}
}

func testApCfg() config.Autopilot {
	return config.Autopilot{
		AllowRedundantHostIPs:    true,
		Heartbeat:                time.Second,
		RevisionSubmissionBuffer: 0,

		MigratorAccountsRefillInterval:   10 * time.Millisecond,
		MigratorHealthCutoff:             0.99,
		MigratorNumThreads:               1,
		MigratorDownloadMaxOverdrive:     5,
		MigratorDownloadOverdriveTimeout: 500 * time.Millisecond,
		MigratorUploadOverdriveTimeout:   500 * time.Millisecond,
		MigratorUploadMaxOverdrive:       5,

		ScannerInterval:   10 * time.Millisecond,
		ScannerBatchSize:  10,
		ScannerNumThreads: 1,
	}
}

func buildStoreConfig(am alerts.Alerter, dir string, slabBufferCompletionThreshold int64, cfg dbConfig, pk types.PrivateKey, logger *zap.Logger) (stores.Config, error) {
	partialSlabDir := filepath.Join(dir, "partial_slabs")

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
		dbMain, err = mysql.NewMainDatabase(connMain, logger, cfg.DatabaseLog.SlowThreshold, cfg.DatabaseLog.SlowThreshold, partialSlabDir)
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
		dbMain, err = sqlite.NewMainDatabase(db, logger, cfg.DatabaseLog.SlowThreshold, cfg.DatabaseLog.SlowThreshold, partialSlabDir)
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
		PartialSlabDir:                partialSlabDir,
		Migrate:                       true,
		SlabBufferCompletionThreshold: slabBufferCompletionThreshold,
		Logger:                        logger,
		WalletAddress:                 types.StandardUnlockHash(pk.PublicKey()),

		LongQueryDuration: cfg.DatabaseLog.SlowThreshold,
		LongTxDuration:    cfg.DatabaseLog.SlowThreshold,
	}, nil
}
