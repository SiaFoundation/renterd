package node

import (
	"bytes"
	"context"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"gitlab.com/NebulousLabs/encoding"
	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/autopilot"
	"go.sia.tech/renterd/bus"
	"go.sia.tech/renterd/internal/stores"
	"go.sia.tech/renterd/wallet"
	"go.sia.tech/renterd/worker"
	"go.sia.tech/siad/modules"
	mconsensus "go.sia.tech/siad/modules/consensus"
	"go.sia.tech/siad/modules/gateway"
	"go.sia.tech/siad/modules/transactionpool"
	stypes "go.sia.tech/siad/types"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/crypto/blake2b"
)

type WorkerConfig struct {
	ID                       string
	InteractionFlushInterval time.Duration
	SessionReconnectTimeout  time.Duration
	SessionTTL               time.Duration
	DownloadSectorTimeout    time.Duration
	UploadSectorTimeout      time.Duration
}

type BusConfig struct {
	Bootstrap       bool
	GatewayAddr     string
	Miner           *Miner
	PersistInterval time.Duration

	api.GougingSettings
	api.RedundancySettings
}

type AutopilotConfig struct {
	Heartbeat         time.Duration
	ScannerInterval   time.Duration
	ScannerBatchSize  uint64
	ScannerNumThreads uint64
}

func convertToSiad(core types.EncoderTo, siad encoding.SiaUnmarshaler) {
	var buf bytes.Buffer
	e := types.NewEncoder(&buf)
	core.EncodeTo(e)
	e.Flush()
	if err := siad.UnmarshalSia(&buf); err != nil {
		panic(err)
	}
}

func convertToCore(siad encoding.SiaMarshaler, core types.DecoderFrom) {
	var buf bytes.Buffer
	siad.MarshalSia(&buf)
	d := types.NewBufDecoder(buf.Bytes())
	core.DecodeFrom(d)
	if d.Err() != nil {
		panic(d.Err())
	}
}

type chainManager struct {
	cs modules.ConsensusSet
}

func (cm chainManager) AcceptBlock(ctx context.Context, b types.Block) error {
	var sb stypes.Block
	convertToSiad(b, &sb)
	return cm.cs.AcceptBlock(sb)
}

func (cm chainManager) Synced(ctx context.Context) bool {
	return cm.cs.Synced()
}

func (cm chainManager) TipState(ctx context.Context) consensus.State {
	return consensus.State{
		Index: types.ChainIndex{
			Height: uint64(cm.cs.Height()),
			ID:     types.BlockID(cm.cs.CurrentBlock().ID()),
		},
	}
}

type syncer struct {
	g  modules.Gateway
	tp modules.TransactionPool
}

func (s syncer) Addr() string {
	return string(s.g.Address())
}

func (s syncer) Peers() []string {
	var peers []string
	for _, p := range s.g.Peers() {
		peers = append(peers, string(p.NetAddress))
	}
	return peers
}

func (s syncer) Connect(addr string) error {
	return s.g.Connect(modules.NetAddress(addr))
}

func (s syncer) BroadcastTransaction(txn types.Transaction, dependsOn []types.Transaction) {
	txnSet := make([]stypes.Transaction, len(dependsOn)+1)
	for i, txn := range dependsOn {
		convertToSiad(txn, &txnSet[i])
	}
	convertToSiad(txn, &txnSet[len(txnSet)-1])
	s.tp.Broadcast(txnSet)
}

func (s syncer) SyncerAddress(ctx context.Context) (string, error) {
	return string(s.g.Address()), nil
}

type txpool struct {
	tp modules.TransactionPool
}

func (tp txpool) RecommendedFee() (fee types.Currency) {
	_, max := tp.tp.FeeEstimation()
	convertToCore(&max, &fee)
	return
}

func (tp txpool) Transactions() []types.Transaction {
	stxns := tp.tp.Transactions()
	txns := make([]types.Transaction, len(stxns))
	for i := range txns {
		convertToCore(&stxns[i], &txns[i])
	}
	return txns
}

func (tp txpool) AddTransactionSet(txns []types.Transaction) error {
	stxns := make([]stypes.Transaction, len(txns))
	for i := range stxns {
		convertToSiad(&txns[i], &stxns[i])
	}
	return tp.tp.AcceptTransactionSet(stxns)
}

func (tp txpool) UnconfirmedParents(txn types.Transaction) ([]types.Transaction, error) {
	pool := tp.Transactions()
	outputToParent := make(map[types.SiacoinOutputID]*types.Transaction)
	for i, txn := range pool {
		for j := range txn.SiacoinOutputs {
			outputToParent[txn.SiacoinOutputID(j)] = &pool[i]
		}
	}
	var parents []types.Transaction
	seen := make(map[types.TransactionID]bool)
	for _, sci := range txn.SiacoinInputs {
		if parent, ok := outputToParent[sci.ParentID]; ok {
			if txid := parent.ID(); !seen[txid] {
				seen[txid] = true
				parents = append(parents, *parent)
			}
		}
	}
	return parents, nil
}

func NewBus(cfg BusConfig, dir string, walletKey types.PrivateKey, l *zap.Logger) (http.Handler, func() error, error) {
	gatewayDir := filepath.Join(dir, "gateway")
	if err := os.MkdirAll(gatewayDir, 0700); err != nil {
		return nil, nil, err
	}
	g, err := gateway.New(cfg.GatewayAddr, cfg.Bootstrap, gatewayDir)
	if err != nil {
		return nil, nil, err
	}
	consensusDir := filepath.Join(dir, "consensus")
	if err := os.MkdirAll(consensusDir, 0700); err != nil {
		return nil, nil, err
	}
	cs, errCh := mconsensus.New(g, cfg.Bootstrap, consensusDir)
	select {
	case err := <-errCh:
		if err != nil {
			return nil, nil, err
		}
	default:
		go func() {
			if err := <-errCh; err != nil {
				log.Println("WARNING: consensus initialization returned an error:", err)
			}
		}()
	}
	tpoolDir := filepath.Join(dir, "transactionpool")
	if err := os.MkdirAll(tpoolDir, 0700); err != nil {
		return nil, nil, err
	}
	tp, err := transactionpool.New(cs, g, tpoolDir)
	if err != nil {
		return nil, nil, err
	}

	walletDir := filepath.Join(dir, "wallet")
	if err := os.MkdirAll(walletDir, 0700); err != nil {
		return nil, nil, err
	}
	walletAddr := wallet.StandardAddress(walletKey.PublicKey())
	ws, ccid, err := stores.NewJSONWalletStore(walletDir, walletAddr)
	if err != nil {
		return nil, nil, err
	} else if err := cs.ConsensusSetSubscribe(ws, ccid, nil); err != nil {
		return nil, nil, err
	}
	w := wallet.NewSingleAddressWallet(walletKey, ws)

	dbDir := filepath.Join(dir, "db")
	if err := os.MkdirAll(dbDir, 0700); err != nil {
		return nil, nil, err
	}
	dbConn := stores.NewSQLiteConnection(filepath.Join(dbDir, "db.sqlite"))

	sqlLogger := stores.NewSQLLogger(l.Named("db"), nil)
	sqlStore, ccid, err := stores.NewSQLStore(dbConn, true, cfg.PersistInterval, sqlLogger)
	if err != nil {
		return nil, nil, err
	} else if err := cs.ConsensusSetSubscribe(sqlStore, ccid, nil); err != nil {
		return nil, nil, err
	}

	if m := cfg.Miner; m != nil {
		if err := cs.ConsensusSetSubscribe(m, ccid, nil); err != nil {
			return nil, nil, err
		}
		tp.TransactionPoolSubscribe(m)
	}

	b, busCleanup, err := bus.New(syncer{g, tp}, chainManager{cs: cs}, txpool{tp}, w, sqlStore, sqlStore, sqlStore, sqlStore, sqlStore, cfg.GougingSettings, cfg.RedundancySettings, l)
	if err != nil {
		return nil, nil, err
	}

	cleanup := func() error {
		errs := []error{
			g.Close(),
			cs.Close(),
			tp.Close(),
			busCleanup(),
			sqlStore.Close(),
		}
		for _, err := range errs {
			if err != nil {
				return err
			}
		}
		return nil
	}
	return b, cleanup, nil
}

func NewWorker(cfg WorkerConfig, b worker.Bus, walletKey types.PrivateKey, l *zap.Logger) (http.Handler, func() error, error) {
	workerKey := blake2b.Sum256(append([]byte("worker"), walletKey...))
	w, cleanup, err := worker.New(workerKey, cfg.ID, b, cfg.SessionReconnectTimeout, cfg.SessionTTL, cfg.InteractionFlushInterval, cfg.DownloadSectorTimeout, cfg.UploadSectorTimeout, l)
	return w, cleanup, err
}

func NewAutopilot(cfg AutopilotConfig, s autopilot.Store, b autopilot.Bus, w autopilot.Worker, l *zap.Logger) (_ *autopilot.Autopilot, cleanup func() error, _ error) {
	ap, err := autopilot.New(s, b, w, l, cfg.Heartbeat, cfg.ScannerInterval, cfg.ScannerBatchSize, cfg.ScannerNumThreads)
	if err != nil {
		return nil, nil, err
	}

	return ap, ap.Stop, nil
}

func NewLogger(path string) (*zap.Logger, func(), error) {
	writer, closeFn, err := zap.Open(path)
	if err != nil {
		return nil, nil, err
	}

	// console
	config := zap.NewProductionEncoderConfig()
	config.EncodeTime = zapcore.RFC3339TimeEncoder
	config.EncodeLevel = zapcore.CapitalColorLevelEncoder
	config.StacktraceKey = ""
	consoleEncoder := zapcore.NewConsoleEncoder(config)

	// file
	config = zap.NewProductionEncoderConfig()
	config.EncodeTime = zapcore.RFC3339TimeEncoder
	config.CallerKey = ""     // hide
	config.StacktraceKey = "" // hide
	config.NameKey = "component"
	config.TimeKey = "date"
	fileEncoder := zapcore.NewJSONEncoder(config)

	core := zapcore.NewTee(
		zapcore.NewCore(fileEncoder, writer, zapcore.DebugLevel),
		zapcore.NewCore(consoleEncoder, zapcore.AddSync(os.Stdout), zapcore.DebugLevel),
	)

	logger := zap.New(
		core,
		zap.AddCaller(),
		zap.AddStacktrace(zapcore.ErrorLevel),
	)

	return logger, func() {
		_ = logger.Sync() // ignore Error
		closeFn()
	}, nil
}
