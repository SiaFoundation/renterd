package node

import (
	"log"
	"os"
	"path/filepath"
	"time"

	"go.sia.tech/renterd/autopilot"
	"go.sia.tech/renterd/bus"
	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/renterd/internal/stores"
	"go.sia.tech/renterd/wallet"
	"go.sia.tech/renterd/worker"
	"go.sia.tech/siad/modules"
	mconsensus "go.sia.tech/siad/modules/consensus"
	"go.sia.tech/siad/modules/gateway"
	"go.sia.tech/siad/modules/transactionpool"
	"go.sia.tech/siad/types"
	"golang.org/x/crypto/blake2b"
)

type WorkerConfig struct {
}

type BusConfig struct {
	Bootstrap   bool
	GatewayAddr string
	Miner       bool
}

type AutopilotConfig struct {
	Heartbeat time.Duration
}

type chainManager struct {
	cs modules.ConsensusSet
}

func (cm chainManager) Synced() bool {
	return cm.cs.Synced()
}

func (cm chainManager) TipState() consensus.State {
	return consensus.State{
		Index: consensus.ChainIndex{
			Height: uint64(cm.cs.Height()),
			ID:     consensus.BlockID(cm.cs.CurrentBlock().ID()),
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
	s.tp.Broadcast(append(dependsOn, txn))
}

type txpool struct {
	tp modules.TransactionPool
}

func (tp txpool) RecommendedFee() types.Currency {
	_, max := tp.tp.FeeEstimation()
	return max
}

func (tp txpool) Transactions() []types.Transaction {
	return tp.tp.Transactions()
}

func (tp txpool) AddTransactionSet(txns []types.Transaction) error {
	return tp.tp.AcceptTransactionSet(txns)
}

func (tp txpool) UnconfirmedParents(txn types.Transaction) ([]types.Transaction, error) {
	pool := tp.Transactions()
	outputToParent := make(map[types.OutputID]*types.Transaction)
	for i, txn := range pool {
		for j := range txn.SiacoinOutputs {
			scoid := txn.SiacoinOutputID(uint64(j))
			outputToParent[types.OutputID(scoid)] = &pool[i]
		}
	}
	var parents []types.Transaction
	seen := make(map[types.TransactionID]bool)
	for _, sci := range txn.SiacoinInputs {
		if parent, ok := outputToParent[types.OutputID(sci.ParentID)]; ok {
			if txid := parent.ID(); !seen[txid] {
				seen[txid] = true
				parents = append(parents, *parent)
			}
		}
	}
	return parents, nil
}

func NewBus(cfg BusConfig, dir string, walletKey consensus.PrivateKey) (*bus.Bus, func() error, error) {
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
	cm, errCh := mconsensus.New(g, cfg.Bootstrap, consensusDir)
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
	tp, err := transactionpool.New(cm, g, tpoolDir)
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
	} else if err := cm.ConsensusSetSubscribe(ws, ccid, nil); err != nil {
		return nil, nil, err
	}
	w := wallet.NewSingleAddressWallet(walletKey, ws)

	dbDir := filepath.Join(dir, "db")
	if err := os.MkdirAll(dbDir, 0700); err != nil {
		return nil, nil, err
	}
	dbConn := stores.NewSQLiteConnection(filepath.Join(dbDir, "db.sqlite"))

	hostdbDir := filepath.Join(dir, "hostdb")
	if err := os.MkdirAll(hostdbDir, 0700); err != nil {
		return nil, nil, err
	}
	sqlStore, ccid, err := stores.NewSQLStore(dbConn, true)
	if err != nil {
		return nil, nil, err
	} else if err := cm.ConsensusSetSubscribe(sqlStore, ccid, nil); err != nil {
		return nil, nil, err
	}

	var m *Miner
	if cfg.Miner {
		m = NewMiner(cm)
		if err := cm.ConsensusSetSubscribe(m, ccid, nil); err != nil {
			return nil, nil, err
		}
	}

	contractsDir := filepath.Join(dir, "contracts")
	if err := os.MkdirAll(contractsDir, 0700); err != nil {
		return nil, nil, err
	}

	objectsDir := filepath.Join(dir, "objects")
	if err := os.MkdirAll(objectsDir, 0700); err != nil {
		return nil, nil, err
	}

	cleanup := func() error {
		errs := []error{
			g.Close(),
			cm.Close(),
			tp.Close(),
			sqlStore.Close(),
		}
		for _, err := range errs {
			if err != nil {
				return err
			}
		}
		return nil
	}

	b := bus.New(syncer{g, tp}, chainManager{cm}, m, txpool{tp}, w, sqlStore, sqlStore, sqlStore, sqlStore)
	return b, cleanup, nil
}

func NewWorker(cfg WorkerConfig, b worker.Bus, walletKey consensus.PrivateKey) (*worker.Worker, func() error, error) {
	workerKey := blake2b.Sum256(append([]byte("worker"), walletKey...))
	w := worker.New(workerKey, b)
	return w, func() error { return nil }, nil
}

func NewAutopilot(cfg AutopilotConfig, b autopilot.Bus, w autopilot.Worker, dir string) (*autopilot.Autopilot, func() error, error) {
	store, err := stores.NewJSONAutopilotStore(dir)
	if err != nil {
		return nil, nil, err
	}
	a, err := autopilot.New(store, b, w, cfg.Heartbeat)
	if err != nil {
		return nil, nil, err
	}
	cleanup := func() error {
		a.Stop()
		return nil
	}
	return a, cleanup, nil
}
