package node

import (
	"context"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"go.sia.tech/jape"
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
	Enabled     bool
	BusAddr     string
	BusPassword string
}

type BusConfig struct {
	Enabled     bool
	Bootstrap   bool
	GatewayAddr string
}

type AutopilotConfig struct {
	Enabled        bool
	BusAddr        string
	BusPassword    string
	WorkerAddr     string
	WorkerPassword string
	LoopInterval   time.Duration
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

func newBus(cfg BusConfig, dir string, walletKey consensus.PrivateKey) (*bus.Bus, func() error, error) {
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
		}
		for _, err := range errs {
			if err != nil {
				return err
			}
		}
		return nil
	}

	b := bus.New(syncer{g, tp}, chainManager{cm}, txpool{tp}, w, sqlStore, sqlStore, sqlStore, sqlStore)
	return b, cleanup, nil
}

func newWorker(cfg WorkerConfig, walletKey consensus.PrivateKey) (*worker.Worker, func() error, error) {
	b := bus.NewClient(cfg.BusAddr, cfg.BusPassword)
	workerKey := blake2b.Sum256(append([]byte("worker"), walletKey...))
	w := worker.New(workerKey, b)
	return w, func() error { return nil }, nil
}

func newAutopilot(cfg AutopilotConfig, dir string) (*autopilot.Autopilot, func() error, error) {
	store, err := stores.NewJSONAutopilotStore(dir)
	if err != nil {
		return nil, nil, err
	}
	b := bus.NewClient(cfg.BusAddr, cfg.BusPassword)
	w := worker.NewClient(cfg.WorkerAddr, cfg.WorkerPassword)
	a, err := autopilot.New(store, b, w, cfg.LoopInterval)
	if err != nil {
		return nil, nil, err
	}
	cleanup := func() error {
		a.Stop()
		return nil
	}
	return a, cleanup, nil
}

type Node struct {
	Bus       *bus.Bus
	Worker    *worker.Worker
	Autopilot *autopilot.Autopilot

	APIAddr string

	cleanup []func() error
	l       net.Listener
	srv     *http.Server
}

type NodeConfig struct {
	APIAddr     string
	APIPassword string
	Dir         string
	UIHandler   http.Handler
}

func (n *Node) Close() error {
	if err := n.srv.Shutdown(context.Background()); err != nil {
		return err
	}
	for _, fn := range n.cleanup {
		if err := fn(); err != nil {
			return err
		}
	}
	return nil
}

func (n *Node) APIAddress() string {
	return n.APIAddr
}

func NewNode(nc NodeConfig, bc BusConfig, wc WorkerConfig, ac AutopilotConfig, wk consensus.PrivateKey) (_ *Node, err error) {
	// create listener first, so that we know the actual apiAddr if the user
	// specifies port :0
	l, err := net.Listen("tcp", nc.APIAddr)
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()

	// Overwrite APIAddr now that we know the exact addr from the listener.
	nc.APIAddr = "http://" + l.Addr().String()

	// authenticate API
	auth := jape.BasicAuth(nc.APIPassword)

	mux := TreeMux{
		H:   nc.UIHandler,
		Sub: make(map[string]http.Handler),
	}

	// Cleanup.
	var cleanupFncs []func() error
	defer func() {
		if err != nil {
			for _, fn := range cleanupFncs {
				fn()
			}
		}
	}()

	node := &Node{
		APIAddr: nc.APIAddr,
		l:       l,
	}

	// Create the bus.
	if bc.Enabled {
		b, cleanup, err := newBus(bc, nc.Dir, wk)
		if err != nil {
			return nil, err
		}
		cleanupFncs = append(cleanupFncs, cleanup)
		node.Bus = b
		mux.Sub["/api/store"] = &TreeMux{H: auth(bus.NewServer(b))}
	}

	// Create the worker. If we also previously created a bus, we overwrite
	// the worker config to connect to that bus.
	if wc.Enabled {
		if bc.Enabled {
			wc.BusAddr = nc.APIAddr
			wc.BusPassword = nc.APIPassword
		}
		w, cleanup, err := newWorker(wc, wk)
		if err != nil {
			return nil, err
		}
		cleanupFncs = append(cleanupFncs, cleanup)
		node.Worker = w
		mux.Sub["/api/worker"] = &TreeMux{H: auth(worker.NewServer(w))}
	}

	// Create the autopilot. If we also previously created a bus, we
	// overwrite the autopilot config to connect to that bus. We do the same
	// thing for the worker.
	if ac.Enabled {
		if bc.Enabled {
			ac.BusAddr = nc.APIAddr
			ac.BusPassword = nc.APIPassword
		}
		if wc.Enabled {
			ac.WorkerAddr = nc.APIAddr
			ac.WorkerPassword = nc.APIPassword
		}
		a, cleanup, err := newAutopilot(ac, nc.Dir)
		if err != nil {
			return nil, err
		}
		defer cleanup()
		go func() {
			err := a.Run()
			if err != nil {
				log.Fatalln("Fatal autopilot error:", err)
			}
		}()
		cleanupFncs = append(cleanupFncs, cleanup)
		node.Autopilot = a
		mux.Sub["/api/autopilot"] = &TreeMux{H: auth(autopilot.NewServer(a))}
	}

	// Prepare API.
	srv := &http.Server{Handler: mux}
	go srv.Serve(l)

	node.cleanup = cleanupFncs
	node.srv = srv

	return node, nil
}
