package node

import (
	"context"
	"errors"
	"fmt"
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
			sqlStore.Close(),
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
	return a, a.Stop, nil
}

// Node describes a single renterd process with one or more enabled components.
// A node always needs to either create a bus or connect to an existing one by
// calling CreateBus or AddBus respectively.
// Afterwards a worker can be created or added with CreateWorker/AddWorker. If
// the worker is created, it will automatically connect to the known bus. If it
// is added, the caller needs to guarantee that the worker is connected to the
// same bus as specified in the node.
// After both a bus and a worker were added, the autopilot can be created. It
// will connect to the bus and worker from the previous steps.
type Node struct {
	busAddr     *string
	busPassword *string
	bus         *bus.Bus

	workerAddr     *string
	workerPassword *string
	w              *worker.Worker

	autopilotAddr     *string
	autopilotPassword *string
	ap                *autopilot.Autopilot

	apiAddr     string
	apiPassword string
	auth        func(http.Handler) http.Handler
	dir         string
	l           net.Listener
	walletKey   consensus.PrivateKey

	mux treeMux
	srv http.Server

	cleanupFuncs []func() error
}

func (n *Node) APICredentials() (string, string) {
	return n.apiAddr, n.apiPassword
}

// NewNode creates a new, empty node without any components. The components are
// then added through the various exposed methods. For a more detailed
// description take a look at the Node's type documentation.
func NewNode(apiAddr, apiPassword, dir string, uiHandler http.Handler, wk consensus.PrivateKey) (*Node, error) {
	// Start listening.
	l, err := net.Listen("tcp", apiAddr)
	if err != nil {
		return nil, err
	}

	mux := treeMux{
		H:   uiHandler,
		Sub: make(map[string]http.Handler),
	}

	// Overwrite APIAddr now that we know the exact addr from the listener.
	return &Node{
		apiAddr:     "http://" + l.Addr().String(),
		apiPassword: apiPassword,
		auth:        jape.BasicAuth(apiPassword),
		dir:         dir,
		l:           l,
		mux:         mux,
		srv:         http.Server{Handler: mux},
		walletKey:   wk,
	}, nil
}

// Close shuts down the API as well as the components that were created.
// Remote components that were added instead of created will remain active.
func (c *Node) Shutdown(ctx context.Context) error {
	defer c.l.Close() // close last

	if err := c.srv.Shutdown(ctx); err != nil {
		return err
	}
	var errs error
	for _, f := range c.cleanupFuncs {
		err := f()
		if err == nil {
			continue
		} else if errs == nil {
			errs = err
		} else {
			errs = fmt.Errorf("%w; %s", errs, err.Error())
		}
	}
	return errs
}

// Serve starts serving the API. This is blocking and can be interrupted by
// calling 'Close'.
func (n *Node) Serve() error {
	return n.srv.Serve(n.l)
}

// AddBus adds an already running, remote bus to the node.
func (n *Node) AddBus(busAddr, busPassword string) error {
	if n.busAddr != nil || n.busPassword != nil {
		return errors.New("cluster already contains a bus")
	}
	// Set bus address and password to remote bus.
	n.busAddr = &busAddr
	n.busPassword = &busPassword
	return nil
}

// CreateBus creates a new bus.
func (n *Node) CreateBus(bootstrap bool, gatewayAddr string) error {
	if n.busAddr != nil || n.busPassword != nil {
		return errors.New("cluster already contains a bus")
	}
	b, cleanup, err := newBus(BusConfig{
		Bootstrap:   bootstrap,
		GatewayAddr: gatewayAddr,
	}, n.dir, n.walletKey)
	if err != nil {
		return err
	}
	// Set bus address and password to local bus.
	n.busAddr = &n.apiAddr
	n.busPassword = &n.apiPassword
	n.cleanupFuncs = append(n.cleanupFuncs, cleanup)
	n.bus = b
	n.mux.Sub["/api/bus"] = &treeMux{H: n.auth(bus.NewServer(b))}
	return nil
}

// GatewayAddress returns the address of the running gateway or 'false' if no
// gateway was created as part of the node.
func (n *Node) GatewayAddress() (string, bool) {
	if n.bus == nil {
		return "", false
	}
	return n.bus.GatewayAddress(), true
}

// AddWorker adds an already running, remote worker to the node.
func (n *Node) AddWorker(workerAddr, workerPassword string) error {
	if n.workerAddr != nil || n.workerPassword != nil {
		return errors.New("cluster already contains a worker")
	}
	// Set worker address and password to remote worker.
	n.workerAddr = &workerAddr
	n.workerPassword = &workerPassword
	return nil
}

// CreateWorker creates a new worker that is connected to the previously
// created/added bus.
func (n *Node) CreateWorker() error {
	if n.busAddr == nil || n.busPassword == nil {
		return errors.New("can't add a worker to a cluster without bus - call CreateBus or AddBus first")
	}
	if n.workerAddr != nil || n.workerPassword != nil {
		return errors.New("cluster already contains a worker")
	}
	w, cleanup, err := newWorker(WorkerConfig{
		BusAddr:     *n.busAddr,
		BusPassword: *n.busPassword,
	}, n.walletKey)
	if err != nil {
		return err
	}
	// Set worker address and password to local worker.
	n.workerAddr = &n.apiAddr
	n.workerPassword = &n.apiPassword
	n.cleanupFuncs = append(n.cleanupFuncs, cleanup)
	n.w = w
	n.mux.Sub["/api/worker"] = &treeMux{H: n.auth(worker.NewServer(w))}
	return nil
}

// CreateAutopilot creates a new autopilot that is connected to the previously
// created/added bus and worker.
func (n *Node) CreateAutopilot(loopInterval time.Duration) error {
	if n.autopilotAddr != nil || n.autopilotPassword != nil {
		return errors.New("cluster already contains an autopilot")
	}
	if n.busAddr == nil || n.busPassword == nil {
		return errors.New("can't add a worker to a cluster without bus - call CreateBus or AddBus first")
	}
	if n.workerAddr == nil || n.workerPassword == nil {
		return errors.New("can't add a worker to a cluster without worker - call CreateWorker or AddWorker first")
	}
	a, cleanup, err := newAutopilot(AutopilotConfig{
		BusAddr:        *n.busAddr,
		BusPassword:    *n.busPassword,
		WorkerAddr:     *n.workerAddr,
		WorkerPassword: *n.workerPassword,
		LoopInterval:   loopInterval,
	}, n.dir)
	if err != nil {
		return err
	}
	go func() {
		err := a.Run()
		if err != nil {
			log.Fatalln("Fatal autopilot error:", err)
		}
	}()
	// Set autopilot address and password to local autopilot.
	n.autopilotAddr = &n.apiAddr
	n.autopilotPassword = &n.apiPassword
	n.cleanupFuncs = append(n.cleanupFuncs, cleanup)
	n.ap = a
	n.mux.Sub["/api/autopilot"] = &treeMux{H: n.auth(autopilot.NewServer(a))}
	return nil
}
