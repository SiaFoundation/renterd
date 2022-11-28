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
	a                 *autopilot.Autopilot

	apiAddr     string
	apiPassword string
	auth        func(http.Handler) http.Handler
	dir         string
	l           net.Listener
	wk          consensus.PrivateKey

	mux TreeMux
	srv *http.Server

	cleanupFuncs []func() error
}

// NewNode creates a new, empty node without any components. The components are
// then added through the various exposed methods. For a more detailed
// description take a look at the Node's type documentation.
func NewNode(apiAddr, apiPassword, dir string, uiHandler http.Handler, wk consensus.PrivateKey) *Node {
	// Start listening.
	l, err := net.Listen("tcp", apiAddr)
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()

	// Overwrite APIAddr now that we know the exact addr from the listener.
	return &Node{
		apiAddr:     "http://" + l.Addr().String(),
		apiPassword: apiPassword,
		auth:        jape.BasicAuth(apiPassword),
		dir:         dir,
		l:           l,
		mux: TreeMux{
			H:   uiHandler,
			Sub: make(map[string]http.Handler),
		},
		wk: wk,
	}
}

// APIAddress returns the node's API address.
func (c *Node) APIAddress() string {
	return c.apiAddr
}

// Close shuts down the API as well as the components that were created.
// Remote components that were added instead of created will remain active.
func (c *Node) Close() error {
	if c.srv != nil {
		if err := c.srv.Shutdown(context.Background()); err != nil {
			return err
		}
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
func (c *Node) Serve() error {
	if c.srv != nil {
		return errors.New("already serving API")
	}
	c.srv = &http.Server{
		Handler: c.mux,
	}
	err := c.srv.Serve(c.l)
	if errors.Is(err, net.ErrClosed) {
		return nil // ignore
	}
	return err
}

// AddBus adds an already running, remote bus to the node.
func (c *Node) AddBus(busAddr, busPassword string) error {
	if c.busAddr != nil || c.busPassword != nil {
		return errors.New("cluster already contains a bus")
	}
	// Set bus address and password to remote bus.
	*c.busAddr = busAddr
	*c.busPassword = busPassword
	return nil
}

// CreateBus creates a new bus.
func (c *Node) CreateBus(bootstrap bool, gatewayAddr string) error {
	if c.busAddr != nil || c.busPassword != nil {
		return errors.New("cluster already contains a bus")
	}
	b, cleanup, err := newBus(BusConfig{
		Bootstrap:   bootstrap,
		GatewayAddr: gatewayAddr,
	}, c.dir, c.wk)
	if err != nil {
		return err
	}
	// Set bus address and password to local bus.
	c.busAddr = &c.apiAddr
	c.busPassword = &c.apiPassword
	c.cleanupFuncs = append(c.cleanupFuncs, cleanup)
	c.bus = b
	c.mux.Sub["/api/store"] = &TreeMux{H: c.auth(bus.NewServer(b))}
	return nil
}

// AddWorker adds an already running, remote worker to the node.
func (c *Node) AddWorker(workerAddr, workerPassword string) error {
	if c.workerAddr != nil || c.workerPassword != nil {
		return errors.New("cluster already contains a worker")
	}
	// Set worker address and password to remote worker.
	*c.workerAddr = workerAddr
	*c.workerPassword = workerPassword
	return nil
}

// CreateWorker creates a new worker that is connected to the previously
// created/added bus.
func (c *Node) CreateWorker() error {
	if c.busAddr == nil || c.busPassword == nil {
		return errors.New("can't add a worker to a cluster without bus - call CreateBus or AddBus first")
	}
	if c.workerAddr != nil || c.workerPassword != nil {
		return errors.New("cluster already contains a worker")
	}
	w, cleanup, err := newWorker(WorkerConfig{
		BusAddr:     *c.busAddr,
		BusPassword: *c.busPassword,
	}, c.wk)
	if err != nil {
		return err
	}
	// Set worker address and password to local worker.
	c.workerAddr = &c.apiAddr
	c.workerPassword = &c.apiPassword
	c.cleanupFuncs = append(c.cleanupFuncs, cleanup)
	c.w = w
	c.mux.Sub["/api/worker"] = &TreeMux{H: c.auth(worker.NewServer(w))}
	return nil
}

// CreateAutopilot creates a new autopilot that is connected to the previously
// created/added bus and worker.
func (c *Node) CreateAutopilot(loopInterval time.Duration) error {
	if c.autopilotAddr != nil || c.autopilotPassword != nil {
		return errors.New("cluster already contains an autopilot")
	}
	if c.busAddr == nil || c.busPassword == nil {
		return errors.New("can't add a worker to a cluster without bus - call CreateBus or AddBus first")
	}
	if c.workerAddr == nil || c.workerPassword == nil {
		return errors.New("can't add a worker to a cluster without worker - call CreateWorker or AddWorker first")
	}
	a, cleanup, err := newAutopilot(AutopilotConfig{
		BusAddr:        *c.busAddr,
		BusPassword:    *c.busPassword,
		WorkerAddr:     *c.workerAddr,
		WorkerPassword: *c.workerPassword,
		LoopInterval:   loopInterval,
	}, c.dir)
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
	c.autopilotAddr = &c.apiAddr
	c.autopilotPassword = &c.apiPassword
	c.cleanupFuncs = append(c.cleanupFuncs, cleanup)
	c.a = a
	c.mux.Sub["/api/autopilot"] = &TreeMux{H: c.auth(autopilot.NewServer(a))}
	return nil
}
