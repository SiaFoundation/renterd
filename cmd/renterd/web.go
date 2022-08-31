package main

import (
	"embed"
	"errors"
	"io/fs"
	"net"
	"net/http"
	"strings"

	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/siad/modules"
	"go.sia.tech/siad/types"
)

//go:embed dist
var dist embed.FS

type clientRouterFS struct {
	fs fs.FS
}

func (cr *clientRouterFS) Open(name string) (fs.File, error) {
	f, err := cr.fs.Open(name)
	if errors.Is(err, fs.ErrNotExist) {
		return cr.fs.Open("index.html")
	}
	return f, err
}

func createUIHandler() http.Handler {
	assets, err := fs.Sub(dist, "dist")
	if err != nil {
		panic(err)
	}
	return http.FileServer(http.FS(&clientRouterFS{fs: assets}))
}

type chainManager struct {
	cs modules.ConsensusSet
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
	min, _ := tp.tp.FeeEstimation()
	return min
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

func startWeb(l net.Listener, node *node, password string) error {
	renter := api.NewServer(&chainManager{node.cm}, &syncer{node.g, node.tp}, txpool{node.tp}, node.w, node.hdb, rhpImpl{}, node.cs, slabMover{}, node.os)
	api := api.AuthMiddleware(renter, password)
	web := createUIHandler()
	return http.Serve(l, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api") {
			r.URL.Path = strings.TrimPrefix(r.URL.Path, "/api")
			api.ServeHTTP(w, r)
			return
		}
		web.ServeHTTP(w, r)
	}))
}
