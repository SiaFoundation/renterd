package node

import (
	"context"

	"go.sia.tech/core/types"
	"go.sia.tech/siad/modules"
	stypes "go.sia.tech/siad/types"
)

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
