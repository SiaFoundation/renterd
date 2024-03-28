package chain

import (
	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
)

type (
	Manager          = chain.Manager
	HostAnnouncement = chain.HostAnnouncement
)

func TestnetZen() (*consensus.Network, types.Block) {
	return chain.TestnetZen()
}

func NewDBStore(db chain.DB, n *consensus.Network, genesisBlock types.Block) (_ *chain.DBStore, _ consensus.State, err error) {
	return chain.NewDBStore(db, n, genesisBlock)
}

func NewManager(store chain.Store, cs consensus.State) *Manager {
	return chain.NewManager(store, cs)
}
