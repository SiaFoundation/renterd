package chain

import (
	"context"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/renterd/api"
)

type (
	Manager          = chain.Manager
	HostAnnouncement = chain.HostAnnouncement
	ApplyUpdate      = chain.ApplyUpdate
	RevertUpdate     = chain.RevertUpdate
)

var ForEachHostAnnouncement = chain.ForEachHostAnnouncement

type (
	ChainManager interface {
		Block(id types.BlockID) (types.Block, bool)
		OnReorg(fn func(types.ChainIndex)) (cancel func())
		Tip() types.ChainIndex
		UpdatesSince(index types.ChainIndex, max int) (rus []chain.RevertUpdate, aus []chain.ApplyUpdate, err error)
	}

	ChainStore interface {
		ProcessChainUpdate(ctx context.Context, fn func(ChainUpdateTx) error) error
		ChainIndex(ctx context.Context) (types.ChainIndex, error)
	}

	ChainUpdateTx interface {
		ContractState(fcid types.FileContractID) (api.ContractState, error)
		UpdateChainIndex(index types.ChainIndex) error
		UpdateContract(fcid types.FileContractID, revisionHeight, revisionNumber, size uint64) error
		UpdateContractState(fcid types.FileContractID, state api.ContractState) error
		UpdateContractProofHeight(fcid types.FileContractID, proofHeight uint64) error
		UpdateFailedContracts(blockHeight uint64) error
		UpdateHost(hk types.PublicKey, ha chain.HostAnnouncement, bh uint64, blockID types.BlockID, ts time.Time) error

		wallet.UpdateTx
	}
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
