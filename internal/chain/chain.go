package chain

import (
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/renterd/api"
)

type ChainUpdateTx interface {
	ContractState(fcid types.FileContractID) (api.ContractState, error)
	UpdateChainIndex(index types.ChainIndex) error
	UpdateContract(fcid types.FileContractID, revisionHeight, revisionNumber, size uint64) error
	UpdateContractState(fcid types.FileContractID, state api.ContractState) error
	UpdateContractProofHeight(fcid types.FileContractID, proofHeight uint64) error
	UpdateFailedContracts(blockHeight uint64) error
	UpdateHost(hk types.PublicKey, ha chain.HostAnnouncement, bh uint64, blockID types.BlockID, ts time.Time) error

	wallet.UpdateTx
}
