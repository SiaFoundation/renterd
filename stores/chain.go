package stores

import (
	"context"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/renterd/internal/chain"
	"go.sia.tech/renterd/stores/sql"
)

var (
	_ chain.ChainStore = (*SQLStore)(nil)
)

// ChainIndex returns the last stored chain index.
func (ss *SQLStore) ChainIndex(ctx context.Context) (types.ChainIndex, error) {
	var ci dbConsensusInfo
	if err := ss.db.
		WithContext(ctx).
		Where(&dbConsensusInfo{Model: Model{ID: consensusInfoID}}).
		FirstOrCreate(&ci).
		Error; err != nil {
		return types.ChainIndex{}, err
	}
	return types.ChainIndex{
		Height: ci.Height,
		ID:     types.BlockID(ci.BlockID),
	}, nil
}

// ProcessChainUpdate returns a callback function that process a chain update
// inside a transaction.
func (s *SQLStore) ProcessChainUpdate(ctx context.Context, applyFn chain.ApplyChainUpdateFn) error {
	return s.bMain.Transaction(ctx, func(tx sql.DatabaseTx) error {
		return tx.ProcessChainUpdate(ctx, applyFn)
	})
}

// UpdateChainState process the given revert and apply updates.
func (s *SQLStore) UpdateChainState(reverted []chain.RevertUpdate, applied []chain.ApplyUpdate) error {
	return s.ProcessChainUpdate(context.Background(), func(tx chain.ChainUpdateTx) error {
		return wallet.UpdateChainState(tx, s.walletAddress, applied, reverted)
	})
}
