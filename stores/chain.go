package stores

import (
	"context"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/chain"
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
