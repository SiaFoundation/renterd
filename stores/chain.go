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
func (s *SQLStore) ChainIndex(ctx context.Context) (ci types.ChainIndex, err error) {
	err = s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		ci, err = tx.Tip(ctx)
		return err
	})
	return
}

// ProcessChainUpdate returns a callback function that process a chain update
// inside a transaction.
func (s *SQLStore) ProcessChainUpdate(ctx context.Context, applyFn chain.ApplyChainUpdateFn) error {
	return s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		return tx.ProcessChainUpdate(ctx, applyFn)
	})
}

// UpdateChainState process the given revert and apply updates.
func (s *SQLStore) UpdateChainState(reverted []chain.RevertUpdate, applied []chain.ApplyUpdate) error {
	return s.ProcessChainUpdate(context.Background(), func(tx chain.ChainUpdateTx) error {
		return wallet.UpdateChainState(tx, s.walletAddress, applied, reverted)
	})
}

// ResetChainState deletes all chain data in the database.
func (s *SQLStore) ResetChainState(ctx context.Context) error {
	return s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		return tx.ResetChainState(ctx)
	})
}
