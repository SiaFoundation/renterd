package stores

import (
	"context"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/stores/sql"
)

// ChainIndex returns the last stored chain index.
func (s *SQLStore) ChainIndex(ctx context.Context) (ci types.ChainIndex, err error) {
	err = s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		ci, err = tx.Tip(ctx)
		return err
	})
	return
}

func (s *SQLStore) FileContractElement(ctx context.Context, fcid types.FileContractID) (fce types.V2FileContractElement, err error) {
	err = s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		fce, err = tx.FileContractElement(ctx, fcid)
		return err
	})
	return
}

// ProcessChainUpdate returns a callback function that process a chain update
// inside a transaction.
func (s *SQLStore) ProcessChainUpdate(ctx context.Context, applyFn func(sql.ChainUpdateTx) error) error {
	return s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		return tx.ProcessChainUpdate(ctx, applyFn)
	})
}

// ResetChainState deletes all chain data in the database.
func (s *SQLStore) ResetChainState(ctx context.Context) error {
	return s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		return tx.ResetChainState(ctx)
	})
}
