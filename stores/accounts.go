package stores

import (
	"context"

	"go.sia.tech/renterd/api"
	sql "go.sia.tech/renterd/stores/sql"
)

// Accounts returns all accounts from the db.
func (s *SQLStore) Accounts(ctx context.Context) (accounts []api.Account, err error) {
	err = s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		accounts, err = tx.Accounts(ctx)
		return err
	})
	return
}

// SetUncleanShutdown sets the clean shutdown flag on the accounts to 'false'
// and also sets the 'requires_sync' flag. That way, the autopilot will know to
// sync all accounts after an unclean shutdown and the bus will know not to
// apply drift.
func (s *SQLStore) SetUncleanShutdown(ctx context.Context) error {
	return s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		return tx.SetUncleanShutdown(ctx)
	})
}

// SaveAccounts saves the given accounts in the db, overwriting any existing
// ones.
func (s *SQLStore) SaveAccounts(ctx context.Context, accounts []api.Account) error {
	return s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		return tx.SaveAccounts(ctx, accounts)
	})
}
