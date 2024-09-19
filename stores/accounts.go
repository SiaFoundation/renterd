package stores

import (
	"context"

	"go.sia.tech/renterd/api"
	sql "go.sia.tech/renterd/stores/sql"
)

// Accounts returns all accounts from the db.
func (s *SQLStore) Accounts(ctx context.Context, owner string) (accounts []api.Account, err error) {
	err = s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		accounts, err = tx.Accounts(ctx, owner)
		return err
	})
	return
}

// SaveAccounts saves the given accounts in the db, overwriting any existing
// ones.
func (s *SQLStore) SaveAccounts(ctx context.Context, accounts []api.Account) error {
	return s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		return tx.SaveAccounts(ctx, accounts)
	})
}
