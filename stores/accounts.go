package stores

import (
	"context"

	"go.sia.tech/renterd/api"
	sql "go.sia.tech/renterd/stores/sql"
	"gorm.io/gorm/clause"
)

type (
	dbAccount struct {
		Model

		// AccountID identifies an account.
		AccountID publicKey `gorm:"unique;NOT NULL;size:32"`

		// CleanShutdown indicates whether the account was saved during a clean
		// shutdown shutdown.
		CleanShutdown bool `gorm:"default:false"`

		// Host describes the host the account was created with.
		Host publicKey `gorm:"NOT NULL"`

		// Balance is the balance of the account.
		Balance *balance

		// Drift is the accumulated delta between the bus' tracked balance for
		// an account and the balance reported by a host.
		Drift *balance

		// RequiresSync indicates whether an account needs to be synced with the
		// host before it can be used again.
		RequiresSync bool `gorm:"index"`
	}
)

func (dbAccount) TableName() string {
	return "ephemeral_accounts"
}

// Accounts returns all accounts from the db.
func (s *SQLStore) Accounts(ctx context.Context) (accounts []api.Account, _ error) {
	err := s.bMain.Transaction(ctx, func(tx sql.DatabaseTx) (err error) {
		accounts, err = tx.Accounts(ctx)
		return
	})
	return accounts, err
}

// SetCleanShutdown sets the clean shutdown flag on the accounts to 'false' and
// also sets the 'requires_sync' flag. That way, the autopilot will know to sync
// all accounts after an unclean shutdown and the bus will know not to apply
// drift.
func (s *SQLStore) SetUncleanShutdown(ctx context.Context) error {
	return s.db.
		WithContext(ctx).
		Model(&dbAccount{}).
		Where("TRUE").
		Updates(map[string]interface{}{
			"clean_shutdown": false,
			"requires_sync":  true,
		}).
		Error
}

// SaveAccounts saves the given accounts in the db, overwriting any existing
// ones.
func (s *SQLStore) SaveAccounts(ctx context.Context, accounts []api.Account) error {
	if len(accounts) == 0 {
		return nil
	}
	dbAccounts := make([]dbAccount, len(accounts))
	for i, acc := range accounts {
		dbAccounts[i] = dbAccount{
			AccountID:    publicKey(acc.ID),
			Host:         publicKey(acc.HostKey),
			Balance:      (*balance)(acc.Balance),
			Drift:        (*balance)(acc.Drift),
			RequiresSync: acc.RequiresSync,
		}
	}
	return s.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "account_id"}},
		UpdateAll: true,
	}).Create(&dbAccounts).Error
}
