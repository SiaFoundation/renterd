package stores

import (
	"context"
	"math/big"

	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"gorm.io/gorm/clause"
)

type (
	dbAccount struct {
		Model

		// AccountID identifies an account.
		AccountID publicKey `gorm:"unique;NOT NULL;size:32"`

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

func (a dbAccount) convert() api.Account {
	return api.Account{
		ID:           rhpv3.Account(a.AccountID),
		Host:         types.PublicKey(a.Host),
		Balance:      (*big.Int)(a.Balance),
		Drift:        (*big.Int)(a.Drift),
		RequiresSync: a.RequiresSync,
	}
}

// Accounts returns all accounts from the db.
func (s *SQLStore) Accounts(ctx context.Context) ([]api.Account, error) {
	var dbAccounts []dbAccount
	if err := s.db.Find(&dbAccounts).Error; err != nil {
		return nil, err
	}
	accounts := make([]api.Account, len(dbAccounts))
	for i, acc := range dbAccounts {
		accounts[i] = acc.convert()
	}
	return accounts, nil
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
			Host:         publicKey(acc.Host),
			Balance:      (*balance)(acc.Balance),
			Drift:        (*balance)(acc.Drift),
			RequiresSync: acc.RequiresSync,
		}
	}
	return s.db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "account_id"}},
		UpdateAll: true,
	}).Create(&dbAccounts).Error
}
