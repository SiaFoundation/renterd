package stores

import (
	"context"
	"math/big"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	rhpv3 "go.sia.tech/renterd/rhp/v3"
	"gorm.io/gorm/clause"
)

type (
	dbAccount struct {
		Model

		Owner string `gorm:"NOT NULL"`

		// AccountID identifies an account.
		AccountID publicKey `gorm:"unique;NOT NULL"`

		// Host describes the host the account was created with.
		Host publicKey `gorm:"NOT NULL"`

		// Balance is the balance of the account.
		Balance *balance
	}
)

func (dbAccount) TableName() string {
	return "ephemeral_accounts"
}

func (a dbAccount) convert() api.Account {
	return api.Account{
		ID:      rhpv3.Account(a.AccountID),
		Host:    types.PublicKey(a.Host),
		Balance: (*big.Int)(a.Balance),
		Owner:   a.Owner,
	}
}

// Accounts returns all accoutns from the db.
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
			Owner:     acc.Owner,
			AccountID: publicKey(acc.ID),
			Host:      publicKey(acc.Host),
			Balance:   (*balance)(acc.Balance),
		}
	}
	return s.db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "account_id"}},
		UpdateAll: true,
	}).Create(&dbAccounts).Error
}
