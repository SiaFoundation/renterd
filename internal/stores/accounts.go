package stores

import (
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

		// AccountID identifies an account. It's a public key.
		AccountID rhpv3.Account `gorm:"unique;NOT NULL"`

		// Host describes the host the account was created with.
		Host types.PublicKey `gorm:"NOT NULL"`

		// Balance is the balance of the account.
		Balance *big.Int `gorm:"type:bytes;serializer:gob"`
	}
)

func (dbAccount) TableName() string {
	return "ephemeral_accounts"
}

// Accounts returns all accoutns from the db.
func (s *SQLStore) Accounts() ([]api.Account, error) {
	var dbAccounts []dbAccount
	if err := s.db.Find(&dbAccounts).Error; err != nil {
		return nil, err
	}
	accounts := make([]api.Account, len(dbAccounts))
	for i, acc := range dbAccounts {
		accounts[i] = api.Account{
			ID:      acc.AccountID,
			Host:    acc.Host,
			Balance: acc.Balance,
			Owner:   acc.Owner,
		}
	}
	return accounts, nil
}

// SaveAccounts saves the given accounts in the db, overwriting any existing
// ones.
func (s *SQLStore) SaveAccounts(accounts []api.Account) error {
	dbAccounts := make([]dbAccount, len(accounts))
	for i, acc := range accounts {
		dbAccounts[i] = dbAccount{
			Owner:     acc.Owner,
			AccountID: acc.ID,
			Host:      acc.Host,
			Balance:   acc.Balance,
		}
	}
	return s.db.Clauses(clause.OnConflict{
		UpdateAll: true,
	}).Create(&dbAccounts).Error
}
