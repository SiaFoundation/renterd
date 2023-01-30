package stores

import (
	"go.sia.tech/renterd/api"
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
		Balance string
	}
)

func (dbAccount) TableName() string {
	return "ephemeral_accounts"
}

func (s *SQLStore) Accounts() ([]api.Account, error) {
	return []api.Account{}, nil // TODO: implement
}

func (s *SQLStore) SaveAccounts([]api.Account) error {
	return nil // TODO: implement
}
