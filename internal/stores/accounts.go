package stores

import (
	"math/big"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/ephemeralaccounts"
	rhpv3 "go.sia.tech/renterd/rhp/v3"
)

type (
	dbEphemeralAccount struct {
		Model

		Owner string `gorm:"NOT NULL"`

		// AccountID identifies an account. It's a public key.
		AccountID rhpv3.Account `gorm:"unique;NOT NULL"`

		// Host describes the host the account was created with.
		Host types.PublicKey `gorm:"unique;NOT NULL"`

		// Balance is the balance of the account.
		Balance *big.Int `gorm:"type:bytes;serializer:gob"`
	}
)

func (dbEphemeralAccount) TableName() string {
	return "ephemeral_accounts"
}

func (s *SQLStore) Accounts() ([]ephemeralaccounts.Account, error) {
	return []ephemeralaccounts.Account{}, nil // TODO: implement
}

func (s *SQLStore) SaveAccounts([]ephemeralaccounts.Account) error {
	return nil // TODO: implement
}
