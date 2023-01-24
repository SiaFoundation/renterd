package stores

import (
	"math/big"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/ephemeralaccounts"
)

type (
	dbEphemeralAccount struct {
		Model

		Owner string `gorm:"NOT NULL"`

		// AccountID identifies an account. It's a public key.
		AccountID ephemeralaccounts.AccountID `gorm:"unique;NOT NULL"`

		// Host describes the host the account was created with.
		Host types.PublicKey `gorm:"unique;NOT NULL"`

		// Balance is the balance of the account.
		Balance *big.Int `gorm:"type:bytes;serializer:gob"`
	}
)

func (dbEphemeralAccount) TableName() string {
	return "ephemeral_accounts"
}

func (dbEphemeralAccount) convert() ephemeralaccounts.Account {
	panic("not implemented yet")
}

func (s *SQLStore) OwnerAccounts(owner string) ([]ephemeralaccounts.Account, error) {
	panic("not implemented yet")
}
