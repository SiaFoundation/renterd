package ephemeralaccounts

import (
	"go.sia.tech/core/types"
)

type (
	AccountID types.PublicKey

	Account struct {
		// ID identifies an account. It's a public key.
		ID AccountID

		// Host describes the host the account was created with.
		Host types.PublicKey

		// Balance is the balance of the account.
		Balance types.Currency
	}
)
