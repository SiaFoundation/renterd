package ephemeralaccounts

import (
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/internal/consensus"
)

type (
	AccountID consensus.PublicKey

	Account struct {
		// ID identifies an account. It's a public key.
		ID AccountID

		// Host describes the host the account was created with.
		Host consensus.PublicKey

		// Balance is the balance of the account.
		Balance types.Currency
	}
)
