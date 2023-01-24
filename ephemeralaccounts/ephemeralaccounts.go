package ephemeralaccounts

import (
	"math/big"

	"go.sia.tech/core/types"
	rhpv3 "go.sia.tech/renterd/rhp/v3"
)

type (
	Account struct {
		// ID identifies an account. It's a public key.
		ID rhpv3.Account

		// Host describes the host the account was created with.
		Host types.PublicKey

		// Balance is the balance of the account.
		Balance *big.Int

		Owner string
	}
)
