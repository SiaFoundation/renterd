package api

import (
	"math/big"

	"go.sia.tech/core/types"
	rhpv3 "go.sia.tech/renterd/rhp/v3"
)

type (
	Account struct {
		// ID identifies an account. It's a public key.
		ID rhpv3.Account `json:"id"`

		// Host describes the host the account was created with.
		Host types.PublicKey `json:"host"`

		// Balance is the balance of the account.
		Balance *big.Int `json:"balance"`

		// Owner marks the owner of an account. This is usually a unique
		// identifier for a worker.
		Owner string `json:"owner"`
	}
)
