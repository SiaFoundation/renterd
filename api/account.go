package api

import (
	"math/big"

	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
)

type (
	Account struct {
		// ID identifies an account. It's a public key.
		ID rhpv3.Account `json:"id"`

		// CleanShutdown indicates whether the account was saved during a clean
		// shutdown
		CleanShutdown bool `json:"cleanShutdown"`

		// HostKey describes the host the account was created with.
		HostKey types.PublicKey `json:"hostKey"`

		// Balance is the balance of the account.
		Balance *big.Int `json:"balance"`

		// Drift is the accumulated delta between the bus' tracked balance for
		// an account and the balance reported by a host.
		Drift *big.Int `json:"drift"`

		// RequiresSync indicates whether an account needs to be synced with the
		// host before it can be used again.
		RequiresSync bool `json:"requiresSync"`
	}
)
