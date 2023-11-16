package api

import (
	"errors"
	"math/big"

	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
)

var (
	// ErrRequiresSyncSetRecently indicates that an account can't be set to sync
	// yet because it has been set too recently.
	ErrRequiresSyncSetRecently = errors.New("account had 'requiresSync' flag set recently")
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

type (
	// AccountsAddBalanceRequest is the request type for /account/:id/add
	// endpoint.
	AccountsAddBalanceRequest struct {
		HostKey types.PublicKey `json:"hostKey"`
		Amount  *big.Int        `json:"amount"`
	}

	// AccountHandlerPOST is the request type for the /account/:id endpoint.
	AccountHandlerPOST struct {
		HostKey types.PublicKey `json:"hostKey"`
	}

	// AccountsRequiresSyncRequest is the request type for
	// /account/:id/requiressync endpoint.
	AccountsRequiresSyncRequest struct {
		HostKey types.PublicKey `json:"hostKey"`
	}

	// AccountsUpdateBalanceRequest is the request type for /account/:id/update
	// endpoint.
	AccountsUpdateBalanceRequest struct {
		HostKey types.PublicKey `json:"hostKey"`
		Amount  *big.Int        `json:"amount"`
	}
)
