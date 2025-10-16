package api

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/big"
	"strings"

	rhpv4 "go.sia.tech/core/rhp/v4"
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
		ID AccountID `json:"id"`

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

		// Owner is the owner of the account which is responsible for funding
		// it.
		Owner string `json:"owner"`

		// RequiresSync indicates whether an account needs to be synced with the
		// host before it can be used again.
		RequiresSync bool `json:"requiresSync"`
	}

	// AccountID aliases rhpv4.Account but has custom marshal/unmarshalers to
	// not break backwards compatibility in the API.
	AccountID rhpv4.Account
)

// String implements fmt.Stringer.
func (a AccountID) String() string { return hex.EncodeToString(a[:]) }

// MarshalText implements encoding.TextMarshaler.
func (a AccountID) MarshalText() ([]byte, error) {
	return []byte(a.String()), nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (a *AccountID) UnmarshalText(b []byte) error {
	s := string(b)
	switch {
	case strings.HasPrefix(s, "ed25519:"):
		s = strings.TrimPrefix(s, "ed25519:")
	case strings.HasPrefix(s, "acct:"):
		s = strings.TrimPrefix(s, "acct:")
	}
	if len(s) != hex.EncodedLen(len(a)) {
		return fmt.Errorf("invalid account length: got %d, want %d hex chars",
			len(s), hex.EncodedLen(len(a)))
	}
	n, err := hex.Decode(a[:], []byte(s))
	if err != nil {
		return fmt.Errorf("decoding account hex failed: %w", err)
	}
	if n != len(a) {
		return io.ErrUnexpectedEOF
	}
	return nil
}

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
