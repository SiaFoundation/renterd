package utils

import (
	"context"
	"errors"
	"fmt"
	"strings"

	rhpv4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
)

// Common i/o related errors
var (
	errBalanceInsufficientV1 = errors.New("ephemeral account balance was insufficient")
	ErrHost                  = errors.New("host responded with error")
	ErrNoRouteToHost         = errors.New("no route to host")
	ErrNoSuchHost            = errors.New("no such host")
	ErrConnectionRefused     = errors.New("connection refused")
	ErrConnectionTimedOut    = errors.New("connection timed out")
	ErrConnectionResetByPeer = errors.New("connection reset by peer")
	ErrIOTimeout             = errors.New("i/o timeout")
)

func IsBalanceInsufficient(err error) bool {
	return IsErr(err, rhpv4.ErrNotEnoughFunds) ||
		IsErr(err, errBalanceInsufficientV1)
}

// IsErr can be used to compare an error to a target and also works when used on
// errors that haven't been wrapped since it will fall back to a string
// comparison. Useful to check errors returned over the network.
func IsErr(err error, target error) bool {
	if (err == nil) != (target == nil) {
		return false
	} else if errors.Is(err, target) {
		return true
	}
	// TODO: we can get rid of the lower casing once siad is gone and
	// renterd/hostd use the same error messages
	return strings.Contains(strings.ToLower(err.Error()), strings.ToLower(target.Error()))
}

// IsErrHost indicates whether an error was returned by a host as part of an RPC.
func IsErrHost(err error) bool {
	return IsErr(err, ErrHost)
}

// WrapErr can be used to defer wrapping an error which is then decorated with
// the provided function name. If the context contains a cause error, it will
// also be included in the wrapping.
func WrapErr(ctx context.Context, fnName string, err *error) {
	if *err != nil {
		*err = fmt.Errorf("%s: %w", fnName, *err)
		if cause := context.Cause(ctx); cause != nil && !IsErr(*err, cause) {
			*err = fmt.Errorf("%w; %w", cause, *err)
		}
	}
}

// A HostErrorSet is a collection of errors from various hosts.
type HostErrorSet map[types.PublicKey]error

// Error implements error.
func (hes HostErrorSet) Error() string {
	if len(hes) == 0 {
		return ""
	}

	var strs []string
	for hk, he := range hes {
		strs = append(strs, fmt.Sprintf("%x: %v", hk[:4], he.Error()))
	}

	// include a leading newline so that the first error isn't printed on the
	// same line as the error context
	return "\n" + strings.Join(strs, "\n")
}
