package utils

import (
	"errors"
	"strings"
)

// Common i/o related errors
var (
	ErrNoRouteToHost         = errors.New("no route to host")
	ErrNoSuchHost            = errors.New("no such host")
	ErrConnectionRefused     = errors.New("connection refused")
	ErrConnectionTimedOut    = errors.New("connection timed out")
	ErrConnectionResetByPeer = errors.New("connection reset by peer")
)

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
