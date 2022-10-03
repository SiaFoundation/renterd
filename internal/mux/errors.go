//go:build !windows
// +build !windows

package mux

import (
	"errors"
	"io"
	"syscall"
)

// isConnCloseError provides cross-platform support for determining
// whether an error should be converted to ErrPeerClosedConn.
func isConnCloseError(err error) bool {
	return errors.Is(err, io.EOF) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, syscall.EPROTOTYPE) ||
		errors.Is(err, syscall.ECONNABORTED)
}
