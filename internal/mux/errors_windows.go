package mux

import (
	"errors"
	"io"
	"syscall"
)

// isConnCloseError returns true if the error is from the peer closing the
// connection early.
func isConnCloseError(err error) bool {
	const WSAEPROTOTYPE = syscall.Errno(10041)
	return errors.Is(err, io.EOF) ||
		errors.Is(err, WSAEPROTOTYPE) ||
		errors.Is(err, syscall.WSAECONNABORTED) ||
		errors.Is(err, syscall.WSAECONNRESET)
}
