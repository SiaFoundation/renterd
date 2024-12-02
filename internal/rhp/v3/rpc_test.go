package rhp

import (
	"errors"
	"fmt"
	"testing"

	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/renterd/internal/utils"
)

func TestWrapRPCErr(t *testing.T) {
	// host error
	err := fmt.Errorf("ReadResponse: %w", &rhpv3.RPCError{
		Description: "some host error",
	})
	if err.Error() != "ReadResponse: some host error" {
		t.Fatal("unexpected error:", err)
	}
	wrapRPCErr(&err, "ReadResponse")
	if err.Error() != "ReadResponse: host responded with error: 'some host error'" {
		t.Fatal("unexpected error:", err)
	} else if !errors.Is(err, utils.ErrHost) {
		t.Fatalf("expected error to be wrapped with %v, got %v", utils.ErrHost, err)
	}

	// transport error
	err = fmt.Errorf("ReadResponse: %w", errors.New("some transport error"))
	wrapRPCErr(&err, "ReadResponse")
	if err.Error() != "ReadResponse: transport error: 'some transport error'" {
		t.Fatal("unexpected error:", err)
	} else if !errors.Is(err, errTransport) {
		t.Fatalf("expected error to be wrapped with %v, got %v", utils.ErrHost, err)
	}
}
