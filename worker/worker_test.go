package worker

import (
	"context"
	"testing"
	"time"

	"go.sia.tech/core/types"
)

type contextCheckingLocker struct {
}

func (l *contextCheckingLocker) AcquireContract(ctx context.Context, fcid types.FileContractID, priority int, d time.Duration) (lockID uint64, err error) {
	return 0, nil
}

func (l *contextCheckingLocker) ReleaseContract(ctx context.Context, fcid types.FileContractID, lockID uint64) (err error) {
	select {
	case <-ctx.Done():
		return context.Canceled
	default:
	}
	return nil
}

// TestReleaseContract is a test to verify that calling `ReleaseContract` on a
// tracedContractLocker with an already cancelled context will not fail.
func TestReleaseContract(t *testing.T) {
	t.Parallel()

	l := &tracedContractLocker{
		l: &contextCheckingLocker{},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := l.ReleaseContract(ctx, types.FileContractID{}, 0); err != nil {
		t.Fatal(err)
	}
}
