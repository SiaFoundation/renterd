package worker

import (
	"context"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.uber.org/zap"
)

type contextCheckingLocker struct {
}

func (l *contextCheckingLocker) AcquireContract(ctx context.Context, fcid types.FileContractID, priority int, d time.Duration) (lockID uint64, err error) {
	return 0, nil
}

func (l *contextCheckingLocker) KeepaliveContract(ctx context.Context, _ types.FileContractID, _ uint64, _ time.Duration) error {
	return nil
}

func (l *contextCheckingLocker) ReleaseContract(ctx context.Context, fcid types.FileContractID, lockID uint64) (err error) {
	select {
	case <-ctx.Done():
		return context.Canceled
	default:
	}
	return nil
}

// TestReleaseContract is a test to verify that calling `Release` on a
// contractLock with an already cancelled context will not fail.
func TestReleaseContract(t *testing.T) {
	t.Parallel()

	l := newContractLock(types.FileContractID{}, 0, 0, &contextCheckingLocker{}, zap.NewNop().Sugar())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := l.Release(ctx); err != nil {
		t.Fatal(err)
	}
}
