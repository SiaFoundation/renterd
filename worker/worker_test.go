package worker

import (
	"context"
	"time"

	"go.sia.tech/core/types"
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
