package worker

import (
	"context"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/internal/locking"
)

type ContractLocker interface {
	AcquireContract(ctx context.Context, fcid types.FileContractID, priority int, d time.Duration) (lockID uint64, err error)
	KeepaliveContract(ctx context.Context, fcid types.FileContractID, lockID uint64, d time.Duration) (err error)
	ReleaseContract(ctx context.Context, fcid types.FileContractID, lockID uint64) (err error)
}

var _ ContractLocker = (Bus)(nil)

func (w *Worker) acquireContractLock(ctx context.Context, fcid types.FileContractID, priority int) (_ *locking.ContractLock, err error) {
	return locking.NewContractLock(w.shutdownCtx, fcid, priority, w.bus, w.logger)
}

func (w *Worker) withContractLock(ctx context.Context, fcid types.FileContractID, priority int, fn func() error) error {
	contractLock, err := w.acquireContractLock(ctx, fcid, priority)
	if err != nil {
		return err
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(w.shutdownCtx, 10*time.Second)
		_ = contractLock.Release(releaseCtx)
		cancel()
	}()

	return fn()
}
