package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.sia.tech/core/types"
	"go.uber.org/zap"
)

type ContractLocker interface {
	AcquireContract(ctx context.Context, fcid types.FileContractID, priority int, d time.Duration) (lockID uint64, err error)
	KeepaliveContract(ctx context.Context, fcid types.FileContractID, lockID uint64, d time.Duration) (err error)
	ReleaseContract(ctx context.Context, fcid types.FileContractID, lockID uint64) (err error)
}

var _ ContractLocker = (Bus)(nil)

type contractLock struct {
	lockID uint64
	fcid   types.FileContractID
	d      time.Duration
	locker ContractLocker
	logger *zap.SugaredLogger

	keepaliveLoopCtx       context.Context
	keepaliveLoopCtxCancel context.CancelFunc
	stopWG                 sync.WaitGroup
}

func newContractLock(ctx context.Context, fcid types.FileContractID, lockID uint64, d time.Duration, locker ContractLocker, logger *zap.SugaredLogger) *contractLock {
	ctx, cancel := context.WithCancel(ctx)
	cl := &contractLock{
		lockID: lockID,
		fcid:   fcid,
		d:      d,
		locker: locker,
		logger: logger,

		keepaliveLoopCtx:       ctx,
		keepaliveLoopCtxCancel: cancel,
	}
	cl.stopWG.Add(1)
	go func() {
		cl.keepaliveLoop()
		cl.stopWG.Done()
	}()
	return cl
}

func (w *worker) acquireContractLock(ctx context.Context, fcid types.FileContractID, priority int) (_ *contractLock, err error) {
	lockID, err := w.bus.AcquireContract(ctx, fcid, priority, w.contractLockingDuration)
	if err != nil {
		return nil, err
	}
	return newContractLock(w.shutdownCtx, fcid, lockID, w.contractLockingDuration, w.bus, w.logger), nil
}

func (w *worker) withContractLock(ctx context.Context, fcid types.FileContractID, priority int, fn func() error) error {
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

func (cl *contractLock) Release(ctx context.Context) error {
	// Stop background loop.
	cl.keepaliveLoopCtxCancel()
	cl.stopWG.Wait()

	// Release the contract.
	return cl.locker.ReleaseContract(ctx, cl.fcid, cl.lockID)
}

func (cl *contractLock) keepaliveLoop() {
	// Create ticker for 20% of the lock duration.
	start := time.Now()
	var lastUpdate time.Time
	tickDuration := cl.d / 5
	t := time.NewTicker(tickDuration)

	// Cleanup
	defer func() {
		t.Stop()
		select {
		case <-t.C:
		default:
		}
	}()

	// Loop until stopped.
	for {
		select {
		case <-cl.keepaliveLoopCtx.Done():
			return // released
		case <-t.C:
		}
		if err := cl.locker.KeepaliveContract(cl.keepaliveLoopCtx, cl.fcid, cl.lockID, cl.d); err != nil && !errors.Is(err, context.Canceled) {
			cl.logger.Errorw(fmt.Sprintf("failed to send keepalive: %v", err),
				"contract", cl.fcid,
				"lockID", cl.lockID,
				"loopStart", start,
				"timeSinceLastUpdate", time.Since(lastUpdate),
				"tickDuration", tickDuration)
			return
		}
		lastUpdate = time.Now()
	}
}
