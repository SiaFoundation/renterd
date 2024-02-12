package worker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.uber.org/zap"
)

type (
	ContractSpendingRecorder interface {
		Record(rev types.FileContractRevision, cs api.ContractSpending)
		Stop(context.Context)
	}

	contractSpendingRecorder struct {
		flushInterval time.Duration

		bus    Bus
		logger *zap.SugaredLogger

		mu                sync.Mutex
		contractSpendings map[types.FileContractID]api.ContractSpendingRecord

		flushCtx   context.Context
		flushTimer *time.Timer
	}
)

var (
	_ ContractSpendingRecorder = (*contractSpendingRecorder)(nil)
)

func (w *worker) initContractSpendingRecorder(flushInterval time.Duration) {
	if w.contractSpendingRecorder != nil {
		panic("ContractSpendingRecorder already initialized") // developer error
	}
	w.contractSpendingRecorder = &contractSpendingRecorder{
		bus:    w.bus,
		logger: w.logger,

		flushCtx:      w.shutdownCtx,
		flushInterval: flushInterval,

		contractSpendings: make(map[types.FileContractID]api.ContractSpendingRecord),
	}
}

// Record stores the given contract spending record until it gets flushed to the bus.
func (r *contractSpendingRecorder) Record(rev types.FileContractRevision, cs api.ContractSpending) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// record the spending
	csr, found := r.contractSpendings[rev.ParentID]
	if !found {
		csr = api.ContractSpendingRecord{
			ContractID: rev.ParentID,
		}
	}
	csr.ContractSpending = csr.ContractSpending.Add(cs)
	if rev.RevisionNumber > csr.RevisionNumber {
		csr.RevisionNumber = rev.RevisionNumber
		csr.Size = rev.Filesize
		csr.ValidRenterPayout = rev.ValidRenterPayout()
		csr.MissedHostPayout = rev.MissedHostPayout()
	}
	r.contractSpendings[rev.ParentID] = csr

	// schedule flush
	if r.flushTimer == nil {
		r.flushTimer = time.AfterFunc(r.flushInterval, r.flush)
	}
}

// Stop stops the flush timer and flushes one last time.
func (r *contractSpendingRecorder) Stop(ctx context.Context) {
	// stop the flush timer
	r.mu.Lock()
	if r.flushTimer != nil {
		r.flushTimer.Stop()
	}
	r.flushCtx = ctx
	r.mu.Unlock()

	// flush all interactions
	r.flush()

	// log if we weren't able to flush them
	r.mu.Lock()
	if len(r.contractSpendings) > 0 {
		r.logger.Errorw(fmt.Sprintf("failed to record %d contract spendings on worker shutdown", len(r.contractSpendings)))
	}
	r.mu.Unlock()
}

func (r *contractSpendingRecorder) flush() {
	r.mu.Lock()
	defer r.mu.Unlock()

	// NOTE: don't bother flushing if the context is cancelled, we can safely
	// ignore the buffered records since we'll flush on shutdown and log in case
	// we weren't able to flush all spendings o the bus
	select {
	case <-r.flushCtx.Done():
		r.flushTimer = nil
		return
	default:
	}

	if len(r.contractSpendings) > 0 {
		records := make([]api.ContractSpendingRecord, 0, len(r.contractSpendings))
		for _, cs := range r.contractSpendings {
			records = append(records, cs)
		}
		if err := r.bus.RecordContractSpending(r.flushCtx, records); err != nil {
			r.logger.Errorw(fmt.Sprintf("failed to record contract spending: %v", err))
		} else {
			r.contractSpendings = make(map[types.FileContractID]api.ContractSpendingRecord)
		}
	}
	r.flushTimer = nil
}
