package migrator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.sia.tech/core/types"
	rhp "go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/renterd/api"
	"go.uber.org/zap"
)

type (
	ContractSpendingRecorder interface {
		RecordV1(types.FileContractRevision, api.ContractSpending)
		RecordV2(rhp.ContractRevision, api.ContractSpending)
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

// RecordV1 stores the given contract spending record until it gets flushed to the bus.
func (r *contractSpendingRecorder) RecordV1(rev types.FileContractRevision, cs api.ContractSpending) {
	r.record(rev.ParentID, rev.RevisionNumber, rev.Filesize, rev.ValidRenterPayout(), rev.MissedHostPayout(), cs)
}

// RecordV2 stores the given contract spending record until it gets flushed to the bus.
func (r *contractSpendingRecorder) RecordV2(rev rhp.ContractRevision, cs api.ContractSpending) {
	r.record(rev.ID, rev.Revision.RevisionNumber, rev.Revision.Filesize, rev.Revision.RenterOutput.Value, rev.Revision.HostOutput.Value, cs)
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

func (r *contractSpendingRecorder) record(fcid types.FileContractID, revisionNumber, size uint64, validRenterPayout, missedHostPayout types.Currency, cs api.ContractSpending) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// record the spending
	csr, found := r.contractSpendings[fcid]
	if !found {
		csr = api.ContractSpendingRecord{ContractID: fcid}
	}
	csr.ContractSpending = csr.ContractSpending.Add(cs)
	if revisionNumber > csr.RevisionNumber {
		csr.RevisionNumber = revisionNumber
		csr.Size = size
		csr.ValidRenterPayout = validRenterPayout
		csr.MissedHostPayout = missedHostPayout
	}
	r.contractSpendings[fcid] = csr

	// schedule flush
	if r.flushTimer == nil {
		r.flushTimer = time.AfterFunc(r.flushInterval, r.flush)
	}
}
