package worker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/tracing"
	"go.uber.org/zap"
)

type (
	// A ContractSpendingRecorder records the spending of a contract.
	ContractSpendingRecorder interface {
		Record(fcid types.FileContractID, revisionNumber, size uint64, cs api.ContractSpending)
	}

	contractSpendingRecorder struct {
		bus           Bus
		flushInterval time.Duration
		logger        *zap.SugaredLogger

		mu                          sync.Mutex
		contractSpendings           map[types.FileContractID]api.ContractSpendingRecord
		contractSpendingsFlushTimer *time.Timer
	}
)

func (w *worker) initContractSpendingRecorder() {
	if w.contractSpendingRecorder != nil {
		panic("contractSpendingRecorder already initialized") // developer error
	}
	w.contractSpendingRecorder = &contractSpendingRecorder{
		bus:               w.bus,
		contractSpendings: make(map[types.FileContractID]api.ContractSpendingRecord),
		flushInterval:     w.busFlushInterval,
		logger:            w.logger,
	}
}

// Record sends contract spending records to the bus.
func (sr *contractSpendingRecorder) Record(fcid types.FileContractID, revisionNumber, size uint64, cs api.ContractSpending) {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	// Update buffer.
	csr, found := sr.contractSpendings[fcid]
	if !found {
		csr = api.ContractSpendingRecord{
			ContractID: fcid,
		}
	}
	csr.ContractSpending = csr.ContractSpending.Add(cs)
	if revisionNumber > csr.RevisionNumber {
		csr.RevisionNumber = revisionNumber
		csr.Size = size
	}
	sr.contractSpendings[fcid] = csr

	// If a thread was scheduled to flush the buffer we are done.
	if sr.contractSpendingsFlushTimer != nil {
		return
	}
	// Otherwise we schedule a flush.
	sr.contractSpendingsFlushTimer = time.AfterFunc(sr.flushInterval, func() {
		sr.mu.Lock()
		sr.flush()
		sr.mu.Unlock()
	})
}

func (sr *contractSpendingRecorder) flush() {
	if len(sr.contractSpendings) > 0 {
		ctx, span := tracing.Tracer.Start(context.Background(), "worker: flushContractSpending")
		defer span.End()
		records := make([]api.ContractSpendingRecord, 0, len(sr.contractSpendings))
		for _, cs := range sr.contractSpendings {
			records = append(records, cs)
		}
		if err := sr.bus.RecordContractSpending(ctx, records); err != nil {
			sr.logger.Errorw(fmt.Sprintf("failed to record contract spending: %v", err))
		} else {
			sr.contractSpendings = make(map[types.FileContractID]api.ContractSpendingRecord)
		}
	}
	sr.contractSpendingsFlushTimer = nil
}

// Stop stops the flush timer.
func (sr *contractSpendingRecorder) Stop() {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	if sr.contractSpendingsFlushTimer != nil {
		sr.contractSpendingsFlushTimer.Stop()
		sr.flush()
	}
}
