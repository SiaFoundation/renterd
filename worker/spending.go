package worker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/tracing"
	"go.uber.org/zap"
)

const keyContractSpendingRecorder contextKey = "ContractSpendingRecorder"

type (
	ContractSpendingRecorder interface {
		record(fcid types.FileContractID, cs api.ContractSpending)
	}

	contractSpendingRecorder struct {
		bus           Bus
		flushInterval time.Duration
		logger        *zap.SugaredLogger

		mu                          sync.Mutex
		contractSpendings           map[types.FileContractID]api.ContractSpending
		contractSpendingsFlushTimer *time.Timer
	}
)

func RecordContractSpending(ctx context.Context, fcid types.FileContractID, cs api.ContractSpending) {
	if sr, ok := ctx.Value(keyContractSpendingRecorder).(ContractSpendingRecorder); ok {
		sr.record(fcid, cs)
		return
	}
	panic("no spending recorder attached to the context") // developer error
}

func WithContractSpendingRecorder(ctx context.Context, sr ContractSpendingRecorder) context.Context {
	return context.WithValue(ctx, keyContractSpendingRecorder, sr)
}

func (w *worker) newContractSpendingRecorder() *contractSpendingRecorder {
	return &contractSpendingRecorder{
		bus:               w.bus,
		contractSpendings: make(map[types.FileContractID]api.ContractSpending),
		flushInterval:     w.busFlushInterval,
		logger:            w.logger,
	}
}

func (sr *contractSpendingRecorder) record(fcid types.FileContractID, cs api.ContractSpending) {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	// Add spending to buffer.
	sr.contractSpendings[fcid] = sr.contractSpendings[fcid].Add(cs)

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
		for fcid, cs := range sr.contractSpendings {
			records = append(records, api.ContractSpendingRecord{
				ContractID:       fcid,
				ContractSpending: cs,
			})
		}
		if err := sr.bus.RecordContractSpending(ctx, records); err != nil {
			sr.logger.Errorw(fmt.Sprintf("failed to record contract spending: %v", err))
		} else {
			sr.contractSpendings = make(map[types.FileContractID]api.ContractSpending)
		}
	}
	sr.contractSpendingsFlushTimer = nil
}

func (sr *contractSpendingRecorder) Stop() {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	if sr.contractSpendingsFlushTimer != nil {
		sr.contractSpendingsFlushTimer.Stop()
		sr.flush()
	}
}
