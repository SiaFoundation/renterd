package worker

import (
	"context"
	"fmt"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/tracing"
)

const keyContractSpendingRecorder contextKey = "ContractSpendingRecorder"

type (
	ContractSpendingRecorder interface {
		recordContractSpending(fcid types.FileContractID, cs api.ContractSpending)
	}
)

func RecordContractSpending(ctx context.Context, fcid types.FileContractID, cs api.ContractSpending) {
	if sr, ok := ctx.Value(keyContractSpendingRecorder).(ContractSpendingRecorder); ok {
		sr.recordContractSpending(fcid, cs)
		return
	}
	panic("no spending recorder attached to the context") // developer error
}

func WithContractSpendingRecorder(ctx context.Context, sr ContractSpendingRecorder) context.Context {
	return context.WithValue(ctx, keyContractSpendingRecorder, sr)
}
func (w *worker) recordContractSpending(fcid types.FileContractID, cs api.ContractSpending) {
	w.contractSpendingsMu.Lock()
	defer w.contractSpendingsMu.Unlock()

	// Add spending to buffer.
	w.contractSpendings[fcid] = w.contractSpendings[fcid].Add(cs)

	// If a thread was scheduled to flush the buffer we are done.
	if w.contractSpendingsFlushTimer != nil {
		return
	}
	// Otherwise we schedule a flush.
	w.contractSpendingsFlushTimer = time.AfterFunc(w.busFlushInterval, func() {
		w.contractSpendingsMu.Lock()
		w.flushContractSpending()
		w.contractSpendingsMu.Unlock()
	})
}

func (w *worker) flushContractSpending() {
	if len(w.contractSpendings) > 0 {
		ctx, span := tracing.Tracer.Start(context.Background(), "worker: flushContractSpending")
		defer span.End()
		records := make([]api.ContractSpendingRecord, 0, len(w.contractSpendings))
		for fcid, cs := range w.contractSpendings {
			records = append(records, api.ContractSpendingRecord{
				ContractID:       fcid,
				ContractSpending: cs,
			})
		}
		if err := w.bus.RecordContractSpending(ctx, records); err != nil {
			w.logger.Errorw(fmt.Sprintf("failed to record contract spending: %v", err))
		} else {
			w.contractSpendings = make(map[types.FileContractID]api.ContractSpending)
		}
	}
	w.contractSpendingsFlushTimer = nil
}
