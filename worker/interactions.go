package worker

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"go.sia.tech/jape"
	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/tracing"
)

const (
	keyInteractionRecorder contextKey = "InteractionRecorder"
)

type (
	InteractionRecorder interface {
		RecordHostScan(...hostdb.HostScan)
		RecordPriceTableUpdate(...hostdb.PriceTableUpdate)
	}
)

var _ InteractionRecorder = &worker{}

func interactionMiddleware(ir InteractionRecorder, routes map[string]jape.Handler) map[string]jape.Handler {
	for route, handler := range routes {
		routes[route] = jape.Adapt(func(h http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				ctx := context.WithValue(r.Context(), keyInteractionRecorder, ir)
				h.ServeHTTP(w, r.WithContext(ctx))
			})
		})(handler)
	}
	return routes
}

func InteractionRecorderFromContext(ctx context.Context) InteractionRecorder {
	ir, ok := ctx.Value(keyInteractionRecorder).(InteractionRecorder)
	if !ok {
		panic("no interaction recorder attached to the context") // developer error
	}
	return ir
}

func (w *worker) RecordHostScan(scans ...hostdb.HostScan) {
	w.interactionsMu.Lock()
	defer w.interactionsMu.Unlock()

	w.interactionsScans = append(w.interactionsScans, scans...)
	w.tryFlushInteractionsBuffer()
}

func (w *worker) RecordPriceTableUpdate(ptUpdates ...hostdb.PriceTableUpdate) {
	w.interactionsMu.Lock()
	defer w.interactionsMu.Unlock()

	w.interactionsPriceTableUpdates = append(w.interactionsPriceTableUpdates, ptUpdates...)
	w.tryFlushInteractionsBuffer()
}

func (w *worker) tryFlushInteractionsBuffer() {
	// If a thread was scheduled to flush the buffer we are done.
	if w.interactionsFlushTimer != nil {
		return
	}

	// Otherwise we schedule a flush.
	w.interactionsFlushTimer = time.AfterFunc(w.busFlushInterval, func() {
		w.interactionsMu.Lock()
		w.flushInteractions()
		w.interactionsMu.Unlock()
	})
}

// flushInteractions flushes the worker's interaction buffer to the bus.
func (w *worker) flushInteractions() {
	if len(w.interactionsScans) > 0 {
		ctx, span := tracing.Tracer.Start(w.shutdownCtx, "worker: recordHostScans")
		defer span.End()
		if err := w.bus.RecordHostScans(ctx, w.interactionsScans); err != nil {
			w.logger.Errorw(fmt.Sprintf("failed to record scans: %v", err))
		} else {
			w.interactionsScans = nil
		}
	}
	if len(w.interactionsPriceTableUpdates) > 0 {
		ctx, span := tracing.Tracer.Start(w.shutdownCtx, "worker: recordPriceTableUpdates")
		defer span.End()
		if err := w.bus.RecordPriceTables(ctx, w.interactionsPriceTableUpdates); err != nil {
			w.logger.Errorw(fmt.Sprintf("failed to record price table updates: %v", err))
		} else {
			w.interactionsPriceTableUpdates = nil
		}
	}
	w.interactionsFlushTimer = nil
}

func isSuccessfulInteraction(err error) bool {
	// No error always means success.
	if err == nil {
		return true
	}
	// List of errors that are considered successful interactions.
	if isInsufficientFunds(err) {
		return true
	}
	if isBalanceInsufficient(err) {
		return true
	}
	return false
}
