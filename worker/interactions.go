package worker

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"go.sia.tech/jape"
	"go.sia.tech/renterd/hostdb"
	"go.uber.org/zap"
)

const (
	keyInteractionRecorder contextKey = "InteractionRecorder"
)

type (
	HostInteractionRecorder interface {
		RecordHostScan(...hostdb.HostScan)
		RecordPriceTableUpdate(...hostdb.PriceTableUpdate)
		Stop(context.Context)
	}

	hostInteractionRecorder struct {
		flushInterval time.Duration

		bus    Bus
		logger *zap.SugaredLogger

		mu                sync.Mutex
		hostScans         []hostdb.HostScan
		priceTableUpdates []hostdb.PriceTableUpdate

		flushCtx   context.Context
		flushTimer *time.Timer
	}
)

var (
	_ HostInteractionRecorder = (*hostInteractionRecorder)(nil)
)

func interactionMiddleware(ir HostInteractionRecorder, routes map[string]jape.Handler) map[string]jape.Handler {
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

func (w *worker) initHostInteractionRecorder(flushInterval time.Duration) {
	if w.hostInteractionRecorder != nil {
		panic("HostInteractionRecorder already initialized") // developer error
	}
	w.hostInteractionRecorder = &hostInteractionRecorder{
		bus:    w.bus,
		logger: w.logger,

		flushCtx:      w.shutdownCtx,
		flushInterval: flushInterval,

		hostScans:         make([]hostdb.HostScan, 0),
		priceTableUpdates: make([]hostdb.PriceTableUpdate, 0),
	}
}

func (r *hostInteractionRecorder) RecordHostScan(scans ...hostdb.HostScan) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.hostScans = append(r.hostScans, scans...)
	r.tryFlushInteractionsBuffer()
}

func (r *hostInteractionRecorder) RecordPriceTableUpdate(ptUpdates ...hostdb.PriceTableUpdate) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.priceTableUpdates = append(r.priceTableUpdates, ptUpdates...)
	r.tryFlushInteractionsBuffer()
}

func (r *hostInteractionRecorder) Stop(ctx context.Context) {
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
	if len(r.hostScans) > 0 {
		r.logger.Errorw(fmt.Sprintf("failed to record %d host scans on worker shutdown", len(r.hostScans)))
	}
	if len(r.priceTableUpdates) > 0 {
		r.logger.Errorw(fmt.Sprintf("failed to record %d price table updates on worker shutdown", len(r.priceTableUpdates)))
	}
	r.mu.Unlock()
}

func (r *hostInteractionRecorder) flush() {
	r.mu.Lock()
	defer r.mu.Unlock()

	// NOTE: don't bother flushing if the context is cancelled, we can safely
	// ignore the buffered scans and price tables since we'll flush on shutdown
	// and log in case we weren't able to flush all interactions to the bus
	select {
	case <-r.flushCtx.Done():
		r.flushTimer = nil
		return
	default:
	}

	if len(r.hostScans) > 0 {
		if err := r.bus.RecordHostScans(r.flushCtx, r.hostScans); err != nil {
			r.logger.Errorw(fmt.Sprintf("failed to record scans: %v", err))
		} else if err == nil {
			r.hostScans = nil
		}
	}
	if len(r.priceTableUpdates) > 0 {
		if err := r.bus.RecordPriceTables(r.flushCtx, r.priceTableUpdates); err != nil {
			r.logger.Errorw(fmt.Sprintf("failed to record price table updates: %v", err))
		} else if err == nil {
			r.priceTableUpdates = nil
		}
	}
	r.flushTimer = nil
}

func (r *hostInteractionRecorder) tryFlushInteractionsBuffer() {
	if r.flushTimer == nil {
		r.flushTimer = time.AfterFunc(r.flushInterval, r.flush)
	}
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
