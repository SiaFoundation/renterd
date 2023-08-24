package worker

import (
	"context"
	"fmt"
	"sync"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/metrics"
	"go.sia.tech/renterd/tracing"
)

func errToStr(err error) string {
	if err != nil {
		return err.Error()
	}
	return ""
}

// recordInteractions adds some interactions to the worker's interaction buffer
// which is periodically flushed to the bus.
func (w *worker) recordInteractions(scans []hostdb.HostScan, priceTableUpdates []hostdb.PriceTableUpdate) {
	w.interactionsMu.Lock()
	defer w.interactionsMu.Unlock()

	// Append interactions to buffer.
	w.interactionsScans = append(w.interactionsScans, scans...)
	w.interactionsPriceTableUpdates = append(w.interactionsPriceTableUpdates, priceTableUpdates...)

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
		ctx, span := tracing.Tracer.Start(context.Background(), "worker: recordHostScans")
		defer span.End()
		if err := w.bus.RecordHostScans(ctx, w.interactionsScans); err != nil {
			w.logger.Errorw(fmt.Sprintf("failed to record scans: %v", err))
		} else {
			w.interactionsScans = nil
		}
	}
	if len(w.interactionsPriceTableUpdates) > 0 {
		ctx, span := tracing.Tracer.Start(context.Background(), "worker: recordPriceTableUpdates")
		defer span.End()
		if err := w.bus.RecordPriceTables(ctx, w.interactionsPriceTableUpdates); err != nil {
			w.logger.Errorw(fmt.Sprintf("failed to record price table updates: %v", err))
		} else {
			w.interactionsPriceTableUpdates = nil
		}
	}
	w.interactionsFlushTimer = nil
}

// recordPriceTableUpdate records a price table metric.
func recordPriceTableUpdate(ctx context.Context, siamuxAddr string, hostKey types.PublicKey, pt *hostdb.HostPriceTable, err *error) func() {
	startTime := time.Now()
	return func() {
		now := time.Now()
		metrics.Record(ctx, MetricPriceTableUpdate{
			metricCommon: metricCommon{
				address:   siamuxAddr,
				hostKey:   hostKey,
				timestamp: now,
				elapsed:   now.Sub(startTime),
				err:       *err,
			},
			pt: *pt,
		})
	}
}

// ephemeralMetricsRecorder can be used to record metrics in memory.
type ephemeralMetricsRecorder struct {
	ms []metrics.Metric
	mu sync.Mutex
}

func (mr *ephemeralMetricsRecorder) RecordMetric(m metrics.Metric) {
	mr.mu.Lock()
	defer mr.mu.Unlock()
	mr.ms = append(mr.ms, m)
}

func (mr *ephemeralMetricsRecorder) interactions() []any {
	// TODO: merge/filter metrics?
	var his []any
	mr.mu.Lock()
	defer mr.mu.Unlock()
	for _, m := range mr.ms {
		his = append(his, metricToInteraction(m))
	}
	return his
}

// metricCommon contains the common fields of all metrics.
type metricCommon struct {
	hostKey   types.PublicKey
	address   string
	timestamp time.Time
	elapsed   time.Duration
	err       error
}

func (m metricCommon) commonResult() hostdb.MetricResultCommon {
	return hostdb.MetricResultCommon{
		Address:   m.address,
		Timestamp: m.timestamp,
		Elapsed:   m.elapsed,
	}
}
func (m metricCommon) HostKey() types.PublicKey { return m.hostKey }
func (m metricCommon) Timestamp() time.Time     { return m.timestamp }

func (m metricCommon) IsSuccess() bool {
	return isSuccessfulInteraction(m.err)
}

// MetricPriceTableUpdate is a metric that contains the result of fetching a
// price table.
type MetricPriceTableUpdate struct {
	metricCommon
	pt hostdb.HostPriceTable
}

func (m MetricPriceTableUpdate) Result() interface{} {
	cr := m.commonResult()
	er := hostdb.ErrorResult{Error: errToStr(m.err)}
	if m.err != nil {
		return struct {
			hostdb.MetricResultCommon
			hostdb.ErrorResult
		}{cr, er}
	} else {
		return struct {
			hostdb.MetricResultCommon
			hostdb.PriceTableUpdateResult
		}{cr, hostdb.PriceTableUpdateResult{ErrorResult: er, PriceTable: m.pt}}
	}
}

func (m MetricPriceTableUpdate) Type() string {
	return hostdb.InteractionTypePriceTableUpdate
}

// MetricHostScan is a metric that contains the result of a host scan.
type MetricHostScan struct {
	metricCommon
	pt       rhpv3.HostPriceTable
	settings rhpv2.HostSettings
}

func (m MetricHostScan) Result() interface{} {
	cr := m.commonResult()
	er := hostdb.ErrorResult{Error: errToStr(m.err)}
	if m.err != nil {
		return struct {
			hostdb.MetricResultCommon
			hostdb.ErrorResult
		}{cr, er}
	} else {
		return struct {
			hostdb.MetricResultCommon
			hostdb.ScanResult
		}{cr, hostdb.ScanResult{ErrorResult: er, PriceTable: m.pt, Settings: m.settings}}
	}
}

func (m MetricHostScan) Type() string {
	return hostdb.InteractionTypeScan
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

func metricToInteraction(m metrics.Metric) any {
	return nil
}
