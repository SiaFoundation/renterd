package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
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
func (w *worker) recordInteractions(interactions []hostdb.Interaction) {
	w.interactionsMu.Lock()
	defer w.interactionsMu.Unlock()

	// Append interactions to buffer.
	w.interactions = append(w.interactions, interactions...)

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
	if len(w.interactions) > 0 {
		ctx, span := tracing.Tracer.Start(context.Background(), "worker: flushInteractions")
		defer span.End()
		if err := w.bus.RecordInteractions(ctx, w.interactions); err != nil {
			w.logger.Errorw(fmt.Sprintf("failed to record interactions: %v", err))
		} else {
			w.interactions = nil
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

// recordScan records a scan metric.
func recordScan(mr metrics.MetricsRecorder, elapsed time.Duration, hostIP string, hostKey types.PublicKey, pt rhpv3.HostPriceTable, settings rhpv2.HostSettings, err error) {
	mr.RecordMetric(MetricHostScan{
		metricCommon: metricCommon{
			address:   hostIP,
			hostKey:   hostKey,
			timestamp: time.Now(),
			elapsed:   elapsed,
			err:       err,
		},
		pt:       pt,
		settings: settings,
	})
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

func (mr *ephemeralMetricsRecorder) interactions() []hostdb.Interaction {
	// TODO: merge/filter metrics?
	var his []hostdb.Interaction
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

type metricResultCommon struct {
	Address   string        `json:"address"`
	Timestamp time.Time     `json:"timestamp"`
	Elapsed   time.Duration `json:"elapsed"`
}

func (m metricCommon) commonResult() metricResultCommon {
	return metricResultCommon{
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

// MetricHostDial is a metric that contains the result of a dial attempt.
type MetricHostDial struct {
	metricCommon
}

func (m MetricHostDial) Result() interface{} {
	cr := m.commonResult()
	er := hostdb.ErrorResult{Error: errToStr(m.err)}
	if m.err != nil {
		return struct {
			metricResultCommon
			hostdb.ErrorResult
		}{cr, er}
	} else {
		return struct {
			metricResultCommon
		}{cr}
	}
}

func (m MetricHostDial) Type() string { return "dial" }

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
			metricResultCommon
			hostdb.ErrorResult
		}{cr, er}
	} else {
		return struct {
			metricResultCommon
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
			metricResultCommon
			hostdb.ErrorResult
		}{cr, er}
	} else {
		return struct {
			metricResultCommon
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
	if errors.Is(err, ErrInsufficientFunds) || strings.Contains(err.Error(), ErrInsufficientFunds.Error()) {
		return true
	}
	if isBalanceInsufficient(err) {
		return true
	}
	return false
}

func metricToInteraction(m metrics.Metric) hostdb.Interaction {
	res, _ := json.Marshal(m.Result())
	return hostdb.Interaction{
		Host:      m.HostKey(),
		Result:    res,
		Success:   m.IsSuccess(),
		Timestamp: m.Timestamp(),
		Type:      m.Type(),
	}
}
