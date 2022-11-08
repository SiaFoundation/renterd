package observability

import (
	"context"
	"sync"
)

type contextKey string

const keyMetricsRecorder contextKey = "metricsRecorder"

type (
	Metric interface {
		IsMetric()
	}

	MetricsRecorder interface {
		Metrics() []Metric
		RecordMetric(m Metric)
	}
)

type (
	recorder struct {
		m  []Metric
		mu sync.Mutex
	}

	noopRecorder struct{}
)

func RecorderFromContext(ctx context.Context) MetricsRecorder {
	if mr, ok := ctx.Value(keyMetricsRecorder).(MetricsRecorder); ok {
		return mr
	}
	return &noopRecorder{}
}

func ContextWithMetricsRecorder(ctx context.Context) context.Context {
	return context.WithValue(ctx, keyMetricsRecorder, &recorder{})
}

func (nr *noopRecorder) Metrics() []Metric     { return nil }
func (nr *noopRecorder) RecordMetric(m Metric) {}

func (r *recorder) Metrics() []Metric {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.m
}

func (r *recorder) RecordMetric(m Metric) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.m = append(r.m, m)
}
