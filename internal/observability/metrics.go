package observability

import (
	"context"
	"sync"
)

type contextKey string

const keyMetricsRecorder contextKey = "metricsRecorder"

type Metric interface {
	IsMetric()
}

type MetricsRecorder interface {
	Metrics() []Metric
	RecordMetric(m Metric)
}

type recorder struct {
	m  []Metric
	mu sync.Mutex
}

func NewRecorder() MetricsRecorder {
	return &recorder{
		m: make([]Metric, 0),
	}
}

func RecorderFromContext(ctx context.Context) MetricsRecorder {
	r := ctx.Value(keyMetricsRecorder)
	mr, ok := r.(MetricsRecorder)
	if !ok {
		return nil
	}
	return mr
}

func ContextWithMetricsRecorder(ctx context.Context) context.Context {
	return context.WithValue(ctx, keyMetricsRecorder, NewRecorder())
}

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
