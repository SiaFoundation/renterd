package slab

import (
	"context"
	"sync"
	"time"

	"go.sia.tech/renterd/internal/consensus"
)

type contextKey string

const keyMetricsRecorder contextKey = "metricsRecorder"

type (
	TransferMetric interface {
		IsMetric()
	}

	MetricsRecorder interface {
		Metrics() []TransferMetric
		RecordMetric(m TransferMetric)
	}

	// A MetricSlabTransfer contains relevant information about an interaction with
	// a host involving uploading and/or downloading slabs.
	MetricSlabTransfer struct {
		Type      string
		Timestamp time.Time
		HostKey   consensus.PublicKey
		Duration  time.Duration
		Err       error

		Size uint64
	}

	// A MetricSlabDeletion contains relevant information about an interaction with
	// a host involving the deletion of slabs.
	MetricSlabDeletion struct {
		Type      string
		Timestamp time.Time
		HostKey   consensus.PublicKey
		Duration  time.Duration
		Err       error

		NumRoots uint64
	}

	recorder struct {
		m  []TransferMetric
		mu sync.Mutex
	}

	noopRecorder struct{}
)

func (sc MetricSlabTransfer) IsMetric() {}
func (sc MetricSlabDeletion) IsMetric() {}

func RecorderFromContext(ctx context.Context) MetricsRecorder {
	if mr, ok := ctx.Value(keyMetricsRecorder).(MetricsRecorder); ok {
		return mr
	}
	return &noopRecorder{}
}

func ContextWithMetricsRecorder(ctx context.Context) context.Context {
	return context.WithValue(ctx, keyMetricsRecorder, &recorder{})
}

func (nr *noopRecorder) Metrics() []TransferMetric     { return nil }
func (nr *noopRecorder) RecordMetric(m TransferMetric) {}

func (r *recorder) Metrics() []TransferMetric {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.m
}

func (r *recorder) RecordMetric(m TransferMetric) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.m = append(r.m, m)
}
