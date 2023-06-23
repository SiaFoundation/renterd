package metrics

import (
	"context"
	"time"

	"go.sia.tech/core/types"
)

// A Metric contains metadata pertaining to a particular operation.
type Metric interface {
	HostKey() types.PublicKey
	Result() interface{}
	IsSuccess() bool
	Timestamp() time.Time
	Type() string
}

// A MetricsRecorder records metrics.
type MetricsRecorder interface {
	RecordMetric(m Metric)
}

type contextKey string

const keyMetricsRecorder contextKey = "MetricsRecorder"

// WithRecorder stores mr in ctx.
func WithRecorder(ctx context.Context, mr MetricsRecorder) context.Context {
	return context.WithValue(ctx, keyMetricsRecorder, mr)
}

// Record records m using the MetricsRecorder stored in ctx, if it exists.
func Record(ctx context.Context, m Metric) {
	if mr, ok := ctx.Value(keyMetricsRecorder).(MetricsRecorder); ok {
		mr.RecordMetric(m)
	}
}
