package prometheus

import (
	"fmt"
	"io"
	"strings"
)

// A Marshaller can be marshalled into Prometheus samples
type Marshaller interface {
	PrometheusMetric() []Metric
}

type marshallerSlice[M Marshaller] struct {
	slice []M
}

func (s marshallerSlice[M]) PrometheusMetric() []Metric {
	var metrics []Metric
	for _, m := range s.slice {
		metrics = append(metrics, m.PrometheusMetric()...)
	}
	return metrics
}

// Slice converts a slice of Prometheus marshallable objects into a
// slice of prometheus.Marshallers.
func Slice[T Marshaller](s []T) Marshaller {
	return marshallerSlice[T]{slice: s}
}

// An Encoder writes Prometheus samples to the writer
type Encoder struct {
	used bool
	sb   strings.Builder
	w    io.Writer
}

// Append marshals a Marshaller and appends it to the encoder's buffer.
func (e *Encoder) Append(m Marshaller) error {
	e.sb.Reset() // reset the string builder

	// if this is not the first, add a newline to separate the samples
	if e.used {
		e.sb.Write([]byte("\n"))
	}
	e.used = true

	for i, m := range m.PrometheusMetric() {
		if i > 0 {
			// each sample must be separated by a newline
			e.sb.Write([]byte("\n"))
		}

		if err := m.encode(&e.sb); err != nil {
			return fmt.Errorf("failed to encode metric: %v", err)
		}
	}

	if _, err := e.w.Write([]byte(e.sb.String())); err != nil {
		return fmt.Errorf("failed to write metric: %v", err)
	}
	return nil
}

// NewEncoder creates a new Prometheus encoder.
func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{
		w: w,
	}
}
