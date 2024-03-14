package prometheus

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

// A Metric is a Prometheus metric.
type Metric struct {
	Name      string
	Labels    map[string]any
	Value     float64
	Timestamp time.Time
}

// encode encodes a Metric into a Prometheus metric string.
func (m *Metric) encode(sb *strings.Builder) error {
	sb.WriteString(m.Name)

	// write optional labels
	if len(m.Labels) > 0 {
		sb.WriteString("{")
		n := len(m.Labels)
		for k, v := range m.Labels {
			sb.WriteString(k)
			sb.WriteString(`="`)
			switch v := v.(type) {
			case string:
				sb.WriteString(v)
			case []byte:
				sb.Write(v)
			case int:
				sb.WriteString(strconv.Itoa(v))
			case int64:
				sb.WriteString(strconv.FormatInt(v, 10))
			case float64:
				sb.WriteString(strconv.FormatFloat(v, 'f', -1, 64))
			case bool:
				sb.WriteString(strconv.FormatBool(v))
			case fmt.Stringer:
				sb.WriteString(v.String())
			default:
				return fmt.Errorf("unsupported label value %T", v)
			}
			sb.WriteString(`"`)

			if n > 1 {
				sb.WriteString(",")
			}
			n--
		}
		sb.WriteString("}")
	}

	// write value
	sb.WriteString(" ")
	sb.WriteString(strconv.FormatFloat(m.Value, 'f', -1, 64))

	// write optional timestamp
	if !m.Timestamp.IsZero() {
		sb.WriteString(" ")
		sb.WriteString(strconv.FormatInt(m.Timestamp.UnixMilli(), 10))
	}
	return nil
}

// A Marshaller can be marshalled into Prometheus samples
type Marshaller interface {
	PrometheusMetric() []Metric
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
