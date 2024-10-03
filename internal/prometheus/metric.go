package prometheus

import (
	"fmt"
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
