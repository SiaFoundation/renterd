package alerts

import "go.sia.tech/renterd/internal/prometheus"

// PrometheusMetric returns Prometheus samples for the hosts alerts.
func (a Alert) PrometheusMetric() (metrics []prometheus.Metric) {
	metrics = append(metrics, prometheus.Metric{
		Name: "renterd_alert",
		Labels: map[string]any{
			"id":        a.ID,
			"severity":  a.Severity.String(),
			"message":   a.Message,
			"timestamp": a.Timestamp,
		},
		Value: 1,
	})
	return
}
