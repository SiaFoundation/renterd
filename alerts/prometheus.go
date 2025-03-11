package alerts

import "go.sia.tech/renterd/v2/internal/prometheus"

// PrometheusMetric implements prometheus.Marshaller.
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

// PrometheusMetric implements prometheus.Marshaller.
func (a AlertsResponse) PrometheusMetric() (metrics []prometheus.Metric) {
	metrics = prometheus.Slice(a.Alerts).PrometheusMetric()
	return
}
