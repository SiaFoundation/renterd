package autopilot

import (
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/prometheus"
)

type AutopilotStateResp api.AutopilotStateResponse

func (asr AutopilotStateResp) PrometheusMetric() (metrics []prometheus.Metric) {
	metrics = append(metrics, prometheus.Metric{
		Name:  "renterd_autopilot_state_uptime",
		Value: float64(asr.UptimeMS),
	})
	metrics = append(metrics, prometheus.Metric{
		Name: "renterd_autopilot_state_configured",
		Value: func() float64 {
			if asr.Configured {
				return 1
			}
			return 0
		}(),
	})
	metrics = append(metrics, prometheus.Metric{
		Name: "renterd_autopilot_state_migrating",
		Value: func() float64 {
			if asr.Migrating {
				return 1
			}
			return 0
		}(),
	})
	metrics = append(metrics, prometheus.Metric{
		Name: "renterd_autopilot_state_pruning",
		Value: func() float64 {
			if asr.Pruning {
				return 1
			}
			return 0
		}(),
	})
	metrics = append(metrics, prometheus.Metric{
		Name: "renterd_autopilot_state_scanning",
		Value: func() float64 {
			if asr.Scanning {
				return 1
			}
			return 0
		}(),
	})
	return
}
