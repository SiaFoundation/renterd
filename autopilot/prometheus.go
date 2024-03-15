package autopilot

import (
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/prometheus"
)

type AutopilotStateResp api.AutopilotStateResponse

func (asr AutopilotStateResp) PrometheusMetric() (metrics []prometheus.Metric) {
	labels := map[string]any{
		"network":   asr.Network,
		"version":   asr.Version,
		"commit":    asr.Commit,
		"os":        asr.OS,
		"buildTime": asr.BuildTime.String(),
	}
	// labels := fmt.Sprintf(`network="%s", version="%s", commit="%s", os="%s", buildTime="%s"`,
	// build.NetworkName(), build.Version(), build.Commit(), runtime.GOOS, api.TimeRFC3339(build.BuildTime()))
	metrics = append(metrics, prometheus.Metric{
		Name:   "renterd_autopilot_state_uptime",
		Labels: labels,
		Value:  float64(asr.UptimeMS),
	})
	metrics = append(metrics, prometheus.Metric{
		Name:   "renterd_autopilot_state_configured",
		Labels: labels,
		Value: func() float64 {
			if asr.Configured {
				return 1
			}
			return 0
		}(),
	})
	metrics = append(metrics, prometheus.Metric{
		Name:   "renterd_autopilot_state_migrating",
		Labels: labels,
		Value: func() float64 {
			if asr.Migrating {
				return 1
			}
			return 0
		}(),
	})
	metrics = append(metrics, prometheus.Metric{
		Name:   "renterd_autopilot_state_pruning",
		Labels: labels,
		Value: func() float64 {
			if asr.Pruning {
				return 1
			}
			return 0
		}(),
	})
	metrics = append(metrics, prometheus.Metric{
		Name:   "renterd_autopilot_state_scanning",
		Labels: labels,
		Value: func() float64 {
			if asr.Scanning {
				return 1
			}
			return 0
		}(),
	})
	return
}
