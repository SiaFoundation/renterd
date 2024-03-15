package worker

import (
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/prometheus"
)

type MemoryResp api.MemoryResponse

func (m MemoryResp) PrometheusMetric() (metrics []prometheus.Metric) {
	metrics = append(metrics, prometheus.Metric{
		Name:  "renterd_worker_memory_download_available",
		Value: float64(m.Download.Available),
	})
	metrics = append(metrics, prometheus.Metric{
		Name:  "renterd_worker_memory_download_total",
		Value: float64(m.Download.Total),
	})
	metrics = append(metrics, prometheus.Metric{
		Name:  "renterd_worker_memory_upload_available",
		Value: float64(m.Upload.Available),
	})
	metrics = append(metrics, prometheus.Metric{
		Name:  "renterd_worker_memory_upload_total",
		Value: float64(m.Upload.Total),
	})
	return
}

type DownloadStatsResp api.DownloadStatsResponse

func (m DownloadStatsResp) PrometheusMetric() (metrics []prometheus.Metric) {
	metrics = append(metrics, prometheus.Metric{
		Name:  "renterd_worker_stats_avgdownloadspeedmbps",
		Value: m.AvgDownloadSpeedMBPS,
	})
	metrics = append(metrics, prometheus.Metric{
		Name:  "renterd_worker_stats_avgoverdrivepct_download",
		Value: m.AvgOverdrivePct,
	})
	metrics = append(metrics, prometheus.Metric{
		Name:  "renterd_worker_stats_healthydownloaders",
		Value: float64(m.HealthyDownloaders),
	})
	metrics = append(metrics, prometheus.Metric{
		Name:  "renterd_worker_stats_numdownloaders",
		Value: float64(m.NumDownloaders),
	})
	return
}

type UploadStatsResp api.UploadStatsResponse

func (m UploadStatsResp) PrometheusMetric() (metrics []prometheus.Metric) {
	metrics = append(metrics, prometheus.Metric{
		Name:  "renterd_worker_stats_avgslabuploadspeedmbps",
		Value: m.AvgSlabUploadSpeedMBPS,
	})
	metrics = append(metrics, prometheus.Metric{
		Name:  "renterd_worker_stats_avgoverdrivepct_upload",
		Value: m.AvgOverdrivePct,
	})
	metrics = append(metrics, prometheus.Metric{
		Name:  "renterd_worker_stats_healthyuploaders",
		Value: float64(m.HealthyUploaders),
	})
	metrics = append(metrics, prometheus.Metric{
		Name:  "renterd_worker_stats_numuploaders",
		Value: float64(m.NumUploaders),
	})
	return
}
