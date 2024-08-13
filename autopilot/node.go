package autopilot

import (
	"context"
	"net/http"

	"go.sia.tech/renterd/config"
	"go.uber.org/zap"
)

func NewNode(cfg config.Autopilot, b Bus, workers []Worker, l *zap.Logger) (http.Handler, func(), func(context.Context) error, error) {
	ap, err := New(cfg.ID, b, workers, l, cfg.Heartbeat, cfg.ScannerInterval, cfg.ScannerBatchSize, cfg.ScannerNumThreads, cfg.MigrationHealthCutoff, cfg.AccountsRefillInterval, cfg.RevisionSubmissionBuffer, cfg.MigratorParallelSlabsPerWorker, cfg.RevisionBroadcastInterval)
	if err != nil {
		return nil, nil, nil, err
	}
	return ap.Handler(), ap.Run, ap.Shutdown, nil
}
