package mysql

import (
	"context"
	"database/sql"
	"time"

	isql "go.sia.tech/renterd/internal/sql"

	"go.uber.org/zap"
)

type MetricsDatabase struct {
	db *isql.DB
}

// NewMetricsDatabase creates a new MySQL backend.
func NewMetricsDatabase(db *sql.DB, log *zap.SugaredLogger, lqd, ltd time.Duration) *MetricsDatabase {
	store := isql.NewDB(db, log.Desugar(), "Deadlock found when trying to get lock", lqd, ltd)
	return &MetricsDatabase{
		db: store,
	}
}

func (b *MetricsDatabase) Close() error {
	return b.db.Close()
}

func (b *MetricsDatabase) Migrate() error {
	panic("implement me")
}

func (b *MetricsDatabase) Version(_ context.Context) (string, string, error) {
	var version string
	if err := b.db.QueryRow("select version()").Scan(&version); err != nil {
		return "", "", err
	}
	return "MySQL", version, nil
}
