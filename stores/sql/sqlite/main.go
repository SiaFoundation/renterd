package sqlite

import (
	"context"
	dsql "database/sql"
	"time"

	"go.sia.tech/renterd/internal/sql"

	"go.uber.org/zap"
)

type MainDatabase struct {
	db  *sql.DB
	log *zap.SugaredLogger
}

// NewMainDatabase creates a new SQLite backend.
func NewMainDatabase(db *dsql.DB, log *zap.SugaredLogger, lqd, ltd time.Duration) *MainDatabase {
	store := sql.NewDB(db, log.Desugar(), "database is locked", lqd, ltd)
	return &MainDatabase{
		db:  store,
		log: log,
	}
}

func (b *MainDatabase) Close() error {
	return b.db.Close()
}

func (b *MainDatabase) Migrate() error {
	return performMigrations(b.db, "main", sql.MainMigrations(migrationsFs, b.log), b.log)
}

func (b *MainDatabase) Version(_ context.Context) (string, string, error) {
	return version(b.db)
}
