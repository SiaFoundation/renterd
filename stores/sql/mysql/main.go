package mysql

import (
	"context"
	dsql "database/sql"
	"time"

	"go.sia.tech/renterd/internal/sql"
	"go.sia.tech/renterd/internal/utils"

	"go.uber.org/zap"
)

type MainDatabase struct {
	db  *sql.DB
	log *zap.SugaredLogger
}

// NewMainDatabase creates a new MySQL backend.
func NewMainDatabase(db *dsql.DB, log *zap.SugaredLogger, lqd, ltd time.Duration) *MainDatabase {
	store := sql.NewDB(db, log.Desugar(), "Deadlock found when trying to get lock", lqd, ltd)
	return &MainDatabase{
		db:  store,
		log: log,
	}
}

func (b *MainDatabase) Close() error {
	return b.db.Close()
}

func (b *MainDatabase) Migrate() error {
	dbIdentifier := "main"
	return performMigrations(b.db, dbIdentifier, []migration{
		{
			ID:      "00001_init",
			Migrate: func(tx sql.Tx) error { return sql.ErrRunV072 },
		},
		{
			ID: "00001_object_metadata",
			Migrate: func(tx sql.Tx) error {
				return performMigration(tx, dbIdentifier, "00001_object_metadata", b.log)
			},
		},
		{
			ID: "00002_prune_slabs_trigger",
			Migrate: func(tx sql.Tx) error {
				err := performMigration(tx, dbIdentifier, "00002_prune_slabs_trigger", b.log)
				if utils.IsErr(err, sql.ErrMySQLNoSuperPrivilege) {
					b.log.Warn("migration 00002_prune_slabs_trigger requires the user to have the SUPER privilege to register triggers")
				}
				return err
			},
		},
		{
			ID: "00003_idx_objects_size",
			Migrate: func(tx sql.Tx) error {
				return performMigration(tx, dbIdentifier, "00003_idx_objects_size", b.log)
			},
		},
		{
			ID: "00004_prune_slabs_cascade",
			Migrate: func(tx sql.Tx) error {
				return performMigration(tx, dbIdentifier, "00004_prune_slabs_cascade", b.log)
			},
		},
		{
			ID: "00005_zero_size_object_health",
			Migrate: func(tx sql.Tx) error {
				return performMigration(tx, dbIdentifier, "00005_zero_size_object_health", b.log)
			},
		},
		{
			ID: "00006_idx_objects_created_at",
			Migrate: func(tx sql.Tx) error {
				return performMigration(tx, dbIdentifier, "00006_idx_objects_created_at", b.log)
			},
		},
		{
			ID: "00007_host_checks",
			Migrate: func(tx sql.Tx) error {
				return performMigration(tx, dbIdentifier, "00007_host_checks", b.log)
			},
		},
	}, b.log)
}

func (b *MainDatabase) Version(_ context.Context) (string, string, error) {
	return version(b.db)
}
