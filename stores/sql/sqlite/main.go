package sqlite

import (
	"context"
	dsql "database/sql"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

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

func (b *MainDatabase) ApplyMigration(fn func(tx sql.Tx) error) error {
	return applyMigration(b.db, fn)
}

func (b *MainDatabase) Close() error {
	return b.db.Close()
}

func (b *MainDatabase) DB() *sql.DB {
	return b.db
}

func (b *MainDatabase) CreateMigrationTable() error {
	return createMigrationTable(b.db)
}

func (b *MainDatabase) MakeDirsForPath(tx sql.Tx, path string) (uint, error) {
	insertDirStmt, err := tx.Prepare("INSERT INTO directories (name, db_parent_id) VALUES (?, ?) ON CONFLICT(name) DO NOTHING")
	if err != nil {
		return 0, fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer insertDirStmt.Close()

	queryDirStmt, err := tx.Prepare("SELECT id FROM directories WHERE name = ?")
	if err != nil {
		return 0, fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer queryDirStmt.Close()

	// Create root dir.
	dirID := uint(sql.DirectoriesRootID)
	if _, err := insertDirStmt.Exec('/', dirID); err != nil {
		return 0, fmt.Errorf("failed to create root directory: %w", err)
	}

	// Create remaining directories.
	path = strings.TrimSuffix(path, "/")
	if path == "/" {
		return dirID, nil
	}
	for i := 0; i < utf8.RuneCountInString(path); i++ {
		if path[i] != '/' {
			continue
		}
		dir := path[:i+1]
		if dir == "/" {
			continue
		}
		if _, err := insertDirStmt.Exec(dir, dirID); err != nil {
			return 0, fmt.Errorf("failed to create directory %v: %w", dir, err)
		}
		var childID uint
		if err := queryDirStmt.QueryRow(dir).Scan(&childID); err != nil {
			return 0, fmt.Errorf("failed to fetch directory id %v: %w", dir, err)
		} else if childID == 0 {
			return 0, fmt.Errorf("dir we just created doesn't exist - shouldn't happen")
		}
		dirID = childID
	}
	return dirID, nil
}

func (b *MainDatabase) Migrate() error {
	return sql.PerformMigrations(b, migrationsFs, "main", sql.MainMigrations(b, migrationsFs, b.log))
}

func (b *MainDatabase) Version(_ context.Context) (string, string, error) {
	return version(b.db)
}
