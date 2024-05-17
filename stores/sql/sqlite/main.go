package sqlite

import (
	"context"
	dsql "database/sql"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/sql"
	ssql "go.sia.tech/renterd/stores/sql"

	"go.uber.org/zap"
)

type (
	MainDatabase struct {
		db  *sql.DB
		log *zap.SugaredLogger
	}

	MainDatabaseTx struct {
		sql.Tx
	}
)

// NewMainDatabase creates a new SQLite backend.
func NewMainDatabase(db *dsql.DB, log *zap.SugaredLogger, lqd, ltd time.Duration) *MainDatabase {
	store := sql.NewDB(db, log.Desugar(), deadlockMsgs, lqd, ltd)
	return &MainDatabase{
		db:  store,
		log: log,
	}
}

func (b *MainDatabase) ApplyMigration(fn func(tx sql.Tx) (bool, error)) error {
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
	mtx := &MainDatabaseTx{tx}
	return mtx.MakeDirsForPath(path)
}

func (b *MainDatabase) Migrate() error {
	return sql.PerformMigrations(b, migrationsFs, "main", sql.MainMigrations(b, migrationsFs, b.log))
}

func (b *MainDatabase) Transaction(fn func(tx ssql.DatabaseTx) error) error {
	return b.db.Transaction(func(tx sql.Tx) error {
		return fn(&MainDatabaseTx{tx})
	})
}

func (b *MainDatabase) Version(_ context.Context) (string, string, error) {
	return version(b.db)
}

func (tx *MainDatabaseTx) DeleteObject(bucket string, key string) (bool, error) {
	resp, err := tx.Exec("DELETE FROM objects WHERE object_id = ? AND db_bucket_id = (SELECT id FROM buckets WHERE buckets.name = ?)", key, bucket)
	if err != nil {
		return false, err
	} else if n, err := resp.RowsAffected(); err != nil {
		return false, err
	} else {
		return n != 0, nil
	}
}

func (tx *MainDatabaseTx) MakeDirsForPath(path string) (uint, error) {
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
	if _, err := tx.Exec("INSERT INTO directories (id, name, db_parent_id) VALUES (?, '/', NULL) ON CONFLICT(id) DO NOTHING", dirID); err != nil {
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

func (tx *MainDatabaseTx) RenameObject(bucket, keyOld, keyNew string, dirID uint) error {
	var exists bool
	if err := tx.QueryRow("SELECT EXISTS (SELECT 1 FROM objects WHERE object_id = ? AND db_bucket_id = (SELECT id FROM buckets WHERE buckets.name = ?))", keyNew, bucket).Scan(&exists); err != nil {
		return err
	} else if exists {
		return api.ErrObjectExists
	}
	resp, err := tx.Exec(`UPDATE objects SET object_id = ?, db_directory_id = ? WHERE object_id = ? AND db_bucket_id = (SELECT id FROM buckets WHERE buckets.name = ?)`, keyNew, dirID, keyOld, bucket)
	if err != nil {
		return err
	} else if n, err := resp.RowsAffected(); err != nil {
		return err
	} else if n == 0 {
		return fmt.Errorf("%w: key %v", api.ErrObjectNotFound, keyOld)
	}
	return nil
}
