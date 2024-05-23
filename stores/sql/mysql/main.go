package mysql

import (
	"context"
	dsql "database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"go.sia.tech/renterd/api"
	ssql "go.sia.tech/renterd/stores/sql"

	"go.sia.tech/renterd/internal/sql"

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

// NewMainDatabase creates a new MySQL backend.
func NewMainDatabase(db *dsql.DB, log *zap.SugaredLogger, lqd, ltd time.Duration) *MainDatabase {
	store := sql.NewDB(db, log.Desugar(), deadlockMsgs, lqd, ltd)
	return &MainDatabase{
		db:  store,
		log: log,
	}
}

func (b *MainDatabase) ApplyMigration(ctx context.Context, fn func(tx sql.Tx) (bool, error)) error {
	return applyMigration(ctx, b.db, fn)
}

func (b *MainDatabase) Close() error {
	return b.db.Close()
}

func (b *MainDatabase) DB() *sql.DB {
	return b.db
}

func (b *MainDatabase) CreateMigrationTable(ctx context.Context) error {
	return createMigrationTable(ctx, b.db)
}

func (b *MainDatabase) MakeDirsForPath(ctx context.Context, tx sql.Tx, path string) (uint, error) {
	mtx := &MainDatabaseTx{tx}
	return mtx.MakeDirsForPath(ctx, path)
}

func (b *MainDatabase) Migrate(ctx context.Context) error {
	return sql.PerformMigrations(ctx, b, migrationsFs, "main", sql.MainMigrations(ctx, b, migrationsFs, b.log))
}

func (b *MainDatabase) Transaction(ctx context.Context, fn func(tx ssql.DatabaseTx) error) error {
	return b.db.Transaction(ctx, func(tx sql.Tx) error {
		return fn(&MainDatabaseTx{tx})
	})
}

func (b *MainDatabase) Version(ctx context.Context) (string, string, error) {
	return version(ctx, b.db)
}

func (tx *MainDatabaseTx) DeleteObject(ctx context.Context, bucket string, key string) (bool, error) {
	// check if the object exists first to avoid unnecessary locking for the
	// common case
	var objID uint
	err := tx.QueryRow(ctx, "SELECT id FROM objects WHERE object_id = ? AND db_bucket_id = (SELECT id FROM buckets WHERE buckets.name = ?)", key, bucket).Scan(&objID)
	if errors.Is(err, dsql.ErrNoRows) {
		return false, nil
	} else if err != nil {
		return false, err
	}

	resp, err := tx.Exec(ctx, "DELETE FROM objects WHERE id = ?", objID)
	if err != nil {
		return false, err
	} else if n, err := resp.RowsAffected(); err != nil {
		return false, err
	} else {
		return n != 0, nil
	}
}

func (tx *MainDatabaseTx) DeleteObjects(ctx context.Context, bucket string, key string, limit int64) (bool, error) {
	resp, err := tx.Exec(ctx, `
	DELETE o
	FROM objects o
	JOIN (
		SELECT id
		FROM objects
		WHERE object_id LIKE ? AND db_bucket_id = (
		    SELECT id FROM buckets WHERE buckets.name = ?
		)
		LIMIT ?
	) AS limited ON o.id = limited.id`,
		key+"%", bucket, limit)
	if err != nil {
		return false, err
	} else if n, err := resp.RowsAffected(); err != nil {
		return false, err
	} else {
		return n != 0, nil
	}
}

func (tx *MainDatabaseTx) MakeDirsForPath(ctx context.Context, path string) (uint, error) {
	insertDirStmt, err := tx.Prepare(ctx, "INSERT INTO directories (name, db_parent_id) VALUES (?, ?) ON DUPLICATE KEY UPDATE id = id")
	if err != nil {
		return 0, fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer insertDirStmt.Close()

	queryDirStmt, err := tx.Prepare(ctx, "SELECT id FROM directories WHERE name = ?")
	if err != nil {
		return 0, fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer queryDirStmt.Close()

	// Create root dir.
	dirID := uint(sql.DirectoriesRootID)
	if _, err := tx.Exec(ctx, "INSERT INTO directories (id, name, db_parent_id) VALUES (?, '/', NULL) ON DUPLICATE KEY UPDATE id = id", dirID); err != nil {
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
		if _, err := insertDirStmt.Exec(ctx, dir, dirID); err != nil {
			return 0, fmt.Errorf("failed to create directory %v: %w", dir, err)
		}
		var childID uint
		if err := queryDirStmt.QueryRow(ctx, dir).Scan(&childID); err != nil {
			return 0, fmt.Errorf("failed to fetch directory id %v: %w", dir, err)
		} else if childID == 0 {
			return 0, fmt.Errorf("dir we just created doesn't exist - shouldn't happen")
		}
		dirID = childID
	}
	return dirID, nil
}

func (tx *MainDatabaseTx) PruneEmptydirs(ctx context.Context) error {
	stmt, err := tx.Prepare(ctx, `
	DELETE
	FROM directories
	WHERE directories.id != 1
	AND NOT EXISTS (SELECT 1 FROM objects WHERE objects.db_directory_id = directories.id)
	AND NOT EXISTS (SELECT 1 FROM (SELECT 1 FROM directories AS d WHERE d.db_parent_id = directories.id) i)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for {
		res, err := stmt.Exec(ctx)
		if err != nil {
			return err
		} else if n, err := res.RowsAffected(); err != nil {
			return err
		} else if n == 0 {
			return nil
		}
	}
}

func (tx *MainDatabaseTx) PruneSlabs(ctx context.Context, limit int64) (int64, error) {
	res, err := tx.Exec(ctx, `
	DELETE FROM slabs
	WHERE id IN (
    SELECT id
    FROM (
        SELECT slabs.id
        FROM slabs
        WHERE NOT EXISTS (
            SELECT 1 FROM slices WHERE slices.db_slab_id = slabs.id
        )
        AND slabs.db_buffered_slab_id IS NULL
        LIMIT ?
    ) AS limited
	)`, limit)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (tx *MainDatabaseTx) RenameObject(ctx context.Context, bucket, keyOld, keyNew string, dirID uint, force bool) error {
	if force {
		// delete potentially existing object at destination
		if _, err := tx.DeleteObject(ctx, bucket, keyNew); err != nil {
			return fmt.Errorf("RenameObject: failed to delete object: %w", err)
		}
	} else {
		var exists bool
		if err := tx.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM objects WHERE object_id = ? AND db_bucket_id = (SELECT id FROM buckets WHERE buckets.name = ?))", keyNew, bucket).Scan(&exists); err != nil {
			return err
		} else if exists {
			return api.ErrObjectExists
		}
	}
	resp, err := tx.Exec(ctx, `UPDATE objects SET object_id = ?, db_directory_id = ? WHERE object_id = ? AND db_bucket_id = (SELECT id FROM buckets WHERE buckets.name = ?)`, keyNew, dirID, keyOld, bucket)
	if err != nil {
		return err
	} else if n, err := resp.RowsAffected(); err != nil {
		return err
	} else if n == 0 {
		return fmt.Errorf("%w: key %v", api.ErrObjectNotFound, keyOld)
	}
	return nil
}

func (tx *MainDatabaseTx) RenameObjects(ctx context.Context, bucket, prefixOld, prefixNew string, dirID uint, force bool) error {
	if force {
		_, err := tx.Exec(ctx, `
		DELETE
		FROM objects
		WHERE object_id IN (
			SELECT *
			FROM (
				SELECT CONCAT(?, SUBSTR(object_id, ?))
				FROM objects
				WHERE object_id LIKE ? 
				AND db_bucket_id = (SELECT id FROM buckets WHERE buckets.name = ?)
			) as i
		)`,
			prefixNew,
			utf8.RuneCountInString(prefixOld)+1,
			prefixOld+"%",
			bucket)
		if err != nil {
			return err
		}
	}
	resp, err := tx.Exec(ctx, `
		UPDATE objects
		SET object_id = CONCAT(?, SUBSTR(object_id, ?)),
		db_directory_id = ?
		WHERE object_id LIKE ? 
		AND db_bucket_id = (SELECT id FROM buckets WHERE buckets.name = ?)`,
		prefixNew, utf8.RuneCountInString(prefixOld)+1,
		dirID,
		prefixOld+"%",
		bucket)
	if err != nil && strings.Contains(err.Error(), "Duplicate entry") {
		return api.ErrObjectExists
	} else if err != nil {
		return err
	} else if n, err := resp.RowsAffected(); err != nil {
		return err
	} else if n == 0 {
		return fmt.Errorf("%w: prefix %v", api.ErrObjectNotFound, prefixOld)
	}
	return nil
}
