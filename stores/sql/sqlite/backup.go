package sqlite

import (
	"context"
	dsql "database/sql"
	"errors"
	"fmt"
	"os"

	"github.com/mattn/go-sqlite3"
)

func sqlConn(ctx context.Context, db *dsql.DB) (c *sqlite3.SQLiteConn, err error) {
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	raw, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection: %w", err)
	}
	err = raw.Raw(func(driverConn any) error {
		var ok bool
		c, ok = driverConn.(*sqlite3.SQLiteConn)
		if !ok {
			return errors.New("connection is not a SQLiteConn")
		}
		return nil
	})
	return
}

// backupDB is a helper function that creates a backup of the source database at
// the specified path. The backup is created using the SQLite backup API, which
// is safe to use with a live database.
func backupDB(ctx context.Context, src *dsql.DB, destPath string) (err error) {
	// create the destination database
	dest, err := dsql.Open("sqlite3", destPath)
	if err != nil {
		return fmt.Errorf("failed to open destination database: %w", err)
	}
	defer func() {
		// errors are ignored
		dest.Close()
		if err != nil {
			// remove the destination file if an error occurred during backup
			os.Remove(destPath)
		}
	}()

	// initialize the source conn
	sc, err := sqlConn(ctx, src)
	if err != nil {
		return fmt.Errorf("failed to create source connection: %w", err)
	}
	defer sc.Close()

	// initialize the destination conn
	dc, err := sqlConn(ctx, dest)
	if err != nil {
		return fmt.Errorf("failed to create destination connection: %w", err)
	}
	defer dc.Close()

	// start the backup
	// NOTE: 'main' referes to the schema of the database
	backup, err := dc.Backup("main", sc, "main")
	if err != nil {
		return fmt.Errorf("failed to create backup: %w", err)
	}
	// ensure the backup is closed
	defer func() {
		if err := backup.Finish(); err != nil {
			panic(fmt.Errorf("failed to finish backup: %w", err))
		}
	}()

	for step := 1; ; step++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if done, err := backup.Step(100); err != nil {
			return fmt.Errorf("backup step %d failed: %w", step, err)
		} else if done {
			break
		}
	}
	if _, err := dest.Exec("VACUUM"); err != nil {
		return fmt.Errorf("failed to vacuum destination database: %w", err)
	}
	return nil
}

func (s *MainDatabase) Backup(ctx context.Context, dbID, destPath string) error {
	return backupDB(ctx, s.db.DB(), destPath)
}

func (s *MetricsDatabase) Backup(ctx context.Context, dbID, destPath string) error {
	return backupDB(ctx, s.db.DB(), destPath)
}

// Backup creates a backup of the database at the specified path. The backup is
// created using the SQLite backup API, which is safe to use with a
// live database.
//
// This function should be used if the database is not already open in the
// current process. If the database is already open, use Store.Backup.
func Backup(ctx context.Context, srcPath, destPath string) (err error) {
	// ensure the source file exists
	if _, err := os.Stat(srcPath); err != nil {
		return fmt.Errorf("source file does not exist: %w", err)
	}

	// prevent overwriting the destination file
	if _, err := os.Stat(destPath); !errors.Is(err, os.ErrNotExist) {
		return errors.New("destination file already exists")
	} else if destPath == "" {
		return errors.New("empty destination path")
	}

	// open a new connection to the source database. We don't want to run
	// any migrations or other operations on the source database since it
	// might be open in another process.
	src, err := dsql.Open("sqlite3", srcPath)
	if err != nil {
		return fmt.Errorf("failed to open source database: %w", err)
	}
	defer src.Close()

	return backupDB(ctx, src, destPath)
}
