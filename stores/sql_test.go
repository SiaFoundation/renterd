package stores

import (
	"context"
	dsql "database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/alerts"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/config"
	isql "go.sia.tech/renterd/internal/sql"
	"go.sia.tech/renterd/object"
	sql "go.sia.tech/renterd/stores/sql"
	"go.sia.tech/renterd/stores/sql/mysql"
	"go.sia.tech/renterd/stores/sql/sqlite"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

const (
	testMimeType = "application/octet-stream"
	testETag     = "d34db33f"
)

var (
	testMetadata = api.ObjectUserMetadata{
		"foo": "bar",
		"baz": "qux",
	}
)

type testSQLStore struct {
	cfg testSQLStoreConfig
	t   *testing.T
	*SQLStore
}

type testSQLStoreConfig struct {
	dbName        string
	dbMetricsName string
	dir           string
	persistent    bool
	skipMigrate   bool
}

var defaultTestSQLStoreConfig = testSQLStoreConfig{}

func randomDBName() string {
	return "db" + hex.EncodeToString(frand.Bytes(16))
}

func (cfg *testSQLStoreConfig) dbConnections(partialSlabDir string) (sql.Database, sql.MetricsDatabase, error) {
	var dbMain sql.Database
	var dbMetrics sql.MetricsDatabase
	if mysqlCfg := config.MySQLConfigFromEnv(); mysqlCfg.URI != "" {
		// create MySQL connections if URI is set

		// sanity check config
		if cfg.persistent {
			return nil, nil, errors.New("invalid store config, can't use both persistent and dbURI")
		}

		// use db names from config if not set
		if mysqlCfg.Database == "" {
			mysqlCfg.Database = cfg.dbName
		}
		if mysqlCfg.MetricsDatabase == "" {
			mysqlCfg.MetricsDatabase = cfg.dbMetricsName
		}

		// precreate the two databases
		if tmpDB, err := mysql.Open(mysqlCfg.User, mysqlCfg.Password, mysqlCfg.URI, ""); err != nil {
			return nil, nil, err
		} else if _, err := tmpDB.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", mysqlCfg.Database)); err != nil {
			return nil, nil, err
		} else if _, err := tmpDB.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", mysqlCfg.MetricsDatabase)); err != nil {
			return nil, nil, err
		} else if err := tmpDB.Close(); err != nil {
			return nil, nil, err
		}

		// create MySQL conns
		connMain, err := mysql.Open(mysqlCfg.User, mysqlCfg.Password, mysqlCfg.URI, mysqlCfg.Database)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to open MySQL main database: %w", err)
		}
		connMetrics, err := mysql.Open(mysqlCfg.User, mysqlCfg.Password, mysqlCfg.URI, mysqlCfg.MetricsDatabase)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to open MySQL metrics database: %w", err)
		}
		dbMain, err = mysql.NewMainDatabase(connMain, zap.NewNop(), 100*time.Millisecond, 100*time.Millisecond, partialSlabDir)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create MySQL main database: %w", err)
		}
		dbMetrics, err = mysql.NewMetricsDatabase(connMetrics, zap.NewNop(), 100*time.Millisecond, 100*time.Millisecond)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create MySQL metrics database: %w", err)
		}
	} else if cfg.persistent {
		// create SQL connections if we want a persistent store
		connMain, err := sqlite.Open(filepath.Join(cfg.dir, "db.sqlite"))
		if err != nil {
			return nil, nil, fmt.Errorf("failed to open SQLite main database: %w", err)
		}
		connMetrics, err := sqlite.Open(filepath.Join(cfg.dir, "metrics.sqlite"))
		if err != nil {
			return nil, nil, fmt.Errorf("failed to open SQLite metrics database: %w", err)
		}
		dbMain, err = sqlite.NewMainDatabase(connMain, zap.NewNop(), 100*time.Millisecond, 100*time.Millisecond, partialSlabDir)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create SQLite main database: %w", err)
		}
		dbMetrics, err = sqlite.NewMetricsDatabase(connMetrics, zap.NewNop(), 100*time.Millisecond, 100*time.Millisecond)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create SQLite metrics database: %w", err)
		}
	} else {
		// otherwise return ephemeral connections
		connMain, err := sqlite.OpenEphemeral(cfg.dbName)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to open ephemeral SQLite metrics database: %w", err)
		}
		connMetrics, err := sqlite.OpenEphemeral(cfg.dbMetricsName)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to open ephemeral SQLite metrics database: %w", err)
		}
		dbMain, err = sqlite.NewMainDatabase(connMain, zap.NewNop(), 100*time.Millisecond, 100*time.Millisecond, partialSlabDir)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create ephemeral SQLite main database: %w", err)
		}
		dbMetrics, err = sqlite.NewMetricsDatabase(connMetrics, zap.NewNop(), 100*time.Millisecond, 100*time.Millisecond)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create ephemeral SQLite metrics database: %w", err)
		}
	}
	return dbMain, dbMetrics, nil
}

// newTestSQLStore creates a new SQLStore for testing.
func newTestSQLStore(t *testing.T, cfg testSQLStoreConfig) *testSQLStore {
	t.Helper()

	// default dir to tmp dir
	if cfg.dir == "" {
		cfg.dir = t.TempDir()
	}

	// default db names to random strings if not set
	if cfg.dbName == "" {
		cfg.dbName = randomDBName()
	}
	if cfg.dbMetricsName == "" {
		cfg.dbMetricsName = randomDBName()
	}

	// create db connections
	partialSlabDir := filepath.Join(cfg.dir, "partial_slabs")
	dbMain, dbMetrics, err := cfg.dbConnections(partialSlabDir)
	if err != nil {
		t.Fatal("failed to create db connections", err)
	}

	alerts := alerts.WithOrigin(alerts.NewManager(), "test")
	sqlStore, err := NewSQLStore(Config{
		Alerts:                        alerts,
		DB:                            dbMain,
		DBMetrics:                     dbMetrics,
		PartialSlabDir:                partialSlabDir,
		Migrate:                       !cfg.skipMigrate,
		SlabBufferCompletionThreshold: 0,
		Logger:                        zap.NewNop(),
		LongQueryDuration:             100 * time.Millisecond,
		LongTxDuration:                100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal("failed to create SQLStore", err)
	}

	err = sqlStore.CreateBucket(context.Background(), testBucket, api.BucketPolicy{})
	if err != nil && !errors.Is(err, api.ErrBucketExists) {
		t.Fatal("failed to create test bucket", err)
	}

	return &testSQLStore{
		cfg:      cfg,
		t:        t,
		SQLStore: sqlStore,
	}
}

func (s *testSQLStore) DB() *isql.DB {
	switch db := s.db.(type) {
	case *sqlite.MainDatabase:
		return db.DB()
	case *mysql.MainDatabase:
		return db.DB()
	default:
		s.t.Fatal("unknown db type", db)
	}
	panic("unreachable")
}

func (s *testSQLStore) ExecDBSpecific(sqliteQuery, mysqlQuery string) (dsql.Result, error) {
	switch db := s.db.(type) {
	case *sqlite.MainDatabase:
		return db.DB().Exec(context.Background(), sqliteQuery)
	case *mysql.MainDatabase:
		return db.DB().Exec(context.Background(), mysqlQuery)
	default:
		s.t.Fatal("unknown db type", db)
	}
	panic("unreachable")
}

func (s *testSQLStore) QueryRowDBSpecific(sqliteQuery, mysqlQuery string, sqliteArgs, mysqlArgs []any) *isql.LoggedRow {
	switch db := s.db.(type) {
	case *sqlite.MainDatabase:
		return db.DB().QueryRow(context.Background(), sqliteQuery, sqliteArgs...)
	case *mysql.MainDatabase:
		return db.DB().QueryRow(context.Background(), mysqlQuery, mysqlArgs...)
	default:
		s.t.Fatal("unknown db type", db)
	}
	panic("unreachable")
}

func (s *testSQLStore) DBMetrics() *isql.DB {
	switch db := s.dbMetrics.(type) {
	case *sqlite.MetricsDatabase:
		return db.DB()
	case *mysql.MetricsDatabase:
		return db.DB()
	default:
		s.t.Fatal("unknown db type", db)
	}
	panic("unreachable")
}

func (s *testSQLStore) Close() error {
	if err := s.SQLStore.Close(); err != nil {
		s.t.Error(err)
	}
	return nil
}

func (s *testSQLStore) DefaultBucketID() (id int64) {
	if err := s.DB().QueryRow(context.Background(), "SELECT id FROM buckets WHERE name = ?", testBucket).
		Scan(&id); err != nil {
		s.t.Fatal(err)
	}
	return
}

func (s *testSQLStore) Reopen() *testSQLStore {
	s.t.Helper()
	cfg := s.cfg
	cfg.skipMigrate = true
	return newTestSQLStore(s.t, cfg)
}

func (s *testSQLStore) Retry(tries int, durationBetweenAttempts time.Duration, fn func() error) {
	s.t.Helper()
	for i := 1; i < tries; i++ {
		err := fn()
		if err == nil {
			return
		}
		time.Sleep(durationBetweenAttempts)
	}
	if err := fn(); err != nil {
		s.t.Fatal(err)
	}
}

func (s *testSQLStore) addTestObject(key string, o object.Object) (api.Object, error) {
	if err := s.UpdateObjectBlocking(context.Background(), testBucket, key, testETag, testMimeType, testMetadata, o); err != nil {
		return api.Object{}, err
	} else if obj, err := s.Object(context.Background(), testBucket, key); err != nil {
		return api.Object{}, err
	} else {
		return obj, nil
	}
}

func (s *testSQLStore) Count(table string) (n int64) {
	if err := s.DB().QueryRow(context.Background(), fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).
		Scan(&n); err != nil {
		s.t.Fatal(err)
	}
	return
}

func (s *testSQLStore) addTestContracts(keys []types.PublicKey) (fcids []types.FileContractID, contracts []api.ContractMetadata, err error) {
	cnt := s.Count("contracts")
	for i, key := range keys {
		fcids = append(fcids, types.FileContractID{byte(int(cnt) + i + 1)})
		contract, err := s.addTestContract(fcids[len(fcids)-1], key)
		if err != nil {
			return nil, nil, err
		}
		contracts = append(contracts, contract)
	}
	return
}

func (s *SQLStore) addTestContract(fcid types.FileContractID, hk types.PublicKey) (api.ContractMetadata, error) {
	if err := s.PutContract(context.Background(), newTestContract(fcid, hk)); err != nil {
		return api.ContractMetadata{}, err
	}
	return s.Contract(context.Background(), fcid)
}

func (s *testSQLStore) overrideSlabHealth(objectID string, health float64) (err error) {
	_, err = s.DB().Exec(context.Background(), fmt.Sprintf(`
	UPDATE slabs SET health = %v WHERE id IN (
		SELECT * FROM (
			SELECT sla.id
			FROM objects o
			INNER JOIN slices sli ON o.id = sli.db_object_id
			INNER JOIN slabs sla ON sli.db_slab_id = sla.id
			WHERE o.object_id = "%s"
		) AS sub
	)`, health, objectID))
	return
}

func (s *testSQLStore) renewTestContract(hk types.PublicKey, renewedFrom, renewedTo types.FileContractID, startHeight uint64) error {
	renewal := newTestContract(renewedTo, hk)
	renewal.StartHeight = startHeight
	renewal.RenewedFrom = renewedFrom
	return s.AddRenewal(context.Background(), renewal)
}
