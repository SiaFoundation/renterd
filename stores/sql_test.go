package stores

import (
	"context"
	dsql "database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
	"go.uber.org/zap/zapcore"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"lukechampine.com/frand"
	"moul.io/zapgorm2"
)

const (
	testContractSet = "test"
	testMimeType    = "application/octet-stream"
	testETag        = "d34db33f"
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
	dbName          string
	dbMetricsName   string
	dir             string
	persistent      bool
	skipMigrate     bool
	skipContractSet bool
}

var defaultTestSQLStoreConfig = testSQLStoreConfig{}

func randomDBName() string {
	return "db" + hex.EncodeToString(frand.Bytes(16))
}

func (cfg *testSQLStoreConfig) dbConnections() (gorm.Dialector, sql.MetricsDatabase, error) {
	var connMain gorm.Dialector
	var dbm *dsql.DB
	var dbMetrics sql.MetricsDatabase
	var err error
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

		// use a tmp connection to precreate the two databases
		if tmpDB, err := gorm.Open(NewMySQLConnection(mysqlCfg.User, mysqlCfg.Password, mysqlCfg.URI, "")); err != nil {
			return nil, nil, err
		} else if err := tmpDB.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", mysqlCfg.Database)).Error; err != nil {
			return nil, nil, err
		} else if err := tmpDB.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", mysqlCfg.MetricsDatabase)).Error; err != nil {
			return nil, nil, err
		}

		connMain = NewMySQLConnection(mysqlCfg.User, mysqlCfg.Password, mysqlCfg.URI, mysqlCfg.Database)
		dbm, err = mysql.Open(mysqlCfg.User, mysqlCfg.Password, mysqlCfg.URI, mysqlCfg.MetricsDatabase)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to open MySQL metrics database: %w", err)
		}
		dbMetrics, err = mysql.NewMetricsDatabase(dbm, zap.NewNop().Sugar(), 100*time.Millisecond, 100*time.Millisecond)
	} else if cfg.persistent {
		// create SQL connections if we want a persistent store
		connMain = NewSQLiteConnection(filepath.Join(cfg.dir, "db.sqlite"))
		dbm, err = sqlite.Open(filepath.Join(cfg.dir, "metrics.sqlite"))
		if err != nil {
			return nil, nil, fmt.Errorf("failed to open SQLite metrics database: %w", err)
		}
		dbMetrics, err = sqlite.NewMetricsDatabase(dbm, zap.NewNop().Sugar(), 100*time.Millisecond, 100*time.Millisecond)
	} else {
		// otherwise return ephemeral connections
		connMain = NewEphemeralSQLiteConnection(cfg.dbName)
		dbm, err = sqlite.OpenEphemeral(cfg.dbMetricsName)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to open ephemeral SQLite metrics database: %w", err)
		}
		dbMetrics, err = sqlite.NewMetricsDatabase(dbm, zap.NewNop().Sugar(), 100*time.Millisecond, 100*time.Millisecond)
	}
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create metrics database: %w", err)
	}
	return connMain, dbMetrics, nil
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
	conn, dbMetrics, err := cfg.dbConnections()
	if err != nil {
		t.Fatal("failed to create db connections", err)
	}

	alerts := alerts.WithOrigin(alerts.NewManager(), "test")
	sqlStore, err := NewSQLStore(Config{
		Conn:                          conn,
		Alerts:                        alerts,
		DBMetrics:                     dbMetrics,
		PartialSlabDir:                cfg.dir,
		Migrate:                       !cfg.skipMigrate,
		SlabBufferCompletionThreshold: 0,
		Logger:                        zap.NewNop().Sugar(),
		LongQueryDuration:             100 * time.Millisecond,
		LongTxDuration:                100 * time.Millisecond,
		GormLogger:                    newTestLogger(),
		RetryTransactionIntervals:     []time.Duration{50 * time.Millisecond, 100 * time.Millisecond, 200 * time.Millisecond},
	})
	if err != nil {
		t.Fatal("failed to create SQLStore", err)
	}

	if !cfg.skipContractSet {
		err = sqlStore.SetContractSet(context.Background(), testContractSet, []types.FileContractID{})
		if err != nil {
			t.Fatal("failed to set contract set", err)
		}
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

func (s *testSQLStore) DefaultBucketID() uint {
	var b dbBucket
	if err := s.gormDB.
		Model(&dbBucket{}).
		Where("name = ?", api.DefaultBucketName).
		Take(&b).
		Error; err != nil {
		s.t.Fatal(err)
	}
	return b.ID
}

func (s *testSQLStore) Reopen() *testSQLStore {
	s.t.Helper()
	cfg := s.cfg
	cfg.skipContractSet = true
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

// newTestLogger creates a console logger used for testing.
func newTestLogger() logger.Interface {
	config := zap.NewProductionEncoderConfig()
	config.EncodeTime = zapcore.RFC3339TimeEncoder
	config.EncodeLevel = zapcore.CapitalColorLevelEncoder
	config.StacktraceKey = ""
	consoleEncoder := zapcore.NewConsoleEncoder(config)

	l := zap.New(
		zapcore.NewCore(consoleEncoder, zapcore.AddSync(os.Stdout), zapcore.DebugLevel),
		zap.AddCaller(),
		zap.AddStacktrace(zapcore.ErrorLevel),
	)
	return zapgorm2.New(l)
}

func (s *testSQLStore) addTestObject(path string, o object.Object) (api.Object, error) {
	if err := s.UpdateObjectBlocking(context.Background(), api.DefaultBucketName, path, testContractSet, testETag, testMimeType, testMetadata, o); err != nil {
		return api.Object{}, err
	} else if obj, err := s.Object(context.Background(), api.DefaultBucketName, path); err != nil {
		return api.Object{}, err
	} else {
		return obj, nil
	}
}

func (s *SQLStore) addTestContracts(keys []types.PublicKey) (fcids []types.FileContractID, contracts []api.ContractMetadata, err error) {
	cnt, err := s.contractsCount()
	if err != nil {
		return nil, nil, err
	}
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
	rev := testContractRevision(fcid, hk)
	return s.AddContract(context.Background(), rev, types.ZeroCurrency, types.ZeroCurrency, 0, api.ContractStatePending)
}

func (s *SQLStore) addTestRenewedContract(fcid, renewedFrom types.FileContractID, hk types.PublicKey, startHeight uint64) (api.ContractMetadata, error) {
	rev := testContractRevision(fcid, hk)
	return s.AddRenewedContract(context.Background(), rev, types.ZeroCurrency, types.ZeroCurrency, startHeight, renewedFrom, api.ContractStatePending)
}

func (s *SQLStore) contractsCount() (cnt int64, err error) {
	err = s.gormDB.
		Model(&dbContract{}).
		Count(&cnt).
		Error
	return
}

func (s *SQLStore) overrideSlabHealth(objectID string, health float64) (err error) {
	err = s.gormDB.Exec(fmt.Sprintf(`
	UPDATE slabs SET health = %v WHERE id IN (
		SELECT * FROM (
			SELECT sla.id
			FROM objects o
			INNER JOIN slices sli ON o.id = sli.db_object_id
			INNER JOIN slabs sla ON sli.db_slab_id = sla.id
			WHERE o.object_id = "%s"
		) AS sub
	)`, health, objectID)).Error
	return
}

type sqliteQueryPlan struct {
	Detail string `json:"detail"`
}

func (p sqliteQueryPlan) usesIndex() bool {
	d := strings.ToLower(p.Detail)
	return strings.Contains(d, "using index") || strings.Contains(d, "using covering index")
}

//nolint:tagliatelle
type mysqlQueryPlan struct {
	Extra        string `json:"Extra"`
	PossibleKeys string `json:"possible_keys"`
}

func (p mysqlQueryPlan) usesIndex() bool {
	d := strings.ToLower(p.Extra)
	return strings.Contains(d, "using index") || strings.Contains(p.PossibleKeys, "idx_")
}

func TestQueryPlan(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	queries := []string{
		// allow_list
		"SELECT * FROM host_allowlist_entry_hosts WHERE db_host_id = 1",
		"SELECT * FROM host_allowlist_entry_hosts WHERE db_allowlist_entry_id = 1",

		// block_list
		"SELECT * FROM host_blocklist_entry_hosts WHERE db_host_id = 1",
		"SELECT * FROM host_blocklist_entry_hosts WHERE db_blocklist_entry_id = 1",

		// contract_sectors
		"SELECT * FROM contract_sectors WHERE db_contract_id = 1",
		"SELECT * FROM contract_sectors WHERE db_sector_id = 1",
		"SELECT COUNT(DISTINCT db_sector_id) FROM contract_sectors",

		// contract_set_contracts
		"SELECT * FROM contract_set_contracts WHERE db_contract_id = 1",
		"SELECT * FROM contract_set_contracts WHERE db_contract_set_id = 1",

		// slabs
		"SELECT * FROM slabs WHERE health_valid_until > 0",
		"SELECT * FROM slabs WHERE health > 0",
		"SELECT * FROM slabs WHERE db_buffered_slab_id = 1",

		// objects
		"SELECT * FROM objects WHERE db_bucket_id = 1",
		"SELECT * FROM objects WHERE etag = ''",
	}

	for _, query := range queries {
		if isSQLite(ss.gormDB) {
			var explain sqliteQueryPlan
			if err := ss.gormDB.Raw(fmt.Sprintf("EXPLAIN QUERY PLAN %s;", query)).Scan(&explain).Error; err != nil {
				t.Fatal(err)
			} else if !explain.usesIndex() {
				t.Fatalf("query '%s' should use an index, instead the plan was %+v", query, explain)
			}
		} else {
			var explain mysqlQueryPlan
			if err := ss.gormDB.Raw(fmt.Sprintf("EXPLAIN %s;", query)).Scan(&explain).Error; err != nil {
				t.Fatal(err)
			} else if !explain.usesIndex() {
				t.Fatalf("query '%s' should use an index, instead the plan was %+v", query, explain)
			}
		}
	}
}
