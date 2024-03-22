package stores

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/alerts"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/object"
	"go.sia.tech/siad/modules"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"lukechampine.com/frand"
	"moul.io/zapgorm2"
)

const (
	testPersistInterval = time.Second
	testContractSet     = "test"
	testMimeType        = "application/octet-stream"
	testETag            = "d34db33f"
)

var (
	testMetadata = api.ObjectUserMetadata{
		"foo": "bar",
		"baz": "qux",
	}
)

type testSQLStore struct {
	t *testing.T
	*SQLStore

	dbName        string
	dbMetricsName string
	dir           string
	ccid          modules.ConsensusChangeID
}

type testSQLStoreConfig struct {
	dbURI           string
	dbUser          string
	dbPassword      string
	dbName          string
	dbMetricsName   string
	dir             string
	persistent      bool
	skipMigrate     bool
	skipContractSet bool
}

var defaultTestSQLStoreConfig = testSQLStoreConfig{}

// newTestSQLStore creates a new SQLStore for testing.
func newTestSQLStore(t *testing.T, cfg testSQLStoreConfig) *testSQLStore {
	t.Helper()
	dir := cfg.dir
	if dir == "" {
		dir = t.TempDir()
	}

	dbURI, dbUser, dbPassword, dbName := DBConfigFromEnv()
	if dbURI == "" {
		dbURI = cfg.dbURI
	}
	if cfg.persistent && dbURI != "" {
		t.Fatal("invalid store config, can't use both persistent and dbURI")
	}
	if dbUser == "" {
		dbUser = cfg.dbUser
	}
	if dbPassword == "" {
		dbPassword = cfg.dbPassword
	}
	if dbName == "" {
		if cfg.dbName != "" {
			dbName = cfg.dbName
		} else {
			dbName = hex.EncodeToString(frand.Bytes(32)) // random name for db
		}
	}
	dbMetricsName := cfg.dbMetricsName
	if dbMetricsName == "" {
		dbMetricsName = hex.EncodeToString(frand.Bytes(32)) // random name for metrics db
	}

	var conn, connMetrics gorm.Dialector
	if dbURI != "" {
		if tmpDB, err := gorm.Open(NewMySQLConnection(dbUser, dbPassword, dbURI, "")); err != nil {
			t.Fatal(err)
		} else if err := tmpDB.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", dbName)).Error; err != nil {
			t.Fatal(err)
		} else if err := tmpDB.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", dbMetricsName)).Error; err != nil {
			t.Fatal(err)
		}

		conn = NewMySQLConnection(dbUser, dbPassword, dbURI, dbName)
		connMetrics = NewMySQLConnection(dbUser, dbPassword, dbURI, dbMetricsName)
	} else if cfg.persistent {
		conn = NewSQLiteConnection(filepath.Join(dir, "db.sqlite"))
		connMetrics = NewSQLiteConnection(filepath.Join(dir, "metrics.sqlite"))
	} else {
		conn = NewEphemeralSQLiteConnection(dbName)
		connMetrics = NewEphemeralSQLiteConnection(dbMetricsName)
	}

	walletAddrs := types.Address(frand.Entropy256())
	alerts := alerts.WithOrigin(alerts.NewManager(), "test")
	sqlStore, ccid, err := NewSQLStore(Config{
		Conn:                          conn,
		ConnMetrics:                   connMetrics,
		Alerts:                        alerts,
		PartialSlabDir:                dir,
		Migrate:                       !cfg.skipMigrate,
		AnnouncementMaxAge:            time.Hour,
		PersistInterval:               time.Second,
		WalletAddress:                 walletAddrs,
		SlabBufferCompletionThreshold: 0,
		Logger:                        zap.NewNop().Sugar(),
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
		SQLStore:      sqlStore,
		dbName:        dbName,
		dbMetricsName: dbMetricsName,
		dir:           dir,
		ccid:          ccid,
		t:             t,
	}
}

func (s *testSQLStore) Close() error {
	if err := s.SQLStore.Close(); err != nil {
		s.t.Error(err)
	}
	return nil
}

func (s *testSQLStore) DefaultBucketID() uint {
	var b dbBucket
	if err := s.db.
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
	cfg := defaultTestSQLStoreConfig
	cfg.dir = s.dir
	cfg.dbName = s.dbName
	cfg.dbMetricsName = s.dbMetricsName
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
	if err := s.UpdateObject(context.Background(), api.DefaultBucketName, path, testContractSet, testETag, testMimeType, testMetadata, o); err != nil {
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
	err = s.db.
		Model(&dbContract{}).
		Count(&cnt).
		Error
	return
}

func (s *SQLStore) overrideSlabHealth(objectID string, health float64) (err error) {
	err = s.db.Exec(fmt.Sprintf(`
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

// TestConsensusReset is a unit test for ResetConsensusSubscription.
func TestConsensusReset(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()
	if ss.ccid != modules.ConsensusChangeBeginning {
		t.Fatal("wrong ccid", ss.ccid, modules.ConsensusChangeBeginning)
	}

	// Manually insert into the consenus_infos, the transactions and siacoin_elements tables.
	ccid2 := modules.ConsensusChangeID{1}
	ss.db.Create(&dbConsensusInfo{
		CCID: ccid2[:],
	})
	ss.db.Create(&dbSiacoinElement{
		OutputID: hash256{2},
	})
	ss.db.Create(&dbTransaction{
		TransactionID: hash256{3},
	})

	// Reset the consensus.
	if err := ss.ResetConsensusSubscription(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Reopen the SQLStore.
	ss = ss.Reopen()
	defer ss.Close()

	// Check tables.
	var count int64
	if err := ss.db.Model(&dbConsensusInfo{}).Count(&count).Error; err != nil || count != 1 {
		t.Fatal("table should have 1 entry", err, count)
	} else if err = ss.db.Model(&dbTransaction{}).Count(&count).Error; err != nil || count > 0 {
		t.Fatal("table not empty", err)
	} else if err = ss.db.Model(&dbSiacoinElement{}).Count(&count).Error; err != nil || count > 0 {
		t.Fatal("table not empty", err)
	}

	// Check consensus info.
	var ci dbConsensusInfo
	if err := ss.db.Take(&ci).Error; err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(ci.CCID, modules.ConsensusChangeBeginning[:]) {
		t.Fatal("wrong ccid", ci.CCID, modules.ConsensusChangeBeginning)
	} else if ci.Height != 0 {
		t.Fatal("wrong height", ci.Height, 0)
	}

	// Check SQLStore.
	if ss.chainIndex.Height != 0 {
		t.Fatal("wrong height", ss.chainIndex.Height, 0)
	} else if ss.chainIndex.ID != (types.BlockID{}) {
		t.Fatal("wrong id", ss.chainIndex.ID, types.BlockID{})
	}
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
		if isSQLite(ss.db) {
			var explain sqliteQueryPlan
			if err := ss.db.Raw(fmt.Sprintf("EXPLAIN QUERY PLAN %s;", query)).Scan(&explain).Error; err != nil {
				t.Fatal(err)
			} else if !explain.usesIndex() {
				t.Fatalf("query '%s' should use an index, instead the plan was %+v", query, explain)
			}
		} else {
			var explain mysqlQueryPlan
			if err := ss.db.Raw(fmt.Sprintf("EXPLAIN %s;", query)).Scan(&explain).Error; err != nil {
				t.Fatal(err)
			} else if !explain.usesIndex() {
				t.Fatalf("query '%s' should use an index, instead the plan was %+v", query, explain)
			}
		}
	}
}

func TestApplyUpdatesErr(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	before := ss.lastSave

	// drop consensus_infos table to cause update to fail
	if err := ss.db.Exec("DROP TABLE consensus_infos").Error; err != nil {
		t.Fatal(err)
	}

	// call applyUpdates with 'force' set to true
	if err := ss.applyUpdates(true); err == nil {
		t.Fatal("expected error")
	}

	// save shouldn't have happened
	if ss.lastSave != before {
		t.Fatal("lastSave should not have changed")
	}
}
