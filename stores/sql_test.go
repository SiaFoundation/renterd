package stores

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/alerts"
	"go.sia.tech/siad/modules"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gorm.io/gorm/logger"
	"lukechampine.com/frand"
)

const (
	testPersistInterval = time.Second
	testContractSet     = "test"
	testMimeType        = "application/octet-stream"
	testETag            = "d34db33f"
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
	dbName          string
	dbMetricsName   string
	dir             string
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
	dbName := cfg.dbName
	if dbName == "" {
		dbName = hex.EncodeToString(frand.Bytes(32)) // random name for db
	}
	dbMetricsName := cfg.dbMetricsName
	if dbMetricsName == "" {
		dbMetricsName = hex.EncodeToString(frand.Bytes(32)) // random name for metrics db
	}
	conn := NewEphemeralSQLiteConnection(dbName)
	walletAddrs := types.Address(frand.Entropy256())
	alerts := alerts.WithOrigin(alerts.NewManager(), "test")
	sqlStore, ccid, err := NewSQLStore(conn, alerts, dir, !cfg.skipMigrate, time.Hour, time.Second, walletAddrs, 0, zap.NewNop().Sugar(), newTestLogger())
	if err != nil {
		t.Fatal("failed to create SQLStore", err)
	}
	detectMissingIndices(sqlStore.db, func(dst interface{}, name string) {
		panic("no index can be missing")
	})
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
	return NewSQLLogger(l, LoggerConfig{
		IgnoreRecordNotFoundError: false,
		LogLevel:                  logger.Warn,
		SlowThreshold:             100 * time.Millisecond,
	})
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
	if err := ss.ResetConsensusSubscription(); err != nil {
		t.Fatal(err)
	}

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

type queryPlanExplain struct {
	ID      int    `json:"id"`
	Parent  int    `json:"parent"`
	NotUsed bool   `json:"notused"`
	Detail  string `json:"detail"`
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
		"SELECT * FROM slabs WHERE health_valid = 1",
		"SELECT * FROM slabs WHERE health > 0",
		"SELECT * FROM slabs WHERE db_buffered_slab_id = 1",

		// objects
		"SELECT * FROM objects WHERE db_bucket_id = 1",
		"SELECT * FROM objects WHERE etag = ''",
	}

	for _, query := range queries {
		var explain queryPlanExplain
		err := ss.db.Raw(fmt.Sprintf("EXPLAIN QUERY PLAN %s;", query)).Scan(&explain).Error
		if err != nil {
			t.Fatal(err)
		}
		if !(strings.Contains(explain.Detail, "USING INDEX") ||
			strings.Contains(explain.Detail, "USING COVERING INDEX")) {
			t.Fatalf("query '%s' should use an index, instead the plan was '%s'", query, explain.Detail)
		}
	}
}
