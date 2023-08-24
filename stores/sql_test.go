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
	"go.sia.tech/renterd/webhooks"
	"go.sia.tech/siad/modules"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gorm.io/gorm/logger"
	"lukechampine.com/frand"
)

const (
	testPersistInterval = time.Second
	testContractSet     = "test"
)

// newTestSQLStore creates a new SQLStore for testing.
func newTestSQLStore(dir string) (*SQLStore, string, modules.ConsensusChangeID, error) {
	dbName := hex.EncodeToString(frand.Bytes(32)) // random name for db
	conn := NewEphemeralSQLiteConnection(dbName)
	walletAddrs := types.Address(frand.Entropy256())
	hooksMgr := webhooks.NewManager(zap.NewNop().Sugar())
	alerts := alerts.WithOrigin(alerts.NewManager(hooksMgr), "test")
	sqlStore, ccid, err := NewSQLStore(conn, alerts, dir, true, time.Second, walletAddrs, 0, newTestLogger())
	if err != nil {
		return nil, "", modules.ConsensusChangeID{}, err
	}
	err = sqlStore.SetContractSet(context.Background(), testContractSet, []types.FileContractID{})
	return sqlStore, dbName, ccid, err
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
	db, _, ccid, err := newTestSQLStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if ccid != modules.ConsensusChangeBeginning {
		t.Fatal("wrong ccid", ccid, modules.ConsensusChangeBeginning)
	}

	// Manually insert into the consenus_infos, the transactions and siacoin_elements tables.
	ccid2 := modules.ConsensusChangeID{1}
	db.db.Create(&dbConsensusInfo{
		CCID: ccid2[:],
	})
	db.db.Create(&dbSiacoinElement{
		OutputID: hash256{2},
	})
	db.db.Create(&dbTransaction{
		TransactionID: hash256{3},
	})

	// Reset the consensus.
	if err := db.ResetConsensusSubscription(); err != nil {
		t.Fatal(err)
	}

	// Check tables.
	var count int64
	if err := db.db.Model(&dbConsensusInfo{}).Count(&count).Error; err != nil || count != 1 {
		t.Fatal("table should have 1 entry", err, count)
	} else if err = db.db.Model(&dbTransaction{}).Count(&count).Error; err != nil || count > 0 {
		t.Fatal("table not empty", err)
	} else if err = db.db.Model(&dbSiacoinElement{}).Count(&count).Error; err != nil || count > 0 {
		t.Fatal("table not empty", err)
	}

	// Check consensus info.
	var ci dbConsensusInfo
	if err := db.db.Take(&ci).Error; err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(ci.CCID, modules.ConsensusChangeBeginning[:]) {
		t.Fatal("wrong ccid", ci.CCID, modules.ConsensusChangeBeginning)
	} else if ci.Height != 0 {
		t.Fatal("wrong height", ci.Height, 0)
	}

	// Check SQLStore.
	if db.chainIndex.Height != 0 {
		t.Fatal("wrong height", db.chainIndex.Height, 0)
	} else if db.chainIndex.ID != (types.BlockID{}) {
		t.Fatal("wrong id", db.chainIndex.ID, types.BlockID{})
	}
}

type queryPlanExplain struct {
	ID      int    `json:"id"`
	Parent  int    `json:"parent"`
	NotUsed bool   `json:"notused"`
	Detail  string `json:"detail"`
}

func TestQueryPlan(t *testing.T) {
	db, _, _, err := newTestSQLStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

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

		// contract_set_contracts
		"SELECT * FROM contract_set_contracts WHERE db_contract_id = 1",
		"SELECT * FROM contract_set_contracts WHERE db_contract_set_id = 1",
	}

	for _, query := range queries {
		var explain queryPlanExplain
		err = db.db.Raw(fmt.Sprintf("EXPLAIN QUERY PLAN %s;", query)).Scan(&explain).Error
		if err != nil {
			t.Fatal(err)
		}
		if !(strings.Contains(explain.Detail, "USING INDEX") ||
			strings.Contains(explain.Detail, "USING COVERING INDEX")) {
			t.Fatalf("query '%s' should use an index, instead the plan was '%s'", query, explain.Detail)
		}
	}
}
