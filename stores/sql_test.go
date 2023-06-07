package stores

import (
	"encoding/hex"
	"os"
	"strings"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/siad/modules"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gorm.io/gorm/logger"
	"lukechampine.com/frand"
)

const testPersistInterval = time.Second

// newTestSQLStore creates a new SQLStore for testing.
func newTestSQLStore() (*SQLStore, string, modules.ConsensusChangeID, error) {
	dbName := hex.EncodeToString(frand.Bytes(32)) // random name for db
	conn := NewEphemeralSQLiteConnection(dbName)
	walletAddrs := types.Address(frand.Entropy256())
	sqlStore, ccid, err := NewSQLStore(conn, true, time.Second, walletAddrs, newTestLogger())
	if err != nil {
		return nil, "", modules.ConsensusChangeID{}, err
	}
	return sqlStore, dbName, ccid, nil
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

func TestConsensusReset(t *testing.T) {
	db, _, ccid, err := newTestSQLStore()
	if err != nil {
		t.Fatal(err)
	}
	if ccid != modules.ConsensusChangeBeginning {
		t.Fatal("wrong ccid", ccid, modules.ConsensusChangeBeginning)
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
	tables, err := db.db.Migrator().GetTables()
	if err != nil {
		t.Fatal(err)
	}
	migratedTables := 0
	for _, table := range tables {
		if strings.HasPrefix(table, "consensus_infos-") ||
			strings.HasPrefix(table, "siacoin_elements-") ||
			strings.HasPrefix(table, "transactions-") {
			migratedTables++
		}
	}
	if migratedTables != 3 {
		t.Fatal("not all migrated tables were found", migratedTables)
	}

	if db.db.Migrator().HasTable(&dbConsensusInfo{}) {
		t.Fatal("consensus_infos table should have been dropped")
	} else if db.db.Migrator().HasTable(&dbSiacoinElement{}) {
		t.Fatal("siacoin_elements table should have been dropped")
	} else if db.db.Migrator().HasTable(&dbTransaction{}) {
		t.Fatal("transactions table should have been dropped")
	}
}
