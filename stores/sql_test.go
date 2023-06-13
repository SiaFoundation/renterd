package stores

import (
	"bytes"
	"encoding/hex"
	"os"
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

// TestConsensusReset is a unit test for ResetConsensusSubscription.
func TestConsensusReset(t *testing.T) {
	db, _, ccid, err := newTestSQLStore()
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
