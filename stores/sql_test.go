package stores

import (
	"encoding/hex"
	"os"
	"time"

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
	sqlStore, ccid, err := NewSQLStore(conn, true, time.Second, newTestLogger())
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
	return NewSQLLogger(l, &LoggerConfig{
		IgnoreRecordNotFoundError: false,
		LogLevel:                  logger.Warn,
		SlowThreshold:             100 * time.Millisecond,
	})
}
