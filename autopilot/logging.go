package autopilot

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func newLogger(path string) (*zap.Logger, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	// console
	config := zap.NewProductionEncoderConfig()
	config.EncodeTime = zapcore.RFC3339TimeEncoder
	config.EncodeLevel = zapcore.CapitalColorLevelEncoder
	consoleEncoder := zapcore.NewConsoleEncoder(config)

	// file
	config = zap.NewProductionEncoderConfig()
	config.EncodeTime = zapcore.RFC3339TimeEncoder
	config.CallerKey = ""     // hide
	config.StacktraceKey = "" // hide
	config.NameKey = "component"
	config.TimeKey = "date"
	fileEncoder := zapcore.NewJSONEncoder(config)

	writer := zapcore.AddSync(file)
	core := zapcore.NewTee(
		zapcore.NewCore(fileEncoder, writer, zapcore.DebugLevel),
		zapcore.NewCore(consoleEncoder, zapcore.AddSync(os.Stdout), zapcore.DebugLevel),
	)

	return zap.New(
		core,
		zap.AddCaller(),
		zap.AddStacktrace(zapcore.ErrorLevel),
	), nil
}

func newTestLogger() *zap.Logger {
	return zap.New(zapcore.NewNopCore())
}
