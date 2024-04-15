package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"go.sia.tech/renterd/config"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func NewLogger(dir string, cfg config.Log) (*zap.Logger, func(context.Context) error, error) {
	// path
	path := filepath.Join(dir, "renterd.log")
	if cfg.Path != "" {
		path = filepath.Join(cfg.Path, "renterd.log")
	}

	if cfg.File.Path != "" {
		path = cfg.File.Path
	}

	// log level
	level, err := zapcore.ParseLevel(cfg.Level)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse log level: %w", err)
	}

	fileLevel := level
	if cfg.File.Level != "" {
		fileLevel, err = zapcore.ParseLevel(cfg.File.Level)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to parse file log level: %w", err)
		}
	}

	stdoutLevel := level
	if cfg.StdOut.Level != "" {
		stdoutLevel, err = zapcore.ParseLevel(cfg.StdOut.Level)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to parse stdout log level: %w", err)
		}
	}

	closeFn := func(_ context.Context) error { return nil }

	// file logger
	var cores []zapcore.Core
	if cfg.File.Enabled {
		writer, cleanup, err := zap.Open(path)
		if err != nil {
			return nil, nil, err
		}
		closeFn = func(_ context.Context) error {
			_ = writer.Sync() // ignore Error
			cleanup()
			return nil
		}

		var encoder zapcore.Encoder
		switch cfg.File.Format {
		case "human":
			encoder = humanEncoder(false) // disable colors in file log
		default: // log file defaults to json
			encoder = jsonEncoder()
		}
		cores = append(cores, zapcore.NewCore(encoder, writer, fileLevel))
	}

	// stdout logger
	if cfg.StdOut.Enabled {
		var encoder zapcore.Encoder
		switch cfg.StdOut.Format {
		case "json":
			encoder = jsonEncoder()
		default: // stdout defaults to human
			encoder = humanEncoder(cfg.StdOut.EnableANSI)
		}
		cores = append(cores, zapcore.NewCore(encoder, zapcore.AddSync(os.Stdout), stdoutLevel))
	}

	return zap.New(
		zapcore.NewTee(cores...),
		zap.AddCaller(),
		zap.AddStacktrace(zapcore.ErrorLevel),
	), closeFn, nil
}

// jsonEncoder returns a zapcore.Encoder that encodes logs as JSON intended for
// parsing.
func jsonEncoder() zapcore.Encoder {
	cfg := zap.NewProductionEncoderConfig()
	cfg.EncodeTime = zapcore.RFC3339TimeEncoder
	cfg.EncodeDuration = zapcore.StringDurationEncoder
	return zapcore.NewJSONEncoder(cfg)
}

// humanEncoder returns a zapcore.Encoder that encodes logs as human-readable
// text.
func humanEncoder(showColors bool) zapcore.Encoder {
	cfg := zap.NewProductionEncoderConfig()
	cfg.EncodeTime = zapcore.RFC3339TimeEncoder
	cfg.EncodeDuration = zapcore.StringDurationEncoder

	if showColors {
		cfg.EncodeLevel = zapcore.CapitalColorLevelEncoder
	} else {
		cfg.EncodeLevel = zapcore.CapitalLevelEncoder
	}

	cfg.StacktraceKey = ""
	cfg.CallerKey = ""
	return zapcore.NewConsoleEncoder(cfg)
}
