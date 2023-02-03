package stores

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type LoggerConfig struct {
	IgnoreRecordNotFoundError bool
	LogLevel                  logger.LogLevel
	SlowThreshold             time.Duration
}

type gormLogger struct {
	LoggerConfig
	logger *zap.SugaredLogger
}

func NewSQLLogger(l *zap.Logger, config *LoggerConfig) logger.Interface {
	l = l.WithOptions(zap.AddCallerSkip(3))

	if config == nil {
		config = &LoggerConfig{
			IgnoreRecordNotFoundError: true,
			LogLevel:                  logger.Warn,
			SlowThreshold:             200 * time.Millisecond,
		}
	}
	return &gormLogger{
		LoggerConfig: *config,
		logger:       l.Sugar(),
	}
}

func (l *gormLogger) LogMode(level logger.LogLevel) logger.Interface {
	newlogger := *l
	newlogger.LogLevel = level
	return &newlogger
}

func (l gormLogger) Info(ctx context.Context, msg string, args ...interface{}) {
	if l.LogLevel < logger.Info {
		return
	}
	l.logger.Infof(msg, args...)
}

func (l gormLogger) Warn(ctx context.Context, msg string, args ...interface{}) {
	if l.LogLevel < logger.Warn {
		return
	}
	l.logger.Warnf(msg, args...)
}

func (l gormLogger) Error(ctx context.Context, msg string, args ...interface{}) {
	if l.LogLevel < logger.Error {
		return
	}
	l.logger.Errorf(msg, args...)
}

func (l gormLogger) Trace(ctx context.Context, start time.Time, fc func() (sql string, rowsAffected int64), err error) {
	if l.LogLevel <= logger.Silent {
		return
	}

	hideError := errors.Is(err, gorm.ErrRecordNotFound) && l.IgnoreRecordNotFoundError
	if err != nil && !hideError && l.LogLevel >= logger.Error {
		var log func(string, ...interface{})
		if errors.Is(err, gorm.ErrRecordNotFound) {
			log = l.logger.Debugw
		} else {
			log = l.logger.Errorw
		}

		sql, rows := fc()
		if rows == -1 {
			log(err.Error(), "elapsed", elapsedMS(start), "sql", sql)
		} else {
			log(err.Error(), "elapsed", elapsedMS(start), "rows", rows, "sql", sql)
		}
		return
	}

	if l.SlowThreshold != 0 && time.Since(start) > l.SlowThreshold && l.LogLevel >= logger.Warn {
		sql, rows := fc()
		if rows == -1 {
			l.logger.Warnw(fmt.Sprintf("SLOW SQL >= %v", l.SlowThreshold), "elapsed", elapsedMS(start), "sql", sql)
		} else {
			l.logger.Warnw(fmt.Sprintf("SLOW SQL >= %v", l.SlowThreshold), "elapsed", elapsedMS(start), "rows", rows, "sql", sql)
		}
		return
	}

	if l.LogLevel >= logger.Info {
		sql, rows := fc()
		l.logger.Debugw("trace", "elapsed", elapsedMS(start), "rows", rows, "sql", sql)
	}
}

func elapsedMS(t time.Time) string {
	return fmt.Sprintf("%.3fms", float64(time.Since(t).Nanoseconds())/1e6)
}
