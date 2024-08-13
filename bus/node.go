package bus

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/renterd/alerts"
	"go.sia.tech/renterd/config"
	"go.sia.tech/renterd/internal/utils"
	"go.sia.tech/renterd/stores"
	"go.sia.tech/renterd/stores/sql"
	"go.sia.tech/renterd/stores/sql/mysql"
	"go.sia.tech/renterd/stores/sql/sqlite"
	"go.sia.tech/renterd/webhooks"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"moul.io/zapgorm2"
)

type NodeConfig struct {
	config.Bus
	Database                    config.Database
	DatabaseLog                 config.DatabaseLog
	Genesis                     types.Block
	Logger                      *zap.Logger
	Network                     *consensus.Network
	RetryTxIntervals            []time.Duration
	SyncerSyncInterval          time.Duration
	SyncerPeerDiscoveryInterval time.Duration
}

func NewNode(cfg NodeConfig, dir string, seed types.PrivateKey, logger *zap.Logger) (http.Handler, func(context.Context) error, *chain.Manager, error) {
	// ensure we don't hang indefinitely
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// create database connections
	var dbConn gorm.Dialector
	var dbMetrics sql.MetricsDatabase
	if cfg.Database.MySQL.URI != "" {
		// create MySQL connections
		dbConn = stores.NewMySQLConnection(
			cfg.Database.MySQL.User,
			cfg.Database.MySQL.Password,
			cfg.Database.MySQL.URI,
			cfg.Database.MySQL.Database,
		)
		dbm, err := mysql.Open(
			cfg.Database.MySQL.User,
			cfg.Database.MySQL.Password,
			cfg.Database.MySQL.URI,
			cfg.Database.MySQL.MetricsDatabase,
		)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("failed to open MySQL metrics database: %w", err)
		}
		dbMetrics, err = mysql.NewMetricsDatabase(dbm, logger.Named("metrics").Sugar(), cfg.DatabaseLog.SlowThreshold, cfg.DatabaseLog.SlowThreshold)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("failed to create MySQL metrics database: %w", err)
		}
	} else {
		// create database directory
		dbDir := filepath.Join(dir, "db")
		if err := os.MkdirAll(dbDir, 0700); err != nil {
			return nil, nil, nil, err
		}

		// create SQLite connections
		dbConn = stores.NewSQLiteConnection(filepath.Join(dbDir, "db.sqlite"))

		dbm, err := sqlite.Open(filepath.Join(dbDir, "metrics.sqlite"))
		if err != nil {
			return nil, nil, nil, fmt.Errorf("failed to open SQLite metrics database: %w", err)
		}
		dbMetrics, err = sqlite.NewMetricsDatabase(dbm, logger.Named("metrics").Sugar(), cfg.DatabaseLog.SlowThreshold, cfg.DatabaseLog.SlowThreshold)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("failed to create SQLite metrics database: %w", err)
		}
	}

	// create database logger
	dbLogger := zapgorm2.Logger{
		ZapLogger:                 cfg.Logger.Named("SQL"),
		LogLevel:                  gormLogLevel(cfg.DatabaseLog),
		SlowThreshold:             cfg.DatabaseLog.SlowThreshold,
		SkipCallerLookup:          false,
		IgnoreRecordNotFoundError: cfg.DatabaseLog.IgnoreRecordNotFoundError,
		Context:                   nil,
	}

	alertsMgr := alerts.NewManager()
	sqlStoreDir := filepath.Join(dir, "partial_slabs")
	sqlStore, err := stores.NewSQLStore(stores.Config{
		Conn:                          dbConn,
		Alerts:                        alerts.WithOrigin(alertsMgr, "bus"),
		DBMetrics:                     dbMetrics,
		PartialSlabDir:                sqlStoreDir,
		Migrate:                       true,
		SlabBufferCompletionThreshold: cfg.SlabBufferCompletionThreshold,
		Logger:                        logger.Sugar(),
		GormLogger:                    dbLogger,
		RetryTransactionIntervals:     cfg.RetryTxIntervals,
		WalletAddress:                 types.StandardUnlockHash(seed.PublicKey()),
		LongQueryDuration:             cfg.DatabaseLog.SlowThreshold,
		LongTxDuration:                cfg.DatabaseLog.SlowThreshold,
	})
	if err != nil {
		return nil, nil, nil, err
	}

	// create webhooks manager
	wh, err := webhooks.NewManager(sqlStore, logger)
	if err != nil {
		return nil, nil, nil, err
	}

	// hookup webhooks <-> alerts
	alertsMgr.RegisterWebhookBroadcaster(wh)

	// create consensus directory
	consensusDir := filepath.Join(dir, "consensus")
	if err := os.MkdirAll(consensusDir, 0700); err != nil {
		return nil, nil, nil, err
	}

	// migrate consensus database
	oldConsensus, err := os.Stat(filepath.Join(consensusDir, "consensus.db"))
	if err != nil && !os.IsNotExist(err) {
		return nil, nil, nil, err
	} else if err == nil {
		logger.Warn("found old consensus.db, indicating a migration is necessary")

		// reset chain state
		logger.Warn("Resetting chain state...")
		if err := sqlStore.ResetChainState(ctx); err != nil {
			return nil, nil, nil, err
		}
		logger.Warn("Chain state was successfully reset.")

		// remove consensus.db and consensus.log file
		logger.Warn("Removing consensus database...")
		_ = os.RemoveAll(filepath.Join(consensusDir, "consensus.log")) // ignore error
		if err := os.Remove(filepath.Join(consensusDir, "consensus.db")); err != nil {
			return nil, nil, nil, err
		}
		logger.Warn(fmt.Sprintf("Old 'consensus.db' was successfully removed, reclaimed %v of disk space.", utils.HumanReadableSize(int(oldConsensus.Size()))))
		logger.Warn("ATTENTION: consensus will now resync from scratch, this process may take several hours to complete")
	}

	// reset chain state if blockchain.db does not exist to make sure deleting
	// it forces a resync
	chainPath := filepath.Join(consensusDir, "blockchain.db")
	if _, err := os.Stat(chainPath); os.IsNotExist(err) {
		if err := sqlStore.ResetChainState(context.Background()); err != nil {
			return nil, nil, nil, err
		}
	}

	// create chain database
	bdb, err := coreutils.OpenBoltChainDB(chainPath)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to open chain database: %w", err)
	}

	// create chain manager
	store, state, err := chain.NewDBStore(bdb, cfg.Network, cfg.Genesis)
	if err != nil {
		return nil, nil, nil, err
	}
	cm := chain.NewManager(store, state)

	// create wallet
	w, err := wallet.NewSingleAddressWallet(seed, cm, sqlStore, wallet.WithReservationDuration(cfg.UsedUTXOExpiry))
	if err != nil {
		return nil, nil, nil, err
	}

	// create syncer
	s, err := NewSyncer(cfg, cm, sqlStore, logger)
	if err != nil {
		return nil, nil, nil, err
	}

	// create bus
	announcementMaxAgeHours := time.Duration(cfg.AnnouncementMaxAgeHours) * time.Hour
	b, err := New(ctx, alertsMgr, wh, cm, s, w, sqlStore, sqlStore, sqlStore, sqlStore, sqlStore, sqlStore, sqlStore, announcementMaxAgeHours, logger)
	if err != nil {
		return nil, nil, nil, err
	}

	shutdownFn := func(ctx context.Context) error {
		return errors.Join(
			s.Close(),
			w.Close(),
			b.Shutdown(ctx),
			sqlStore.Close(),
			bdb.Close(),
		)
	}
	return b.Handler(), shutdownFn, cm, nil
}

func gormLogLevel(cfg config.DatabaseLog) logger.LogLevel {
	level := logger.Silent
	if cfg.Enabled {
		switch strings.ToLower(cfg.Level) {
		case "":
			level = logger.Warn // default to 'warn' if not set
		case "error":
			level = logger.Error
		case "warn":
			level = logger.Warn
		case "info":
			level = logger.Info
		case "debug":
			level = logger.Info
		default:
			log.Fatalf("invalid log level %q, options are: silent, error, warn, info", cfg.Level)
		}
	}
	return level
}
