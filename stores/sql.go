package stores

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"strings"
	"sync"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/alerts"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/stores/sql"
	"go.sia.tech/renterd/stores/sql/mysql"
	"go.sia.tech/renterd/stores/sql/sqlite"
	"go.sia.tech/siad/modules"
	"go.uber.org/zap"
	gmysql "gorm.io/driver/mysql"
	gsqlite "gorm.io/driver/sqlite"
	"gorm.io/gorm"
	glogger "gorm.io/gorm/logger"
)

var (
	exprTRUE = gorm.Expr("TRUE")
)

type (
	// Model defines the common fields of every table. Same as Model
	// but excludes soft deletion since it breaks cascading deletes.
	Model struct {
		ID        uint `gorm:"primarykey"`
		CreatedAt time.Time
	}

	// Config contains all params for creating a SQLStore
	Config struct {
		Conn                          gorm.Dialector
		DBMetrics                     sql.MetricsDatabase
		Alerts                        alerts.Alerter
		PartialSlabDir                string
		Migrate                       bool
		AnnouncementMaxAge            time.Duration
		PersistInterval               time.Duration
		WalletAddress                 types.Address
		SlabBufferCompletionThreshold int64
		Logger                        *zap.SugaredLogger
		GormLogger                    glogger.Interface
		RetryTransactionIntervals     []time.Duration
		LongQueryDuration             time.Duration
		LongTxDuration                time.Duration
	}

	// SQLStore is a helper type for interacting with a SQL-based backend.
	SQLStore struct {
		alerts   alerts.Alerter
		db       *gorm.DB
		bMain    sql.Database
		bMetrics sql.MetricsDatabase
		logger   *zap.SugaredLogger

		slabBufferMgr *SlabBufferManager

		retryTransactionIntervals []time.Duration

		// Persistence buffer - related fields.
		lastSave               time.Time
		persistInterval        time.Duration
		persistMu              sync.Mutex
		persistTimer           *time.Timer
		unappliedAnnouncements []announcement
		unappliedContractState map[types.FileContractID]contractState
		unappliedHostKeys      map[types.PublicKey]struct{}
		unappliedRevisions     map[types.FileContractID]revisionUpdate
		unappliedProofs        map[types.FileContractID]uint64
		unappliedOutputChanges []outputChange
		unappliedTxnChanges    []txnChange

		// HostDB related fields
		announcementMaxAge time.Duration

		// SettingsDB related fields.
		settingsMu sync.Mutex
		settings   map[string]string

		// WalletDB related fields.
		walletAddress types.Address

		// Consensus related fields.
		ccid       modules.ConsensusChangeID
		chainIndex types.ChainIndex

		shutdownCtx       context.Context
		shutdownCtxCancel context.CancelFunc

		slabPruneSigChan chan struct{}

		wg           sync.WaitGroup
		mu           sync.Mutex
		lastPrunedAt time.Time
		closed       bool

		knownContracts map[types.FileContractID]struct{}
	}

	revisionUpdate struct {
		height uint64
		number uint64
		size   uint64
	}
)

// NewEphemeralSQLiteConnection creates a connection to an in-memory SQLite DB.
// NOTE: Use simple names such as a random hex identifier or the filepath.Base
// of a test's name. Certain symbols will break the cfg string and cause a file
// to be created on disk.
//
//	mode: set to memory for in-memory database
//	cache: set to shared which is required for in-memory databases
//	_foreign_keys: enforce foreign_key relations
func NewEphemeralSQLiteConnection(name string) gorm.Dialector {
	return gsqlite.Open(fmt.Sprintf("file:%s?mode=memory&cache=shared&_foreign_keys=1", name))
}

// NewSQLiteConnection opens a sqlite db at the given path.
//
//	_busy_timeout: set to prevent concurrent transactions from failing and
//	  instead have them block
//	_foreign_keys: enforce foreign_key relations
//	_journal_mode: set to WAL instead of delete since it's usually the fastest.
//	  Only downside is that the db won't work on network drives. In that case this
//	  should be made configurable and set to TRUNCATE or any of the other options.
//	  For reference see https://github.com/mattn/go-sqlite3#connection-string.
func NewSQLiteConnection(path string) gorm.Dialector {
	return gsqlite.Open(fmt.Sprintf("file:%s?_busy_timeout=30000&_foreign_keys=1&_journal_mode=WAL&_secure_delete=false&_cache_size=65536", path))
}

// NewMetricsSQLiteConnection opens a sqlite db at the given path similarly to
// NewSQLiteConnection but with weaker consistency guarantees since it's
// optimised for recording metrics.
func NewMetricsSQLiteConnection(path string) gorm.Dialector {
	return gsqlite.Open(fmt.Sprintf("file:%s?_busy_timeout=30000&_foreign_keys=1&_journal_mode=WAL&_synchronous=NORMAL", path))
}

// NewMySQLConnection creates a connection to a MySQL database.
func NewMySQLConnection(user, password, addr, dbName string) gorm.Dialector {
	return gmysql.Open(fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8mb4&parseTime=True&loc=Local&multiStatements=true", user, password, addr, dbName))
}

// NewSQLStore uses a given Dialector to connect to a SQL database.  NOTE: Only
// pass migrate=true for the first instance of SQLHostDB if you connect via the
// same Dialector multiple times.
func NewSQLStore(cfg Config) (*SQLStore, modules.ConsensusChangeID, error) {
	// Sanity check announcement max age.
	if cfg.AnnouncementMaxAge == 0 {
		return nil, modules.ConsensusChangeID{}, errors.New("announcementMaxAge must be non-zero")
	}

	if err := os.MkdirAll(cfg.PartialSlabDir, 0700); err != nil {
		return nil, modules.ConsensusChangeID{}, fmt.Errorf("failed to create partial slab dir: %v", err)
	}
	db, err := gorm.Open(cfg.Conn, &gorm.Config{
		Logger:                   cfg.GormLogger, // custom logger
		SkipDefaultTransaction:   true,
		DisableNestedTransaction: true,
	})
	if err != nil {
		return nil, modules.ConsensusChangeID{}, fmt.Errorf("failed to open SQL db")
	}
	l := cfg.Logger.Named("sql")

	sqlDB, err := db.DB()
	if err != nil {
		return nil, modules.ConsensusChangeID{}, fmt.Errorf("failed to fetch db: %v", err)
	}

	// Print DB version
	var dbMain sql.Database
	dbMetrics := cfg.DBMetrics
	var mainErr error
	if cfg.Conn.Name() == "sqlite" {
		dbMain, mainErr = sqlite.NewMainDatabase(sqlDB, l, cfg.LongQueryDuration, cfg.LongTxDuration)
	} else {
		dbMain, mainErr = mysql.NewMainDatabase(sqlDB, l, cfg.LongQueryDuration, cfg.LongTxDuration)
	}
	if mainErr != nil {
		return nil, modules.ConsensusChangeID{}, fmt.Errorf("failed to create main database: %v", mainErr)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	dbName, dbVersion, err := dbMain.Version(ctx)
	if err != nil {
		return nil, modules.ConsensusChangeID{}, fmt.Errorf("failed to fetch db version: %v", err)
	}
	l.Infof("Using %s version %s", dbName, dbVersion)

	// Perform migrations.
	if cfg.Migrate {
		if err := dbMain.Migrate(context.Background()); err != nil {
			return nil, modules.ConsensusChangeID{}, fmt.Errorf("failed to perform migrations: %v", err)
		} else if err := dbMetrics.Migrate(context.Background()); err != nil {
			return nil, modules.ConsensusChangeID{}, fmt.Errorf("failed to perform migrations for metrics db: %v", err)
		}
	}

	// Get latest consensus change ID or init db.
	ci, ccid, err := initConsensusInfo(db)
	if err != nil {
		return nil, modules.ConsensusChangeID{}, err
	}

	// Fetch contract ids.
	var activeFCIDs, archivedFCIDs []fileContractID
	if err := db.Model(&dbContract{}).
		Select("fcid").
		Find(&activeFCIDs).Error; err != nil {
		return nil, modules.ConsensusChangeID{}, err
	}
	if err := db.Model(&dbArchivedContract{}).
		Select("fcid").
		Find(&archivedFCIDs).Error; err != nil {
		return nil, modules.ConsensusChangeID{}, err
	}
	isOurContract := make(map[types.FileContractID]struct{})
	for _, fcid := range append(activeFCIDs, archivedFCIDs...) {
		isOurContract[types.FileContractID(fcid)] = struct{}{}
	}

	shutdownCtx, shutdownCtxCancel := context.WithCancel(context.Background())
	ss := &SQLStore{
		alerts:                 cfg.Alerts,
		ccid:                   ccid,
		db:                     db,
		bMain:                  dbMain,
		bMetrics:               dbMetrics,
		logger:                 l,
		knownContracts:         isOurContract,
		lastSave:               time.Now(),
		persistInterval:        cfg.PersistInterval,
		settings:               make(map[string]string),
		slabPruneSigChan:       make(chan struct{}, 1),
		unappliedContractState: make(map[types.FileContractID]contractState),
		unappliedHostKeys:      make(map[types.PublicKey]struct{}),
		unappliedRevisions:     make(map[types.FileContractID]revisionUpdate),
		unappliedProofs:        make(map[types.FileContractID]uint64),

		announcementMaxAge: cfg.AnnouncementMaxAge,

		walletAddress: cfg.WalletAddress,
		chainIndex: types.ChainIndex{
			Height: ci.Height,
			ID:     types.BlockID(ci.BlockID),
		},

		lastPrunedAt:              time.Now(),
		retryTransactionIntervals: cfg.RetryTransactionIntervals,

		shutdownCtx:       shutdownCtx,
		shutdownCtxCancel: shutdownCtxCancel,
	}

	ss.slabBufferMgr, err = newSlabBufferManager(shutdownCtx, cfg.Alerts, dbMain, l.Named("slabbuffers"), cfg.SlabBufferCompletionThreshold, cfg.PartialSlabDir)
	if err != nil {
		return nil, modules.ConsensusChangeID{}, err
	}
	if err := ss.initSlabPruning(); err != nil {
		return nil, modules.ConsensusChangeID{}, err
	}
	return ss, ccid, nil
}

func isSQLite(db *gorm.DB) bool {
	switch db.Dialector.(type) {
	case *gsqlite.Dialector:
		return true
	case *gmysql.Dialector:
		return false
	default:
		panic(fmt.Sprintf("unknown dialector: %t", db.Dialector))
	}
}

func (s *SQLStore) initSlabPruning() error {
	// start pruning loop
	s.wg.Add(1)
	go func() {
		s.pruneSlabsLoop()
		s.wg.Done()
	}()

	// prune once to guarantee consistency on startup
	return s.bMain.Transaction(s.shutdownCtx, func(tx sql.DatabaseTx) error {
		_, err := tx.PruneSlabs(s.shutdownCtx, math.MaxInt64)
		return err
	})
}

// Close closes the underlying database connection of the store.
func (s *SQLStore) Close() error {
	s.shutdownCtxCancel()
	s.wg.Wait()

	err := s.bMain.Close()
	if err != nil {
		return err
	}
	err = s.bMetrics.Close()
	if err != nil {
		return err
	}

	err = s.slabBufferMgr.Close()
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	return nil
}

// ProcessConsensusChange implements consensus.Subscriber.
func (ss *SQLStore) ProcessConsensusChange(cc modules.ConsensusChange) {
	ss.persistMu.Lock()
	defer ss.persistMu.Unlock()

	ss.processConsensusChangeHostDB(cc)
	ss.processConsensusChangeContracts(cc)
	ss.processConsensusChangeWallet(cc)

	// Update consensus fields.
	ss.ccid = cc.ID
	ss.chainIndex = types.ChainIndex{
		Height: uint64(cc.BlockHeight),
		ID:     types.BlockID(cc.AppliedBlocks[len(cc.AppliedBlocks)-1].ID()),
	}

	// Try to apply the updates.
	if err := ss.applyUpdates(false); err != nil {
		ss.logger.Error(fmt.Sprintf("failed to apply updates, err: %v", err))
	}

	// Force a persist if no block has been received for some time.
	if ss.persistTimer != nil {
		ss.persistTimer.Stop()
		select {
		case <-ss.persistTimer.C:
		default:
		}
	}
	ss.persistTimer = time.AfterFunc(10*time.Second, func() {
		ss.mu.Lock()
		if ss.closed {
			ss.mu.Unlock()
			return
		}
		ss.mu.Unlock()

		ss.persistMu.Lock()
		defer ss.persistMu.Unlock()
		if err := ss.applyUpdates(true); err != nil {
			ss.logger.Error(fmt.Sprintf("failed to apply updates, err: %v", err))
		}
	})
}

// applyUpdates applies all unapplied updates to the database.
func (ss *SQLStore) applyUpdates(force bool) error {
	// Check if we need to apply changes
	persistIntervalPassed := time.Since(ss.lastSave) > ss.persistInterval                           // enough time has passed since last persist
	softLimitReached := len(ss.unappliedAnnouncements) >= announcementBatchSoftLimit                // enough announcements have accumulated
	unappliedRevisionsOrProofs := len(ss.unappliedRevisions) > 0 || len(ss.unappliedProofs) > 0     // enough revisions/proofs have accumulated
	unappliedOutputsOrTxns := len(ss.unappliedOutputChanges) > 0 || len(ss.unappliedTxnChanges) > 0 // enough outputs/txns have accumualted
	unappliedContractState := len(ss.unappliedContractState) > 0                                    // the chain state of a contract changed
	if !force && !persistIntervalPassed && !softLimitReached && !unappliedRevisionsOrProofs && !unappliedOutputsOrTxns && !unappliedContractState {
		return nil
	}

	// Fetch allowlist
	var allowlist []dbAllowlistEntry
	if err := ss.db.
		Model(&dbAllowlistEntry{}).
		Find(&allowlist).
		Error; err != nil {
		ss.logger.Error(fmt.Sprintf("failed to fetch allowlist, err: %v", err))
	}

	// Fetch blocklist
	var blocklist []dbBlocklistEntry
	if err := ss.db.
		Model(&dbBlocklistEntry{}).
		Find(&blocklist).
		Error; err != nil {
		ss.logger.Error(fmt.Sprintf("failed to fetch blocklist, err: %v", err))
	}

	err := ss.retryTransaction(context.Background(), func(tx *gorm.DB) (err error) {
		if len(ss.unappliedAnnouncements) > 0 {
			if err = insertAnnouncements(tx, ss.unappliedAnnouncements); err != nil {
				return fmt.Errorf("%w; failed to insert %d announcements", err, len(ss.unappliedAnnouncements))
			}
		}
		if len(ss.unappliedHostKeys) > 0 && (len(allowlist)+len(blocklist)) > 0 {
			for host := range ss.unappliedHostKeys {
				if err := updateBlocklist(tx, host, allowlist, blocklist); err != nil {
					ss.logger.Error(fmt.Sprintf("failed to update blocklist, err: %v", err))
				}
			}
		}
		for fcid, rev := range ss.unappliedRevisions {
			if err := applyRevisionUpdate(tx, types.FileContractID(fcid), rev); err != nil {
				return fmt.Errorf("%w; failed to update revision number and height", err)
			}
		}
		for fcid, proofHeight := range ss.unappliedProofs {
			if err := updateProofHeight(tx, types.FileContractID(fcid), proofHeight); err != nil {
				return fmt.Errorf("%w; failed to update proof height", err)
			}
		}
		for _, oc := range ss.unappliedOutputChanges {
			if oc.addition {
				err = applyUnappliedOutputAdditions(tx, oc.sco)
			} else {
				err = applyUnappliedOutputRemovals(tx, oc.oid)
			}
			if err != nil {
				return fmt.Errorf("%w; failed to apply unapplied output change", err)
			}
		}
		for _, tc := range ss.unappliedTxnChanges {
			if tc.addition {
				err = applyUnappliedTxnAdditions(tx, tc.txn)
			} else {
				err = applyUnappliedTxnRemovals(tx, tc.txnID)
			}
			if err != nil {
				return fmt.Errorf("%w; failed to apply unapplied txn change", err)
			}
		}
		for fcid, cs := range ss.unappliedContractState {
			if err := updateContractState(tx, fcid, cs); err != nil {
				return fmt.Errorf("%w; failed to update chain state", err)
			}
		}
		if err := markFailedContracts(tx, ss.chainIndex.Height); err != nil {
			return err
		}
		return updateCCID(tx, ss.ccid, ss.chainIndex)
	})
	if err != nil {
		return fmt.Errorf("%w; failed to apply updates", err)
	}

	ss.unappliedContractState = make(map[types.FileContractID]contractState)
	ss.unappliedProofs = make(map[types.FileContractID]uint64)
	ss.unappliedRevisions = make(map[types.FileContractID]revisionUpdate)
	ss.unappliedHostKeys = make(map[types.PublicKey]struct{})
	ss.unappliedAnnouncements = ss.unappliedAnnouncements[:0]
	ss.lastSave = time.Now()
	ss.unappliedOutputChanges = nil
	ss.unappliedTxnChanges = nil
	return nil
}

func (s *SQLStore) retryTransaction(ctx context.Context, fc func(tx *gorm.DB) error) error {
	abortRetry := func(err error) bool {
		if err == nil ||
			errors.Is(err, context.Canceled) ||
			errors.Is(err, context.DeadlineExceeded) ||
			errors.Is(err, gorm.ErrRecordNotFound) ||
			errors.Is(err, api.ErrContractNotFound) ||
			errors.Is(err, api.ErrObjectNotFound) ||
			errors.Is(err, api.ErrObjectCorrupted) ||
			errors.Is(err, api.ErrBucketExists) ||
			errors.Is(err, api.ErrBucketNotFound) ||
			errors.Is(err, api.ErrBucketNotEmpty) ||
			errors.Is(err, api.ErrContractNotFound) ||
			errors.Is(err, api.ErrMultipartUploadNotFound) ||
			errors.Is(err, api.ErrObjectExists) ||
			strings.Contains(err.Error(), "no such table") ||
			strings.Contains(err.Error(), "Duplicate entry") ||
			errors.Is(err, api.ErrPartNotFound) ||
			errors.Is(err, api.ErrSlabNotFound) {
			return true
		}
		return false
	}

	var err error
	attempts := len(s.retryTransactionIntervals) + 1
	for i := 0; i < attempts; i++ {
		// execute the transaction
		err = s.db.WithContext(ctx).Transaction(fc)
		if abortRetry(err) {
			return err
		}

		// if this was the last attempt, return the error
		if i == len(s.retryTransactionIntervals) {
			s.logger.Warn(fmt.Sprintf("transaction attempt %d/%d failed, err: %v", i+1, attempts, err))
			return err
		}

		// log the failed attempt and sleep before retrying
		interval := s.retryTransactionIntervals[i]
		s.logger.Warn(fmt.Sprintf("transaction attempt %d/%d failed, retry in %v,  err: %v", i+1, attempts, interval, err))
		time.Sleep(interval)
	}
	return fmt.Errorf("retryTransaction failed: %w", err)
}

func initConsensusInfo(db *gorm.DB) (dbConsensusInfo, modules.ConsensusChangeID, error) {
	var ci dbConsensusInfo
	if err := db.
		Where(&dbConsensusInfo{Model: Model{ID: consensusInfoID}}).
		Attrs(dbConsensusInfo{
			Model: Model{ID: consensusInfoID},
			CCID:  modules.ConsensusChangeBeginning[:],
		}).
		FirstOrCreate(&ci).
		Error; err != nil {
		return dbConsensusInfo{}, modules.ConsensusChangeID{}, err
	}
	var ccid modules.ConsensusChangeID
	copy(ccid[:], ci.CCID)
	return ci, ccid, nil
}

func (s *SQLStore) ResetConsensusSubscription(ctx context.Context) error {
	// empty tables and reinit consensus_infos
	var ci dbConsensusInfo
	err := s.retryTransaction(ctx, func(tx *gorm.DB) error {
		if err := s.db.Exec("DELETE FROM consensus_infos").Error; err != nil {
			return err
		} else if err := s.db.Exec("DELETE FROM siacoin_elements").Error; err != nil {
			return err
		} else if err := s.db.Exec("DELETE FROM transactions").Error; err != nil {
			return err
		} else if ci, _, err = initConsensusInfo(tx); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	// reset in-memory state.
	s.persistMu.Lock()
	s.chainIndex = types.ChainIndex{
		Height: ci.Height,
		ID:     types.BlockID(ci.BlockID),
	}
	s.persistMu.Unlock()
	return nil
}
