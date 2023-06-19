package stores

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/siad/modules"
	"gorm.io/driver/mysql"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	glogger "gorm.io/gorm/logger"
)

const (
	// maxSQLVars is the maximum number of variables in an sql query. This
	// number matches the sqlite default of 32766 rounded down to the nearest
	// 1000. This is also lower than the mysql default of 65535.
	maxSQLVars = 32000
)

type (
	// Model defines the common fields of every table. Same as Model
	// but excludes soft deletion since it breaks cascading deletes.
	Model struct {
		ID        uint `gorm:"primarykey"`
		CreatedAt time.Time
	}

	// SQLStore is a helper type for interacting with a SQL-based backend.
	SQLStore struct {
		db     *gorm.DB
		logger glogger.Interface

		// Persistence buffer - related fields.
		lastSave               time.Time
		persistInterval        time.Duration
		persistMu              sync.Mutex
		persistTimer           *time.Timer
		unappliedAnnouncements []announcement
		unappliedHostKeys      map[types.PublicKey]struct{}
		unappliedRevisions     map[types.FileContractID]revisionUpdate
		unappliedProofs        map[types.FileContractID]uint64
		unappliedOutputChanges []outputChange
		unappliedTxnChanges    []txnChange

		// SettingsDB related fields.
		settingsMu sync.Mutex
		settings   map[string]string

		// WalletDB related fields.
		walletAddress types.Address

		// Consensus related fields.
		ccid       modules.ConsensusChangeID
		chainIndex types.ChainIndex

		mu           sync.Mutex
		hasAllowlist bool
		hasBlocklist bool
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
	return sqlite.Open(fmt.Sprintf("file:%s?mode=memory&cache=shared&_foreign_keys=1", name))
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
	return sqlite.Open(fmt.Sprintf("file:%s?_busy_timeout=30000&_foreign_keys=1&_journal_mode=WAL", path))
}

// NewMySQLConnection creates a connection to a MySQL database.
func NewMySQLConnection(user, password, addr, dbName string) gorm.Dialector {
	return mysql.Open(fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8mb4&parseTime=True&loc=Local", user, password, addr, dbName))
}

func DBConfigFromEnv() (uri, user, password, dbName string) {
	uri = os.Getenv("RENTERD_DB_URI")
	user = os.Getenv("RENTERD_DB_USER")
	password = os.Getenv("RENTERD_DB_PASSWORD")
	dbName = os.Getenv("RENTERD_DB_NAME")
	return
}

// NewSQLStore uses a given Dialector to connect to a SQL database.  NOTE: Only
// pass migrate=true for the first instance of SQLHostDB if you connect via the
// same Dialector multiple times.
func NewSQLStore(conn gorm.Dialector, migrate bool, persistInterval time.Duration, walletAddress types.Address, logger glogger.Interface) (*SQLStore, modules.ConsensusChangeID, error) {
	db, err := gorm.Open(conn, &gorm.Config{
		DisableNestedTransaction: true,   // disable nesting transactions
		Logger:                   logger, // custom logger
	})
	if err != nil {
		return nil, modules.ConsensusChangeID{}, err
	}

	// Perform migrations.
	if migrate {
		if err := performMigrations(db, logger); err != nil {
			return nil, modules.ConsensusChangeID{}, err
		}
	}

	// Get latest consensus change ID or init db.
	ci, ccid, err := initConsensusInfo(db)
	if err != nil {
		return nil, modules.ConsensusChangeID{}, err
	}

	// Check allowlist and blocklist counts
	allowlistCnt, err := tableCount(db, &dbAllowlistEntry{})
	if err != nil {
		return nil, modules.ConsensusChangeID{}, err
	}
	blocklistCnt, err := tableCount(db, &dbBlocklistEntry{})
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

	ss := &SQLStore{
		db:                 db,
		logger:             logger,
		knownContracts:     isOurContract,
		lastSave:           time.Now(),
		persistInterval:    persistInterval,
		hasAllowlist:       allowlistCnt > 0,
		hasBlocklist:       blocklistCnt > 0,
		settings:           make(map[string]string),
		unappliedHostKeys:  make(map[types.PublicKey]struct{}),
		unappliedRevisions: make(map[types.FileContractID]revisionUpdate),
		unappliedProofs:    make(map[types.FileContractID]uint64),

		walletAddress: walletAddress,
		chainIndex: types.ChainIndex{
			Height: ci.Height,
			ID:     types.BlockID(ci.BlockID),
		},
	}

	return ss, ccid, nil
}

func isSQLite(db *gorm.DB) bool {
	switch db.Dialector.(type) {
	case *sqlite.Dialector:
		return true
	case *mysql.Dialector:
		return false
	default:
		panic(fmt.Sprintf("unknown dialector: %t", db.Dialector))
	}
}

func (ss *SQLStore) updateHasAllowlist(err *error) {
	if *err != nil {
		return
	}

	cnt, cErr := tableCount(ss.db, &dbAllowlistEntry{})
	if cErr != nil {
		*err = cErr
		return
	}

	ss.mu.Lock()
	ss.hasAllowlist = cnt > 0
	ss.mu.Unlock()
}

func (ss *SQLStore) updateHasBlocklist(err *error) {
	if *err != nil {
		return
	}

	cnt, cErr := tableCount(ss.db, &dbBlocklistEntry{})
	if cErr != nil {
		*err = cErr
		return
	}

	ss.mu.Lock()
	ss.hasBlocklist = cnt > 0
	ss.mu.Unlock()
}

func tableCount(db *gorm.DB, model interface{}) (cnt int64, err error) {
	err = db.Model(model).Count(&cnt).Error
	return
}

// Close closes the underlying database connection of the store.
func (s *SQLStore) Close() error {
	db, err := s.db.DB()
	if err != nil {
		return err
	}

	err = db.Close()
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
	ss.processConsensusChangeWallet(cc)

	// Update consensus fields.
	ss.ccid = cc.ID
	ss.chainIndex = types.ChainIndex{
		Height: uint64(cc.BlockHeight),
		ID:     types.BlockID(cc.AppliedBlocks[len(cc.AppliedBlocks)-1].ID()),
	}

	// Try to apply the updates.
	if err := ss.applyUpdates(false); err != nil {
		ss.logger.Error(context.Background(), fmt.Sprintf("failed to apply updates, err: %v", err))
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
			ss.logger.Error(context.Background(), fmt.Sprintf("failed to apply updates, err: %v", err))
		}
	})
}

// applyUpdates applies all unapplied updates to the database.
func (ss *SQLStore) applyUpdates(force bool) (err error) {
	// Check if we need to apply changes
	persistIntervalPassed := time.Since(ss.lastSave) > ss.persistInterval                           // enough time has passed since last persist
	softLimitReached := len(ss.unappliedAnnouncements) >= announcementBatchSoftLimit                // enough announcements have accumulated
	unappliedRevisionsOrProofs := len(ss.unappliedRevisions) > 0 || len(ss.unappliedProofs) > 0     // enough revisions/proofs have accumulated
	unappliedOutputsOrTxns := len(ss.unappliedOutputChanges) > 0 || len(ss.unappliedTxnChanges) > 0 // enough outputs/txns have accumualted
	if !force && !persistIntervalPassed && !softLimitReached && !unappliedRevisionsOrProofs && !unappliedOutputsOrTxns {
		return nil
	}

	// Fetch allowlist
	var allowlist []dbAllowlistEntry
	if err := ss.db.
		Model(&dbAllowlistEntry{}).
		Find(&allowlist).
		Error; err != nil {
		ss.logger.Error(context.Background(), fmt.Sprintf("failed to fetch allowlist, err: %v", err))
	}

	// Fetch blocklist
	var blocklist []dbBlocklistEntry
	if err := ss.db.
		Model(&dbBlocklistEntry{}).
		Find(&blocklist).
		Error; err != nil {
		ss.logger.Error(context.Background(), fmt.Sprintf("failed to fetch blocklist, err: %v", err))
	}

	err = ss.retryTransaction(func(tx *gorm.DB) (err error) {
		if len(ss.unappliedAnnouncements) > 0 {
			if err = insertAnnouncements(tx, ss.unappliedAnnouncements); err != nil {
				return fmt.Errorf("%w; failed to insert %d announcements", err, len(ss.unappliedAnnouncements))
			}
		}
		if len(ss.unappliedHostKeys) > 0 && (len(allowlist)+len(blocklist)) > 0 {
			for host := range ss.unappliedHostKeys {
				if err := updateBlocklist(tx, host, allowlist, blocklist); err != nil {
					ss.logger.Error(context.Background(), fmt.Sprintf("failed to update blocklist, err: %v", err))
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
		return updateCCID(tx, ss.ccid, ss.chainIndex)
	})

	ss.unappliedProofs = make(map[types.FileContractID]uint64)
	ss.unappliedRevisions = make(map[types.FileContractID]revisionUpdate)
	ss.unappliedHostKeys = make(map[types.PublicKey]struct{})
	ss.unappliedAnnouncements = ss.unappliedAnnouncements[:0]
	ss.lastSave = time.Now()
	ss.unappliedOutputChanges = nil
	ss.unappliedTxnChanges = nil
	return
}

func (s *SQLStore) retryTransaction(fc func(tx *gorm.DB) error, opts ...*sql.TxOptions) error {
	var err error
	timeoutIntervals := []time.Duration{200 * time.Millisecond, 500 * time.Millisecond, time.Second, 3 * time.Second, 10 * time.Second}
	for i := 0; i < len(timeoutIntervals); i++ {
		err = s.db.Transaction(fc, opts...)
		if err == nil {
			return nil
		}
		s.logger.Warn(context.Background(), fmt.Sprintf("transaction attempt %d/%d failed, err: %v", i+1, 5, err))
		time.Sleep(timeoutIntervals[i])
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

func (s *SQLStore) ResetConsensusSubscription() error {
	// drop tables
	err := s.db.Migrator().DropTable(&dbConsensusInfo{}, &dbSiacoinElement{}, &dbTransaction{})
	if err != nil {
		return err
	}
	// recreate the tables.
	err = s.db.Migrator().AutoMigrate(&dbConsensusInfo{}, &dbSiacoinElement{}, &dbTransaction{})
	if err != nil {
		return err
	}
	// initialise the consenus_info table.
	ci, _, err := initConsensusInfo(s.db)
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
