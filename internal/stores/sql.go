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

		// HostDB related fields.
		lastAnnouncementSave   time.Time
		persistInterval        time.Duration
		unappliedAnnouncements []announcement
		unappliedCCID          modules.ConsensusChangeID
		unappliedRevisions     map[types.FileContractID]revisionUpdate
		unappliedProofs        map[types.FileContractID]uint64

		mu           sync.Mutex
		hasAllowlist bool
		hasBlocklist bool

		knownContracts map[types.FileContractID]struct{}
	}

	revisionUpdate struct {
		height uint64
		number uint64
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
func NewSQLStore(conn gorm.Dialector, migrate bool, persistInterval time.Duration, logger glogger.Interface) (*SQLStore, modules.ConsensusChangeID, error) {
	db, err := gorm.Open(conn, &gorm.Config{
		DisableNestedTransaction: true,   // disable nesting transactions
		PrepareStmt:              true,   // caches queries as prepared statements
		Logger:                   logger, // custom logger
	})
	if err != nil {
		return nil, modules.ConsensusChangeID{}, err
	}

	if migrate {
		// Create the tables.
		tables := []interface{}{
			// bus.MetadataStore tables
			&dbArchivedContract{},
			&dbContract{},
			&dbContractSet{},
			&dbObject{},
			&dbSector{},
			&dbShard{},
			&dbSlab{},
			&dbSlice{},

			// bus.HostDB tables
			&dbAnnouncement{},
			&dbConsensusInfo{},
			&dbHost{},
			&dbInteraction{},
			&dbAllowlistEntry{},
			&dbBlocklistEntry{},

			// bus.SettingStore tables
			&dbSetting{},

			// bus.EphemeralAccountStore tables
			&dbAccount{},
		}
		if err := db.AutoMigrate(tables...); err != nil {
			return nil, modules.ConsensusChangeID{}, err
		}
	}

	// Ensure the join table has an index on `db_host_id`.
	switch conn.(type) {
	case *sqlite.Dialector:
		if err := db.Exec("CREATE INDEX IF NOT EXISTS idx_host_blocklist_entry_hosts ON host_blocklist_entry_hosts (db_host_id)").Error; err != nil {
			return nil, modules.ConsensusChangeID{}, err
		}
	case *mysql.Dialector:
		var found int
		err := db.Raw("SELECT COUNT(1) IndexIsThere FROM INFORMATION_SCHEMA.STATISTICS WHERE table_schema=DATABASE() AND table_name='host_blocklist_entry_hosts' AND index_name='idx_host_blocklist_entry_hosts'").Scan(&found).Error
		if err != nil {
			return nil, modules.ConsensusChangeID{}, err
		}
		if found == 0 {
			if err := db.Exec("CREATE INDEX idx_host_blocklist_entry_hosts ON host_blocklist_entry_hosts (db_host_id)").Error; err != nil {
				return nil, modules.ConsensusChangeID{}, err
			}
		}
	default:
		panic("unknown dialector")
	}

	// Get latest consensus change ID or init db.
	var ci dbConsensusInfo
	if err := db.
		Where(&dbConsensusInfo{Model: Model{ID: consensusInfoID}}).
		Attrs(dbConsensusInfo{
			Model: Model{ID: consensusInfoID},
			CCID:  modules.ConsensusChangeBeginning[:],
		}).
		FirstOrCreate(&ci).
		Error; err != nil {
		return nil, modules.ConsensusChangeID{}, err
	}
	var ccid modules.ConsensusChangeID
	copy(ccid[:], ci.CCID)

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
		db:                   db,
		logger:               logger,
		knownContracts:       isOurContract,
		lastAnnouncementSave: time.Now(),
		persistInterval:      persistInterval,
		hasAllowlist:         allowlistCnt > 0,
		hasBlocklist:         blocklistCnt > 0,
		unappliedRevisions:   make(map[types.FileContractID]revisionUpdate),
		unappliedProofs:      make(map[types.FileContractID]uint64),
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
	return db.Close()
}

func (s *SQLStore) retryTransaction(fc func(tx *gorm.DB) error, opts ...*sql.TxOptions) error {
	var err error
	for i := 0; i < 5; i++ {
		err = s.db.Transaction(fc, opts...)
		if err == nil {
			return nil
		}
		s.logger.Warn(context.Background(), fmt.Sprintf("transaction attempt %d/%d failed, err: %v", i+1, 5, err))
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("retryTransaction failed: %w", err)
}
