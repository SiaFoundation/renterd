package stores

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/siad/modules"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type (
	// Model defines the common fields of every table. Same as Model
	// but excludes soft deletion since it breaks cascading deletes.
	Model struct {
		ID        uint `gorm:"primarykey"`
		CreatedAt time.Time
		UpdatedAt time.Time
	}

	// SQLStore is a helper type for interacting with a SQL-based backend.
	SQLStore struct {
		db *gorm.DB

		ctx    context.Context
		cancel context.CancelFunc

		persistInterval  time.Duration
		newAnnouncements chan *announcementBatch
		wg               sync.WaitGroup
	}

	announcementBatch struct {
		announcements []announcement
		cc            modules.ConsensusChange
	}

	announcement struct {
		hostKey      consensus.PublicKey
		announcement hostdb.Announcement
	}
)

func (s *SQLStore) threadedProcessAnnouncements() {
	lastInsert := time.Now()
	for {
		var announcements []announcement
		var ccid modules.ConsensusChange

		// Block for next batch.
		select {
		case nextBatch := <-s.newAnnouncements:
			announcements = append(announcements, nextBatch.announcements...)
			ccid = nextBatch.cc
		case <-s.ctx.Done():
			return // shutdown
		}

		// Try to get a few more batches up to a soft limit of
		// announcements.
	ADD_MORE_LOOP:
		for len(announcements) < 100 {
			select {
			case nextBatch := <-s.newAnnouncements:
				if nextBatch != nil {
					announcements = append(announcements, nextBatch.announcements...)
					ccid = nextBatch.cc
				}
			default:
				// No more batches.
				break ADD_MORE_LOOP
			}
		}

		// If there is nothing to insert at all, block again. Unless too
		// much time has passed since the last insertion. Then we want
		// to at least update the ccid.
		if len(announcements) == 0 && time.Since(lastInsert) < s.persistInterval {
			continue
		}

		for {
			err := s.db.Transaction(func(tx *gorm.DB) error {
				// Insert announcements.
				if len(announcements) > 0 {
					if err := insertAnnouncements(tx, announcements); err != nil {
						return err
					}
				}
				// Update consensus change id.
				return updateCCID(tx, ccid)
			})

			// If the update failed, try again after some time.
			if err != nil {
				println("failed to persist host announcements... retrying: " + err.Error()) // TODO: replace with proper logging
				select {
				case <-s.ctx.Done():
					return // shutdown
				case <-time.After(time.Second):
				}
				continue
			}
			lastInsert = time.Now()
			break
		}
	}
}

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
	return sqlite.Open(fmt.Sprintf("file:%s?_busy_timeout=5000&_foreign_keys=1&_journal_mode=WAL", path))
}

// NewSQLStore uses a given Dialector to connect to a SQL database.  NOTE: Only
// pass migrate=true for the first instance of SQLHostDB if you connect via the
// same Dialector multiple times.
func NewSQLStore(conn gorm.Dialector, migrate bool, persistInterval time.Duration) (*SQLStore, modules.ConsensusChangeID, error) {
	db, err := gorm.Open(conn, &gorm.Config{})
	if err != nil {
		return nil, modules.ConsensusChangeID{}, err
	}
	if migrate {
		// Create the tables.
		tables := []interface{}{
			// bus.ContractStore tables
			&dbArchivedContract{},
			&dbContract{},
			&dbContractSet{},
			&dbContractSetContract{},

			// bus.HostDB tables
			&dbHost{},
			&dbInteraction{},
			&dbAnnouncement{},
			&dbConsensusInfo{},

			// bus.ObjectStore tables
			&dbObject{},
			&dbSlice{},
			&dbSlab{},
			&dbSector{},
			&dbShard{},

			// bus.SettingStore tables
			&dbSetting{},
		}
		if err := db.AutoMigrate(tables...); err != nil {
			return nil, modules.ConsensusChangeID{}, err
		}
	}

	// Get latest consensus change ID or init db.
	var ci dbConsensusInfo
	err = db.Where(&dbConsensusInfo{Model: Model{ID: consensusInfoID}}).
		Attrs(dbConsensusInfo{
			Model: Model{
				ID: consensusInfoID,
			},
			CCID: modules.ConsensusChangeBeginning[:],
		}).
		FirstOrCreate(&ci).Error
	if err != nil {
		return nil, modules.ConsensusChangeID{}, err
	}
	var ccid modules.ConsensusChangeID
	copy(ccid[:], ci.CCID)

	ctx, cancel := context.WithCancel(context.Background())
	ss := &SQLStore{
		ctx:              ctx,
		cancel:           cancel,
		db:               db,
		newAnnouncements: make(chan *announcementBatch, 1000),
		persistInterval:  persistInterval,
	}

	// Start background thread.
	ss.wg.Add(1)
	go func() {
		defer ss.wg.Done()
		ss.threadedProcessAnnouncements()
	}()

	return ss, ccid, nil
}

// Close closes the underlying database connection of the store.
func (s *SQLStore) Close() error {
	// Close context.
	s.cancel()

	// Block for threads to finish.
	s.wg.Wait()

	// Close db connection.
	db, err := s.db.DB()
	if err != nil {
		return err
	}
	return db.Close()
}
