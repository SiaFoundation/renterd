package stores

import (
	"fmt"
	"time"

	"go.sia.tech/renterd/bus"
	"go.sia.tech/siad/modules"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type (
	// dbCommon specifies all fields that every table in the database should have.
	dbCommon struct {
		CreatedAt time.Time
		UpdatedAt time.Time
		DeletedAt gorm.DeletedAt `gorm:"index"`
	}

	// SQLStore is a helper type for interacting with a SQL-based backend.
	SQLStore struct {
		db *gorm.DB
	}
)

// Check that the SQLStore implements all the required interfaces.
var (
	_ bus.ContractStore = &SQLStore{}
	_ bus.HostDB        = &SQLStore{}
	_ bus.HostSetStore  = &SQLStore{}
	_ bus.ObjectStore   = &SQLStore{}
)

// NewEphemeralSQLiteConnection creates a connection to an in-memory SQLite DB.
// NOTE: Use simple names such as a random hex identifier or the filepath.Base
// of a test's name. Certain symbols will break the cfg string and cause a file
// to be created on disk.
func NewEphemeralSQLiteConnection(name string) gorm.Dialector {
	return sqlite.Open(fmt.Sprintf("file:%s?mode=memory&cache=shared", name))
}

// NewSQLiteConnection opens a sqlite db at the given path.
func NewSQLiteConnection(path string) gorm.Dialector {
	return sqlite.Open(path)
}

// NewSQLStore uses a given Dialector to connect to a SQL database.  NOTE: Only
// pass migrate=true for the first instance of SQLHostDB if you connect via the
// same Dialector multiple times.
func NewSQLStore(conn gorm.Dialector, migrate bool) (*SQLStore, modules.ConsensusChangeID, error) {
	db, err := gorm.Open(conn, &gorm.Config{})
	if err != nil {
		return nil, modules.ConsensusChangeID{}, err
	}

	if migrate {
		// Create the tables.
		tables := []interface{}{
			// bus.ContractStore tables
			&dbContractRHPv2{},
			&dbFileContractRevision{},
			&dbTransactionSignature{},
			&dbValidSiacoinOutput{},
			&dbMissedSiacoinOutput{},

			// bus.HostDB tables
			&dbHost{},
			&dbInteraction{},
			&dbAnnouncement{},
			&dbConsensusInfo{},

			// bus.HostSetStore tables
			&dbHostSet{},
			&dbHostSetEntry{},

			// bus.ObjectStore tables
			&dbObject{},
			&dbSlice{},
			&dbSlab{},
			&dbSector{},
		}
		if err := db.AutoMigrate(tables...); err != nil {
			return nil, modules.ConsensusChangeID{}, err
		}
		if res := db.Exec("PRAGMA foreign_keys = ON", nil); res.Error != nil {
			return nil, modules.ConsensusChangeID{}, res.Error
		}
	}

	// Get latest consensus change ID or init db.
	var ci dbConsensusInfo
	err = db.Where(&dbConsensusInfo{ID: consensusInfoID}).
		Attrs(dbConsensusInfo{
			ID:   consensusInfoID,
			CCID: modules.ConsensusChangeBeginning[:],
		}).
		FirstOrCreate(&ci).Error
	if err != nil {
		return nil, modules.ConsensusChangeID{}, err
	}
	var ccid modules.ConsensusChangeID
	copy(ccid[:], ci.CCID)

	return &SQLStore{
		db: db,
	}, ccid, nil
}
