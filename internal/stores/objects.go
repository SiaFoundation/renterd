package stores

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"go.sia.tech/renterd/bus"
	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/renterd/object"
	"go.sia.tech/siad/types"
	"gorm.io/gorm"
)

var (
	// ErrOBjectNotFound is returned if get is unable to retrieve an object
	// from the database.
	ErrObjectNotFound = errors.New("object not found in database")

	// ErrSlabNotFound is returned if get is unable to retrieve a slab from
	// the database.
	ErrSlabNotFound = errors.New("slab not found in database")
)

type (
	// dbObject describes an object.Object in the database.
	dbObject struct {
		Model

		Key      []byte
		ObjectID string    `gorm:"index;unique"`
		Slabs    []dbSlice `gorm:"constraint:OnDelete:CASCADE"` // CASCADE to delete slices too
	}

	// dbSlice describes a reference to a object.Slab in the database.
	dbSlice struct {
		Model
		DBObjectID uint `gorm:"index"`

		// Slice related fields.
		Slab   dbSlab `gorm:"constraint:OnDelete:CASCADE"` // CASCADE to delete slabs too
		Offset uint32
		Length uint32
	}

	// dbSlab describes a object.Slab in the database.
	// NOTE: A Slab is uniquely identified by its key.
	dbSlab struct {
		Model
		DBSliceID uint `gorm:"index"`

		Key         []byte    `gorm:"unique;NOT NULL"` // json string
		LastFailure time.Time `gorm:"index"`
		MinShards   uint8
		Shards      []dbShard `gorm:"constraint:OnDelete:CASCADE"` // CASCADE to delete shards too
	}

	// dbShard is a join table between dbSlab and dbSector.
	dbShard struct {
		ID         uint `gorm:"primaryKey"`
		DBSlabID   uint `gorm:"index"`
		DBSector   dbSector
		DBSectorID uint `gorm:"index"`
	}

	// dbSector describes a sector in the database. A sector can exist
	// multiple times in the sectors table since it can belong to multiple
	// slabs.
	dbSector struct {
		Model

		Contracts []dbContract      `gorm:"many2many:contract_sectors;constraint:OnDelete:CASCADE"`
		Hosts     []dbHost          `gorm:"many2many:host_sectors;constraint:OnDelete:CASCADE"`
		Root      consensus.Hash256 `gorm:"index;unique;NOT NULL;type:bytes;serializer:gob"`
	}
)

// TableName implements the gorm.Tabler interface.
func (dbShard) TableName() string { return "shards" }

// TableName implements the gorm.Tabler interface.
func (dbObject) TableName() string { return "objects" }

// TableName implements the gorm.Tabler interface.
func (dbSlice) TableName() string { return "slices" }

// TableName implements the gorm.Tabler interface.
func (dbSlab) TableName() string { return "slabs" }

// TableName implements the gorm.Tabler interface.
func (dbSector) TableName() string { return "sectors" }

// convert turns a dbObject into a object.Slab.
func (s dbSlab) convert() (object.Slab, error) {
	var slabKey object.EncryptionKey
	if err := slabKey.UnmarshalText(s.Key); err != nil {
		return object.Slab{}, err
	}
	return object.Slab{
		Key:       slabKey,
		MinShards: s.MinShards,
		Shards:    make([]object.Sector, len(s.Shards)),
	}, nil
}

// convert turns a dbObject into a object.Object.
func (o dbObject) convert() (object.Object, error) {
	var objKey object.EncryptionKey
	if err := objKey.UnmarshalText(o.Key); err != nil {
		return object.Object{}, err
	}
	obj := object.Object{
		Key:   objKey,
		Slabs: make([]object.SlabSlice, len(o.Slabs)),
	}
	for i, sl := range o.Slabs {
		slab, err := sl.Slab.convert()
		if err != nil {
			return object.Object{}, err
		}
		obj.Slabs[i] = object.SlabSlice{
			Slab:   slab,
			Offset: sl.Offset,
			Length: sl.Length,
		}
		for j, sh := range sl.Slab.Shards {
			// Return first contract that's good for upload.
			for _, c := range sh.DBSector.Contracts {
				// TODO: This might cause issues. For now we can
				// ignore the edge case for a shard existing on
				// multiple hosts but we might want to be
				// smarter about what host we return here.
				obj.Slabs[i].Shards[j].Host = c.Host.PublicKey
			}
			obj.Slabs[i].Shards[j].Root = sh.DBSector.Root
		}
	}
	return obj, nil
}

// List implements the bus.ObjectStore interface.
func (s *SQLStore) List(path string) ([]string, error) {
	if !strings.HasSuffix(path, "/") {
		panic("path must end in /")
	}

	inner := s.db.Model(&dbObject{}).Select("SUBSTR(object_id, ?) AS trimmed", len(path)+1).
		Where("object_id LIKE ?", path+"%")
	middle := s.db.Table("(?)", inner).
		Select("trimmed, INSTR(trimmed, ?) AS slashindex", "/")
	outer := s.db.Table("(?)", middle).
		Select("CASE slashindex WHEN 0 THEN ? || trimmed ELSE ? || substr(trimmed, 0, slashindex+1) END AS result", path, path).
		Group("result")

	var ids []string
	err := outer.Find(&ids).Error
	if err != nil {
		return nil, err
	}
	return ids, nil
}

// Get implements the bus.ObjectStore interface.
func (s *SQLStore) Get(key string) (object.Object, error) {
	obj, err := s.get(key)
	if err != nil {
		return object.Object{}, err
	}
	return obj.convert()
}

// Put implements the bus.ObjectStore interface.
func (s *SQLStore) Put(key string, o object.Object, usedContracts map[consensus.PublicKey]types.FileContractID) error {
	// Sanity check input.
	for _, ss := range o.Slabs {
		for _, shard := range ss.Shards {
			// Verify that all hosts have a contract.
			_, exists := usedContracts[shard.Host]
			if !exists {
				return fmt.Errorf("missing contract for host %v", shard.Host)
			}
		}
	}

	// Put is ACID.
	return s.db.Transaction(func(tx *gorm.DB) error {
		// Try to delete first. We want to get rid of the object and its
		// slabs if it exists.
		err := deleteObject(tx, key)
		if err != nil {
			return err
		}

		// Insert a new object.
		objKey, err := o.Key.MarshalText()
		if err != nil {
			return err
		}
		obj := dbObject{
			ObjectID: key,
			Key:      objKey,
		}
		err = tx.Create(&obj).Error
		if err != nil {
			return err
		}

		for _, ss := range o.Slabs {
			// Create Slice.
			slice := dbSlice{
				DBObjectID: obj.ID,
				Offset:     ss.Offset,
				Length:     ss.Length,
			}
			err = tx.Create(&slice).Error
			if err != nil {
				return err
			}

			// Create Slab.
			slabKey, err := ss.Key.MarshalText()
			if err != nil {
				return err
			}
			slab := &dbSlab{
				DBSliceID: slice.ID,
				Key:       slabKey,
				MinShards: ss.MinShards,
			}
			err = tx.Create(&slab).Error
			if err != nil {
				return err
			}

			for _, shard := range ss.Shards {
				// Translate pubkey to contract.
				fcid := usedContracts[shard.Host]

				// Create sector if it doesn't exist yet.
				var sector dbSector
				err = tx.FirstOrCreate(&sector, &dbSector{
					Root: shard.Root,
				}).Error
				if err != nil {
					return err
				}

				// Add the slab-sector link to the sector to the
				// shards table.
				err = tx.Create(&dbShard{
					DBSlabID:   slab.ID,
					DBSectorID: sector.ID,
				}).Error
				if err != nil {
					return err
				}

				// Look for the contract referenced by the shard.
				contractFound := true
				var contract dbContract
				err = tx.Model(&dbContract{}).
					Where(&dbContract{FCID: fcid}).
					Take(&contract).Error
				if errors.Is(err, gorm.ErrRecordNotFound) {
					contractFound = false
				} else if err != nil {
					return err
				}

				// Look for the host referenced by the shard.
				hostFound := true
				var host dbHost
				err = tx.Model(&dbHost{}).
					Where(&dbHost{PublicKey: shard.Host}).
					Take(&host).Error
				if errors.Is(err, gorm.ErrRecordNotFound) {
					hostFound = false
				} else if err != nil {
					return err
				}

				// Add contract and host to join tables.
				if contractFound {
					err = tx.Model(&sector).Association("Contracts").Append(&contract)
					if err != nil {
						return err
					}
				}
				if hostFound {
					err = tx.Model(&sector).Association("Hosts").Append(&host)
					if err != nil {
						return err
					}
				}
			}
		}
		return nil
	})
}

// Delete implements the bus.ObjectStore interface.
func (s *SQLStore) Delete(key string) error {
	return deleteObject(s.db, key)
}

// deleteObject deletes an object from the store.
func deleteObject(tx *gorm.DB, key string) error {
	return tx.Where(&dbObject{ObjectID: key}).Delete(&dbObject{}).Error
}

// get retrieves an object from the database.
func (s *SQLStore) get(key string) (dbObject, error) {
	var obj dbObject
	tx := s.db.Where(&dbObject{ObjectID: key}).
		Preload("Slabs.Slab.Shards.DBSector.Contracts.Host").
		Take(&obj)
	if errors.Is(tx.Error, gorm.ErrRecordNotFound) {
		return dbObject{}, ErrObjectNotFound
	}
	return obj, nil
}

// SlabsForMigration returns up to n slabs which require repair. Only slabs are
// considered which haven't failed since failureCutoff. TODO: consider that we
// don't want to migrate slabs above a given health.
func (s *SQLStore) SlabsForMigration(n int, failureCutoff time.Time, goodContracts []types.FileContractID) ([]object.Slab, error) {
	// Serialize contract ids.
	var fcids [][]byte
	for _, fcid := range goodContracts {
		fcidGob, err := gobFCID(fcid)
		if err != nil {
			return nil, err
		}
		fcids = append(fcids, fcidGob)
	}
	failureQuery := s.db.Model(&dbSlab{}).
		Select("id as slab_id").
		Where("last_failure < ?", failureCutoff.UTC())
	inner := s.db.Table("(?)", failureQuery).
		Select("slab_id, db_sector_id as sector_id").
		Joins("LEFT JOIN shards ON slab_id = shards.db_slab_id")
	middle := s.db.Table("(?)", inner).
		Select("slab_id, sector_id, db_contract_id as contract_id").
		Joins("LEFT JOIN contract_sectors ON sector_id = contract_sectors.db_sector_id")
	outer := s.db.Table("(?)", middle).
		Select("slab_id, fcid").
		Joins("LEFT JOIN contracts ON contract_id = contracts.id").
		Where("contract_id IS NULL OR fcid NOT IN ?", fcids).
		Group("slab_id").
		Order("COUNT(slab_id) DESC").
		Limit(n)

	var slabIDs []bus.SlabID
	err := outer.Select("slab_id").Find(&slabIDs).Error
	if err != nil {
		return nil, err
	}

	// TODO: inline this
	var slabs []object.Slab
	for _, slabID := range slabIDs {
		slab, err := s.slabForMigration(slabID)
		if err != nil {
			return nil, err
		}
		slabs = append(slabs, slab)
	}

	return slabs, err
}

func (s *SQLStore) host(id uint) (dbHost, bool, error) {
	var h dbHost
	err := s.db.Where(&dbHost{Model: Model{ID: id}}).
		Take(&h).
		Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return dbHost{}, false, nil
	} else if err != nil {
		return dbHost{}, false, err
	}
	return h, true, nil
}

// slabForMigration returns all the info about a slab necessary for migrating
// it to better hosts/contracts.
func (s *SQLStore) slabForMigration(slabID bus.SlabID) (object.Slab, error) {
	var dSlab dbSlab
	// TODO: This could be slightly more efficient by not fetching whole
	// contracts.
	tx := s.db.Where(&dbSlab{Model: Model{ID: uint(slabID)}}).
		Preload("Shards.DBSector.Contracts.Host").
		Preload("Shards.DBSector.Hosts").
		Take(&dSlab)
	if errors.Is(tx.Error, gorm.ErrRecordNotFound) {
		return object.Slab{}, ErrSlabNotFound
	}
	slab, err := dSlab.convert()
	if err != nil {
		return object.Slab{}, err
	}
	for i, shard := range dSlab.Shards {
		// Check contracts first for a valid host key. We prefer hosts
		// that we have contracts with.
		if len(shard.DBSector.Contracts) > 0 {
			slab.Shards[i].Host = shard.DBSector.Contracts[0].Host.PublicKey
		} else if len(shard.DBSector.Hosts) > 0 {
			slab.Shards[i].Host = shard.DBSector.Hosts[0].PublicKey
		}
		slab.Shards[i].Root = shard.DBSector.Root
	}
	return slab, nil
}

// MarkSlabsMigrationFailure sets the last_failure field for the given slabs to
// the current time.
func (s *SQLStore) MarkSlabsMigrationFailure(slabIDs []bus.SlabID) (int, error) {
	now := time.Now().UTC()
	txn := s.db.Model(&dbSlab{}).
		Where("id in ?", slabIDs).
		Update("last_failure", now)
	if txn.Error != nil {
		return 0, txn.Error
	}
	return int(txn.RowsAffected), nil
}
