package stores

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/object"
	"gorm.io/gorm"
)

const (
	// slabRetrievalBatchSize is the number of slabs we fetch from the
	// database per batch
	slabRetrievalBatchSize = 10000
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
		TotalShards uint8
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

		LatestHost publicKey `gorm:"NOT NULL"`
		Root       []byte    `gorm:"index;unique;NOT NULL"`

		Contracts []dbContract `gorm:"many2many:contract_sectors;constraint:OnDelete:CASCADE"`
		Hosts     []dbHost     `gorm:"many2many:host_sectors;constraint:OnDelete:CASCADE"`
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
func (s dbSlab) convert() (slab object.Slab, err error) {
	// unmarshal key
	err = slab.Key.UnmarshalText(s.Key)
	if err != nil {
		return
	}

	// set shards
	slab.MinShards = s.MinShards
	slab.Shards = make([]object.Sector, len(s.Shards))

	// hydrate shards if possible
	for i, shard := range s.Shards {
		if shard.DBSector.ID == 0 {
			continue // sector wasn't preloaded
		}

		slab.Shards[i].Host = types.PublicKey(shard.DBSector.LatestHost)
		slab.Shards[i].Root = *(*types.Hash256)(shard.DBSector.Root)
	}

	return
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
func (s *SQLStore) Put(key string, o object.Object, usedContracts map[types.PublicKey]types.FileContractID) error {
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
				DBSliceID:   slice.ID,
				Key:         slabKey,
				MinShards:   ss.MinShards,
				TotalShards: uint8(len(ss.Shards)),
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
				err := tx.
					Where(dbSector{Root: shard.Root[:]}).
					Assign(dbSector{LatestHost: publicKey(shard.Host)}).
					FirstOrCreate(&sector).
					Error
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
					Where(&dbContract{FCID: fileContractID(fcid)}).
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
					Where(&dbHost{PublicKey: publicKey(shard.Host)}).
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

func (ss *SQLStore) PutSlab(s object.Slab, usedContracts map[types.PublicKey]types.FileContractID) error {
	// extract the slab key
	key, err := s.Key.MarshalText()
	if err != nil {
		return err
	}

	// extract host keys
	hostkeys := make([]publicKey, 0, len(usedContracts))
	for key := range usedContracts {
		hostkeys = append(hostkeys, publicKey(key))
	}

	// extract file contract ids
	fcids := make([]fileContractID, 0, len(usedContracts))
	for _, fcid := range usedContracts {
		fcids = append(fcids, fileContractID(fcid))
	}

	// find all hosts
	var dbHosts []dbHost
	if err := ss.db.
		Model(&dbHost{}).
		Where("public_key IN (?)", hostkeys).
		Find(&dbHosts).
		Error; err != nil {
		return err
	}

	// find all contracts
	var dbContracts []dbContract
	if err := ss.db.
		Model(&dbContract{}).
		Where("fcid IN (?)", fcids).
		Find(&dbContracts).
		Error; err != nil {
		return err
	}

	// make a hosts map
	hosts := make(map[publicKey]*dbHost)
	for i := range dbHosts {
		hosts[dbHosts[i].PublicKey] = &dbHosts[i]
	}

	// make a contracts map
	contracts := make(map[fileContractID]*dbContract)
	for i := range dbContracts {
		contracts[fileContractID(dbContracts[i].FCID)] = &dbContracts[i]
	}

	return ss.db.Transaction(func(tx *gorm.DB) (err error) {
		// find existing slab
		var slab dbSlab
		if err = tx.
			Where(&dbSlab{Key: key}).
			Assign(&dbSlab{TotalShards: uint8(len(slab.Shards))}).
			Preload("Shards.DBSector").
			Take(&slab).
			Error; err != nil {
			return
		}

		// build map out of current shards
		shards := make(map[uint]struct{})
		for _, shard := range slab.Shards {
			shards[shard.DBSectorID] = struct{}{}
		}

		// loop updated shards
		for _, shard := range s.Shards {
			// ensure the sector exists
			var sector dbSector
			if err := tx.
				Where(dbSector{Root: shard.Root[:]}).
				Assign(dbSector{LatestHost: publicKey(shard.Host)}).
				FirstOrCreate(&sector).
				Error; err != nil {
				return err
			}

			// ensure the join table has an entry
			_, exists := shards[sector.ID]
			if !exists {
				if err := tx.
					Create(&dbShard{
						DBSlabID:   slab.ID,
						DBSectorID: sector.ID,
					}).Error; err != nil {
					return err
				}
			}

			// ensure the associations are updated
			if contract := contracts[fileContractID(usedContracts[shard.Host])]; contract != nil {
				if err := tx.
					Model(&sector).
					Association("Contracts").
					Append(contract); err != nil {
					return err
				}
			}
			if host := hosts[publicKey(shard.Host)]; host != nil {
				if err := tx.
					Model(&sector).
					Association("Hosts").
					Append(host); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// SlabsForMigration returns up to 'limit' slabs that do not belong to contracts
// in the given set. These slabs need to be migrated to good contracts so they
// are restored to full health.
//
// TODO: consider that we don't want to migrate slabs above a given health.
func (s *SQLStore) SlabsForMigration(goodContracts []types.FileContractID, limit int) ([]object.Slab, error) {
	var dbBatch []dbSlab
	var slabs []object.Slab

	fcids := make([]fileContractID, len(goodContracts))
	for i, fcid := range goodContracts {
		fcids[i] = fileContractID(fcid)
	}

	if err := s.db.
		Select("slabs.*, COUNT(DISTINCT(c.host_id)) as num_good_sectors, slabs.total_shards as num_required_sectors, slabs.total_shards-COUNT(DISTINCT(c.host_id)) as num_bad_sectors").
		Model(&dbSlab{}).
		Joins("INNER JOIN shards sh ON sh.db_slab_id = slabs.id").
		Joins("LEFT JOIN contract_sectors se USING (db_sector_id)").
		Joins("LEFT JOIN contracts c ON se.db_contract_id = c.id").
		Where("c.fcid IN (?)", fcids).
		Group("slabs.id").
		Having("num_good_sectors < num_required_sectors").
		Order("num_bad_sectors DESC").
		Limit(limit).
		Preload("Shards.DBSector").
		FindInBatches(&dbBatch, slabRetrievalBatchSize, func(tx *gorm.DB, batch int) error {
			for _, dbSlab := range dbBatch {
				if slab, err := dbSlab.convert(); err == nil {
					slabs = append(slabs, slab)
				} else {
					panic(err)
				}
			}
			return nil
		}).
		Error; err != nil {
		return nil, err
	}

	return slabs, nil
}
