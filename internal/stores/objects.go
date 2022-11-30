package stores

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
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

type refSector struct {
	HostID uint32
	Root   consensus.Hash256
}

type refSlab struct {
	MinShards uint8
	Shards    []refSector
	Refs      uint32
}

type refSlice struct {
	SlabID object.EncryptionKey
	Offset uint32
	Length uint32
}

type refObject struct {
	Key   object.EncryptionKey
	Slabs []refSlice
}

// EphemeralObjectStore implements api.ObjectStore in memory.
type EphemeralObjectStore struct {
	hosts   []consensus.PublicKey
	slabs   map[string]refSlab
	objects map[string]refObject
	mu      sync.Mutex
}

func (es *EphemeralObjectStore) addHost(hostKey consensus.PublicKey) uint32 {
	for id, host := range es.hosts {
		if host == hostKey {
			return uint32(id)
		}
	}
	es.hosts = append(es.hosts, hostKey)
	return uint32(len(es.hosts) - 1)
}

// Put implements api.ObjectStore.
func (es *EphemeralObjectStore) Put(key string, o object.Object) error {
	es.mu.Lock()
	defer es.mu.Unlock()
	ro := refObject{
		Key:   o.Key,
		Slabs: make([]refSlice, len(o.Slabs)),
	}
	for i, ss := range o.Slabs {
		slabKey, err := ss.Key.MarshalText()
		if err != nil {
			return err
		}
		rs, ok := es.slabs[string(slabKey)]
		if !ok {
			shards := make([]refSector, len(ss.Shards))
			for i, sector := range ss.Shards {
				shards[i] = refSector{
					HostID: es.addHost(sector.Host),
					Root:   sector.Root,
				}
			}
			rs = refSlab{ss.MinShards, shards, 0}
		}
		rs.Refs++
		es.slabs[string(slabKey)] = rs
		ro.Slabs[i] = refSlice{ss.Key, ss.Offset, ss.Length}
	}
	es.objects[key] = ro
	return nil
}

// Get implements api.ObjectStore.
func (es *EphemeralObjectStore) Get(key string) (object.Object, error) {
	es.mu.Lock()
	defer es.mu.Unlock()
	ro, ok := es.objects[key]
	if !ok {
		return object.Object{}, errors.New("not found")
	}
	slabs := make([]object.SlabSlice, len(ro.Slabs))
	for i, rss := range ro.Slabs {
		slabKey, err := rss.SlabID.MarshalText()
		if err != nil {
			return object.Object{}, err
		}
		rs, ok := es.slabs[string(slabKey)]
		if !ok {
			return object.Object{}, errors.New("slab not found")
		}
		shards := make([]object.Sector, len(rs.Shards))
		for i := range rs.Shards {
			shards[i] = object.Sector{
				Host: es.hosts[rs.Shards[i].HostID],
				Root: rs.Shards[i].Root,
			}
		}
		slabs[i] = object.SlabSlice{
			Slab: object.Slab{
				Key:       rss.SlabID,
				MinShards: rs.MinShards,
				Shards:    shards,
			},
			Offset: rss.Offset,
			Length: rss.Length,
		}
	}
	return object.Object{
		Key:   ro.Key,
		Slabs: slabs,
	}, nil
}

// Delete implements api.ObjectStore.
func (es *EphemeralObjectStore) Delete(key string) error {
	es.mu.Lock()
	defer es.mu.Unlock()
	o, ok := es.objects[key]
	if !ok {
		return nil
	}
	// decrement slab refcounts
	for _, s := range o.Slabs {
		slabKey, err := s.SlabID.MarshalText()
		if err != nil {
			return err
		}
		rs, ok := es.slabs[string(slabKey)]
		if !ok || rs.Refs == 0 {
			continue // shouldn't happen, but benign
		}
		rs.Refs--
		if rs.Refs == 0 {
			delete(es.slabs, string(slabKey))
		} else {
			es.slabs[string(slabKey)] = rs
		}
	}
	delete(es.objects, key)
	return nil
}

// List implements api.ObjectStore.
func (es *EphemeralObjectStore) List(path string) []string {
	if !strings.HasSuffix(path, "/") {
		panic("path must end in /")
	}
	es.mu.Lock()
	defer es.mu.Unlock()
	var keys []string
	seen := make(map[string]bool)
	for k := range es.objects {
		if strings.HasPrefix(k, path) {
			if rem := k[len(path):]; strings.ContainsRune(rem, '/') {
				k = path + rem[:strings.IndexRune(rem, '/')+1]
			}
			if !seen[k] {
				keys = append(keys, k)
				seen[k] = true
			}
		}
	}
	sort.Strings(keys)
	return keys
}

// NewEphemeralObjectStore returns a new EphemeralObjectStore.
func NewEphemeralObjectStore() *EphemeralObjectStore {
	return &EphemeralObjectStore{
		slabs:   make(map[string]refSlab),
		objects: make(map[string]refObject),
	}
}

// JSONObjectStore implements api.ObjectStore in memory, backed by a JSON file.
type JSONObjectStore struct {
	*EphemeralObjectStore
	dir string
}

type jsonObjectPersistData struct {
	Hosts   []consensus.PublicKey
	Slabs   map[string]refSlab
	Objects map[string]refObject
}

func (s *JSONObjectStore) save() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := jsonObjectPersistData{
		Hosts:   s.hosts,
		Slabs:   s.slabs,
		Objects: s.objects,
	}
	js, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to encode JSON: %w", err)
	}

	// atomic save
	dst := filepath.Join(s.dir, "objects.json")
	f, err := os.OpenFile(dst+"_tmp", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0660)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write(js); err != nil {
		return err
	} else if err := f.Sync(); err != nil {
		return err
	} else if err := f.Close(); err != nil {
		return err
	} else if err := os.Rename(dst+"_tmp", dst); err != nil {
		return err
	}
	return nil
}

func (s *JSONObjectStore) load() error {
	var p jsonObjectPersistData
	if js, err := os.ReadFile(filepath.Join(s.dir, "objects.json")); os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	} else if err := json.Unmarshal(js, &p); err != nil {
		return err
	}
	s.EphemeralObjectStore.hosts = p.Hosts
	s.EphemeralObjectStore.slabs = p.Slabs
	s.EphemeralObjectStore.objects = p.Objects
	return nil
}

// Put implements api.ObjectStore.
func (s *JSONObjectStore) Put(key string, o object.Object) error {
	s.EphemeralObjectStore.Put(key, o)
	return s.save()
}

// Delete implements api.ObjectStore.
func (s *JSONObjectStore) Delete(key string) error {
	s.EphemeralObjectStore.Delete(key)
	return s.save()
}

// NewJSONObjectStore returns a new JSONObjectStore.
func NewJSONObjectStore(dir string) (*JSONObjectStore, error) {
	s := &JSONObjectStore{
		EphemeralObjectStore: NewEphemeralObjectStore(),
		dir:                  dir,
	}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

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

		Contracts []dbContract      `gorm:"many2many:contract_sectors"`
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
				return fmt.Errorf("missing contract id for host pubkey %v", shard.Host)
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
				var contract dbContract
				err = tx.Model(&dbContract{}).
					Where(&dbContract{FCID: fcid}).
					Take(&contract).Error
				if errors.Is(err, gorm.ErrRecordNotFound) {
					continue // don't set contract
				} else if err != nil {
					return err
				}

				// Add the sector-contract link to the
				// contract_sectors table if it doesn't exist
				// yet.
				err = tx.Model(&sector).Association("Contracts").Append(&contract)
				if err != nil {
					return err
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

// SlabsForMigration returns up to n IDs of slabs which require repair. Only
// slabs are considered which haven't failed since failureCutoff.
// TODO: consider that we don't want to migrate slabs above a given health.
func (s *SQLStore) SlabsForMigration(n int, failureCutoff time.Time, goodContracts []types.FileContractID) ([]bus.SlabID, error) {
	// Serialize contract ids.
	var fcids [][]byte
	for _, fcid := range goodContracts {
		buf := bytes.NewBuffer(nil)
		enc := gob.NewEncoder(buf)
		if err := enc.Encode(fcid); err != nil {
			return nil, err
		}
		fcids = append(fcids, buf.Bytes())
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
		Select("slab_id, fc_id").
		Joins("LEFT JOIN contracts ON contract_id = contracts.id").
		Where("contract_id IS NULL OR fc_id NOT IN ?", fcids).
		Group("slab_id").
		Order("COUNT(slab_id) DESC").
		Limit(n)

	var slabIDs []bus.SlabID
	err := outer.Select("slab_id").Find(&slabIDs).Error
	return slabIDs, err
}

// SlabForMigration returns all the info about a s lab necessary for migrating
// it to better hosts/contracts.
func (s *SQLStore) SlabForMigration(slabID bus.SlabID) (object.Slab, []bus.MigrationContract, error) {
	var dSlab dbSlab
	tx := s.db.Where(&dbSlab{Model: Model{ID: uint(slabID)}}).
		Preload("Shards.DBSector.Contracts.Host").
		Take(&dSlab)
	if errors.Is(tx.Error, gorm.ErrRecordNotFound) {
		return object.Slab{}, nil, ErrSlabNotFound
	}
	slab, err := dSlab.convert()
	if err != nil {
		return object.Slab{}, nil, err
	}

	// Return all contracts that have ever stored any shard of the slab.
	addedContracts := make(map[types.FileContractID]struct{})
	var contracts []bus.MigrationContract
	for _, shard := range dSlab.Shards {
		for _, c := range shard.DBSector.Contracts {
			if _, exists := addedContracts[c.FCID]; exists {
				continue
			}
			addedContracts[c.FCID] = struct{}{}

			contracts = append(contracts, bus.MigrationContract{
				ID:      c.FCID,
				HostKey: c.Host.PublicKey,
				HostIP:  c.Host.NetAddress(),
			})
		}
	}
	return slab, contracts, nil
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
