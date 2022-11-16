package stores

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/renterd/object"
	"go.sia.tech/renterd/slab"
	"gorm.io/gorm"
)

// ErrOBjectNotFound is returned if get is unable to retrieve an object from the
// database.
var ErrObjectNotFound = errors.New("object not found in database")

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
	SlabID slab.EncryptionKey
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
	slabs := make([]slab.Slice, len(ro.Slabs))
	for i, rss := range ro.Slabs {
		slabKey, err := rss.SlabID.MarshalText()
		if err != nil {
			return object.Object{}, err
		}
		rs, ok := es.slabs[string(slabKey)]
		if !ok {
			return object.Object{}, errors.New("slab not found")
		}
		shards := make([]slab.Sector, len(rs.Shards))
		for i := range rs.Shards {
			shards[i] = slab.Sector{
				Host: es.hosts[rs.Shards[i].HostID],
				Root: rs.Shards[i].Root,
			}
		}
		slabs[i] = slab.Slice{
			Slab: slab.Slab{
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
	SQLObjectStore struct {
		db *gorm.DB
	}

	// dbObject describes an object.Object in the database.
	dbObject struct {
		// ID uniquely identifies an Object within the database. Since
		// this ID is also exposed via the API it's a string for
		// convenience.
		ID string `gorm:"primaryKey"`

		// Object related fields.
		Key   []byte
		Slabs []dbSlice `gorm:"constraint:OnDelete:CASCADE;foreignKey:ObjectID;references:ID"` // CASCADE to delete slices too
	}

	// dbSlice describes a reference to a slab.Slab in the database.
	dbSlice struct {
		// ID uniquely identifies a slice in the db.
		ID uint64 `gorm:"primaryKey"`

		// ObjectID identifies the object the slice belongs to. It is
		// the foreign key of the object table.
		ObjectID string `gorm:"index;NOT NULL"`

		// Slice related fields.
		Slab   dbSlab `gorm:"constraint:OnDelete:CASCADE;foreignKey:ID"` // CASCADE to delete slabs too
		Offset uint32
		Length uint32
	}

	// dbSlab describes a slab.Slab in the database.
	// NOTE: A Slab is uniquely identified by its key.
	dbSlab struct {
		ID        uint64 `gorm:"primaryKey"`
		Key       []byte `gorm:"unique;NOT NULL"` // json string
		MinShards uint8
		Shards    []dbSector `gorm:"constraint:OnDelete:CASCADE;foreignKey:SlabID;references:ID"` // CASCADE to delete shards too
	}

	// dbSector describes a sector in the database. A sector can exist
	// multiple times in the sectors table since it can belong to multiple
	// slabs.
	dbSector struct {
		ID     uint64 `gorm:"primaryKey"`
		SlabID uint64 `gorm:"index;NOT NULL"`

		// Root uniquely identifies a sector and is therefore the primary key.
		Root consensus.Hash256 `gorm:"index;NOT NULL;type:bytes;serializer:gob"`

		// Host is the key of the host that stores the sector.
		// TODO: once we migrate the contract store over to a relational
		// db as well, we might want to put the contract ID here and
		// have a contract table with a mapping of contract ID to host.
		// That makes for better migrations.
		Host consensus.PublicKey `gorm:"index;NOT NULL;type:bytes;serializer:gob"`
	}
)

// TableName implements the gorm.Tabler interface.
func (dbObject) TableName() string { return "objects" }

// TableName implements the gorm.Tabler interface.
func (dbSlice) TableName() string { return "slices" }

// TableName implements the gorm.Tabler interface.
func (dbSlab) TableName() string { return "slabs" }

// TableName implements the gorm.Tabler interface.
func (dbSector) TableName() string { return "sectors" }

// NewSQLObjectStore creates a new SQLObjectStore connected to a DB through
// conn.
func NewSQLObjectStore(conn gorm.Dialector, migrate bool) (*SQLObjectStore, error) {
	db, err := gorm.Open(conn, &gorm.Config{})
	if err != nil {
		return nil, err
	}

	if migrate {
		// Create the tables.
		tables := []interface{}{
			&dbObject{},
			&dbSlice{},
			&dbSlab{},
			&dbSector{},
		}
		if err := db.AutoMigrate(tables...); err != nil {
			return nil, err
		}
		if res := db.Exec("PRAGMA foreign_keys = ON", nil); res.Error != nil {
			return nil, res.Error
		}
	}

	return &SQLObjectStore{
		db: db,
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
		Slabs: make([]slab.Slice, len(o.Slabs)),
	}
	for i, sl := range o.Slabs {
		var slabKey slab.EncryptionKey
		if err := slabKey.UnmarshalText(sl.Slab.Key); err != nil {
			return object.Object{}, err
		}
		obj.Slabs[i] = slab.Slice{
			Slab: slab.Slab{
				Key:       slabKey,
				MinShards: sl.Slab.MinShards,
				Shards:    make([]slab.Sector, len(sl.Slab.Shards)),
			},
			Offset: sl.Offset,
			Length: sl.Length,
		}
		for j, sh := range sl.Slab.Shards {
			obj.Slabs[i].Shards[j].Host = sh.Host
			obj.Slabs[i].Shards[j].Root = sh.Root
		}
	}
	return obj, nil
}

// List implements the bus.ObjectStore interface.
func (s *SQLObjectStore) List(path string) ([]string, error) {
	if !strings.HasSuffix(path, "/") {
		panic("path must end in /")
	}

	inner := s.db.Model(&dbObject{}).Select("SUBSTR(id, ?) AS trimmed", len(path)+1).
		Where("id LIKE ?", path+"%")
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
func (s *SQLObjectStore) Get(key string) (object.Object, error) {
	obj, err := s.get(key)
	if err != nil {
		return object.Object{}, err
	}
	return obj.convert()
}

// Put implements the bus.ObjectStore interface.
func (s *SQLObjectStore) Put(key string, o object.Object) error {
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
		err = tx.Create(&dbObject{
			ID:  key,
			Key: objKey,
		}).Error
		if err != nil {
			return err
		}

		for _, ss := range o.Slabs {
			// Create Slice.
			err = tx.Create(&dbSlice{
				ObjectID: key,
				Offset:   ss.Offset,
				Length:   ss.Length,
			}).Error
			if err != nil {
				return err
			}

			// Create Slab.
			slabKey, err := ss.Key.MarshalText()
			if err != nil {
				return err
			}
			var slab dbSlab
			err = tx.FirstOrCreate(&slab, &dbSlab{
				Key:       slabKey,
				MinShards: ss.MinShards,
			}).Error
			if err != nil {
				return err
			}

			for _, shard := range ss.Shards {
				// Create sector. Might exist already.
				var sector dbSector
				err = tx.FirstOrCreate(&sector, &dbSector{
					Host: shard.Host,
					Root: shard.Root,
				}).Error
				if err != nil {
					return err
				}
				// Create shard.
				err = tx.Create(&dbSector{
					SlabID: slab.ID,
					Host:   shard.Host,
					Root:   shard.Root,
				}).Error
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// Delete implements the bus.ObjectStore interface.
func (s *SQLObjectStore) Delete(key string) error {
	return deleteObject(s.db, key)
}

// deleteObject deletes an object from the store.
func deleteObject(tx *gorm.DB, key string) error {
	return tx.Delete(&dbObject{ID: key}).Error
}

// get retrieves an object from the database.
func (s *SQLObjectStore) get(key string) (dbObject, error) {
	var obj dbObject
	tx := s.db.Where(&dbObject{ID: key}).
		Preload("Slabs").
		Preload("Slabs.Slab.Shards").
		Take(&obj)
	if errors.Is(tx.Error, gorm.ErrRecordNotFound) {
		return dbObject{}, ErrObjectNotFound
	}
	return obj, nil
}
