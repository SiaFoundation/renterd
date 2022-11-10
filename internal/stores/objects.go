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
		rs, ok := es.slabs[ss.Key.String()]
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
		es.slabs[ss.Key.String()] = rs
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
		rs, ok := es.slabs[rss.SlabID.String()]
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
		rs, ok := es.slabs[s.SlabID.String()]
		if !ok || rs.Refs == 0 {
			continue // shouldn't happen, but benign
		}
		rs.Refs--
		if rs.Refs == 0 {
			delete(es.slabs, s.SlabID.String())
		} else {
			es.slabs[s.SlabID.String()] = rs
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
		Slabs []dbSlice `gorm:"foreignKey:ObjectID;OnDelete:CASCADE"` // CASCADE to delete slices too
	}

	// dbObject describes a reference to a slab.Slab in the database.
	dbSlice struct {
		// ID uniquely identifies a slice in the db.
		ID uint64 `gorm:"primaryKey"`

		// ObjectID identifies the object the slice belongs to. It is
		// the foreign key of the object table.
		ObjectID string `gorm:"index"`

		// Slice related fields.
		Slab   dbSlab `gorm:"foreignKey:ID"` // No CASCADE to keep slabs
		Offset uint32
		Length uint32
	}

	// dbObject describes a slab.Slab in the database.
	dbSlab struct {
		// ID uniquely identifies a slab in the database.
		ID uint64 `gorm:"primaryKey"`

		// Slab related fields.
		Key       []byte
		MinShards uint8
		Shards    []dbSector `gorm:"foreignKey:Root"` // No CASCADE to keep sectors
	}

	// dbSector describes a slab.Sector in the database.
	dbSector struct {
		// Root uniquely identifies a sector and is therefore the primary key.
		Root []byte `gorm:"primaryKey"`
		Host []byte
	}
)

// NewSQLObjectStore creates a new SQLObjectStore connected to a DB through
// conn.
func NewSQLObjectStore(conn gorm.Dialector, migrate bool) (*SQLObjectStore, error) {
	db, err := gorm.Open(conn, &gorm.Config{})
	if err != nil {
		return nil, err
	}

	if migrate {
		// Create the tables.
		tables := []interface {
		}{
			&dbObject{},
			&dbSlice{},
			&dbSlab{},
			&dbSector{},
		}
		if err := db.AutoMigrate(tables...); err != nil {
			return nil, err
		}
	}

	return &SQLObjectStore{
		db: db,
	}, nil
}

// List implements the bus.ObjectStore interface.
func (s *SQLObjectStore) List(key string) []string {
	panic("not implemented yet")
}

// Get implements the bus.ObjectStore interface.
func (s *SQLObjectStore) Get(key string) (object.Object, error) {
	panic("not implemented yet")
}

// Put implements the bus.ObjectStore interface.
func (s *SQLObjectStore) Put(key string, o object.Object) error {
	panic("not implemented yet")
}

// Delete implements the bus.ObjectStore interface.
func (s *SQLObjectStore) Delete(key string) error {
	panic("not implemented yet")
}
