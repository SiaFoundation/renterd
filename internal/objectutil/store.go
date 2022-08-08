package objectutil

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/renterd/object"
	"go.sia.tech/renterd/slab"
)

type refSector struct {
	hostID uint32
	root   consensus.Hash256
}

type refSlab struct {
	minShards uint8
	shards    []refSector
	refs      uint32
}

type refSlabSlice struct {
	slabID slab.EncryptionKey
	offset uint32
	length uint32
}

type refObject struct {
	key   slab.EncryptionKey
	slabs []refSlabSlice
}

// EphemeralStore implements server.ObjectStore in memory.
type EphemeralStore struct {
	hosts   []consensus.PublicKey
	slabs   map[slab.EncryptionKey]refSlab
	objects map[string]refObject
	mu      sync.Mutex
}

func (es *EphemeralStore) addHost(hostKey consensus.PublicKey) uint32 {
	for id, host := range es.hosts {
		if host == hostKey {
			return uint32(id)
		}
	}
	es.hosts = append(es.hosts, hostKey)
	return uint32(len(es.hosts) - 1)
}

// Put implements server.ObjectStore.
func (es *EphemeralStore) Put(key string, o object.Object) error {
	es.mu.Lock()
	defer es.mu.Unlock()
	ro := refObject{
		key:   o.Key,
		slabs: make([]refSlabSlice, len(o.Slabs)),
	}
	for i, ss := range o.Slabs {
		rs, ok := es.slabs[ss.Key]
		if !ok {
			shards := make([]refSector, len(ss.Shards))
			for i, sector := range ss.Shards {
				shards[i] = refSector{
					hostID: es.addHost(sector.Host),
					root:   sector.Root,
				}
			}
			rs = refSlab{ss.MinShards, shards, 1}
		}
		rs.refs++
		es.slabs[ss.Key] = rs
		ro.slabs[i] = refSlabSlice{ss.Key, ss.Offset, ss.Length}
	}
	es.objects[key] = ro
	return nil
}

// Get implements server.ObjectStore.
func (es *EphemeralStore) Get(key string) (object.Object, error) {
	es.mu.Lock()
	defer es.mu.Unlock()
	ro, ok := es.objects[key]
	if !ok {
		return object.Object{}, errors.New("not found")
	}
	slabs := make([]slab.SlabSlice, len(ro.slabs))
	for i, rss := range ro.slabs {
		rs := es.slabs[rss.slabID]
		shards := make([]slab.Sector, len(rs.shards))
		for i := range rs.shards {
			shards[i] = slab.Sector{
				Host: es.hosts[rs.shards[i].hostID],
				Root: rs.shards[i].root,
			}
		}
		slabs[i] = slab.SlabSlice{
			Slab: slab.Slab{
				Key:       rss.slabID,
				MinShards: rs.minShards,
				Shards:    shards,
			},
			Offset: rss.offset,
			Length: rss.length,
		}
	}
	return object.Object{
		Key:   ro.key,
		Slabs: slabs,
	}, nil
}

// Delete implements server.ObjectStore.
func (es *EphemeralStore) Delete(key string) error {
	es.mu.Lock()
	defer es.mu.Unlock()
	o, ok := es.objects[key]
	if !ok {
		return nil
	}
	// decrement slab refcounts
	for _, s := range o.slabs {
		rs, ok := es.slabs[s.slabID]
		if !ok || rs.refs == 0 {
			continue // shouldn't happen, but benign
		}
		rs.refs--
		if rs.refs == 0 {
			delete(es.slabs, s.slabID)
		} else {
			es.slabs[s.slabID] = rs
		}
	}
	delete(es.objects, key)
	return nil
}

// List implements server.ObjectStore.
func (es *EphemeralStore) List(prefix string) []string {
	es.mu.Lock()
	defer es.mu.Unlock()
	var keys []string
	for k := range es.objects {
		if strings.HasPrefix(k, prefix) && !strings.ContainsRune(k[len(prefix):], '/') {
			keys = append(keys, k)
		}
	}
	return keys
}

func NewEphemeralStore() *EphemeralStore {
	return &EphemeralStore{
		slabs:   make(map[slab.EncryptionKey]refSlab),
		objects: make(map[string]refObject),
	}
}

// JSONStore implements server.ObjectStore in memory, backed by a JSON file.
type JSONStore struct {
	*EphemeralStore
	dir string
}

type jsonPersistData struct {
	Hosts   []consensus.PublicKey
	Slabs   map[slab.EncryptionKey]refSlab
	Objects map[string]refObject
}

func (s *JSONStore) save() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := jsonPersistData{
		Hosts:   s.hosts,
		Slabs:   s.slabs,
		Objects: s.objects,
	}
	js, _ := json.MarshalIndent(p, "", "  ")

	// atomic save
	dst := filepath.Join(s.dir, "objects.json")
	f, err := os.OpenFile(dst+"_tmp", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0660)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err = f.Write(js); err != nil {
		return err
	} else if f.Sync(); err != nil {
		return err
	} else if f.Close(); err != nil {
		return err
	} else if err := os.Rename(dst+"_tmp", dst); err != nil {
		return err
	}
	return nil
}

func (s *JSONStore) load() error {
	var p jsonPersistData
	if js, err := os.ReadFile(filepath.Join(s.dir, "objects.json")); os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	} else if err := json.Unmarshal(js, &p); err != nil {
		return err
	}
	s.EphemeralStore.hosts = p.Hosts
	s.EphemeralStore.slabs = p.Slabs
	s.EphemeralStore.objects = p.Objects
	return nil
}

func (s *JSONStore) Put(key string, o object.Object) error {
	s.EphemeralStore.Put(key, o)
	return s.save()
}

func (s *JSONStore) Delete(key string) error {
	s.EphemeralStore.Delete(key)
	return s.save()
}

// NewJSONStore returns a new JSONStore.
func NewJSONStore(dir string) (*JSONStore, error) {
	s := &JSONStore{
		EphemeralStore: NewEphemeralStore(),
		dir:            dir,
	}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}
