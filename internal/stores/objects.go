package stores

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
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

type refSlice struct {
	slabID slab.EncryptionKey
	offset uint32
	length uint32
}

type refObject struct {
	key   object.EncryptionKey
	slabs []refSlice
}

// EphemeralObjectStore implements api.ObjectStore in memory.
type EphemeralObjectStore struct {
	hosts   []consensus.PublicKey
	slabs   map[slab.EncryptionKey]refSlab
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
		key:   o.Key,
		slabs: make([]refSlice, len(o.Slabs)),
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
		ro.slabs[i] = refSlice{ss.Key, ss.Offset, ss.Length}
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
	slabs := make([]slab.Slice, len(ro.slabs))
	for i, rss := range ro.slabs {
		rs := es.slabs[rss.slabID]
		shards := make([]slab.Sector, len(rs.shards))
		for i := range rs.shards {
			shards[i] = slab.Sector{
				Host: es.hosts[rs.shards[i].hostID],
				Root: rs.shards[i].root,
			}
		}
		slabs[i] = slab.Slice{
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

// Delete implements api.ObjectStore.
func (es *EphemeralObjectStore) Delete(key string) error {
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
		slabs:   make(map[slab.EncryptionKey]refSlab),
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
	Slabs   map[slab.EncryptionKey]refSlab
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
