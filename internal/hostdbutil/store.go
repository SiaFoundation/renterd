package hostdbutil

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"

	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/internal/consensus"
)

// EphemeralDB implements a HostDB in memory.
type EphemeralDB struct {
	hosts map[consensus.PublicKey]hostdb.Host
	mu    sync.Mutex
}

func (db *EphemeralDB) modifyHost(hostKey consensus.PublicKey, fn func(*hostdb.Host)) {
	h, ok := db.hosts[hostKey]
	if !ok {
		h = hostdb.Host{PublicKey: hostKey}
	}
	fn(&h)
	db.hosts[hostKey] = h
}

// Host returns information about a host.
func (db *EphemeralDB) Host(hostKey consensus.PublicKey) (hostdb.Host, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.hosts[hostKey], nil
}

// RecordInteraction records an interaction with a host. If the host is not in
// the store, a new entry is created for it.
func (db *EphemeralDB) RecordInteraction(hostKey consensus.PublicKey, hi hostdb.Interaction) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.modifyHost(hostKey, func(h *hostdb.Host) {
		h.Interactions = append(h.Interactions, hi)
	})
	return nil
}

// SetScore sets the score associated with the specified host. If the host is
// not in the store, a new entry is created for it.
func (db *EphemeralDB) SetScore(hostKey consensus.PublicKey, score float64) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.modifyHost(hostKey, func(h *hostdb.Host) {
		h.Score = score
	})
	return nil
}

// SelectHosts returns up to n hosts for which the supplied filter returns true.
func (db *EphemeralDB) SelectHosts(n int, filter func(hostdb.Host) bool) ([]hostdb.Host, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	var hosts []hostdb.Host
	for _, host := range db.hosts {
		if len(hosts) == n {
			break
		} else if filter(host) {
			hosts = append(hosts, host)
		}
	}
	return hosts, nil
}

// NewEphemeralDB returns a new EphemeralDB.
func NewEphemeralDB() *EphemeralDB {
	return &EphemeralDB{
		hosts: make(map[consensus.PublicKey]hostdb.Host),
	}
}

// JSONDB implements a HostDB in memory, backed by a JSON file.
type JSONDB struct {
	*EphemeralDB
	dir string
}

type jsonPersistData struct {
	Hosts map[consensus.PublicKey]hostdb.Host
}

func (db *JSONDB) save() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	p := jsonPersistData{db.hosts}
	js, _ := json.MarshalIndent(p, "", "  ")

	dst := filepath.Join(db.dir, "hostdb.json")
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

func (db *JSONDB) load() error {
	var p jsonPersistData
	if js, err := os.ReadFile(filepath.Join(db.dir, "hostdb.json")); os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	} else if err := json.Unmarshal(js, &p); err != nil {
		return err
	}
	db.EphemeralDB.hosts = p.Hosts
	return nil
}

// RecordInteraction records an interaction with a host. If the host is not in
// the store, a new entry is created for it.
func (db *JSONDB) RecordInteraction(hostKey consensus.PublicKey, hi hostdb.Interaction) error {
	db.EphemeralDB.RecordInteraction(hostKey, hi)
	return db.save()
}

// SetScore sets the score associated with the specified host. If the host is
// not in the store, a new entry is created for it.
func (db *JSONDB) SetScore(hostKey consensus.PublicKey, score float64) error {
	db.EphemeralDB.SetScore(hostKey, score)
	return db.save()
}

// NewJSONDB returns a new JSONDB.
func NewJSONDB(dir string) (*JSONDB, error) {
	db := &JSONDB{
		EphemeralDB: NewEphemeralDB(),
		dir:         dir,
	}
	if err := db.load(); err != nil {
		return nil, err
	}
	return db, nil
}
