package stores

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/siad/modules"
)

// EphemeralHostDB implements a HostDB in memory.
type EphemeralHostDB struct {
	tip   consensus.ChainIndex
	ccid  modules.ConsensusChangeID
	hosts map[consensus.PublicKey]hostdb.Host
	mu    sync.Mutex
}

func (db *EphemeralHostDB) modifyHost(hostKey consensus.PublicKey, fn func(*hostdb.Host)) {
	h, ok := db.hosts[hostKey]
	if !ok {
		h = hostdb.Host{PublicKey: hostKey}
	}
	fn(&h)
	db.hosts[hostKey] = h
}

// Host returns information about a host.
func (db *EphemeralHostDB) Host(hostKey consensus.PublicKey) (hostdb.Host, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.hosts[hostKey], nil
}

// RecordInteraction records an interaction with a host. If the host is not in
// the store, a new entry is created for it.
func (db *EphemeralHostDB) RecordInteraction(hostKey consensus.PublicKey, hi hostdb.Interaction) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.modifyHost(hostKey, func(h *hostdb.Host) {
		h.Interactions = append(h.Interactions, hi)
	})
	return nil
}

// SetScore sets the score associated with the specified host. If the host is
// not in the store, a new entry is created for it.
func (db *EphemeralHostDB) SetScore(hostKey consensus.PublicKey, score float64) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.modifyHost(hostKey, func(h *hostdb.Host) {
		h.Score = score
	})
	return nil
}

// SelectHosts returns up to n hosts for which the supplied filter returns true.
func (db *EphemeralHostDB) SelectHosts(n int, filter func(hostdb.Host) bool) ([]hostdb.Host, error) {
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

// ProcessConsensusChange implements consensus.Subscriber.
func (db *EphemeralHostDB) ProcessConsensusChange(cc modules.ConsensusChange) {
	height := cc.InitialHeight()
	for range cc.RevertedBlocks {
		height--
	}
	for _, b := range cc.AppliedBlocks {
		hostdb.ForEachAnnouncement(b, height, func(hostKey consensus.PublicKey, ha hostdb.Announcement) {
			db.modifyHost(hostKey, func(h *hostdb.Host) {
				h.Announcements = append(h.Announcements, ha)
			})
		})
		height++
	}
	db.tip.Height = uint64(height)
	db.tip.ID = consensus.BlockID(cc.AppliedBlocks[len(cc.AppliedBlocks)-1].ID())
	db.ccid = cc.ID
}

// NewEphemeralHostDB returns a new EphemeralHostDB.
func NewEphemeralHostDB() *EphemeralHostDB {
	return &EphemeralHostDB{
		hosts: make(map[consensus.PublicKey]hostdb.Host),
	}
}

// JSONHostDB implements a HostDB in memory, backed by a JSON file.
type JSONHostDB struct {
	*EphemeralHostDB
	dir      string
	lastSave time.Time
}

type jsonHostDBPersistData struct {
	Tip   consensus.ChainIndex
	CCID  modules.ConsensusChangeID
	Hosts map[consensus.PublicKey]hostdb.Host
}

func (db *JSONHostDB) save() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	p := jsonHostDBPersistData{db.tip, db.ccid, db.hosts}
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

func (db *JSONHostDB) load() (modules.ConsensusChangeID, error) {
	var p jsonHostDBPersistData
	if js, err := os.ReadFile(filepath.Join(db.dir, "hostdb.json")); os.IsNotExist(err) {
		// set defaults
		db.ccid = modules.ConsensusChangeBeginning
		return db.ccid, nil
	} else if err != nil {
		return modules.ConsensusChangeID{}, err
	} else if err := json.Unmarshal(js, &p); err != nil {
		return modules.ConsensusChangeID{}, err
	}
	db.tip = p.Tip
	db.ccid = p.CCID
	db.hosts = p.Hosts
	return db.ccid, nil
}

// RecordInteraction records an interaction with a host. If the host is not in
// the store, a new entry is created for it.
func (db *JSONHostDB) RecordInteraction(hostKey consensus.PublicKey, hi hostdb.Interaction) error {
	db.EphemeralHostDB.RecordInteraction(hostKey, hi)
	return db.save()
}

// SetScore sets the score associated with the specified host. If the host is
// not in the store, a new entry is created for it.
func (db *JSONHostDB) SetScore(hostKey consensus.PublicKey, score float64) error {
	db.EphemeralHostDB.SetScore(hostKey, score)
	return db.save()
}

// ProcessConsensusChange implements chain.Subscriber.
func (db *JSONHostDB) ProcessConsensusChange(cc modules.ConsensusChange) {
	db.EphemeralHostDB.ProcessConsensusChange(cc)
	if time.Since(db.lastSave) > 2*time.Minute {
		if err := db.save(); err != nil {
			log.Fatalln("Couldn't save hostdb state:", err)
		}
		db.lastSave = time.Now()
	}
}

// NewJSONHostDB returns a new JSONHostDB.
func NewJSONHostDB(dir string) (*JSONHostDB, modules.ConsensusChangeID, error) {
	db := &JSONHostDB{
		EphemeralHostDB: NewEphemeralHostDB(),
		dir:             dir,
		lastSave:        time.Now(),
	}
	ccid, err := db.load()
	if err != nil {
		return nil, modules.ConsensusChangeID{}, err
	}
	return db, ccid, nil
}
