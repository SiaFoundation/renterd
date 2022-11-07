package stores

import (
	"encoding/json"
	"errors"
	"log"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/siad/modules"
	"gorm.io/gorm"
)

// ErrHostNotFound is returned if a specific host can't be retrieved from the hostdb.
var ErrHostNotFound = errors.New("host doesn't exist in hostdb")

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
	h, exists := db.hosts[hostKey]
	if !exists {
		return hostdb.Host{}, ErrHostNotFound
	}
	return h, nil
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

// Hosts returns up to max hosts that have not been interacted with since
// the specified time.
func (db *EphemeralHostDB) Hosts(notSince time.Time, max int) ([]hostdb.Host, error) {
	lastInteraction := func(h hostdb.Host) time.Time {
		if len(h.Interactions) == 0 {
			return time.Time{}
		}
		return h.Interactions[len(h.Interactions)-1].Timestamp
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	var hosts []hostdb.Host
	for _, host := range db.hosts {
		if len(hosts) == max {
			break
		} else if lastInteraction(host).Before(notSince) {
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
		err := hostdb.ForEachAnnouncement(b, height, func(hostKey consensus.PublicKey, ha hostdb.Announcement) error {
			db.modifyHost(hostKey, func(h *hostdb.Host) {
				h.Announcements = append(h.Announcements, ha)
			})
			return nil
		})
		if err != nil {
			log.Fatalln("Couldn't save hostdb state:", err)
		}
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

// consensusInfoID defines the primary key of the entry in the consensusInfo
// table.
const consensusInfoID = 1

type (
	// SQLHostDB is a helper type for interacting with an SQL-based HostDB.
	SQLHostDB struct {
		staticDB *gorm.DB
	}

	// host defines a hostdb.Interaction as persisted in the DB.
	host struct {
		PublicKey     []byte `gorm:"primaryKey"`
		Score         float64
		Announcements []announcement `gorm:"foreignKey:Host;references:PublicKey"`
		Interactions  []interaction  `gorm:"foreignKey:Host;references:PublicKey"`
	}

	announcement struct {
		ID          uint64 `gorm:"primaryKey"`
		Host        []byte
		BlockHeight uint64
		BlockID     []byte
		Timestamp   time.Time
		NetAddress  string
	}

	// interaction defines a hostdb.Interaction as persisted in the DB.
	interaction struct {
		ID     uint64 `gorm:"primaryKey"`
		Type   string
		Result json.RawMessage

		// Host and Timestamp form a composite index where Hosts has the
		// higher priority.
		Host      []byte    `gorm:"index:idx_host:2"`
		Timestamp time.Time `gorm:"index:idx_host:1"`
	}

	// consensusInfo defines table which stores the latest consensus info
	// known to the hostdb. It should only ever contain a single entry with
	// the consensusInfoID primary key.
	consensusInfo struct {
		ID   uint8 `gorm:"primaryKey"`
		CCID []byte
	}
)

// Host converts a host into a hostdb.Host.
func (h host) Host() hostdb.Host {
	hdbHost := hostdb.Host{
		Score: h.Score,
	}
	copy(hdbHost.PublicKey[:], h.PublicKey)
	for _, a := range h.Announcements {
		hdbHost.Announcements = append(hdbHost.Announcements, a.Announcement())
	}
	for _, i := range h.Interactions {
		hdbHost.Interactions = append(hdbHost.Interactions, i.Interaction())
	}
	return hdbHost
}

// Announcement converts a host into a hostdb.Announcement.
func (a announcement) Announcement() hostdb.Announcement {
	hostdbAnnouncement := hostdb.Announcement{
		Index: consensus.ChainIndex{
			Height: a.BlockHeight,
		},
		Timestamp:  a.Timestamp,
		NetAddress: a.NetAddress,
	}
	copy(hostdbAnnouncement.Index.ID[:], a.BlockID)
	return hostdbAnnouncement
}

// Interaction converts an interaction into a hostdb.Interaction.
func (i interaction) Interaction() hostdb.Interaction {
	return hostdb.Interaction{
		Timestamp: i.Timestamp,
		Type:      i.Type,
		Result:    i.Result,
	}
}

// NewSQLHostDB uses a given Dialector to connect to a SQL database.  NOTE: Only
// pass migrate=true for the first instance of SQLHostDB if you connect via the
// same Dialector multiple times.
func NewSQLHostDB(conn gorm.Dialector, migrate bool) (*SQLHostDB, modules.ConsensusChangeID, error) {
	db, err := gorm.Open(conn, &gorm.Config{})
	if err != nil {
		return nil, modules.ConsensusChangeID{}, err
	}

	if migrate {
		// Create the tables.
		if err := db.AutoMigrate(&host{}); err != nil {
			return nil, modules.ConsensusChangeID{}, err
		}
		if err := db.AutoMigrate(&interaction{}); err != nil {
			return nil, modules.ConsensusChangeID{}, err
		}
		if err := db.AutoMigrate(&announcement{}); err != nil {
			return nil, modules.ConsensusChangeID{}, err
		}
		if err := db.AutoMigrate(&consensusInfo{}); err != nil {
			return nil, modules.ConsensusChangeID{}, err
		}
	}

	// Get latest consensus change ID or init db.
	var ci consensusInfo
	err = db.Where(&consensusInfo{ID: consensusInfoID}).
		Attrs(consensusInfo{
			ID:   consensusInfoID,
			CCID: modules.ConsensusChangeBeginning[:],
		}).
		FirstOrCreate(&ci).Error
	if err != nil {
		return nil, modules.ConsensusChangeID{}, err
	}
	var ccid modules.ConsensusChangeID
	copy(ccid[:], ci.CCID)

	return &SQLHostDB{
		staticDB: db,
	}, ccid, nil
}

// Host returns information about a host.
func (db *SQLHostDB) Host(hostKey consensus.PublicKey) (hostdb.Host, error) {
	var h host
	tx := db.staticDB.Where(&host{PublicKey: hostKey[:]}).
		Preload("Interactions").
		Preload("Announcements").
		Find(&h)
	if tx.Error == nil && tx.RowsAffected == 0 {
		return hostdb.Host{}, ErrHostNotFound
	}
	return h.Host(), tx.Error
}

// Hosts returns up to max hosts that have not been interacted with since
// the specified time.
func (db *SQLHostDB) Hosts(notSince time.Time, max int) ([]hostdb.Host, error) {
	lastInteraction := func(h hostdb.Host) time.Time {
		if len(h.Interactions) == 0 {
			return time.Time{}
		}
		return h.Interactions[len(h.Interactions)-1].Timestamp
	}
	hosts, err := db.SelectHosts(math.MaxInt, func(h hostdb.Host) bool { return true })
	if err != nil {
		return nil, err
	}
	for _, host := range hosts {
		if len(hosts) == max {
			break
		} else if lastInteraction(host).Before(notSince) {
			hosts = append(hosts, host)
		}
	}
	return hosts, nil
}

// RecordInteraction records an interaction with a host. If the host is not in
// the store, a new entry is created for it.
func (db *SQLHostDB) RecordInteraction(hostKey consensus.PublicKey, hi hostdb.Interaction) error {
	return db.staticDB.Transaction(func(tx *gorm.DB) error {
		// Create a host if it doesn't exist yet.
		if err := tx.FirstOrCreate(&host{}, &host{PublicKey: hostKey[:]}).Error; err != nil {
			return err
		}

		// Create an interaction.
		return tx.Create(&interaction{
			Host:      hostKey[:],
			Timestamp: hi.Timestamp.UTC(), // use UTC in DB
			Type:      hi.Type,
			Result:    hi.Result,
		}).Error
	})
}

// SelectHosts returns up to n hosts for which the supplied filter returns true.
func (db *SQLHostDB) SelectHosts(n int, filter func(hostdb.Host) bool) ([]hostdb.Host, error) {
	var hosts []host
	tx := db.staticDB.Preload("Interactions").
		Preload("Announcements").
		Find(&hosts)
	if tx.Error != nil {
		return nil, tx.Error
	}

	var drawnHosts []hostdb.Host
	for i := 0; i < len(hosts) && len(drawnHosts) < n; i++ {
		h := hosts[i].Host()
		if filter(h) {
			drawnHosts = append(drawnHosts, hosts[i].Host())
		}
	}
	return drawnHosts, nil
}

// SetScore sets the score associated with the specified host. If the host is
// not in the store, a new entry is created for it.
func (db *SQLHostDB) SetScore(hostKey consensus.PublicKey, score float64) error {
	return db.staticDB.Where(&host{PublicKey: hostKey[:]}).Update("Score", score).Error
}

// ProcessConsensusChange implements consensus.Subscriber.
func (db *SQLHostDB) ProcessConsensusChange(cc modules.ConsensusChange) {
	height := cc.InitialHeight()
	for range cc.RevertedBlocks {
		height--
	}

	// Atomically apply ConsensusChange.
	err := db.staticDB.Transaction(func(tx *gorm.DB) error {
		for _, b := range cc.AppliedBlocks {
			hostdb.ForEachAnnouncement(b, height, func(hostKey consensus.PublicKey, ha hostdb.Announcement) error {
				return insertAnnouncement(tx, hostKey, ha)
			})
			height++
		}
		return tx.Model(&consensusInfo{}).Where(&consensusInfo{ID: consensusInfoID}).Update("CCID", cc.ID[:]).Error
	})
	if err != nil {
		log.Fatalln("Failed to apply consensus change to hostdb", err)
	}
}

func insertAnnouncement(tx *gorm.DB, hostKey consensus.PublicKey, a hostdb.Announcement) error {
	return tx.Create(&announcement{
		Host:        hostKey[:],
		BlockHeight: a.Index.Height,
		BlockID:     a.Index.ID[:],
		Timestamp:   a.Timestamp.UTC(), // use UTC in DB
		NetAddress:  a.NetAddress,
	}).Error
}
