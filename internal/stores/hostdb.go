package stores

import (
	"encoding/json"
	"errors"
	"log"
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
		db *gorm.DB
	}

	// dbHost defines a hostdb.Interaction as persisted in the DB.
	dbHost struct {
		PublicKey     []byte           `gorm:"primaryKey"`
		Announcements []dbAnnouncement `gorm:"foreignKey:Host;references:PublicKey"`
		Interactions  []dbInteraction  `gorm:"foreignKey:Host;references:PublicKey"`
	}

	dbAnnouncement struct {
		ID          uint64 `gorm:"primaryKey"`
		Host        []byte
		BlockHeight uint64
		BlockID     []byte
		Timestamp   time.Time
		NetAddress  string
	}

	// dbInteraction defines a hostdb.Interaction as persisted in the DB.
	dbInteraction struct {
		ID     uint64 `gorm:"primaryKey"`
		Type   string
		Result json.RawMessage

		// Host and Timestamp form a composite index where Hosts has the
		// higher priority.
		Host      []byte    `gorm:"index:idx_host:2"`
		Timestamp time.Time `gorm:"index:idx_host:1"`
	}

	// dbConsensusInfo defines table which stores the latest consensus info
	// known to the hostdb. It should only ever contain a single entry with
	// the consensusInfoID primary key.
	dbConsensusInfo struct {
		ID   uint8 `gorm:"primaryKey"`
		CCID []byte
	}
)

// TableName implements the gorm.Tabler interface.
func (dbHost) TableName() string {
	return "hosts"
}

// TableName implements the gorm.Tabler interface.
func (dbAnnouncement) TableName() string {
	return "announcements"
}

// TableName implements the gorm.Tabler interface.
func (dbInteraction) TableName() string {
	return "host_interactions"
}

// TableName implements the gorm.Tabler interface.
func (dbConsensusInfo) TableName() string {
	return "consensus_infos"
}

// Host converts a host into a hostdb.Host.
func (h dbHost) Host() hostdb.Host {
	hdbHost := hostdb.Host{}
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
func (a dbAnnouncement) Announcement() hostdb.Announcement {
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
func (i dbInteraction) Interaction() hostdb.Interaction {
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
		tables := []interface {
		}{
			&dbHost{},
			&dbInteraction{},
			&dbAnnouncement{},
			&dbConsensusInfo{},
		}
		if err := db.AutoMigrate(tables...); err != nil {
			return nil, modules.ConsensusChangeID{}, err
		}
	}

	// Get latest consensus change ID or init db.
	var ci dbConsensusInfo
	err = db.Where(&dbConsensusInfo{ID: consensusInfoID}).
		Attrs(dbConsensusInfo{
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
		db: db,
	}, ccid, nil
}

// Host returns information about a host.
func (db *SQLHostDB) Host(hostKey consensus.PublicKey) (hostdb.Host, error) {
	var h dbHost
	tx := db.db.Where(&dbHost{PublicKey: hostKey[:]}).
		Preload("Interactions").
		Preload("Announcements").
		Take(&h)
	if errors.Is(tx.Error, gorm.ErrRecordNotFound) {
		return hostdb.Host{}, ErrHostNotFound
	}
	return h.Host(), tx.Error
}

// Hosts returns up to max hosts that have not been interacted with since
// the specified time.
func (db *SQLHostDB) Hosts(notSince time.Time, max int) ([]hostdb.Host, error) {
	// Filter all hosts for the ones that have not been updated since a
	// given time.
	var foundHosts [][]byte
	err := db.db.Table("hosts").
		Joins("JOIN host_interactions ON host_interactions.Host = hosts.Public_Key").
		Select("Public_key").
		Group("Public_Key").
		Having("MAX(Timestamp) < ?", notSince).
		Limit(max).
		Find(&foundHosts).
		Error
	if err != nil {
		return nil, err
	}
	// Fetch the full host information for all the keys.
	var fullHosts []dbHost
	err = db.db.Where("public_key IN ?", foundHosts).
		Preload("Interactions").
		Preload("Announcements").
		Find(&fullHosts).Error
	if err != nil {
		return nil, err
	}
	var hosts []hostdb.Host
	for _, fh := range fullHosts {
		hosts = append(hosts, fh.Host())
	}
	return hosts, err
}

// RecordInteraction records an interaction with a host. If the host is not in
// the store, a new entry is created for it.
func (db *SQLHostDB) RecordInteraction(hostKey consensus.PublicKey, hi hostdb.Interaction) error {
	return db.db.Transaction(func(tx *gorm.DB) error {
		// Create a host if it doesn't exist yet.
		if err := tx.FirstOrCreate(&dbHost{}, &dbHost{PublicKey: hostKey[:]}).Error; err != nil {
			return err
		}

		// Create an interaction.
		return tx.Create(&dbInteraction{
			Host:      hostKey[:],
			Timestamp: hi.Timestamp,
			Type:      hi.Type,
			Result:    hi.Result,
		}).Error
	})
}

// hosts returns all hosts int he db.
func (db *SQLHostDB) hosts() ([]dbHost, error) {
	var hosts []dbHost
	tx := db.db.Preload("Interactions").
		Preload("Announcements").
		Find(&hosts)
	if tx.Error != nil {
		return nil, tx.Error
	}
	return hosts, nil
}

// ProcessConsensusChange implements consensus.Subscriber.
func (db *SQLHostDB) ProcessConsensusChange(cc modules.ConsensusChange) {
	height := cc.InitialHeight()
	for range cc.RevertedBlocks {
		height--
	}

	// Atomically apply ConsensusChange.
	err := db.db.Transaction(func(tx *gorm.DB) error {
		var err error
		for _, b := range cc.AppliedBlocks {
			hostdb.ForEachAnnouncement(b, height, func(hostKey consensus.PublicKey, ha hostdb.Announcement) {
				if err == nil {
					err = insertAnnouncement(tx, hostKey, ha)
				}
			})
			height++
		}
		if err != nil {
			return err
		}
		return tx.Model(&dbConsensusInfo{}).Where(&dbConsensusInfo{ID: consensusInfoID}).Update("CCID", cc.ID[:]).Error
	})
	if err != nil {
		log.Fatalln("Failed to apply consensus change to hostdb", err)
	}
}

func insertAnnouncement(tx *gorm.DB, hostKey consensus.PublicKey, a hostdb.Announcement) error {
	return tx.Create(&dbAnnouncement{
		Host:        hostKey[:],
		BlockHeight: a.Index.Height,
		BlockID:     a.Index.ID[:],
		Timestamp:   a.Timestamp,
		NetAddress:  a.NetAddress,
	}).Error
}
