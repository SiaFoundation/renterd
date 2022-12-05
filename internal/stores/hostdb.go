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
	// dbHost defines a hostdb.Interaction as persisted in the DB.
	// Deleting a host from the db will cascade the deletion and also delete
	// the corresponding announcements and interactions with that host.
	dbHost struct {
		Model

		PublicKey     consensus.PublicKey `gorm:"unique;index;type:bytes;serializer:gob;NOT NULL"`
		Announcements []dbAnnouncement    `gorm:"OnDelete:CASCADE"`
		Interactions  []dbInteraction     `gorm:"OnDelete:CASCADE"`
	}

	dbAnnouncement struct {
		Model
		DBHostID uint `gorm:"index"`

		BlockHeight uint64            `gorm:"NOT NULL"`
		BlockID     consensus.BlockID `gorm:"NOT NULL;type:bytes;serializer:gob"`
		Timestamp   time.Time         `gorm:"NOT NULL"`
		NetAddress  string            `gorm:"NOT NULL"`
	}

	// dbInteraction defines a hostdb.Interaction as persisted in the DB.
	dbInteraction struct {
		Model
		DBHostID uint `gorm:"index"`

		Result    json.RawMessage
		Timestamp time.Time `gorm:"index; NOT NULL"`
		Type      string
	}

	// dbConsensusInfo defines table which stores the latest consensus info
	// known to the hostdb. It should only ever contain a single entry with
	// the consensusInfoID primary key.
	dbConsensusInfo struct {
		Model
		CCID []byte
	}
)

// TableName implements the gorm.Tabler interface.
func (dbHost) TableName() string { return "hosts" }

// NetAddress returns the latest announced address of a host.
func (h dbHost) NetAddress() string {
	if len(h.Announcements) == 0 {
		return ""
	}
	return h.Announcements[len(h.Announcements)-1].NetAddress
}

// TableName implements the gorm.Tabler interface.
func (dbAnnouncement) TableName() string { return "announcements" }

// TableName implements the gorm.Tabler interface.
func (dbInteraction) TableName() string { return "host_interactions" }

// TableName implements the gorm.Tabler interface.
func (dbConsensusInfo) TableName() string { return "consensus_infos" }

// convert converts a host into a hostdb.Host.
func (h dbHost) convert() hostdb.Host {
	hdbHost := hostdb.Host{
		Announcements: make([]hostdb.Announcement, len(h.Announcements)),
		Interactions:  make([]hostdb.Interaction, len(h.Interactions)),
		PublicKey:     h.PublicKey,
	}
	for i, announcement := range h.Announcements {
		hdbHost.Announcements[i] = announcement.convert()
	}
	for i, interaction := range h.Interactions {
		hdbHost.Interactions[i] = interaction.convert()
	}
	return hdbHost
}

// convert converts a host into a hostdb.Announcement.
func (a dbAnnouncement) convert() hostdb.Announcement {
	hostdbAnnouncement := hostdb.Announcement{
		Index: consensus.ChainIndex{
			Height: a.BlockHeight,
			ID:     consensus.BlockID(a.BlockID),
		},
		Timestamp:  a.Timestamp,
		NetAddress: a.NetAddress,
	}
	return hostdbAnnouncement
}

// convert converts an interaction into a hostdb.Interaction.
func (i dbInteraction) convert() hostdb.Interaction {
	return hostdb.Interaction{
		Timestamp: i.Timestamp,
		Type:      i.Type,
		Result:    i.Result,
	}
}

// Host returns information about a host.
func (db *SQLStore) Host(hostKey consensus.PublicKey) (hostdb.Host, error) {
	var h dbHost
	tx := db.db.Where(&dbHost{PublicKey: hostKey}).
		Preload("Interactions").
		Preload("Announcements").
		Take(&h)
	if errors.Is(tx.Error, gorm.ErrRecordNotFound) {
		return hostdb.Host{}, ErrHostNotFound
	}
	return h.convert(), tx.Error
}

// Hosts returns up to max hosts that have not been interacted with since
// the specified time.
func (db *SQLStore) Hosts(notSince time.Time, max int) ([]hostdb.Host, error) {
	// Filter all hosts for the ones that have not been updated since a
	// given time.
	var foundHosts [][]byte
	err := db.db.Table("hosts").
		Joins("LEFT JOIN host_interactions ON host_interactions.db_host_id = hosts.ID").
		Select("Public_key").
		Group("Public_Key").
		Having("MAX(COALESCE(Timestamp, CAST(0 as datetime))) < ?", notSince.UTC()). // use UTC since we stored timestamps in UTC
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
		hosts = append(hosts, fh.convert())
	}
	return hosts, err
}

// RecordInteraction records an interaction with a host. If the host is not in
// the store, a new entry is created for it.
func (db *SQLStore) RecordInteraction(hostKey consensus.PublicKey, hi hostdb.Interaction) error {
	return db.db.Transaction(func(tx *gorm.DB) error {
		// Create a host if it doesn't exist yet.
		var host dbHost
		if err := tx.FirstOrCreate(&host, &dbHost{PublicKey: hostKey}).Error; err != nil {
			return err
		}

		// Create an interaction.
		return tx.Create(&dbInteraction{
			DBHostID:  host.ID,
			Timestamp: hi.Timestamp.UTC(), // explicitly store timestamp as UTC
			Type:      hi.Type,
			Result:    hi.Result,
		}).Error
	})
}

// hosts returns all hosts int he db.
func (db *SQLStore) hosts() ([]dbHost, error) {
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
func (db *SQLStore) ProcessConsensusChange(cc modules.ConsensusChange) {
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
		return tx.Model(&dbConsensusInfo{}).Where(&dbConsensusInfo{
			Model: Model{
				ID: consensusInfoID,
			},
		}).Update("CCID", cc.ID[:]).Error
	})
	if err != nil {
		log.Fatalln("Failed to apply consensus change to hostdb", err)
	}
}

func insertAnnouncement(tx *gorm.DB, hostKey consensus.PublicKey, a hostdb.Announcement) error {
	// Create a host if it doesn't exist yet.
	var host dbHost
	if err := tx.FirstOrCreate(&host, &dbHost{PublicKey: hostKey}).Error; err != nil {
		return err
	}

	// Create the announcement.
	return tx.Create(&dbAnnouncement{
		DBHostID:    host.ID,
		BlockHeight: a.Index.Height,
		BlockID:     a.Index.ID,
		Timestamp:   a.Timestamp.UTC(), // explicitly store timestamp as UTC
		NetAddress:  a.NetAddress,
	}).Error
}
