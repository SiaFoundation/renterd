package stores

import (
	"encoding/json"
	"errors"
	"log"
	"time"

	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/siad/modules"
	"gorm.io/gorm"
)

// consensusInfoID defines the primary key of the entry in the consensusInfo
// table.
const consensusInfoID = 1

// ErrHostNotFound is returned if a specific host can't be retrieved from the hostdb.
var ErrHostNotFound = errors.New("host doesn't exist in hostdb")

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

// TableName implements the gorm.Tabler interface.
func (dbAnnouncement) TableName() string { return "announcements" }

// TableName implements the gorm.Tabler interface.
func (dbInteraction) TableName() string { return "host_interactions" }

// TableName implements the gorm.Tabler interface.
func (dbConsensusInfo) TableName() string { return "consensus_infos" }

// NetAddress returns the latest announced address of a host.
func (h dbHost) NetAddress() string {
	if len(h.Announcements) == 0 {
		return ""
	}
	return h.Announcements[len(h.Announcements)-1].NetAddress
}

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
		Having("IFNULL(MAX(Timestamp), 0) < ?", notSince.UTC()). // use UTC since we stored timestamps in UTC
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
		//		var host dbHost
		//		if err := tx.FirstOrCreate(&host, &dbHost{PublicKey: hostKey}).Error; err != nil {
		//			return err
		//		}

		// Create an interaction.
		return tx.Create(&dbInteraction{
			DBHostID:  0,
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
