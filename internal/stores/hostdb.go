package stores

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/siad/modules"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	// announcementBatchSoftLimit is the limit above which
	// threadedProcessAnnouncements will stop merging batches of
	// announcements and apply them to the db.
	announcementBatchSoftLimit = 1000

	// consensusInfoID defines the primary key of the entry in the consensusInfo
	// table.
	consensusInfoID = 1
)

// ErrHostNotFound is returned if a specific host can't be retrieved from the hostdb.
var ErrHostNotFound = errors.New("host doesn't exist in hostdb")

type (
	// dbHost defines a hostdb.Interaction as persisted in the DB.
	// Deleting a host from the db will cascade the deletion and also delete
	// the corresponding announcements and interactions with that host.
	dbHost struct {
		Model

		PublicKey    consensus.PublicKey `gorm:"unique;index;type:bytes;serializer:gob;NOT NULL"`
		Interactions []dbInteraction     `gorm:"OnDelete:CASCADE"`

		LastAnnouncement time.Time
		NetAddress       string
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

	// announcementBatch contains all announcements from a single consensus
	// change. After applying the announcements to the db, the persisted
	// consensus change id should be updated to cc.
	announcementBatch struct {
		announcements []announcement
		cc            modules.ConsensusChange
	}

	// announcement describes a an announcement for a single host.
	announcement struct {
		hostKey      consensus.PublicKey
		announcement hostdb.Announcement
	}
)

// TableName implements the gorm.Tabler interface.
func (dbHost) TableName() string { return "hosts" }

// TableName implements the gorm.Tabler interface.
func (dbInteraction) TableName() string { return "host_interactions" }

// TableName implements the gorm.Tabler interface.
func (dbConsensusInfo) TableName() string { return "consensus_infos" }

// convert converts a host into a hostdb.Host.
func (h dbHost) convert() hostdb.Host {
	hdbHost := hostdb.Host{
		KnownSince:   h.CreatedAt,
		NetAddress:   h.NetAddress,
		Interactions: make([]hostdb.Interaction, len(h.Interactions)),
		PublicKey:    h.PublicKey,
	}
	for i, interaction := range h.Interactions {
		hdbHost.Interactions[i] = interaction.convert()
	}
	return hdbHost
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
	var fullHosts []dbHost
	err := db.db.Table("hosts").
		Joins("LEFT JOIN host_interactions ON host_interactions.db_host_id = hosts.ID").
		Group("Public_Key").
		Having("IFNULL(MAX(Timestamp), 0) < ?", notSince.UTC()). // use UTC since we stored timestamps in UTC
		Limit(max).
		Preload("Interactions").
		Find(&fullHosts).
		Error
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

// ProcessConsensusChange implements consensus.Subscriber.
func (db *SQLStore) ProcessConsensusChange(cc modules.ConsensusChange) {
	height := cc.InitialHeight()
	for range cc.RevertedBlocks {
		height--
	}

	// Fetch announcements and add them to the queue.
	var newAnnouncements []announcement
	for _, b := range cc.AppliedBlocks {
		hostdb.ForEachAnnouncement(b, height, func(hostKey consensus.PublicKey, ha hostdb.Announcement) {
			newAnnouncements = append(newAnnouncements, announcement{
				hostKey:      hostKey,
				announcement: ha,
			})
		})
		height++
	}

	db.unappliedAnnouncements = append(db.unappliedAnnouncements, newAnnouncements...)
	db.unappliedCCID = cc.ID

	// Apply new announcements
	if time.Since(db.lastAnnouncementSave) > db.persistInterval || len(db.unappliedAnnouncements) >= announcementBatchSoftLimit {
		err := db.db.Transaction(func(tx *gorm.DB) error {
			if len(db.unappliedAnnouncements) > 0 {
				if err := insertAnnouncements(tx, db.unappliedAnnouncements); err != nil {
					return err
				}
			}
			return updateCCID(tx, db.unappliedCCID)
		})
		if err != nil {
			// NOTE: print error. If we failed due to a temporary error
			println(fmt.Sprintf("failed to apply %v announcements - should never happen", len(db.unappliedAnnouncements)))
		}

		db.unappliedAnnouncements = db.unappliedAnnouncements[:0]
		db.lastAnnouncementSave = time.Now()
	}
}

func updateCCID(tx *gorm.DB, newCCID modules.ConsensusChangeID) error {
	return tx.Model(&dbConsensusInfo{}).Where(&dbConsensusInfo{
		Model: Model{
			ID: consensusInfoID,
		},
	}).Update("CCID", newCCID[:]).Error
}

func insertAnnouncements(tx *gorm.DB, as []announcement) error {
	var hosts []dbHost
	for _, a := range as {
		hosts = append(hosts, dbHost{
			PublicKey:        a.hostKey,
			LastAnnouncement: a.announcement.Timestamp.UTC(),
			NetAddress:       a.announcement.NetAddress,
		})
	}
	// Create hosts that don't exist and regardless of whether they exist or
	// not update the last_announcement and net_address fields.
	return tx.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "public_key"}},
		DoUpdates: clause.AssignmentColumns([]string{"last_announcement", "net_address"}),
	}).Create(&hosts).Error
}
