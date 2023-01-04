package stores

import (
	"encoding/json"
	"errors"
	"time"

	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/siad/modules"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
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
	var fullHosts []dbHost
	err := db.db.Table("hosts").
		Joins("LEFT JOIN host_interactions ON host_interactions.db_host_id = hosts.ID").
		Group("Public_Key").
		Having("IFNULL(MAX(Timestamp), 0) < ?", notSince.UTC()). // use UTC since we stored timestamps in UTC
		Limit(max).
		Preload("Interactions").
		Preload("Announcements").
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

	// Fetch announcements and add them to the queue.
	var announcements []announcement
	for _, b := range cc.AppliedBlocks {
		hostdb.ForEachAnnouncement(b, height, func(hostKey consensus.PublicKey, ha hostdb.Announcement) {
			announcements = append(announcements, announcement{
				hostKey:      hostKey,
				announcement: ha,
			})
		})
		height++
	}
	db.newAnnouncements <- &announcementBatch{
		announcements: announcements,
		cc:            cc,
	}
}

func insertAnnouncements(tx *gorm.DB, as []announcement) error {
	// Create all missing hosts.
	var hosts []dbHost
	var hks [][]byte
	for _, a := range as {
		hosts = append(hosts, dbHost{PublicKey: a.hostKey})
		hks = append(hks, gobEncode(a.hostKey))
	}
	createdHosts := append([]dbHost{}, hosts...)
	if err := tx.Clauses(clause.OnConflict{DoNothing: true}, clause.Returning{}).Create(&createdHosts).Error; err != nil {
		return err
	}

	// Fill in their ids using the public_key.
	res := tx.Where("public_key IN ?", hks).Find(&hosts)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected != int64(len(hks)) {
		return errors.New("not all hosts found - should never happen")
	}

	// Create a map of host key to its id.
	hostMap := make(map[consensus.PublicKey]uint)
	for _, host := range hosts {
		hostMap[host.PublicKey] = host.ID
	}

	// Create announcements.
	announcements := make([]dbAnnouncement, len(as))
	for i, a := range as {
		announcements[i] = dbAnnouncement{
			DBHostID:    hostMap[a.hostKey],
			BlockHeight: a.announcement.Index.Height,
			BlockID:     a.announcement.Index.ID,
			Timestamp:   a.announcement.Timestamp.UTC(), // explicitly store timestamp as UTC
			NetAddress:  a.announcement.NetAddress,
		}
	}
	if err := tx.Create(&announcements).Error; err != nil {
		return err
	}
	return nil
}

func updateCCID(tx *gorm.DB, newCCID modules.ConsensusChange) error {
	return tx.Model(&dbConsensusInfo{}).Where(&dbConsensusInfo{
		Model: Model{
			ID: consensusInfoID,
		},
	}).Update("CCID", newCCID.ID[:]).Error
}
