package stores

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"time"

	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/renterd/rhp/v2"
	"go.sia.tech/siad/modules"
	"go.sia.tech/siad/types"
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

		PublicKey consensus.PublicKey `gorm:"unique;index;type:bytes;serializer:gob;NOT NULL"`
		Settings  hostSettings        `gorm:"type:bytes;serializer:gob"`

		TotalScans          uint64
		LastScan            time.Time
		LastScanSuccess     bool
		PreviousScanSuccess bool
		Uptime              time.Duration
		Downtime            time.Duration

		SuccessfulInteractions float64
		FailedInteractions     float64

		LastAnnouncement time.Time
		NetAddress       string
	}

	// dbInteraction defines a hostdb.Interaction as persisted in the DB.
	// dbConsensusInfo defines table which stores the latest consensus info
	// known to the hostdb. It should only ever contain a single entry with
	// the consensusInfoID primary key.
	dbInteraction struct {
		Model

		Result    json.RawMessage
		Success   bool
		Timestamp time.Time `gorm:"index; NOT NULL"`
		Type      string    `gorm:"NOT NULL"`
	}

	dbConsensusInfo struct {
		Model
		CCID []byte
	}

	// hostSettings are the settings and prices used when interacting with a host.
	// TODO: might be useful to have this be a table.
	hostSettings struct {
		AcceptingContracts         bool             `json:"acceptingcontracts"`
		MaxDownloadBatchSize       uint64           `json:"maxdownloadbatchsize"`
		MaxDuration                uint64           `json:"maxduration"`
		MaxReviseBatchSize         uint64           `json:"maxrevisebatchsize"`
		NetAddress                 string           `json:"netaddress"`
		RemainingStorage           uint64           `json:"remainingstorage"`
		SectorSize                 uint64           `json:"sectorsize"`
		TotalStorage               uint64           `json:"totalstorage"`
		UnlockHash                 types.UnlockHash `json:"unlockhash"`
		WindowSize                 uint64           `json:"windowsize"`
		Collateral                 *big.Int         `json:"collateral"`
		MaxCollateral              *big.Int         `json:"maxcollateral"`
		BaseRPCPrice               *big.Int         `json:"baserpcprice"`
		ContractPrice              *big.Int         `json:"contractprice"`
		DownloadBandwidthPrice     *big.Int         `json:"downloadbandwidthprice"`
		SectorAccessPrice          *big.Int         `json:"sectoraccessprice"`
		StoragePrice               *big.Int         `json:"storageprice"`
		UploadBandwidthPrice       *big.Int         `json:"uploadbandwidthprice"`
		EphemeralAccountExpiry     time.Duration    `json:"ephemeralaccountexpiry"`
		MaxEphemeralAccountBalance *big.Int         `json:"maxephemeralaccountbalance"`
		RevisionNumber             uint64           `json:"revisionnumber"`
		Version                    string           `json:"version"`
		SiaMuxPort                 string           `json:"siamuxport"`
	}

	// dbAnnouncement is a table used for storing all announcements. It
	// doesn't have any relations to dbHost which means it won't
	// automatically prune when a host is deleted.
	dbAnnouncement struct {
		Model
		HostKey consensus.PublicKey `gorm:"NOT NULL;type:bytes;serializer:gob"`

		BlockHeight uint64
		BlockID     string
		NetAddress  string
	}

	// announcement describes an announcement for a single host.
	announcement struct {
		hostKey      consensus.PublicKey
		announcement hostdb.Announcement
	}
)

// convert converts hostSettings to rhp.HostSettings
func (s hostSettings) convert() rhp.HostSettings {
	return rhp.HostSettings{
		AcceptingContracts:         s.AcceptingContracts,
		MaxDownloadBatchSize:       s.MaxDownloadBatchSize,
		MaxDuration:                s.MaxDuration,
		MaxReviseBatchSize:         s.MaxReviseBatchSize,
		NetAddress:                 s.NetAddress,
		RemainingStorage:           s.RemainingStorage,
		SectorSize:                 s.SectorSize,
		TotalStorage:               s.TotalStorage,
		UnlockHash:                 s.UnlockHash,
		WindowSize:                 s.WindowSize,
		Collateral:                 types.NewCurrency(s.Collateral),
		MaxCollateral:              types.NewCurrency(s.MaxCollateral),
		BaseRPCPrice:               types.NewCurrency(s.BaseRPCPrice),
		ContractPrice:              types.NewCurrency(s.ContractPrice),
		DownloadBandwidthPrice:     types.NewCurrency(s.DownloadBandwidthPrice),
		SectorAccessPrice:          types.NewCurrency(s.SectorAccessPrice),
		StoragePrice:               types.NewCurrency(s.StoragePrice),
		UploadBandwidthPrice:       types.NewCurrency(s.UploadBandwidthPrice),
		EphemeralAccountExpiry:     s.EphemeralAccountExpiry,
		MaxEphemeralAccountBalance: types.NewCurrency(s.MaxEphemeralAccountBalance),
		RevisionNumber:             s.RevisionNumber,
		Version:                    s.Version,
		SiaMuxPort:                 s.SiaMuxPort,
	}
}

func convertHostSettings(settings rhp.HostSettings) hostSettings {
	return hostSettings{
		AcceptingContracts:         settings.AcceptingContracts,
		MaxDownloadBatchSize:       settings.MaxDownloadBatchSize,
		MaxDuration:                settings.MaxDuration,
		MaxReviseBatchSize:         settings.MaxReviseBatchSize,
		NetAddress:                 settings.NetAddress,
		RemainingStorage:           settings.RemainingStorage,
		SectorSize:                 settings.SectorSize,
		TotalStorage:               settings.TotalStorage,
		UnlockHash:                 settings.UnlockHash,
		WindowSize:                 settings.WindowSize,
		Collateral:                 settings.Collateral.Big(),
		MaxCollateral:              settings.MaxCollateral.Big(),
		BaseRPCPrice:               settings.BaseRPCPrice.Big(),
		ContractPrice:              settings.ContractPrice.Big(),
		DownloadBandwidthPrice:     settings.DownloadBandwidthPrice.Big(),
		SectorAccessPrice:          settings.SectorAccessPrice.Big(),
		StoragePrice:               settings.StoragePrice.Big(),
		UploadBandwidthPrice:       settings.UploadBandwidthPrice.Big(),
		EphemeralAccountExpiry:     settings.EphemeralAccountExpiry,
		MaxEphemeralAccountBalance: settings.MaxEphemeralAccountBalance.Big(),
		RevisionNumber:             settings.RevisionNumber,
		Version:                    settings.Version,
		SiaMuxPort:                 settings.SiaMuxPort,
	}
}

// TableName implements the gorm.Tabler interface.
func (dbAnnouncement) TableName() string { return "host_announcements" }

// TableName implements the gorm.Tabler interface.
func (dbConsensusInfo) TableName() string { return "consensus_infos" }

// TableName implements the gorm.Tabler interface.
func (dbHost) TableName() string { return "hosts" }

// TableName implements the gorm.Tabler interface.
func (dbInteraction) TableName() string { return "host_interactions" }

// convert converts a host into a hostdb.Host.
func (h dbHost) convert() hostdb.Host {
	hdbHost := hostdb.Host{
		KnownSince: h.CreatedAt,
		NetAddress: h.NetAddress,
		Interactions: hostdb.Interactions{
			TotalScans:             h.TotalScans,
			LastScan:               h.LastScan,
			LastScanSuccess:        h.LastScanSuccess,
			PreviousScanSuccess:    h.PreviousScanSuccess,
			Uptime:                 h.Uptime,
			Downtime:               h.Downtime,
			SuccessfulInteractions: h.SuccessfulInteractions,
			FailedInteractions:     h.FailedInteractions,
		},
		PublicKey: h.PublicKey,
	}
	if h.Settings == (hostSettings{}) {
		hdbHost.Settings = nil
	} else {
		s := h.Settings.convert()
		hdbHost.Settings = &s
	}
	return hdbHost
}

// Host returns information about a host.
func (db *SQLStore) Host(hostKey consensus.PublicKey) (hostdb.Host, error) {
	var h dbHost
	tx := db.db.Where(&dbHost{PublicKey: hostKey}).
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
	var hosts []hostdb.Host
	err := db.db.Table("hosts").
		Limit(max).
		Find(&fullHosts).
		Error
	if err != nil {
		return nil, err
	}
	return hosts, err
}

func hostByPubKey(tx *gorm.DB, hostKey consensus.PublicKey) (dbHost, error) {
	var h dbHost
	err := tx.Where("public_key", gobEncode(hostKey)).
		Take(&h).Error
	return h, err
}

// RecordInteraction records an interaction with a host. If the host is not in
// the store, a new entry is created for it.
func (db *SQLStore) RecordHostInteractions(hostKey consensus.PublicKey, interactions []hostdb.Interaction) error {
	dbInteractions := make([]dbInteraction, len(interactions))
	var successful, failed float64
	for i, interaction := range interactions {
		dbInteractions[i] = dbInteraction{
			Result:    interaction.Result,
			Success:   interaction.Success,
			Timestamp: interaction.Timestamp.UTC(),
			Type:      interaction.Type,
		}
		if interaction.Success {
			successful++
		} else {
			failed++
		}
	}
	return db.db.Transaction(func(tx *gorm.DB) error {
		// Create interactions.
		if err := tx.CreateInBatches(&dbInteractions, 100).Error; err != nil {
			return err
		}
		// TODO: Add decay
		return tx.Exec("UPDATE hosts SET successful_interactions = successful_interactions + ?, failed_interactions = failed_interactions + ? WHERE public_key = ?", successful, failed, gobEncode(hostKey)).Error
	})
}

// RecordHostScan recors a scan for the supplied host.
func (db *SQLStore) RecordHostScan(hostKey consensus.PublicKey, t time.Time, success bool, settings rhp.HostSettings) error {
	return db.db.Transaction(func(tx *gorm.DB) error {
		// Get host.
		var h dbHost
		h, err := hostByPubKey(tx, hostKey)
		if err != nil {
			return err
		}

		// Update it.
		h.TotalScans++
		h.PreviousScanSuccess = h.LastScanSuccess
		h.LastScanSuccess = success
		if success {
			if !h.LastScan.IsZero() {
				h.Uptime = t.Sub(h.LastScan)
			}
			h.SuccessfulInteractions++
		} else {
			if !h.LastScan.IsZero() {
				h.Downtime = t.Sub(h.LastScan)
			}
			h.FailedInteractions++
		}
		h.LastScan = t.UTC()
		h.Settings = convertHostSettings(settings)
		return tx.Model(&dbHost{}).
			Where("public_key", gobEncode(hostKey)).
			Updates(map[string]interface{}{
				"total_scans":             h.TotalScans,
				"last_scan":               h.LastScan,
				"previous_scan_success":   h.PreviousScanSuccess,
				"last_scan_success":       h.LastScanSuccess,
				"uptime":                  h.Uptime,
				"downtime":                h.Downtime,
				"successful_interactions": h.SuccessfulInteractions,
				"failed_interactions":     h.FailedInteractions,
				"settings":                gobEncode(h.Settings),
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
	var announcements []dbAnnouncement
	for _, a := range as {
		hosts = append(hosts, dbHost{
			PublicKey:        a.hostKey,
			LastAnnouncement: a.announcement.Timestamp.UTC(),
			NetAddress:       a.announcement.NetAddress,
		})
		announcements = append(announcements, dbAnnouncement{
			HostKey:     a.hostKey,
			BlockHeight: a.announcement.Index.Height,
			BlockID:     a.announcement.Index.ID.String(),
			NetAddress:  a.announcement.NetAddress,
		})
	}
	if err := tx.Create(&announcements).Error; err != nil {
		return err
	}
	// Create hosts that don't exist and regardless of whether they exist or
	// not update the last_announcement and net_address fields.
	return tx.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "public_key"}},
		DoUpdates: clause.AssignmentColumns([]string{"last_announcement", "net_address"}),
	}).Create(&hosts).Error
}
