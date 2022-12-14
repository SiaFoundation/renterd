package stores

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net"
	"strings"
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

var (
	ErrHostNotFound   = errors.New("host doesn't exist in hostdb")
	ErrNegativeOffset = errors.New("offset can not be negative")
)

type (
	// dbHost defines a hostdb.Interaction as persisted in the DB.
	// Deleting a host from the db will cascade the deletion and also delete
	// the corresponding announcements and interactions with that host.
	dbHost struct {
		Model

		PublicKey consensus.PublicKey `gorm:"unique;index;type:bytes;serializer:gob;NOT NULL"`
		Settings  hostSettings        `gorm:"type:bytes;serializer:gob"`

		TotalScans              uint64
		LastScan                int64 // Unix nano
		LastScanSuccess         bool
		SecondToLastScanSuccess bool
		Uptime                  time.Duration
		Downtime                time.Duration

		SuccessfulInteractions float64
		FailedInteractions     float64

		LastAnnouncement time.Time
		NetAddress       string

		Blocklist []dbBlocklistEntry `gorm:"many2many:host_blocklist_entry_hosts;constraint:OnDelete:CASCADE"`
	}

	// dbBlocklistEntry defines a table that stores the host blocklist.
	dbBlocklistEntry struct {
		Model
		Entry string   `gorm:"unique;index;NOT NULL"`
		Hosts []dbHost `gorm:"many2many:host_blocklist_entry_hosts;constraint:OnDelete:CASCADE"`
	}

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

// TableName implements the gorm.Tabler interface.
func (dbBlocklistEntry) TableName() string { return "host_blocklist_entries" }

// convert converts a host into a hostdb.Host.
func (h dbHost) convert() hostdb.Host {
	var lastScan time.Time
	if h.LastScan > 0 {
		lastScan = time.Unix(0, h.LastScan)
	}
	hdbHost := hostdb.Host{
		KnownSince: h.CreatedAt,
		NetAddress: h.NetAddress,
		Interactions: hostdb.Interactions{
			TotalScans:              h.TotalScans,
			LastScan:                lastScan,
			LastScanSuccess:         h.LastScanSuccess,
			SecondToLastScanSuccess: h.SecondToLastScanSuccess,
			Uptime:                  h.Uptime,
			Downtime:                h.Downtime,
			SuccessfulInteractions:  h.SuccessfulInteractions,
			FailedInteractions:      h.FailedInteractions,
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

func (h *dbHost) AfterCreate(tx *gorm.DB) (err error) {
	var dbBlocklist []dbBlocklistEntry
	if err := tx.
		Model(&dbBlocklistEntry{}).
		Find(&dbBlocklist).
		Error; err != nil {
		return err
	}

	filtered := dbBlocklist[:0]
	for _, entry := range dbBlocklist {
		if entry.blocks(h) {
			filtered = append(filtered, entry)
		}
	}
	return tx.Model(h).Association("Blocklist").Replace(&filtered)
}

func (h *dbHost) BeforeCreate(tx *gorm.DB) (err error) {
	tx.Statement.AddClause(clause.OnConflict{
		Columns:   []clause.Column{{Name: "public_key"}},
		DoUpdates: clause.AssignmentColumns([]string{"last_announcement", "net_address"}),
	})
	return nil
}

func (e *dbBlocklistEntry) AfterCreate(tx *gorm.DB) (err error) {
	// NOTE: the ID is zero here if we ignore a conflict on create
	if e.ID == 0 {
		return nil
	}

	params := map[string]interface{}{
		"entry_id":    e.ID,
		"exact_entry": e.Entry,
		"like_entry":  fmt.Sprintf("%%.%s", e.Entry),
	}

	err = tx.Exec(`
	INSERT INTO host_blocklist_entry_hosts (db_blocklist_entry_id, db_host_id)
	SELECT @entry_id, id FROM (
		SELECT id, rtrim(rtrim(net_address, replace(net_address, ':', '')),':') as net_host
		FROM hosts
		WHERE net_host == @exact_entry OR net_host LIKE @like_entry
	)
	`, params).Error
	return
}

func (e *dbBlocklistEntry) BeforeCreate(tx *gorm.DB) (err error) {
	tx.Statement.AddClause(clause.OnConflict{
		Columns:   []clause.Column{{Name: "entry"}},
		DoNothing: true,
	})
	return nil
}

func (e *dbBlocklistEntry) blocks(h *dbHost) bool {
	host, _, err := net.SplitHostPort(h.NetAddress)
	if err != nil {
		return false // do nothing
	}

	return host == e.Entry || strings.HasSuffix(host, "."+e.Entry)
}

// Host returns information about a host.
func (ss *SQLStore) Host(hostKey consensus.PublicKey) (hostdb.Host, error) {
	var h dbHost

	tx := ss.db.
		Scopes(ExcludeBlockedHosts).
		Where(&dbHost{PublicKey: hostKey}).
		Take(&h)
	if errors.Is(tx.Error, gorm.ErrRecordNotFound) {
		return hostdb.Host{}, ErrHostNotFound
	}
	return h.convert(), tx.Error
}

// Hosts returns hosts at given offset and limit.
func (ss *SQLStore) Hosts(offset, limit int) ([]hostdb.Host, error) {
	if offset < 0 {
		return nil, ErrNegativeOffset
	}

	var hosts []hostdb.Host
	var fullHosts []dbHost

	err := ss.db.
		Scopes(ExcludeBlockedHosts).
		Offset(offset).
		Limit(limit).
		FindInBatches(&fullHosts, 10000, func(tx *gorm.DB, batch int) error {
			for _, fh := range fullHosts {
				hosts = append(hosts, fh.convert())
			}
			return nil
		}).
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

func (ss *SQLStore) AddHostBlocklistEntry(entry string) error {
	return ss.db.Create(&dbBlocklistEntry{Entry: entry}).Error
}

func (db *SQLStore) RemoveHostBlocklistEntry(entry string) (err error) {
	err = db.db.Where(&dbBlocklistEntry{Entry: entry}).Delete(&dbBlocklistEntry{}).Error
	return
}

func (db *SQLStore) HostBlocklist() (blocklist []string, err error) {
	err = db.db.
		Model(&dbBlocklistEntry{}).
		Pluck("entry", &blocklist).
		Error
	return
}

// RecordHostInteraction records an interaction with a host. If the host is not in
// the store, a new entry is created for it.
func (db *SQLStore) RecordInteractions(interactions []hostdb.Interaction) error {
	// Get keys from input.
	keyMap := make(map[consensus.PublicKey]struct{})
	var hks [][]byte
	for _, interaction := range interactions {
		if _, exists := keyMap[interaction.Host]; !exists {
			hks = append(hks, gobEncode(interaction.Host))
			keyMap[interaction.Host] = struct{}{}
		}
	}

	// Fetch hosts for which to add interactions. This can be done
	// outsisde the transaction to reduce the time we spend in the
	// transaction since we don't need it to be perfectly
	// consistent.
	var hosts []dbHost
	if err := db.db.Where("public_key IN ?", hks).
		Find(&hosts).Error; err != nil {
		return err
	}
	hostMap := make(map[consensus.PublicKey]dbHost)
	for _, h := range hosts {
		hostMap[h.PublicKey] = h
	}

	// Write the interactions and update to the hosts atmomically within a
	// single transaction.
	return db.db.Transaction(func(tx *gorm.DB) error {
		// Apply all the interactions to the hosts.
		dbInteractions := make([]dbInteraction, 0, len(interactions))
		for _, interaction := range interactions {
			host, exists := hostMap[interaction.Host]
			if !exists {
				continue // host doesn't exist
			}
			isScan := interaction.Type == hostdb.InteractionTypeScan
			dbInteractions = append(dbInteractions, dbInteraction{
				Result:    interaction.Result,
				Success:   interaction.Success,
				Timestamp: interaction.Timestamp.UTC(),
				Type:      interaction.Type,
			})
			interactionTime := interaction.Timestamp.UnixNano()
			if interaction.Success {
				host.SuccessfulInteractions++
				if isScan && host.LastScan > 0 && host.LastScan < interactionTime {
					host.Uptime += time.Duration(interactionTime - host.LastScan)
				}
			} else {
				host.FailedInteractions++
				if isScan && host.LastScan > 0 && host.LastScan < interactionTime {
					host.Downtime += time.Duration(interactionTime - host.LastScan)
				}
			}
			if isScan {
				host.TotalScans++
				host.SecondToLastScanSuccess = host.LastScanSuccess
				host.LastScanSuccess = interaction.Success
				host.LastScan = interaction.Timestamp.UnixNano()
				var sr hostdb.ScanResult
				if interaction.Success {
					if err := json.Unmarshal(interaction.Result, &sr); err != nil {
						return err
					}
				}
				host.Settings = convertHostSettings(sr.Settings)
			}

			// Save to map again.
			hostMap[host.PublicKey] = host
		}

		// Save everything to the db.
		if err := tx.CreateInBatches(&dbInteractions, 100).Error; err != nil {
			return err
		}
		for _, h := range hostMap {
			err := tx.Model(&dbHost{}).
				Where("public_key", gobEncode(h.PublicKey)).
				Updates(map[string]interface{}{
					"total_scans":                 h.TotalScans,
					"second_to_last_scan_success": h.SecondToLastScanSuccess,
					"last_scan_success":           h.LastScanSuccess,
					"downtime":                    h.Downtime,
					"uptime":                      h.Uptime,
					"last_scan":                   h.LastScan,
					"settings":                    gobEncode(h.Settings),
					"successful_interactions":     h.SuccessfulInteractions,
					"failed_interactions":         h.FailedInteractions,
				}).Error
			if err != nil {
				return err
			}
		}
		return nil
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

func ExcludeBlockedHosts(db *gorm.DB) *gorm.DB {
	return db.Where("NOT EXISTS (SELECT 1 FROM host_blocklist_entry_hosts hbeh WHERE hbeh.db_host_id = hosts.id)")
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
	return tx.Create(&hosts).Error
}
