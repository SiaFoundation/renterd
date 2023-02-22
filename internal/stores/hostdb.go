package stores

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/hostdb"
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

	// hostRetrievalBatchSize is the number of hosts we fetch from the
	// database per batch. Empirically tested to verify that this is a value
	// that performs reasonably well.
	hostRetrievalBatchSize = 10000
)

var (
	ErrNegativeOffset = errors.New("offset can not be negative")
)

type (
	// dbHost defines a hostdb.Interaction as persisted in the DB.
	// Deleting a host from the db will cascade the deletion and also delete
	// the corresponding announcements and interactions with that host.
	dbHost struct {
		Model

		PublicKey publicKey `gorm:"unique;index;NOT NULL;size:32"`
		Settings  hostSettings

		TotalScans              uint64
		LastScan                int64 `gorm:"index"` // unix nano
		LastScanSuccess         bool
		SecondToLastScanSuccess bool
		Uptime                  time.Duration
		Downtime                time.Duration

		// RecentDowntime and RecentScanFailures are used to determine whether a
		// host is eligible for pruning.
		RecentDowntime     time.Duration `gorm:"index"`
		RecentScanFailures uint64        `gorm:"index"`

		SuccessfulInteractions float64
		FailedInteractions     float64

		LastAnnouncement time.Time
		NetAddress       string `gorm:"index"`

		Allowlist []dbAllowlistEntry `gorm:"many2many:host_allowlist_entry_hosts;constraint:OnDelete:CASCADE"`
		Blocklist []dbBlocklistEntry `gorm:"many2many:host_blocklist_entry_hosts;constraint:OnDelete:CASCADE"`
	}

	// dbAllowlistEntry defines a table that stores the host blocklist.
	dbAllowlistEntry struct {
		Model
		Entry publicKey `gorm:"unique;index;NOT NULL;size:32"`
		Hosts []dbHost  `gorm:"many2many:host_allowlist_entry_hosts;constraint:OnDelete:CASCADE"`
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

		Host      publicKey
		Result    json.RawMessage
		Success   bool
		Timestamp time.Time `gorm:"index; NOT NULL"`
		Type      string    `gorm:"NOT NULL"`
	}

	dbConsensusInfo struct {
		Model
		CCID []byte
	}

	// dbAnnouncement is a table used for storing all announcements. It
	// doesn't have any relations to dbHost which means it won't
	// automatically prune when a host is deleted.
	dbAnnouncement struct {
		Model
		HostKey publicKey `gorm:"NOT NULL"`

		BlockHeight uint64
		BlockID     string
		NetAddress  string
	}

	// announcement describes an announcement for a single host.
	announcement struct {
		hostKey      publicKey
		announcement hostdb.Announcement
	}
)

// convert converts hostSettings to rhp.HostSettings
func (s hostSettings) convert() rhpv2.HostSettings {
	return rhpv2.HostSettings{
		AcceptingContracts:         s.AcceptingContracts,
		MaxDownloadBatchSize:       s.MaxDownloadBatchSize,
		MaxDuration:                s.MaxDuration,
		MaxReviseBatchSize:         s.MaxReviseBatchSize,
		NetAddress:                 s.NetAddress,
		RemainingStorage:           s.RemainingStorage,
		SectorSize:                 s.SectorSize,
		TotalStorage:               s.TotalStorage,
		Address:                    s.Address,
		WindowSize:                 s.WindowSize,
		Collateral:                 s.Collateral,
		MaxCollateral:              s.MaxCollateral,
		BaseRPCPrice:               s.BaseRPCPrice,
		ContractPrice:              s.ContractPrice,
		DownloadBandwidthPrice:     s.DownloadBandwidthPrice,
		SectorAccessPrice:          s.SectorAccessPrice,
		StoragePrice:               s.StoragePrice,
		UploadBandwidthPrice:       s.UploadBandwidthPrice,
		EphemeralAccountExpiry:     s.EphemeralAccountExpiry,
		MaxEphemeralAccountBalance: s.MaxEphemeralAccountBalance,
		RevisionNumber:             s.RevisionNumber,
		Version:                    s.Version,
		SiaMuxPort:                 s.SiaMuxPort,
	}
}

func convertHostSettings(settings rhpv2.HostSettings) hostSettings {
	return hostSettings{
		AcceptingContracts:         settings.AcceptingContracts,
		MaxDownloadBatchSize:       settings.MaxDownloadBatchSize,
		MaxDuration:                settings.MaxDuration,
		MaxReviseBatchSize:         settings.MaxReviseBatchSize,
		NetAddress:                 settings.NetAddress,
		RemainingStorage:           settings.RemainingStorage,
		SectorSize:                 settings.SectorSize,
		TotalStorage:               settings.TotalStorage,
		Address:                    settings.Address,
		WindowSize:                 settings.WindowSize,
		Collateral:                 settings.Collateral,
		MaxCollateral:              settings.MaxCollateral,
		BaseRPCPrice:               settings.BaseRPCPrice,
		ContractPrice:              settings.ContractPrice,
		DownloadBandwidthPrice:     settings.DownloadBandwidthPrice,
		SectorAccessPrice:          settings.SectorAccessPrice,
		StoragePrice:               settings.StoragePrice,
		UploadBandwidthPrice:       settings.UploadBandwidthPrice,
		EphemeralAccountExpiry:     settings.EphemeralAccountExpiry,
		MaxEphemeralAccountBalance: settings.MaxEphemeralAccountBalance,
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
func (dbAllowlistEntry) TableName() string { return "host_allowlist_entries" }

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
		PublicKey: types.PublicKey(h.PublicKey),
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
	// fetch allowlist and filter the entries that apply to this host
	var dbAllowlist []dbAllowlistEntry
	if err := tx.
		Model(&dbAllowlistEntry{}).
		Find(&dbAllowlist).
		Error; err != nil {
		return err
	}
	allowlist := dbAllowlist[:0]
	for _, entry := range dbAllowlist {
		if entry.Entry == h.PublicKey {
			allowlist = append(allowlist, entry)
		}
	}

	// update the association on the host
	if err := tx.Model(h).Association("Allowlist").Replace(&allowlist); err != nil {
		return err
	}

	// fetch blocklist and filter the entries that apply to this host
	var dbBlocklist []dbBlocklistEntry
	if err := tx.
		Model(&dbBlocklistEntry{}).
		Find(&dbBlocklist).
		Error; err != nil {
		return err
	}
	blocklist := dbBlocklist[:0]
	for _, entry := range dbBlocklist {
		if entry.blocks(h) {
			blocklist = append(blocklist, entry)
		}
	}

	// update the association on the host
	return tx.Model(h).Association("Blocklist").Replace(&blocklist)
}

func (h *dbHost) BeforeCreate(tx *gorm.DB) (err error) {
	tx.Statement.AddClause(clause.OnConflict{
		Columns:   []clause.Column{{Name: "public_key"}},
		DoUpdates: clause.AssignmentColumns([]string{"last_announcement", "net_address"}),
	})
	return nil
}

func (e *dbAllowlistEntry) AfterCreate(tx *gorm.DB) error {
	// NOTE: the ID is zero here if we ignore a conflict on create
	if e.ID == 0 {
		return nil
	}

	params := map[string]interface{}{
		"entry_id":    e.ID,
		"exact_entry": publicKey(e.Entry),
	}

	// insert entries into the allowlist
	switch tx.Config.Dialector.Name() {
	case "sqlite":
		return tx.Exec(`INSERT OR IGNORE INTO host_allowlist_entry_hosts (db_allowlist_entry_id, db_host_id)
SELECT @entry_id, id FROM (
	SELECT id
	FROM hosts
	WHERE public_key = @exact_entry
)`, params).Error
	default:
		return tx.Exec(`INSERT IGNORE INTO host_allowlist_entry_hosts (db_allowlist_entry_id, db_host_id)
SELECT @entry_id, id FROM (
	SELECT id
	FROM hosts
	WHERE public_key=@exact_entry
) AS _`, params).Error
	}
}

func (e *dbAllowlistEntry) BeforeCreate(tx *gorm.DB) (err error) {
	tx.Statement.AddClause(clause.OnConflict{
		Columns:   []clause.Column{{Name: "entry"}},
		DoNothing: true,
	})
	return nil
}

func (e *dbBlocklistEntry) AfterCreate(tx *gorm.DB) error {
	// NOTE: the ID is zero here if we ignore a conflict on create
	if e.ID == 0 {
		return nil
	}

	params := map[string]interface{}{
		"entry_id":    e.ID,
		"exact_entry": e.Entry,
		"like_entry":  fmt.Sprintf("%%.%s", e.Entry),
	}

	// insert entries into the blocklist
	switch tx.Config.Dialector.Name() {
	case "sqlite":
		return tx.Exec(`INSERT OR IGNORE INTO host_blocklist_entry_hosts (db_blocklist_entry_id, db_host_id)
		SELECT @entry_id, id FROM (
			SELECT id, rtrim(rtrim(net_address, replace(net_address, ':', '')),':') as net_host
			FROM hosts
			WHERE net_address == @exact_entry OR net_host == @exact_entry OR net_host LIKE @like_entry
		)`, params).Error
	default:
		return tx.Exec(`INSERT IGNORE INTO host_blocklist_entry_hosts (db_blocklist_entry_id, db_host_id)
SELECT @entry_id, id FROM (
	SELECT id
	FROM hosts
	WHERE net_address=@exact_entry OR trim(TRAILING ':' FROM trim(TRAILING replace(net_address, ':', '') from net_address))=@exact_entry OR trim(TRAILING ':' FROM trim(TRAILING replace(net_address, ':', '') from net_address)) LIKE @like_entry
) AS _`, params).Error
	}
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
func (ss *SQLStore) Host(ctx context.Context, hostKey types.PublicKey) (hostdb.HostInfo, error) {
	var h dbHost

	tx := ss.db.
		Where(&dbHost{PublicKey: publicKey(hostKey)}).
		Preload("Allowlist").
		Preload("Blocklist").
		Take(&h)
	if errors.Is(tx.Error, gorm.ErrRecordNotFound) {
		return hostdb.HostInfo{}, api.ErrHostNotFound
	} else if tx.Error != nil {
		return hostdb.HostInfo{}, tx.Error
	}

	return hostdb.HostInfo{
		Host:    h.convert(),
		Blocked: ss.isBlocked(h),
	}, nil
}

// HostsForScanning returns the address of hosts for scanning.
func (ss *SQLStore) HostsForScanning(ctx context.Context, maxLastScan time.Time, offset, limit int) ([]hostdb.HostAddress, error) {
	if offset < 0 {
		return nil, ErrNegativeOffset
	}

	var hosts []struct {
		PublicKey  publicKey `gorm:"unique;index;NOT NULL"`
		NetAddress string
	}
	var hostAddresses []hostdb.HostAddress

	err := ss.db.
		Scopes(ss.blocklist).
		Model(&dbHost{}).
		Where("last_scan < ?", maxLastScan.UnixNano()).
		Offset(offset).
		Limit(limit).
		Order("last_scan ASC").
		FindInBatches(&hosts, hostRetrievalBatchSize, func(tx *gorm.DB, batch int) error {
			for _, h := range hosts {
				hostAddresses = append(hostAddresses, hostdb.HostAddress{
					PublicKey:  types.PublicKey(h.PublicKey),
					NetAddress: h.NetAddress,
				})
			}
			return nil
		}).
		Error
	if err != nil {
		return nil, err
	}
	return hostAddresses, err
}

// Hosts returns hosts at given offset and limit.
func (ss *SQLStore) Hosts(ctx context.Context, offset, limit int) ([]hostdb.Host, error) {
	if offset < 0 {
		return nil, ErrNegativeOffset
	}

	var hosts []hostdb.Host
	var fullHosts []dbHost

	err := ss.db.
		Scopes(ss.blocklist).
		Offset(offset).
		Limit(limit).
		FindInBatches(&fullHosts, hostRetrievalBatchSize, func(tx *gorm.DB, batch int) error {
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

func (ss *SQLStore) RemoveOfflineHosts(ctx context.Context, minRecentFailures uint64, maxDowntime time.Duration) (removed uint64, err error) {
	tx := ss.db.
		Model(&dbHost{}).
		Where("recent_downtime >= ? AND recent_scan_failures >= ?", maxDowntime, minRecentFailures).
		Delete(&dbHost{})

	removed = uint64(tx.RowsAffected)
	err = tx.Error
	return
}

func (ss *SQLStore) AddHostAllowlistEntry(ctx context.Context, entry types.PublicKey) (err error) {
	defer ss.updateHasAllowlist(&err)
	return ss.db.Create(&dbAllowlistEntry{Entry: publicKey(entry)}).Error
}

func (ss *SQLStore) RemoveHostAllowlistEntry(ctx context.Context, entry types.PublicKey) (err error) {
	defer ss.updateHasAllowlist(&err)
	return ss.db.Where(&dbAllowlistEntry{Entry: publicKey(entry)}).Delete(&dbAllowlistEntry{}).Error
}

func (ss *SQLStore) AddHostBlocklistEntry(ctx context.Context, entry string) (err error) {
	defer ss.updateHasBlocklist(&err)
	return ss.db.Create(&dbBlocklistEntry{Entry: entry}).Error
}

func (ss *SQLStore) RemoveHostBlocklistEntry(ctx context.Context, entry string) (err error) {
	defer ss.updateHasBlocklist(&err)
	err = ss.db.Where(&dbBlocklistEntry{Entry: entry}).Delete(&dbBlocklistEntry{}).Error
	return
}

func (ss *SQLStore) HostAllowlist(ctx context.Context) (allowlist []types.PublicKey, err error) {
	var pubkeys []publicKey
	err = ss.db.
		Model(&dbAllowlistEntry{}).
		Pluck("entry", &pubkeys).
		Error

	for _, pubkey := range pubkeys {
		allowlist = append(allowlist, types.PublicKey(pubkey))
	}
	return
}

func (ss *SQLStore) HostBlocklist(ctx context.Context) (blocklist []string, err error) {
	err = ss.db.
		Model(&dbBlocklistEntry{}).
		Pluck("entry", &blocklist).
		Error
	return
}

// RecordHostInteraction records an interaction with a host. If the host is not in
// the store, a new entry is created for it.
func (ss *SQLStore) RecordInteractions(ctx context.Context, interactions []hostdb.Interaction) error {
	if len(interactions) == 0 {
		return nil // nothing to do
	}

	// Get keys from input.
	keyMap := make(map[publicKey]struct{})
	var hks []publicKey
	for _, interaction := range interactions {
		if _, exists := keyMap[publicKey(interaction.Host)]; !exists {
			hks = append(hks, publicKey(interaction.Host))
			keyMap[publicKey(interaction.Host)] = struct{}{}
		}
	}

	// Fetch hosts for which to add interactions. This can be done
	// outsisde the transaction to reduce the time we spend in the
	// transaction since we don't need it to be perfectly
	// consistent.
	var hosts []dbHost
	if err := ss.db.Where("public_key IN ?", hks).
		Find(&hosts).Error; err != nil {
		return err
	}
	hostMap := make(map[publicKey]dbHost)
	for _, h := range hosts {
		hostMap[h.PublicKey] = h
	}

	// Write the interactions and update to the hosts atmomically within a
	// single transaction.
	return ss.db.Transaction(func(tx *gorm.DB) error {
		// Apply all the interactions to the hosts.
		dbInteractions := make([]dbInteraction, 0, len(interactions))
		for _, interaction := range interactions {
			host, exists := hostMap[publicKey(interaction.Host)]
			if !exists {
				continue // host doesn't exist
			}
			isScan := interaction.Type == hostdb.InteractionTypeScan
			dbInteractions = append(dbInteractions, dbInteraction{
				Host:      publicKey(interaction.Host),
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
				host.RecentDowntime = 0
				host.RecentScanFailures = 0
			} else {
				host.FailedInteractions++
				if isScan {
					host.RecentScanFailures++
					if host.LastScan > 0 && host.LastScan < interactionTime {
						host.Downtime += time.Duration(interactionTime - host.LastScan)
						host.RecentDowntime += time.Duration(interactionTime - host.LastScan)
					}
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
					host.Settings = convertHostSettings(sr.Settings)
				}
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
				Where("public_key", h.PublicKey).
				Updates(map[string]interface{}{
					"total_scans":                 h.TotalScans,
					"second_to_last_scan_success": h.SecondToLastScanSuccess,
					"last_scan_success":           h.LastScanSuccess,
					"recent_downtime":             h.RecentDowntime,
					"recent_scan_failures":        h.RecentScanFailures,
					"downtime":                    h.Downtime,
					"uptime":                      h.Uptime,
					"last_scan":                   h.LastScan,
					"settings":                    h.Settings,
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
func (ss *SQLStore) ProcessConsensusChange(cc modules.ConsensusChange) {
	height := uint64(cc.InitialHeight())
	for range cc.RevertedBlocks {
		height--
	}

	// Fetch announcements and add them to the queue.
	var newAnnouncements []announcement
	for _, sb := range cc.AppliedBlocks {
		var b types.Block
		convertToCore(sb, &b)
		hostdb.ForEachAnnouncement(b, height, func(hostKey types.PublicKey, ha hostdb.Announcement) {
			newAnnouncements = append(newAnnouncements, announcement{
				hostKey:      publicKey(hostKey),
				announcement: ha,
			})
		})
		height++
	}

	ss.unappliedAnnouncements = append(ss.unappliedAnnouncements, newAnnouncements...)
	ss.unappliedCCID = cc.ID

	// Apply new announcements
	if time.Since(ss.lastAnnouncementSave) > ss.persistInterval || len(ss.unappliedAnnouncements) >= announcementBatchSoftLimit {
		err := ss.db.Transaction(func(tx *gorm.DB) error {
			if len(ss.unappliedAnnouncements) > 0 {
				if err := insertAnnouncements(tx, ss.unappliedAnnouncements); err != nil {
					return err
				}
			}
			return updateCCID(tx, ss.unappliedCCID)
		})
		if err != nil {
			// NOTE: print error. If we failed due to a temporary error
			println(fmt.Sprintf("failed to apply %v announcements - should never happen", len(ss.unappliedAnnouncements)))
		}

		ss.unappliedAnnouncements = ss.unappliedAnnouncements[:0]
		ss.lastAnnouncementSave = time.Now()
	}
}

func (ss *SQLStore) blocklist(db *gorm.DB) *gorm.DB {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	if ss.hasAllowlist {
		db = db.Where("EXISTS (SELECT 1 FROM host_allowlist_entry_hosts hbeh WHERE hbeh.db_host_id = hosts.id)")
	}
	if ss.hasBlocklist {
		db = db.Where("NOT EXISTS (SELECT 1 FROM host_blocklist_entry_hosts hbeh WHERE hbeh.db_host_id = hosts.id)")
	}
	return db
}

func (ss *SQLStore) isBlocked(h dbHost) (blocked bool) {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	if ss.hasAllowlist && len(h.Allowlist) == 0 {
		blocked = true
	}
	if ss.hasBlocklist && len(h.Blocklist) > 0 {
		blocked = true
	}
	return
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
