package stores

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
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
	ErrNegativeOffset      = errors.New("offset can not be negative")
	ErrNegativeMaxDowntime = errors.New("max downtime can not be negative")
)

type (
	// dbHost defines a hostdb.Interaction as persisted in the DB. Deleting a
	// host from the db will cascade the deletion and also delete the
	// corresponding announcements and interactions with that host.
	//
	// NOTE: updating the host entity requires an update to the field map passed
	// to 'Update' when recording host interactions
	dbHost struct {
		Model

		PublicKey        publicKey `gorm:"unique;index;NOT NULL;size:32"`
		Settings         hostSettings
		PriceTable       hostPriceTable
		PriceTableExpiry sql.NullTime

		TotalScans              uint64
		LastScan                int64 `gorm:"index"` // unix nano
		LastScanSuccess         bool
		SecondToLastScanSuccess bool
		Scanned                 bool `gorm:"index"`
		Uptime                  time.Duration
		Downtime                time.Duration

		// RecentDowntime and RecentScanFailures are used to determine whether a
		// host is eligible for pruning.
		RecentDowntime     time.Duration `gorm:"index"`
		RecentScanFailures uint64        `gorm:"index"`

		SuccessfulInteractions float64
		FailedInteractions     float64

		LostSectors uint64

		LastAnnouncement time.Time
		NetAddress       string `gorm:"index"`

		Allowlist []dbAllowlistEntry `gorm:"many2many:host_allowlist_entry_hosts;constraint:OnDelete:CASCADE"`
		Blocklist []dbBlocklistEntry `gorm:"many2many:host_blocklist_entry_hosts;constraint:OnDelete:CASCADE"`
		Checks    []dbHostCheck      `gorm:"foreignKey:DBHostID;constraint:OnDelete:CASCADE"`
	}

	// dbHostCheck contains information about a host that is collected and used
	// by the autopilot.
	dbHostCheck struct {
		Model

		DBAutopilotID uint
		DBAutopilot   dbAutopilot

		DBHostID uint
		DBHost   dbHost

		// usability
		UsabilityBlocked               bool
		UsabilityOffline               bool
		UsabilityLowScore              bool
		UsabilityRedundantIP           bool
		UsabilityGouging               bool
		UsabilityNotAcceptingContracts bool
		UsabilityNotAnnounced          bool
		UsabilityNotCompletingScan     bool

		// score
		ScoreAge              float64
		ScoreCollateral       float64
		ScoreInteractions     float64
		ScoreStorageRemaining float64
		ScoreUptime           float64
		ScoreVersion          float64
		ScorePrices           float64

		// gouging
		GougingContractErr string
		GougingDownloadErr string
		GougingGougingErr  string
		GougingPruneErr    string
		GougingUploadErr   string
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

	dbConsensusInfo struct {
		Model
		CCID    []byte
		Height  uint64
		BlockID hash256
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

// convert converts hostSettings to rhp.HostSettings
func (pt hostPriceTable) convert() rhpv3.HostPriceTable {
	return rhpv3.HostPriceTable{
		UID:                          pt.UID,
		Validity:                     pt.Validity,
		HostBlockHeight:              pt.HostBlockHeight,
		UpdatePriceTableCost:         pt.UpdatePriceTableCost,
		AccountBalanceCost:           pt.AccountBalanceCost,
		FundAccountCost:              pt.FundAccountCost,
		LatestRevisionCost:           pt.LatestRevisionCost,
		SubscriptionMemoryCost:       pt.SubscriptionMemoryCost,
		SubscriptionNotificationCost: pt.SubscriptionNotificationCost,
		InitBaseCost:                 pt.InitBaseCost,
		MemoryTimeCost:               pt.MemoryTimeCost,
		DownloadBandwidthCost:        pt.DownloadBandwidthCost,
		UploadBandwidthCost:          pt.UploadBandwidthCost,
		DropSectorsBaseCost:          pt.DropSectorsBaseCost,
		DropSectorsUnitCost:          pt.DropSectorsUnitCost,
		HasSectorBaseCost:            pt.HasSectorBaseCost,
		ReadBaseCost:                 pt.ReadBaseCost,
		ReadLengthCost:               pt.ReadLengthCost,
		RenewContractCost:            pt.RenewContractCost,
		RevisionBaseCost:             pt.RevisionBaseCost,
		SwapSectorBaseCost:           pt.SwapSectorBaseCost,
		WriteBaseCost:                pt.WriteBaseCost,
		WriteLengthCost:              pt.WriteLengthCost,
		WriteStoreCost:               pt.WriteStoreCost,
		TxnFeeMinRecommended:         pt.TxnFeeMinRecommended,
		TxnFeeMaxRecommended:         pt.TxnFeeMaxRecommended,
		ContractPrice:                pt.ContractPrice,
		CollateralCost:               pt.CollateralCost,
		MaxCollateral:                pt.MaxCollateral,
		MaxDuration:                  pt.MaxDuration,
		WindowSize:                   pt.WindowSize,
		RegistryEntriesLeft:          pt.RegistryEntriesLeft,
		RegistryEntriesTotal:         pt.RegistryEntriesTotal,
	}
}

func convertHostPriceTable(pt rhpv3.HostPriceTable) hostPriceTable {
	return hostPriceTable{
		UID:                          pt.UID,
		Validity:                     pt.Validity,
		HostBlockHeight:              pt.HostBlockHeight,
		UpdatePriceTableCost:         pt.UpdatePriceTableCost,
		AccountBalanceCost:           pt.AccountBalanceCost,
		FundAccountCost:              pt.FundAccountCost,
		LatestRevisionCost:           pt.LatestRevisionCost,
		SubscriptionMemoryCost:       pt.SubscriptionMemoryCost,
		SubscriptionNotificationCost: pt.SubscriptionNotificationCost,
		InitBaseCost:                 pt.InitBaseCost,
		MemoryTimeCost:               pt.MemoryTimeCost,
		DownloadBandwidthCost:        pt.DownloadBandwidthCost,
		UploadBandwidthCost:          pt.UploadBandwidthCost,
		DropSectorsBaseCost:          pt.DropSectorsBaseCost,
		DropSectorsUnitCost:          pt.DropSectorsUnitCost,
		HasSectorBaseCost:            pt.HasSectorBaseCost,
		ReadBaseCost:                 pt.ReadBaseCost,
		ReadLengthCost:               pt.ReadLengthCost,
		RenewContractCost:            pt.RenewContractCost,
		RevisionBaseCost:             pt.RevisionBaseCost,
		SwapSectorBaseCost:           pt.SwapSectorBaseCost,
		WriteBaseCost:                pt.WriteBaseCost,
		WriteLengthCost:              pt.WriteLengthCost,
		WriteStoreCost:               pt.WriteStoreCost,
		TxnFeeMinRecommended:         pt.TxnFeeMinRecommended,
		TxnFeeMaxRecommended:         pt.TxnFeeMaxRecommended,
		ContractPrice:                pt.ContractPrice,
		CollateralCost:               pt.CollateralCost,
		MaxCollateral:                pt.MaxCollateral,
		MaxDuration:                  pt.MaxDuration,
		WindowSize:                   pt.WindowSize,
		RegistryEntriesLeft:          pt.RegistryEntriesLeft,
		RegistryEntriesTotal:         pt.RegistryEntriesTotal,
	}
}

// TableName implements the gorm.Tabler interface.
func (dbAnnouncement) TableName() string { return "host_announcements" }

// TableName implements the gorm.Tabler interface.
func (dbConsensusInfo) TableName() string { return "consensus_infos" }

// TableName implements the gorm.Tabler interface.
func (dbHost) TableName() string { return "hosts" }

// TableName implements the gorm.Tabler interface.
func (dbHostCheck) TableName() string { return "host_checks" }

// TableName implements the gorm.Tabler interface.
func (dbAllowlistEntry) TableName() string { return "host_allowlist_entries" }

// TableName implements the gorm.Tabler interface.
func (dbBlocklistEntry) TableName() string { return "host_blocklist_entries" }

// convert converts a host into a api.HostInfo
func (h dbHost) convert(blocked bool) api.Host {
	var lastScan time.Time
	if h.LastScan > 0 {
		lastScan = time.Unix(0, h.LastScan)
	}
	checks := make(map[string]api.HostCheck)
	for _, check := range h.Checks {
		checks[check.DBAutopilot.Identifier] = check.convert()
	}
	return api.Host{
		Host: hostdb.Host{
			KnownSince:       h.CreatedAt,
			LastAnnouncement: h.LastAnnouncement,
			NetAddress:       h.NetAddress,
			Interactions: hostdb.Interactions{
				TotalScans:              h.TotalScans,
				LastScan:                lastScan,
				LastScanSuccess:         h.LastScanSuccess,
				SecondToLastScanSuccess: h.SecondToLastScanSuccess,
				Uptime:                  h.Uptime,
				Downtime:                h.Downtime,
				SuccessfulInteractions:  h.SuccessfulInteractions,
				FailedInteractions:      h.FailedInteractions,
				LostSectors:             h.LostSectors,
			},
			PriceTable: hostdb.HostPriceTable{
				HostPriceTable: h.PriceTable.convert(),
				Expiry:         h.PriceTableExpiry.Time,
			},
			PublicKey: types.PublicKey(h.PublicKey),
			Scanned:   h.Scanned,
			Settings:  h.Settings.convert(),
		},
		Blocked: blocked,
		Checks:  checks,
	}
}

func (hi dbHostCheck) convert() api.HostCheck {
	return api.HostCheck{
		Gouging: api.HostGougingBreakdown{
			ContractErr: hi.GougingContractErr,
			DownloadErr: hi.GougingDownloadErr,
			GougingErr:  hi.GougingGougingErr,
			PruneErr:    hi.GougingPruneErr,
			UploadErr:   hi.GougingUploadErr,
		},
		Score: api.HostScoreBreakdown{
			Age:              hi.ScoreAge,
			Collateral:       hi.ScoreCollateral,
			Interactions:     hi.ScoreInteractions,
			StorageRemaining: hi.ScoreStorageRemaining,
			Uptime:           hi.ScoreUptime,
			Version:          hi.ScoreVersion,
			Prices:           hi.ScorePrices,
		},
		Usability: api.HostUsabilityBreakdown{
			Blocked:               hi.UsabilityBlocked,
			Offline:               hi.UsabilityOffline,
			LowScore:              hi.UsabilityLowScore,
			RedundantIP:           hi.UsabilityRedundantIP,
			Gouging:               hi.UsabilityGouging,
			NotAcceptingContracts: hi.UsabilityNotAcceptingContracts,
			NotAnnounced:          hi.UsabilityNotAnnounced,
			NotCompletingScan:     hi.UsabilityNotCompletingScan,
		},
	}
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
	if isSQLite(tx) {
		return tx.Exec(`INSERT OR IGNORE INTO host_allowlist_entry_hosts (db_allowlist_entry_id, db_host_id)
SELECT @entry_id, id FROM (
SELECT id
FROM hosts
WHERE public_key = @exact_entry
)`, params).Error
	}

	return tx.Exec(`INSERT IGNORE INTO host_allowlist_entry_hosts (db_allowlist_entry_id, db_host_id)
SELECT @entry_id, id FROM (
	SELECT id
	FROM hosts
	WHERE public_key=@exact_entry
) AS _`, params).Error
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
	if isSQLite(tx) {
		return tx.Exec(`
INSERT OR IGNORE INTO host_blocklist_entry_hosts (db_blocklist_entry_id, db_host_id)
SELECT @entry_id, id FROM (
	SELECT id
	FROM hosts
	WHERE net_address == @exact_entry OR
		rtrim(rtrim(net_address, replace(net_address, ':', '')),':') == @exact_entry OR
		rtrim(rtrim(net_address, replace(net_address, ':', '')),':') LIKE @like_entry
)`, params).Error
	}

	return tx.Exec(`
INSERT IGNORE INTO host_blocklist_entry_hosts (db_blocklist_entry_id, db_host_id)
SELECT @entry_id, id FROM (
	SELECT id
	FROM hosts
	WHERE net_address=@exact_entry OR
		SUBSTRING_INDEX(net_address,':',1)=@exact_entry OR
		SUBSTRING_INDEX(net_address,':',1) LIKE @like_entry
) AS _`, params).Error
}

func (e *dbBlocklistEntry) BeforeCreate(tx *gorm.DB) (err error) {
	tx.Statement.AddClause(clause.OnConflict{
		Columns:   []clause.Column{{Name: "entry"}},
		DoNothing: true,
	})
	return nil
}

func (e *dbBlocklistEntry) blocks(h dbHost) bool {
	values := []string{h.NetAddress}
	host, _, err := net.SplitHostPort(h.NetAddress)
	if err == nil {
		values = append(values, host)
	}

	for _, value := range values {
		if value == e.Entry || strings.HasSuffix(value, "."+e.Entry) {
			return true
		}
	}
	return false
}

// Host returns information about a host.
func (ss *SQLStore) Host(ctx context.Context, hostKey types.PublicKey) (api.Host, error) {
	var h dbHost

	tx := ss.db.
		WithContext(ctx).
		Where(&dbHost{PublicKey: publicKey(hostKey)}).
		Preload("Allowlist").
		Preload("Blocklist").
		Take(&h)
	if errors.Is(tx.Error, gorm.ErrRecordNotFound) {
		return api.Host{}, api.ErrHostNotFound
	} else if tx.Error != nil {
		return api.Host{}, tx.Error
	}

	return h.convert(ss.isBlocked(h)), nil
}

func (ss *SQLStore) UpdateHostCheck(ctx context.Context, autopilotID string, hk types.PublicKey, hc api.HostCheck) (err error) {
	err = ss.db.Transaction(func(tx *gorm.DB) error {
		// fetch ap id
		var apID uint
		if err := tx.
			Model(&dbAutopilot{}).
			Where("identifier = ?", autopilotID).
			Select("id").
			Take(&apID).
			Error; errors.Is(err, gorm.ErrRecordNotFound) {
			return api.ErrAutopilotNotFound
		} else if err != nil {
			return err
		}

		// fetch host id
		var hID uint
		if err := tx.
			Model(&dbHost{}).
			Where("public_key = ?", publicKey(hk)).
			Select("id").
			Take(&hID).
			Error; errors.Is(err, gorm.ErrRecordNotFound) {
			return api.ErrHostNotFound
		} else if err != nil {
			return err
		}

		// update host info
		return tx.
			Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "db_autopilot_id"}, {Name: "db_host_id"}},
				UpdateAll: true,
			}).
			Create(&dbHostCheck{
				DBAutopilotID: apID,
				DBHostID:      hID,

				UsabilityBlocked:               hc.Usability.Blocked,
				UsabilityOffline:               hc.Usability.Offline,
				UsabilityLowScore:              hc.Usability.LowScore,
				UsabilityRedundantIP:           hc.Usability.RedundantIP,
				UsabilityGouging:               hc.Usability.Gouging,
				UsabilityNotAcceptingContracts: hc.Usability.NotAcceptingContracts,
				UsabilityNotAnnounced:          hc.Usability.NotAnnounced,
				UsabilityNotCompletingScan:     hc.Usability.NotCompletingScan,

				ScoreAge:              hc.Score.Age,
				ScoreCollateral:       hc.Score.Collateral,
				ScoreInteractions:     hc.Score.Interactions,
				ScoreStorageRemaining: hc.Score.StorageRemaining,
				ScoreUptime:           hc.Score.Uptime,
				ScoreVersion:          hc.Score.Version,
				ScorePrices:           hc.Score.Prices,

				GougingContractErr: hc.Gouging.ContractErr,
				GougingDownloadErr: hc.Gouging.DownloadErr,
				GougingGougingErr:  hc.Gouging.GougingErr,
				GougingPruneErr:    hc.Gouging.PruneErr,
				GougingUploadErr:   hc.Gouging.UploadErr,
			}).
			Error
	})
	return
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
		WithContext(ctx).
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

func (ss *SQLStore) SearchHosts(ctx context.Context, filterMode, addressContains string, keyIn []types.PublicKey, offset, limit int) ([]api.Host, error) {
	if offset < 0 {
		return nil, ErrNegativeOffset
	}

	// validate filterMode
	switch filterMode {
	case api.HostFilterModeAllowed:
	case api.HostFilterModeBlocked:
	case api.HostFilterModeAll:
	default:
		return nil, fmt.Errorf("invalid filter mode: %v", filterMode)
	}

	// prepare query
	query := ss.db.
		Model(&dbHost{}).
		Scopes(
			hostFilter(filterMode, ss.hasAllowlist(), ss.hasBlocklist()),
			hostNetAddress(addressContains),
			hostPublicKey(keyIn),
		)

	// preload allowlist and blocklist
	if filterMode == api.HostFilterModeAll {
		query = query.
			Preload("Allowlist").
			Preload("Blocklist")
	}

	// preload host checks
	query = query.Preload("Checks.DBAutopilot")

	var hosts []api.Host
	var fullHosts []dbHost
	err := query.
		Offset(offset).
		Limit(limit).
		FindInBatches(&fullHosts, hostRetrievalBatchSize, func(tx *gorm.DB, batch int) error {
			for _, fh := range fullHosts {
				var blocked bool
				if filterMode == api.HostFilterModeAll {
					blocked = ss.isBlocked(fh)
				} else {
					blocked = filterMode == api.HostFilterModeBlocked
				}
				hosts = append(hosts, fh.convert(blocked))
			}
			return nil
		}).
		Error
	if err != nil {
		return nil, err
	}
	return hosts, err
}

// Hosts returns non-blocked hosts at given offset and limit.
func (ss *SQLStore) Hosts(ctx context.Context, offset, limit int) ([]api.Host, error) {
	return ss.SearchHosts(ctx, api.HostFilterModeAllowed, "", nil, offset, limit)
}

func (ss *SQLStore) RemoveOfflineHosts(ctx context.Context, minRecentFailures uint64, maxDowntime time.Duration) (removed uint64, err error) {
	// sanity check 'maxDowntime'
	if maxDowntime < 0 {
		return 0, ErrNegativeMaxDowntime
	}

	// fetch all hosts outside of the transaction
	var hosts []dbHost
	if err := ss.db.
		WithContext(ctx).
		Model(&dbHost{}).
		Where("recent_downtime >= ? AND recent_scan_failures >= ?", maxDowntime, minRecentFailures).
		Find(&hosts).
		Error; err != nil {
		return 0, err
	}

	// return early
	if len(hosts) == 0 {
		return 0, nil
	}

	// remove every host one by one
	var errs []error
	for _, h := range hosts {
		if err := ss.retryTransaction(ctx, func(tx *gorm.DB) error {
			// fetch host contracts
			hcs, err := contractsForHost(tx, h)
			if err != nil {
				return err
			}

			// create map
			toArchive := make(map[types.FileContractID]string)
			for _, c := range hcs {
				toArchive[types.FileContractID(c.FCID)] = api.ContractArchivalReasonHostPruned
			}

			// archive host contracts
			if err := archiveContracts(tx, hcs, toArchive); err != nil {
				return err
			}

			// remove the host
			if err := tx.Delete(&h).Error; err != nil {
				return err
			}
			removed++
			return nil
		}); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		var msgs []string
		for _, err := range errs {
			msgs = append(msgs, err.Error())
		}
		err = errors.New(strings.Join(msgs, ";"))
	}
	return
}

func (ss *SQLStore) UpdateHostAllowlistEntries(ctx context.Context, add, remove []types.PublicKey, clear bool) (err error) {
	// nothing to do
	if len(add)+len(remove) == 0 && !clear {
		return nil
	}
	defer ss.updateHasAllowlist(&err)

	// clear allowlist
	if clear {
		return ss.retryTransaction(ctx, func(tx *gorm.DB) error {
			return tx.Where("TRUE").Delete(&dbAllowlistEntry{}).Error
		})
	}

	var toInsert []dbAllowlistEntry
	for _, entry := range add {
		toInsert = append(toInsert, dbAllowlistEntry{Entry: publicKey(entry)})
	}

	toDelete := make([]publicKey, len(remove))
	for i, entry := range remove {
		toDelete[i] = publicKey(entry)
	}

	return ss.retryTransaction(ctx, func(tx *gorm.DB) error {
		if len(toInsert) > 0 {
			if err := tx.Create(&toInsert).Error; err != nil {
				return err
			}
		}
		if len(toDelete) > 0 {
			if err := tx.Delete(&dbAllowlistEntry{}, "entry IN ?", toDelete).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

func (ss *SQLStore) UpdateHostBlocklistEntries(ctx context.Context, add, remove []string, clear bool) (err error) {
	// nothing to do
	if len(add)+len(remove) == 0 && !clear {
		return nil
	}
	defer ss.updateHasBlocklist(&err)

	// clear blocklist
	if clear {
		return ss.retryTransaction(ctx, func(tx *gorm.DB) error {
			return tx.Where("TRUE").Delete(&dbBlocklistEntry{}).Error
		})
	}

	var toInsert []dbBlocklistEntry
	for _, entry := range add {
		toInsert = append(toInsert, dbBlocklistEntry{Entry: entry})
	}

	return ss.retryTransaction(ctx, func(tx *gorm.DB) error {
		if len(toInsert) > 0 {
			if err := tx.Create(&toInsert).Error; err != nil {
				return err
			}
		}
		if len(remove) > 0 {
			if err := tx.Delete(&dbBlocklistEntry{}, "entry IN ?", remove).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

func (ss *SQLStore) HostAllowlist(ctx context.Context) (allowlist []types.PublicKey, err error) {
	var pubkeys []publicKey
	err = ss.db.
		WithContext(ctx).
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
		WithContext(ctx).
		Model(&dbBlocklistEntry{}).
		Pluck("entry", &blocklist).
		Error
	return
}

func (ss *SQLStore) RecordHostScans(ctx context.Context, scans []hostdb.HostScan) error {
	if len(scans) == 0 {
		return nil // nothing to do
	}

	// Get keys from input.
	keyMap := make(map[publicKey]struct{})
	var hks []publicKey
	for _, scan := range scans {
		if _, exists := keyMap[publicKey(scan.HostKey)]; !exists {
			hks = append(hks, publicKey(scan.HostKey))
			keyMap[publicKey(scan.HostKey)] = struct{}{}
		}
	}

	// Fetch hosts for which to add scans. This can be done outsisde the
	// transaction to reduce the time we spend in the transaction since we don't
	// need it to be perfectly consistent.
	var hosts []dbHost
	for i := 0; i < len(hks); i += maxSQLVars {
		end := i + maxSQLVars
		if end > len(hks) {
			end = len(hks)
		}
		var batchHosts []dbHost
		if err := ss.db.WithContext(ctx).Where("public_key IN (?)", hks[i:end]).
			Find(&batchHosts).Error; err != nil {
			return err
		}
		hosts = append(hosts, batchHosts...)
	}
	hostMap := make(map[publicKey]dbHost)
	for _, h := range hosts {
		hostMap[h.PublicKey] = h
	}

	// Write the interactions and update to the hosts atomically within a single
	// transaction.
	return ss.retryTransaction(ctx, func(tx *gorm.DB) error {
		// Handle scans
		for _, scan := range scans {
			host, exists := hostMap[publicKey(scan.HostKey)]
			if !exists {
				continue // host doesn't exist
			}
			lastScan := time.Unix(0, host.LastScan)

			if scan.Success {
				// Handle successful scan.
				host.SuccessfulInteractions++
				if host.LastScan > 0 && lastScan.Before(scan.Timestamp) {
					host.Uptime += scan.Timestamp.Sub(lastScan)
				}
				host.RecentDowntime = 0
				host.RecentScanFailures = 0

				// overwrite the NetAddress in the settings with the one we
				// received through the host announcement
				scan.Settings.NetAddress = host.NetAddress
				host.Settings = convertHostSettings(scan.Settings)

				// scans can only update the price table if the current
				// pricetable is expired anyway, ensuring scans never
				// overwrite a valid price table since the price table from
				// scans are not paid for and thus not useful for anything
				// aside from gouging checks
				if time.Now().After(host.PriceTableExpiry.Time) {
					host.PriceTable = convertHostPriceTable(scan.PriceTable)
					host.PriceTableExpiry = sql.NullTime{
						Time:  time.Now(),
						Valid: true,
					}
				}
			} else {
				// Handle failed scan.
				host.FailedInteractions++
				host.RecentScanFailures++
				if host.LastScan > 0 && lastScan.Before(scan.Timestamp) {
					host.Downtime += scan.Timestamp.Sub(lastScan)
					host.RecentDowntime += scan.Timestamp.Sub(lastScan)
				}
			}

			host.TotalScans++
			host.Scanned = host.Scanned || scan.Success
			host.SecondToLastScanSuccess = host.LastScanSuccess
			host.LastScanSuccess = scan.Success
			host.LastScan = scan.Timestamp.UnixNano()

			// Save to map again.
			hostMap[host.PublicKey] = host
		}

		// Persist.
		for _, h := range hostMap {
			err := tx.Model(&dbHost{}).
				Where("public_key", h.PublicKey).
				Updates(map[string]interface{}{
					"scanned":                     h.Scanned,
					"total_scans":                 h.TotalScans,
					"second_to_last_scan_success": h.SecondToLastScanSuccess,
					"last_scan_success":           h.LastScanSuccess,
					"recent_downtime":             h.RecentDowntime,
					"recent_scan_failures":        h.RecentScanFailures,
					"downtime":                    h.Downtime,
					"uptime":                      h.Uptime,
					"last_scan":                   h.LastScan,
					"settings":                    h.Settings,
					"price_table":                 h.PriceTable,
					"price_table_expiry":          h.PriceTableExpiry,
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

func (ss *SQLStore) RecordPriceTables(ctx context.Context, priceTableUpdate []hostdb.PriceTableUpdate) error {
	if len(priceTableUpdate) == 0 {
		return nil // nothing to do
	}

	// Get keys from input.
	keyMap := make(map[publicKey]struct{})
	var hks []publicKey
	for _, ptu := range priceTableUpdate {
		if _, exists := keyMap[publicKey(ptu.HostKey)]; !exists {
			hks = append(hks, publicKey(ptu.HostKey))
			keyMap[publicKey(ptu.HostKey)] = struct{}{}
		}
	}

	// Fetch hosts for which to add interactions. This can be done
	// outsisde the transaction to reduce the time we spend in the
	// transaction since we don't need it to be perfectly
	// consistent.
	var hosts []dbHost
	for i := 0; i < len(hks); i += maxSQLVars {
		end := i + maxSQLVars
		if end > len(hks) {
			end = len(hks)
		}
		var batchHosts []dbHost
		if err := ss.db.WithContext(ctx).Where("public_key IN (?)", hks[i:end]).
			Find(&batchHosts).Error; err != nil {
			return err
		}
		hosts = append(hosts, batchHosts...)
	}
	hostMap := make(map[publicKey]dbHost)
	for _, h := range hosts {
		hostMap[h.PublicKey] = h
	}

	// Write the interactions and update to the hosts atomically within a single
	// transaction.
	return ss.retryTransaction(ctx, func(tx *gorm.DB) error {
		// Handle price table updates
		for _, ptu := range priceTableUpdate {
			host, exists := hostMap[publicKey(ptu.HostKey)]
			if !exists {
				continue // host doesn't exist
			}
			if ptu.Success {
				// Handle successful update.
				host.SuccessfulInteractions++
				host.RecentDowntime = 0
				host.RecentScanFailures = 0

				// Update pricetable.
				host.PriceTable = convertHostPriceTable(ptu.PriceTable.HostPriceTable)
				host.PriceTableExpiry = sql.NullTime{
					Time:  ptu.PriceTable.Expiry,
					Valid: ptu.PriceTable.Expiry != time.Time{},
				}
			} else {
				// Handle failed update.
				host.FailedInteractions++
			}

			// Save to map again.
			hostMap[host.PublicKey] = host
		}

		// Persist.
		for _, h := range hostMap {
			err := tx.Model(&dbHost{}).
				Where("public_key", h.PublicKey).
				Updates(map[string]interface{}{
					"recent_downtime":         h.RecentDowntime,
					"recent_scan_failures":    h.RecentScanFailures,
					"price_table":             h.PriceTable,
					"price_table_expiry":      h.PriceTableExpiry,
					"successful_interactions": h.SuccessfulInteractions,
					"failed_interactions":     h.FailedInteractions,
				}).Error
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (ss *SQLStore) processConsensusChangeHostDB(cc modules.ConsensusChange) {
	height := uint64(cc.InitialHeight())
	for range cc.RevertedBlocks {
		height--
	}

	var newAnnouncements []announcement
	for _, sb := range cc.AppliedBlocks {
		var b types.Block
		convertToCore(sb, (*types.V1Block)(&b))

		// Process announcements, but only if they are not too old.
		if b.Timestamp.After(time.Now().Add(-ss.announcementMaxAge)) {
			hostdb.ForEachAnnouncement(types.Block(b), height, func(hostKey types.PublicKey, ha hostdb.Announcement) {
				newAnnouncements = append(newAnnouncements, announcement{
					hostKey:      publicKey(hostKey),
					announcement: ha,
				})
				ss.unappliedHostKeys[hostKey] = struct{}{}
			})
		}
		height++
	}

	ss.unappliedAnnouncements = append(ss.unappliedAnnouncements, newAnnouncements...)
}

// hostNetAddress can be used as a scope to filter hosts by their net address.
func hostNetAddress(addressContains string) func(*gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		if addressContains != "" {
			return db.Where("net_address LIKE ?", "%"+addressContains+"%")
		}
		return db
	}
}

func hostPublicKey(keyIn []types.PublicKey) func(*gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		if len(keyIn) > 0 {
			pubKeys := make([]publicKey, len(keyIn))
			for i, pk := range keyIn {
				pubKeys[i] = publicKey(pk)
			}
			return db.Where("public_key IN ?", pubKeys)
		}
		return db
	}
}

// hostFilter can be used as a scope to filter hosts based on their filter mode,
// returning either all, allowed or blocked hosts.
func hostFilter(filterMode string, hasAllowlist, hasBlocklist bool) func(*gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		switch filterMode {
		case api.HostFilterModeAllowed:
			if hasAllowlist {
				db = db.Where("EXISTS (SELECT 1 FROM host_allowlist_entry_hosts hbeh WHERE hbeh.db_host_id = hosts.id)")
			}
			if hasBlocklist {
				db = db.Where("NOT EXISTS (SELECT 1 FROM host_blocklist_entry_hosts hbeh WHERE hbeh.db_host_id = hosts.id)")
			}
		case api.HostFilterModeBlocked:
			if hasAllowlist {
				db = db.Where("NOT EXISTS (SELECT 1 FROM host_allowlist_entry_hosts hbeh WHERE hbeh.db_host_id = hosts.id)")
			}
			if hasBlocklist {
				db = db.Where("EXISTS (SELECT 1 FROM host_blocklist_entry_hosts hbeh WHERE hbeh.db_host_id = hosts.id)")
			}
			if !hasAllowlist && !hasBlocklist {
				// if neither an allowlist nor a blocklist exist, all hosts are allowed
				// which means we return none
				db = db.Where("1 = 0")
			}
		case api.HostFilterModeAll:
			// do nothing
		}
		return db
	}
}

func (ss *SQLStore) isBlocked(h dbHost) (blocked bool) {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	if ss.allowListCnt > 0 && len(h.Allowlist) == 0 {
		blocked = true
	}
	if ss.blockListCnt > 0 && len(h.Blocklist) > 0 {
		blocked = true
	}
	return
}

func updateCCID(tx *gorm.DB, newCCID modules.ConsensusChangeID, newTip types.ChainIndex) error {
	return tx.Model(&dbConsensusInfo{}).Where(&dbConsensusInfo{
		Model: Model{
			ID: consensusInfoID,
		},
	}).Updates(map[string]interface{}{
		"CCID":     newCCID[:],
		"height":   newTip.Height,
		"block_id": hash256(newTip.ID),
	}).Error
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

func applyRevisionUpdate(db *gorm.DB, fcid types.FileContractID, rev revisionUpdate) error {
	return updateActiveAndArchivedContract(db, fcid, map[string]interface{}{
		"revision_height": rev.height,
		"revision_number": fmt.Sprint(rev.number),
		"size":            rev.size,
	})
}

func updateContractState(db *gorm.DB, fcid types.FileContractID, cs contractState) error {
	return updateActiveAndArchivedContract(db, fcid, map[string]interface{}{
		"state": cs,
	})
}

func markFailedContracts(db *gorm.DB, height uint64) error {
	if err := db.Model(&dbContract{}).
		Where("state = ? AND ? > window_end", contractStateActive, height).
		Update("state", contractStateFailed).Error; err != nil {
		return fmt.Errorf("failed to mark failed contracts: %w", err)
	}
	return nil
}

func updateProofHeight(db *gorm.DB, fcid types.FileContractID, blockHeight uint64) error {
	return updateActiveAndArchivedContract(db, fcid, map[string]interface{}{
		"proof_height": blockHeight,
	})
}

func updateActiveAndArchivedContract(tx *gorm.DB, fcid types.FileContractID, updates map[string]interface{}) error {
	err1 := tx.Model(&dbContract{}).
		Where("fcid = ?", fileContractID(fcid)).
		Updates(updates).Error
	err2 := tx.Model(&dbArchivedContract{}).
		Where("fcid = ?", fileContractID(fcid)).
		Updates(updates).Error
	if err1 != nil || err2 != nil {
		return fmt.Errorf("%s; %s", err1, err2)
	}
	return nil
}

func updateBlocklist(tx *gorm.DB, hk types.PublicKey, allowlist []dbAllowlistEntry, blocklist []dbBlocklistEntry) error {
	// fetch the host
	var host dbHost
	if err := tx.
		Model(&dbHost{}).
		Where("public_key = ?", publicKey(hk)).
		First(&host).
		Error; err != nil {
		return err
	}

	// update host allowlist
	var dbAllowlist []dbAllowlistEntry
	for _, entry := range allowlist {
		if entry.Entry == host.PublicKey {
			dbAllowlist = append(dbAllowlist, entry)
		}
	}
	if err := tx.Model(&host).Association("Allowlist").Replace(&dbAllowlist); err != nil {
		return err
	}

	// update host blocklist
	var dbBlocklist []dbBlocklistEntry
	for _, entry := range blocklist {
		if entry.blocks(host) {
			dbBlocklist = append(dbBlocklist, entry)
		}
	}
	return tx.Model(&host).Association("Blocklist").Replace(&dbBlocklist)
}

func (s *SQLStore) ResetLostSectors(ctx context.Context, hk types.PublicKey) error {
	return s.retryTransaction(ctx, func(tx *gorm.DB) error {
		return tx.Model(&dbHost{}).
			Where("public_key", publicKey(hk)).
			Update("lost_sectors", 0).
			Error
	})
}
