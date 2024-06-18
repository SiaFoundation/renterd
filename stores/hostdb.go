package stores

import (
	"context"
	dsql "database/sql"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/renterd/api"
	sql "go.sia.tech/renterd/stores/sql"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	// consensusInfoID defines the primary key of the entry in the consensusInfo
	// table.
	consensusInfoID = 1
)

var (
	ErrNegativeMaxDowntime = errors.New("max downtime can not be negative")
)

type (
	// dbHost defines a api.Interaction as persisted in the DB. Deleting a
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
		PriceTableExpiry dsql.NullTime

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
		chain.HostAnnouncement
		blockHeight uint64
		blockID     types.BlockID
		hk          types.PublicKey
		timestamp   time.Time
	}
)

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
	hosts, err := ss.SearchHosts(ctx, "", api.HostFilterModeAll, api.UsabilityFilterModeAll, "", []types.PublicKey{hostKey}, 0, 1)
	if err != nil {
		return api.Host{}, err
	} else if len(hosts) == 0 {
		return api.Host{}, fmt.Errorf("%w %v", api.ErrHostNotFound, hostKey)
	} else {
		return hosts[0], nil
	}
}

func (ss *SQLStore) UpdateHostCheck(ctx context.Context, autopilotID string, hk types.PublicKey, hc api.HostCheck) (err error) {
	return ss.bMain.Transaction(ctx, func(tx sql.DatabaseTx) error {
		return tx.UpdateHostCheck(ctx, autopilotID, hk, hc)
	})
}

// HostsForScanning returns the address of hosts for scanning.
func (ss *SQLStore) HostsForScanning(ctx context.Context, maxLastScan time.Time, offset, limit int) (hosts []api.HostAddress, err error) {
	err = ss.bMain.Transaction(ctx, func(tx sql.DatabaseTx) error {
		hosts, err = tx.HostsForScanning(ctx, maxLastScan, offset, limit)
		return err
	})
	return
}

func (s *SQLStore) ResetLostSectors(ctx context.Context, hk types.PublicKey) error {
	return s.retryTransaction(ctx, func(tx *gorm.DB) error {
		return tx.Model(&dbHost{}).
			Where("public_key", publicKey(hk)).
			Update("lost_sectors", 0).
			Error
	})
}

func (ss *SQLStore) SearchHosts(ctx context.Context, autopilotID, filterMode, usabilityMode, addressContains string, keyIn []types.PublicKey, offset, limit int) ([]api.Host, error) {
	var hosts []api.Host
	err := ss.bMain.Transaction(ctx, func(tx sql.DatabaseTx) (err error) {
		hosts, err = tx.SearchHosts(ctx, autopilotID, filterMode, usabilityMode, addressContains, keyIn, offset, limit)
		return
	})
	return hosts, err
}

// Hosts returns non-blocked hosts at given offset and limit.
func (ss *SQLStore) Hosts(ctx context.Context, offset, limit int) ([]api.Host, error) {
	return ss.SearchHosts(ctx, "", api.HostFilterModeAllowed, api.UsabilityFilterModeAll, "", nil, offset, limit)
}

func (ss *SQLStore) RemoveOfflineHosts(ctx context.Context, minRecentFailures uint64, maxDowntime time.Duration) (removed uint64, err error) {
	// sanity check 'maxDowntime'
	if maxDowntime < 0 {
		return 0, ErrNegativeMaxDowntime
	}
	err = ss.bMain.Transaction(ctx, func(tx sql.DatabaseTx) error {
		n, err := tx.RemoveOfflineHosts(ctx, minRecentFailures, maxDowntime)
		removed = uint64(n)
		return err
	})
	return
}

func (ss *SQLStore) UpdateHostAllowlistEntries(ctx context.Context, add, remove []types.PublicKey, clear bool) (err error) {
	// nothing to do
	if len(add)+len(remove) == 0 && !clear {
		return nil
	}
	return ss.bMain.Transaction(ctx, func(tx sql.DatabaseTx) error {
		return tx.UpdateHostAllowlistEntries(ctx, add, remove, clear)
	})
}

func (ss *SQLStore) UpdateHostBlocklistEntries(ctx context.Context, add, remove []string, clear bool) (err error) {
	// nothing to do
	if len(add)+len(remove) == 0 && !clear {
		return nil
	}
	return ss.bMain.Transaction(ctx, func(tx sql.DatabaseTx) error {
		return tx.UpdateHostBlocklistEntries(ctx, add, remove, clear)
	})
}

func (ss *SQLStore) HostAllowlist(ctx context.Context) (allowlist []types.PublicKey, err error) {
	err = ss.bMain.Transaction(ctx, func(tx sql.DatabaseTx) error {
		allowlist, err = tx.HostAllowlist(ctx)
		return err
	})
	return
}

func (ss *SQLStore) HostBlocklist(ctx context.Context) (blocklist []string, err error) {
	err = ss.bMain.Transaction(ctx, func(tx sql.DatabaseTx) error {
		blocklist, err = tx.HostBlocklist(ctx)
		return err
	})
	return
}

func (ss *SQLStore) RecordHostScans(ctx context.Context, scans []api.HostScan) error {
	return ss.bMain.Transaction(ctx, func(tx sql.DatabaseTx) error {
		return tx.RecordHostScans(ctx, scans)
	})
}

func (ss *SQLStore) RecordPriceTables(ctx context.Context, priceTableUpdate []api.HostPriceTableUpdate) error {
	return ss.bMain.Transaction(ctx, func(tx sql.DatabaseTx) error {
		return tx.RecordPriceTables(ctx, priceTableUpdate)
	})
}

func insertAnnouncements(tx *gorm.DB, as []announcement) error {
	var hosts []dbHost
	var announcements []dbAnnouncement
	for _, a := range as {
		hosts = append(hosts, dbHost{
			PublicKey:        publicKey(a.hk),
			LastAnnouncement: a.timestamp.UTC(),
			NetAddress:       a.NetAddress,
		})
		announcements = append(announcements, dbAnnouncement{
			HostKey:     publicKey(a.hk),
			BlockHeight: a.blockHeight,
			BlockID:     a.blockID.String(),
			NetAddress:  a.NetAddress,
		})
	}
	if err := tx.Create(&announcements).Error; err != nil {
		return err
	}
	return tx.Create(&hosts).Error
}

func getBlocklists(tx *gorm.DB) ([]dbAllowlistEntry, []dbBlocklistEntry, error) {
	var allowlist []dbAllowlistEntry
	if err := tx.
		Model(&dbAllowlistEntry{}).
		Find(&allowlist).
		Error; err != nil {
		return nil, nil, err
	}

	var blocklist []dbBlocklistEntry
	if err := tx.
		Model(&dbBlocklistEntry{}).
		Find(&blocklist).
		Error; err != nil {
		return nil, nil, err
	}

	return allowlist, blocklist, nil
}
