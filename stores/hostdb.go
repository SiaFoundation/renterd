package stores

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	sql "go.sia.tech/renterd/stores/sql"
)

var (
	ErrNegativeMaxDowntime = errors.New("max downtime can not be negative")
)

// Host returns information about a host.
func (s *SQLStore) Host(ctx context.Context, hostKey types.PublicKey) (api.Host, error) {
	hosts, err := s.SearchHosts(ctx, "", api.HostFilterModeAll, api.UsabilityFilterModeAll, "", []types.PublicKey{hostKey}, 0, 1)
	if err != nil {
		return api.Host{}, err
	} else if len(hosts) == 0 {
		return api.Host{}, fmt.Errorf("%w %v", api.ErrHostNotFound, hostKey)
	} else {
		return hosts[0], nil
	}
}

func (s *SQLStore) UpdateHostCheck(ctx context.Context, autopilotID string, hk types.PublicKey, hc api.HostCheck) (err error) {
	return s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		return tx.UpdateHostCheck(ctx, autopilotID, hk, hc)
	})
}

// HostsForScanning returns the address of hosts for scanning.
func (s *SQLStore) HostsForScanning(ctx context.Context, maxLastScan time.Time, offset, limit int) (hosts []api.HostAddress, err error) {
	err = s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		hosts, err = tx.HostsForScanning(ctx, maxLastScan, offset, limit)
		return err
	})
	return
}

func (s *SQLStore) ResetLostSectors(ctx context.Context, hk types.PublicKey) error {
	return s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		return tx.ResetLostSectors(ctx, hk)
	})
}

func (s *SQLStore) SearchHosts(ctx context.Context, autopilotID, filterMode, usabilityMode, addressContains string, keyIn []types.PublicKey, offset, limit int) ([]api.Host, error) {
	var hosts []api.Host
	err := s.db.Transaction(ctx, func(tx sql.DatabaseTx) (err error) {
		hosts, err = tx.SearchHosts(ctx, autopilotID, filterMode, usabilityMode, addressContains, keyIn, offset, limit)
		return
	})
	return hosts, err
}

// Hosts returns non-blocked hosts at given offset and limit.
func (s *SQLStore) Hosts(ctx context.Context, offset, limit int) ([]api.Host, error) {
	return s.SearchHosts(ctx, "", api.HostFilterModeAllowed, api.UsabilityFilterModeAll, "", nil, offset, limit)
}

func (s *SQLStore) RemoveOfflineHosts(ctx context.Context, minRecentFailures uint64, maxDowntime time.Duration) (removed uint64, err error) {
	// sanity check 'maxDowntime'
	if maxDowntime < 0 {
		return 0, ErrNegativeMaxDowntime
	}
	err = s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		n, err := tx.RemoveOfflineHosts(ctx, minRecentFailures, maxDowntime)
		removed = uint64(n)
		return err
	})
	return
}

func (s *SQLStore) UpdateHostAllowlistEntries(ctx context.Context, add, remove []types.PublicKey, clear bool) (err error) {
	// nothing to do
	if len(add)+len(remove) == 0 && !clear {
		return nil
	}
	return s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		return tx.UpdateHostAllowlistEntries(ctx, add, remove, clear)
	})
}

func (s *SQLStore) UpdateHostBlocklistEntries(ctx context.Context, add, remove []string, clear bool) (err error) {
	// nothing to do
	if len(add)+len(remove) == 0 && !clear {
		return nil
	}
	return s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		return tx.UpdateHostBlocklistEntries(ctx, add, remove, clear)
	})
}

func (s *SQLStore) HostAllowlist(ctx context.Context) (allowlist []types.PublicKey, err error) {
	err = s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		allowlist, err = tx.HostAllowlist(ctx)
		return err
	})
	return
}

func (s *SQLStore) HostBlocklist(ctx context.Context) (blocklist []string, err error) {
	err = s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		blocklist, err = tx.HostBlocklist(ctx)
		return err
	})
	return
}

func (s *SQLStore) RecordHostScans(ctx context.Context, scans []api.HostScan) error {
	return s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		return tx.RecordHostScans(ctx, scans)
	})
}

func (s *SQLStore) RecordPriceTables(ctx context.Context, priceTableUpdate []api.HostPriceTableUpdate) error {
	return s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		return tx.RecordPriceTables(ctx, priceTableUpdate)
	})
}
