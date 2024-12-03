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
	hosts, err := s.Hosts(ctx, api.HostOptions{
		AddressContains: "",
		FilterMode:      api.HostFilterModeAll,
		UsabilityMode:   api.UsabilityFilterModeAll,
		KeyIn:           []types.PublicKey{hostKey},
		Offset:          0,
		Limit:           1,
	})
	if err != nil {
		return api.Host{}, err
	} else if len(hosts) == 0 {
		return api.Host{}, fmt.Errorf("%w %v", api.ErrHostNotFound, hostKey)
	} else {
		return hosts[0], nil
	}
}

func (s *SQLStore) UpdateHostCheck(ctx context.Context, hk types.PublicKey, hc api.HostChecks) (err error) {
	return s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		return tx.UpdateHostCheck(ctx, hk, hc)
	})
}

func (s *SQLStore) ResetLostSectors(ctx context.Context, hk types.PublicKey) error {
	return s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		return tx.ResetLostSectors(ctx, hk)
	})
}

func (s *SQLStore) Hosts(ctx context.Context, opts api.HostOptions) ([]api.Host, error) {
	var hosts []api.Host
	err := s.db.Transaction(ctx, func(tx sql.DatabaseTx) (err error) {
		hosts, err = tx.Hosts(ctx, opts)
		return
	})
	return hosts, err
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

func (s *SQLStore) UsableHosts(ctx context.Context) (hosts []sql.HostInfo, err error) {
	err = s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		hosts, err = tx.UsableHosts(ctx)
		return err
	})
	return
}
