package client

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/hostdb"
)

// Host returns information about a particular host known to the server.
func (c *Client) Host(ctx context.Context, hostKey types.PublicKey) (h api.Host, err error) {
	err = c.c.WithContext(ctx).GET(fmt.Sprintf("/host/%s", hostKey), &h)
	return
}

// HostAllowlist returns the allowlist.
func (c *Client) HostAllowlist(ctx context.Context) (allowlist []types.PublicKey, err error) {
	err = c.c.WithContext(ctx).GET("/hosts/allowlist", &allowlist)
	return
}

// HostBlocklist returns a host blocklist.
func (c *Client) HostBlocklist(ctx context.Context) (blocklist []string, err error) {
	err = c.c.WithContext(ctx).GET("/hosts/blocklist", &blocklist)
	return
}

// Hosts returns 'limit' hosts at given 'offset'.
func (c *Client) Hosts(ctx context.Context, opts api.GetHostsOptions) (hosts []api.Host, err error) {
	values := url.Values{}
	opts.Apply(values)
	err = c.c.WithContext(ctx).GET("/hosts?"+values.Encode(), &hosts)
	return
}

// HostsForScanning returns 'limit' host addresses at given 'offset' which
// haven't been scanned after lastScan.
func (c *Client) HostsForScanning(ctx context.Context, opts api.HostsForScanningOptions) (hosts []hostdb.HostAddress, err error) {
	values := url.Values{}
	opts.Apply(values)
	err = c.c.WithContext(ctx).GET("/hosts/scanning?"+values.Encode(), &hosts)
	return
}

// RecordHostInteraction records an interaction for the supplied host.
func (c *Client) RecordHostScans(ctx context.Context, scans []hostdb.HostScan) (err error) {
	err = c.c.WithContext(ctx).POST("/hosts/scans", api.HostsScanRequest{
		Scans: scans,
	}, nil)
	return
}

// RecordHostInteraction records an interaction for the supplied host.
func (c *Client) RecordPriceTables(ctx context.Context, priceTableUpdates []hostdb.PriceTableUpdate) (err error) {
	err = c.c.WithContext(ctx).POST("/hosts/pricetables", api.HostsPriceTablesRequest{
		PriceTableUpdates: priceTableUpdates,
	}, nil)
	return
}

// RemoveOfflineHosts removes all hosts that have been offline for longer than the given max downtime.
func (c *Client) RemoveOfflineHosts(ctx context.Context, minRecentScanFailures uint64, maxDowntime time.Duration) (removed uint64, err error) {
	err = c.c.WithContext(ctx).POST("/hosts/remove", api.HostsRemoveRequest{
		MaxDowntimeHours:      api.DurationH(maxDowntime),
		MinRecentScanFailures: minRecentScanFailures,
	}, &removed)
	return
}

// ResetLostSectors resets the lost sector count for a host.
func (c *Client) ResetLostSectors(ctx context.Context, hostKey types.PublicKey) (err error) {
	err = c.c.WithContext(ctx).POST(fmt.Sprintf("/host/%s/resetlostsectors", hostKey), nil, nil)
	return
}

// SearchHosts returns all hosts that match certain search criteria.
func (c *Client) SearchHosts(ctx context.Context, opts api.SearchHostOptions) (hosts []api.Host, err error) {
	err = c.c.WithContext(ctx).POST("/search/hosts", api.SearchHostsRequest{
		Offset:          opts.Offset,
		Limit:           opts.Limit,
		FilterMode:      opts.FilterMode,
		AddressContains: opts.AddressContains,
		KeyIn:           opts.KeyIn,
	}, &hosts)
	return
}

// UpdateHostAllowlist updates the host allowlist, adding and removing the given entries.
func (c *Client) UpdateHostAllowlist(ctx context.Context, add, remove []types.PublicKey, clear bool) (err error) {
	err = c.c.WithContext(ctx).PUT("/hosts/allowlist", api.UpdateAllowlistRequest{Add: add, Remove: remove, Clear: clear})
	return
}

// UpdateHostBlocklist updates the host blocklist, adding and removing the given entries.
func (c *Client) UpdateHostBlocklist(ctx context.Context, add, remove []string, clear bool) (err error) {
	err = c.c.WithContext(ctx).PUT("/hosts/blocklist", api.UpdateBlocklistRequest{Add: add, Remove: remove, Clear: clear})
	return
}
