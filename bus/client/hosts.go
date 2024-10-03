package client

import (
	"context"
	"fmt"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
)

// Host returns information about a particular host known to the server.
func (c *Client) Host(ctx context.Context, hostKey types.PublicKey) (h api.Host, err error) {
	err = c.c.WithContext(ctx).GET(fmt.Sprintf("/host/%s", hostKey), &h)
	return
}

// Hosts returns all hosts that match certain search criteria.
func (c *Client) Hosts(ctx context.Context, opts api.HostOptions) (hosts []api.Host, err error) {
	err = c.c.WithContext(ctx).POST("/hosts", api.HostsRequest{
		AutopilotID:     opts.AutopilotID,
		Offset:          opts.Offset,
		Limit:           opts.Limit,
		FilterMode:      opts.FilterMode,
		UsabilityMode:   opts.UsabilityMode,
		AddressContains: opts.AddressContains,
		KeyIn:           opts.KeyIn,
		MaxLastScan:     opts.MaxLastScan,
	}, &hosts)
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

// RecordHostInteraction records an interaction for the supplied host.
func (c *Client) RecordHostScans(ctx context.Context, scans []api.HostScan) (err error) {
	err = c.c.WithContext(ctx).POST("/hosts/scans", api.HostsScanRequest{
		Scans: scans,
	}, nil)
	return
}

// RecordHostInteraction records an interaction for the supplied host.
func (c *Client) RecordPriceTables(ctx context.Context, priceTableUpdates []api.HostPriceTableUpdate) (err error) {
	err = c.c.WithContext(ctx).POST("/hosts/pricetables", api.HostsPriceTablesRequest{
		PriceTableUpdates: priceTableUpdates,
	}, nil)
	return
}

// RemoveOfflineHosts removes all hosts that have been offline for longer than the given max downtime.
func (c *Client) RemoveOfflineHosts(ctx context.Context, maxConsecutiveScanFailures uint64, maxDowntime time.Duration) (removed uint64, err error) {
	err = c.c.WithContext(ctx).POST("/hosts/remove", api.HostsRemoveRequest{
		MaxDowntimeHours:           api.DurationH(maxDowntime),
		MaxConsecutiveScanFailures: maxConsecutiveScanFailures,
	}, &removed)
	return
}

// ResetLostSectors resets the lost sector count for a host.
func (c *Client) ResetLostSectors(ctx context.Context, hostKey types.PublicKey) (err error) {
	err = c.c.WithContext(ctx).POST(fmt.Sprintf("/host/%s/resetlostsectors", hostKey), nil, nil)
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

// UpdateHostCheck updates the host with the most recent check performed by the
// autopilot with given id.
func (c *Client) UpdateHostCheck(ctx context.Context, autopilotID string, hostKey types.PublicKey, hostCheck api.HostCheck) (err error) {
	err = c.c.WithContext(ctx).PUT(fmt.Sprintf("/autopilot/%s/host/%s/check", autopilotID, hostKey), hostCheck)
	return
}
