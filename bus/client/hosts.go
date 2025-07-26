package client

import (
	"context"
	"fmt"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/v2/api"
)

// Host returns information about a particular host known to the server.
func (c *Client) Host(ctx context.Context, hostKey types.PublicKey) (h api.Host, err error) {
	err = c.c.GET(ctx, fmt.Sprintf("/host/%s", hostKey), &h)
	return
}

// Hosts returns all hosts that match certain search criteria.
func (c *Client) Hosts(ctx context.Context, opts api.HostOptions) (hosts []api.Host, err error) {
	err = c.c.POST(ctx, "/hosts", api.HostsRequest{
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
	err = c.c.GET(ctx, "/hosts/allowlist", &allowlist)
	return
}

// HostBlocklist returns a host blocklist.
func (c *Client) HostBlocklist(ctx context.Context) (blocklist []string, err error) {
	err = c.c.GET(ctx, "/hosts/blocklist", &blocklist)
	return
}

// RemoveOfflineHosts removes all hosts that have been offline for longer than the given max downtime.
func (c *Client) RemoveOfflineHosts(ctx context.Context, maxConsecutiveScanFailures uint64, maxDowntime time.Duration) (removed uint64, err error) {
	err = c.c.POST(ctx, "/hosts/remove", api.HostsRemoveRequest{
		MaxDowntimeHours:           api.DurationH(maxDowntime),
		MaxConsecutiveScanFailures: maxConsecutiveScanFailures,
	}, &removed)
	return
}

// ResetLostSectors resets the lost sector count for a host.
func (c *Client) ResetLostSectors(ctx context.Context, hostKey types.PublicKey) (err error) {
	err = c.c.POST(ctx, fmt.Sprintf("/host/%s/resetlostsectors", hostKey), nil, nil)
	return
}

// UpdateHostAllowlist updates the host allowlist, adding and removing the given entries.
func (c *Client) UpdateHostAllowlist(ctx context.Context, add, remove []types.PublicKey, empty bool) (err error) {
	err = c.c.PUT(ctx, "/hosts/allowlist", api.UpdateAllowlistRequest{Add: add, Remove: remove, Clear: empty})
	return
}

// UpdateHostBlocklist updates the host blocklist, adding and removing the given entries.
func (c *Client) UpdateHostBlocklist(ctx context.Context, add, remove []string, empty bool) (err error) {
	err = c.c.PUT(ctx, "/hosts/blocklist", api.UpdateBlocklistRequest{Add: add, Remove: remove, Clear: empty})
	return
}

// UpdateHostCheck updates the host with the most recent check performed by the
// autopilot with given id.
func (c *Client) UpdateHostCheck(ctx context.Context, hostKey types.PublicKey, hostCheck api.HostChecks) (err error) {
	err = c.c.PUT(ctx, fmt.Sprintf("/host/%s/check", hostKey), hostCheck)
	return
}

// UsableHosts returns a list of hosts that are ready to be used. That means
// they are deemed usable by the autopilot, they are not gouging, not blocked,
// not offline, etc.
func (c *Client) UsableHosts(ctx context.Context) (hosts []api.HostInfo, err error) {
	err = c.c.GET(ctx, "/hosts", &hosts)
	return
}
