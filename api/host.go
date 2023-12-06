package api

import (
	"errors"
	"fmt"
	"net/url"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/hostdb"
)

const (
	HostFilterModeAll     = "all"
	HostFilterModeAllowed = "allowed"
	HostFilterModeBlocked = "blocked"

	UsabilityFilterModeAll      = "all"
	UsabilityFilterModeUsable   = "usable"
	UsabilityFilterModeUnusable = "unusable"
)

var (
	// ErrHostNotFound is returned when a host can't be retrieved from the
	// database.
	ErrHostNotFound = errors.New("host doesn't exist in hostdb")
)

type (
	// HostsScanRequest is the request type for the /hosts/scans endpoint.
	HostsScanRequest struct {
		Scans []hostdb.HostScan `json:"scans"`
	}

	// HostsPriceTablesRequest is the request type for the /hosts/pricetables endpoint.
	HostsPriceTablesRequest struct {
		PriceTableUpdates []hostdb.PriceTableUpdate `json:"priceTableUpdates"`
	}

	// HostsRemoveRequest is the request type for the /hosts/remove endpoint.
	HostsRemoveRequest struct {
		MaxDowntimeHours      DurationH `json:"maxDowntimeHours"`
		MinRecentScanFailures uint64    `json:"minRecentScanFailures"`
	}

	SearchHostsRequest struct {
		Offset          int               `json:"offset"`
		Limit           int               `json:"limit"`
		FilterMode      string            `json:"filterMode"`
		UsabilityMode   string            `json:"usabilityMode"`
		AddressContains string            `json:"addressContains"`
		KeyIn           []types.PublicKey `json:"keyIn"`
	}
)

type (
	// UpdateAllowlistRequest is the request type for /hosts/allowlist endpoint.
	UpdateAllowlistRequest struct {
		Add    []types.PublicKey `json:"add"`
		Remove []types.PublicKey `json:"remove"`
		Clear  bool              `json:"clear"`
	}

	// UpdateBlocklistRequest is the request type for /hosts/blocklist endpoint.
	UpdateBlocklistRequest struct {
		Add    []string `json:"add"`
		Remove []string `json:"remove"`
		Clear  bool     `json:"clear"`
	}
)

// Option types.
type (
	GetHostsOptions struct {
		Offset int
		Limit  int
	}
	HostsForScanningOptions struct {
		MaxLastScan TimeRFC3339
		Limit       int
		Offset      int
	}
	SearchHostOptions struct {
		AddressContains string
		FilterMode      string
		KeyIn           []types.PublicKey
		Limit           int
		Offset          int
	}
)

func DefaultSearchHostOptions() SearchHostOptions {
	return SearchHostOptions{
		Limit:      -1,
		FilterMode: HostFilterModeAll,
	}
}

func (opts GetHostsOptions) Apply(values url.Values) {
	if opts.Offset != 0 {
		values.Set("offset", fmt.Sprint(opts.Offset))
	}
	if opts.Limit != 0 {
		values.Set("limit", fmt.Sprint(opts.Limit))
	}
}

func (opts HostsForScanningOptions) Apply(values url.Values) {
	if opts.Offset != 0 {
		values.Set("offset", fmt.Sprint(opts.Offset))
	}
	if opts.Limit != 0 {
		values.Set("limit", fmt.Sprint(opts.Limit))
	}
	if !opts.MaxLastScan.IsZero() {
		values.Set("maxLastScan", fmt.Sprint(TimeRFC3339(opts.MaxLastScan)))
	}
}
