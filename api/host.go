package api

import (
	"errors"
	"fmt"
	"net/url"
	"strings"

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

	// SearchHostsRequest is the request type for the /api/bus/search/hosts
	// endpoint.
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
		values.Set("lastScan", TimeRFC3339(opts.MaxLastScan).String())
	}
}

type (
	Host struct {
		hostdb.Host
		Blocked bool                 `json:"blocked"`
		Checks  map[string]HostCheck `json:"checks"`
	}

	HostCheck struct {
		Gouging   HostGougingBreakdown   `json:"gouging"`
		Score     HostScoreBreakdown     `json:"score"`
		Usability HostUsabilityBreakdown `json:"usability"`
	}

	HostGougingBreakdown struct {
		ContractErr string `json:"contractErr"`
		DownloadErr string `json:"downloadErr"`
		GougingErr  string `json:"gougingErr"`
		PruneErr    string `json:"pruneErr"`
		UploadErr   string `json:"uploadErr"`
	}

	HostScoreBreakdown struct {
		Age              float64 `json:"age"`
		Collateral       float64 `json:"collateral"`
		Interactions     float64 `json:"interactions"`
		StorageRemaining float64 `json:"storageRemaining"`
		Uptime           float64 `json:"uptime"`
		Version          float64 `json:"version"`
		Prices           float64 `json:"prices"`
	}

	HostUsabilityBreakdown struct {
		Blocked               bool `json:"blocked"`
		Offline               bool `json:"offline"`
		LowScore              bool `json:"lowScore"`
		RedundantIP           bool `json:"redundantIP"`
		Gouging               bool `json:"gouging"`
		NotAcceptingContracts bool `json:"notAcceptingContracts"`
		NotAnnounced          bool `json:"notAnnounced"`
		NotCompletingScan     bool `json:"notCompletingScan"`
	}
)

func (sb HostScoreBreakdown) String() string {
	return fmt.Sprintf("Age: %v, Col: %v, Int: %v, SR: %v, UT: %v, V: %v, Pr: %v", sb.Age, sb.Collateral, sb.Interactions, sb.StorageRemaining, sb.Uptime, sb.Version, sb.Prices)
}

func (hgb HostGougingBreakdown) Gouging() bool {
	for _, err := range []string{
		hgb.ContractErr,
		hgb.DownloadErr,
		hgb.GougingErr,
		hgb.PruneErr,
		hgb.UploadErr,
	} {
		if err != "" {
			return true
		}
	}
	return false
}

func (hgb HostGougingBreakdown) String() string {
	var reasons []string
	for _, errStr := range []string{
		hgb.ContractErr,
		hgb.DownloadErr,
		hgb.GougingErr,
		hgb.PruneErr,
		hgb.UploadErr,
	} {
		if errStr != "" {
			reasons = append(reasons, errStr)
		}
	}
	return strings.Join(reasons, ";")
}

func (sb HostScoreBreakdown) Score() float64 {
	return sb.Age * sb.Collateral * sb.Interactions * sb.StorageRemaining * sb.Uptime * sb.Version * sb.Prices
}
