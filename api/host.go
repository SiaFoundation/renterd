package api

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
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

var (
	ErrUsabilityHostBlocked               = errors.New("host is blocked")
	ErrUsabilityHostNotFound              = errors.New("host not found")
	ErrUsabilityHostOffline               = errors.New("host is offline")
	ErrUsabilityHostLowScore              = errors.New("host's score is below minimum")
	ErrUsabilityHostRedundantIP           = errors.New("host has redundant IP")
	ErrUsabilityHostPriceGouging          = errors.New("host is price gouging")
	ErrUsabilityHostNotAcceptingContracts = errors.New("host is not accepting contracts")
	ErrUsabilityHostNotCompletingScan     = errors.New("host is not completing scan")
	ErrUsabilityHostNotAnnounced          = errors.New("host is not announced")
)

type (
	// HostsScanRequest is the request type for the /hosts/scans endpoint.
	HostsScanRequest struct {
		Scans []HostScan `json:"scans"`
	}

	// HostsPriceTablesRequest is the request type for the /hosts/pricetables endpoint.
	HostsPriceTablesRequest struct {
		PriceTableUpdates []HostPriceTableUpdate `json:"priceTableUpdates"`
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
		AutopilotID     string            `json:"autopilotID"`
		FilterMode      string            `json:"filterMode"`
		UsabilityMode   string            `json:"usabilityMode"`
		AddressContains string            `json:"addressContains"`
		KeyIn           []types.PublicKey `json:"keyIn"`
	}

	// HostResponse is the response type for the GET
	// /api/autopilot/host/:hostkey endpoint.
	HostResponse struct {
		Host   Host        `json:"host"`
		Checks *HostChecks `json:"checks,omitempty"`
	}

	HostChecks struct {
		Gouging          bool                 `json:"gouging"`
		GougingBreakdown HostGougingBreakdown `json:"gougingBreakdown"`
		Score            float64              `json:"score"`
		ScoreBreakdown   HostScoreBreakdown   `json:"scoreBreakdown"`
		Usable           bool                 `json:"usable"`
		UnusableReasons  []string             `json:"unusableReasons,omitempty"`
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
		AutopilotID     string
		AddressContains string
		FilterMode      string
		UsabilityMode   string
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
		KnownSince       time.Time            `json:"knownSince"`
		LastAnnouncement time.Time            `json:"lastAnnouncement"`
		PublicKey        types.PublicKey      `json:"publicKey"`
		NetAddress       string               `json:"netAddress"`
		PriceTable       HostPriceTable       `json:"priceTable"`
		Settings         rhpv2.HostSettings   `json:"settings"`
		Interactions     HostInteractions     `json:"interactions"`
		Scanned          bool                 `json:"scanned"`
		Blocked          bool                 `json:"blocked"`
		Checks           map[string]HostCheck `json:"checks"`
		StoredData       uint64               `json:"storedData"`
	}

	HostAddress struct {
		PublicKey  types.PublicKey `json:"publicKey"`
		NetAddress string          `json:"netAddress"`
	}

	HostInteractions struct {
		TotalScans              uint64        `json:"totalScans"`
		LastScan                time.Time     `json:"lastScan"`
		LastScanSuccess         bool          `json:"lastScanSuccess"`
		LostSectors             uint64        `json:"lostSectors"`
		SecondToLastScanSuccess bool          `json:"secondToLastScanSuccess"`
		Uptime                  time.Duration `json:"uptime"`
		Downtime                time.Duration `json:"downtime"`

		SuccessfulInteractions float64 `json:"successfulInteractions"`
		FailedInteractions     float64 `json:"failedInteractions"`
	}

	HostScan struct {
		HostKey    types.PublicKey `json:"hostKey"`
		Success    bool
		Timestamp  time.Time
		Settings   rhpv2.HostSettings
		PriceTable rhpv3.HostPriceTable
	}

	HostPriceTable struct {
		rhpv3.HostPriceTable
		Expiry time.Time `json:"expiry"`
	}

	HostPriceTableUpdate struct {
		HostKey    types.PublicKey `json:"hostKey"`
		Success    bool
		Timestamp  time.Time
		PriceTable HostPriceTable
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

// IsAnnounced returns whether the host has been announced.
func (h Host) IsAnnounced() bool {
	return !h.LastAnnouncement.IsZero()
}

// IsOnline returns whether a host is considered online.
func (h Host) IsOnline() bool {
	if h.Interactions.TotalScans == 0 {
		return false
	} else if h.Interactions.TotalScans == 1 {
		return h.Interactions.LastScanSuccess
	}
	return h.Interactions.LastScanSuccess || h.Interactions.SecondToLastScanSuccess
}

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

func (ub HostUsabilityBreakdown) IsUsable() bool {
	return !ub.Blocked && !ub.Offline && !ub.LowScore && !ub.RedundantIP && !ub.Gouging && !ub.NotAcceptingContracts && !ub.NotAnnounced && !ub.NotCompletingScan
}

func (ub HostUsabilityBreakdown) UnusableReasons() []string {
	var reasons []string
	if ub.Blocked {
		reasons = append(reasons, ErrUsabilityHostBlocked.Error())
	}
	if ub.Offline {
		reasons = append(reasons, ErrUsabilityHostOffline.Error())
	}
	if ub.LowScore {
		reasons = append(reasons, ErrUsabilityHostLowScore.Error())
	}
	if ub.RedundantIP {
		reasons = append(reasons, ErrUsabilityHostRedundantIP.Error())
	}
	if ub.Gouging {
		reasons = append(reasons, ErrUsabilityHostPriceGouging.Error())
	}
	if ub.NotAcceptingContracts {
		reasons = append(reasons, ErrUsabilityHostNotAcceptingContracts.Error())
	}
	if ub.NotAnnounced {
		reasons = append(reasons, ErrUsabilityHostNotAnnounced.Error())
	}
	if ub.NotCompletingScan {
		reasons = append(reasons, ErrUsabilityHostNotCompletingScan.Error())
	}
	return reasons
}
