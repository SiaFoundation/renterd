package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	rhp4 "go.sia.tech/renterd/internal/rhp/v4"
)

const (
	ContractFilterModeAll      = "all"
	ContractFilterModeActive   = "active"
	ContractFilterModeArchived = "archived"

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
	// HostsPriceTablesRequest is the request type for the /hosts/pricetables endpoint.
	HostsPriceTablesRequest struct {
		PriceTableUpdates []HostPriceTableUpdate `json:"priceTableUpdates"`
	}

	// HostsRemoveRequest is the request type for the delete /hosts endpoint.
	HostsRemoveRequest struct {
		MaxDowntimeHours           DurationH `json:"maxDowntimeHours"`
		MaxConsecutiveScanFailures uint64    `json:"maxConsecutiveScanFailures"`
	}

	// HostsRequest is the request type for the /api/bus/hosts endpoint.
	HostsRequest struct {
		Offset          int               `json:"offset"`
		Limit           int               `json:"limit"`
		AutopilotID     string            `json:"autopilotID"`
		FilterMode      string            `json:"filterMode"`
		UsabilityMode   string            `json:"usabilityMode"`
		AddressContains string            `json:"addressContains"`
		KeyIn           []types.PublicKey `json:"keyIn"`
		MaxLastScan     TimeRFC3339       `json:"maxLastScan"`
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
	HostOptions struct {
		AutopilotID     string
		AddressContains string
		FilterMode      string
		UsabilityMode   string
		KeyIn           []types.PublicKey
		Limit           int
		MaxLastScan     TimeRFC3339
		Offset          int
	}
)

type (
	Host struct {
		KnownSince        time.Time            `json:"knownSince"`
		LastAnnouncement  time.Time            `json:"lastAnnouncement"`
		PublicKey         types.PublicKey      `json:"publicKey"`
		NetAddress        string               `json:"netAddress"`
		PriceTable        HostPriceTable       `json:"priceTable"`
		Settings          rhpv2.HostSettings   `json:"settings,omitempty"`
		V2Settings        rhp4.HostSettings    `json:"v2Settings,omitempty"`
		Interactions      HostInteractions     `json:"interactions"`
		Scanned           bool                 `json:"scanned"`
		Blocked           bool                 `json:"blocked"`
		Checks            map[string]HostCheck `json:"checks"`
		StoredData        uint64               `json:"storedData"`
		V2SiamuxAddresses []string             `json:"v2SiamuxAddresses"`
	}

	HostInfo struct {
		PublicKey         types.PublicKey `json:"publicKey"`
		SiamuxAddr        string          `json:"siamuxAddr"`
		V2SiamuxAddresses []string        `json:"v2SiamuxAddresses"`
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
		HostKey    types.PublicKey      `json:"hostKey"`
		PriceTable rhpv3.HostPriceTable `json:"priceTable,omitempty"`
		Settings   rhpv2.HostSettings   `json:"settings,omitempty"`
		V2Settings rhp4.HostSettings    `json:"v2Settings,omitempty"`
		Success    bool                 `json:"success"`
		Timestamp  time.Time            `json:"timestamp"`
	}

	HostPriceTable struct {
		rhpv3.HostPriceTable
		Expiry time.Time `json:"expiry"`
	}

	HostPriceTableUpdate struct {
		HostKey    types.PublicKey `json:"hostKey"`
		Success    bool            `json:"success"`
		Timestamp  time.Time       `json:"timestamp"`
		PriceTable HostPriceTable  `json:"priceTable"`
	}

	HostCheck struct {
		GougingBreakdown   HostGougingBreakdown   `json:"gougingBreakdown"`
		ScoreBreakdown     HostScoreBreakdown     `json:"scoreBreakdown"`
		UsabilityBreakdown HostUsabilityBreakdown `json:"usabilityBreakdown"`
	}

	HostGougingBreakdown struct {
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
		LowMaxDuration        bool `json:"lowMaxDuration"`
		LowScore              bool `json:"lowScore"`
		RedundantIP           bool `json:"redundantIP"`
		Gouging               bool `json:"gouging"`
		NotAcceptingContracts bool `json:"notAcceptingContracts"`
		NotAnnounced          bool `json:"notAnnounced"`
		NotCompletingScan     bool `json:"notCompletingScan"`
	}
)

func (hc HostCheck) MarshalJSON() ([]byte, error) {
	type check HostCheck
	return json.Marshal(struct {
		check
		Score  float64 `json:"score"`
		Usable bool    `json:"usable"`
	}{
		check:  check(hc),
		Score:  hc.ScoreBreakdown.Score(),
		Usable: hc.UsabilityBreakdown.IsUsable(),
	})
}

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

func (h Host) IsV2() bool {
	// consider a host to be v2 if it has announced a v2 address
	return len(h.V2SiamuxAddresses) > 0
}

func (h Host) V2SiamuxAddr() string {
	// NOTE: eventually we can improve this by implementing a dialer wrapper that
	// can be created from a slice of addresses and tries them in order. It
	// should also be aware of whether we support v4 or v6 and pick addresses
	// accordingly.
	if len(h.V2SiamuxAddresses) > 0 {
		return h.V2SiamuxAddresses[0]
	}
	return ""
}

func (sb HostScoreBreakdown) String() string {
	return fmt.Sprintf("Age: %v, Col: %v, Int: %v, SR: %v, UT: %v, V: %v, Pr: %v", sb.Age, sb.Collateral, sb.Interactions, sb.StorageRemaining, sb.Uptime, sb.Version, sb.Prices)
}

func (hgb HostGougingBreakdown) Gouging() bool {
	for _, err := range []string{
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
