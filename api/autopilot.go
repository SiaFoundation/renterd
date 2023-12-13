package api

import (
	"errors"
	"fmt"
	"strings"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/hostdb"
)

const (
	// BlocksPerDay defines the amount of blocks that are mined in a day (one
	// block every 10 minutes roughly)
	BlocksPerDay = 144

	// DefaultAutopilotID is the id of the autopilot.
	DefaultAutopilotID = "autopilot"
)

var (
	// ErrAutopilotNotFound is returned when an autopilot can't be found.
	ErrAutopilotNotFound = errors.New("couldn't find autopilot")

	// ErrMaxDowntimeHoursTooHigh is returned if the autopilot config is updated
	// with a value that exceeds the maximum of 99 years.
	ErrMaxDowntimeHoursTooHigh = errors.New("MaxDowntimeHours is too high, exceeds max value of 99 years")
)

type (
	// Autopilot contains the autopilot's config and current period.
	Autopilot struct {
		ID            string          `json:"id"`
		Config        AutopilotConfig `json:"config"`
		CurrentPeriod uint64          `json:"currentPeriod"`
	}

	// AutopilotConfig contains all autopilot configuration.
	AutopilotConfig struct {
		Contracts ContractsConfig `json:"contracts"`
		Hosts     HostsConfig     `json:"hosts"`
		Wallet    WalletConfig    `json:"wallet"`
	}

	// ContractsConfig contains all contract settings used in the autopilot.
	ContractsConfig struct {
		Set         string         `json:"set"`
		Amount      uint64         `json:"amount"`
		Allowance   types.Currency `json:"allowance"`
		Period      uint64         `json:"period"`
		RenewWindow uint64         `json:"renewWindow"`
		Download    uint64         `json:"download"`
		Upload      uint64         `json:"upload"`
		Storage     uint64         `json:"storage"`
		Prune       bool           `json:"prune"`
	}

	// HostsConfig contains all hosts settings used in the autopilot.
	HostsConfig struct {
		AllowRedundantIPs     bool                        `json:"allowRedundantIPs"`
		MaxDowntimeHours      uint64                      `json:"maxDowntimeHours"`
		MinRecentScanFailures uint64                      `json:"minRecentScanFailures"`
		ScoreOverrides        map[types.PublicKey]float64 `json:"scoreOverrides"`
	}

	// WalletConfig contains all wallet settings used in the autopilot.
	WalletConfig struct {
		DefragThreshold uint64 `json:"defragThreshold"`
	}
)

type (
	// AutopilotTriggerRequest is the request object used by the /trigger
	// endpoint
	AutopilotTriggerRequest struct {
		ForceScan bool `json:"forceScan"`
	}

	// AutopilotTriggerResponse is the response returned by the /trigger
	// endpoint, indicating whether an autopilot loop was triggered.
	AutopilotTriggerResponse struct {
		Triggered bool `json:"triggered"`
	}

	// AutopilotStateResponse is the response type for the /autopilot/state
	// endpoint.
	AutopilotStateResponse struct {
		Configured         bool        `json:"configured"`
		Migrating          bool        `json:"migrating"`
		MigratingLastStart TimeRFC3339 `json:"migratingLastStart"`
		Pruning            bool        `json:"pruning"`
		PruningLastStart   TimeRFC3339 `json:"pruningLastStart"`
		Scanning           bool        `json:"scanning"`
		ScanningLastStart  TimeRFC3339 `json:"scanningLastStart"`
		UptimeMS           DurationMS  `json:"uptimeMs"`

		StartTime TimeRFC3339 `json:"startTime"`
		BuildState
	}
)

type (
	// HostHandlerResponse is the response type for the /host/:hostkey endpoint.
	HostHandlerResponse struct {
		Host   hostdb.Host                `json:"host"`
		Checks *HostHandlerResponseChecks `json:"checks,omitempty"`
	}

	HostHandlerResponseChecks struct {
		Gouging          bool                 `json:"gouging"`
		GougingBreakdown HostGougingBreakdown `json:"gougingBreakdown"`
		Score            float64              `json:"score"`
		ScoreBreakdown   HostScoreBreakdown   `json:"scoreBreakdown"`
		Usable           bool                 `json:"usable"`
		UnusableReasons  []string             `json:"unusableReasons"`
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

func (c AutopilotConfig) Validate() error {
	if c.Hosts.MaxDowntimeHours > 99*365*24 {
		return ErrMaxDowntimeHoursTooHigh
	}
	return nil
}
