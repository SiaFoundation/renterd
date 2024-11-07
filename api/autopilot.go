package api

import (
	"errors"
	"fmt"

	"go.sia.tech/renterd/internal/utils"
)

const (
	// BlocksPerDay defines the amount of blocks that are mined in a day (one
	// block every 10 minutes roughly)
	BlocksPerDay = 144
)

var (
	// ErrMaxDowntimeHoursTooHigh is returned if the contracts config is updated
	// with a value that exceeds the maximum of 99 years.
	ErrMaxDowntimeHoursTooHigh = errors.New("MaxDowntimeHours is too high, exceeds max value of 99 years")
)

type (
	// AutopilotConfig contains configuration settings for the autopilot.
	AutopilotConfig struct {
		Enabled   bool            `json:"enabled"`
		Contracts ContractsConfig `json:"contracts"`
		Hosts     HostsConfig     `json:"hosts"`
	}

	// ContractsConfig contains all contract settings used in the autopilot.
	ContractsConfig struct {
		Set         string `json:"set"`
		Amount      uint64 `json:"amount"`
		Period      uint64 `json:"period"`
		RenewWindow uint64 `json:"renewWindow"`
		Download    uint64 `json:"download"`
		Upload      uint64 `json:"upload"`
		Storage     uint64 `json:"storage"`
		Prune       bool   `json:"prune"`
	}

	// HostsConfig contains all hosts settings used in the autopilot.
	HostsConfig struct {
		AllowRedundantIPs          bool   `json:"allowRedundantIPs"`
		MaxDowntimeHours           uint64 `json:"maxDowntimeHours"`
		MinProtocolVersion         string `json:"minProtocolVersion"`
		MaxConsecutiveScanFailures uint64 `json:"maxConsecutiveScanFailures"`
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
		Enabled            bool        `json:"enabled"`
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

	ConfigEvaluationRequest struct {
		AutopilotConfig    AutopilotConfig    `json:"autopilotConfig"`
		GougingSettings    GougingSettings    `json:"gougingSettings"`
		RedundancySettings RedundancySettings `json:"redundancySettings"`
	}

	ConfigRecommendation struct {
		GougingSettings GougingSettings `json:"gougingSettings"`
	}

	// ConfigEvaluationResponse is the response type for /evaluate
	ConfigEvaluationResponse struct {
		Hosts    uint64 `json:"hosts"`
		Usable   uint64 `json:"usable"`
		Unusable struct {
			Blocked uint64 `json:"blocked"`
			Gouging struct {
				Contract uint64 `json:"contract"`
				Download uint64 `json:"download"`
				Gouging  uint64 `json:"gouging"`
				Pruning  uint64 `json:"pruning"`
				Upload   uint64 `json:"upload"`
			} `json:"gouging"`
			NotAcceptingContracts uint64 `json:"notAcceptingContracts"`
			NotScanned            uint64 `json:"notScanned"`
		} `json:"unusable"`
		Recommendation *ConfigRecommendation `json:"recommendation,omitempty"`
	}
)

func (cc ContractsConfig) Validate() error {
	if cc.Amount == 0 {
		return errors.New("amount must be greater than 0")
	} else if cc.Period == 0 {
		return errors.New("period must be greater than 0")
	} else if cc.RenewWindow == 0 {
		return errors.New("renewWindow must be greater than 0")
	}
	return nil
}

func (hc HostsConfig) Validate() error {
	if hc.MaxDowntimeHours > 99*365*24 {
		return ErrMaxDowntimeHoursTooHigh
	} else if hc.MinProtocolVersion != "" && !utils.IsVersion(hc.MinProtocolVersion) {
		return fmt.Errorf("invalid min protocol version '%s'", hc.MinProtocolVersion)
	}
	return nil
}
