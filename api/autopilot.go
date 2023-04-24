package api

import (
	"errors"
	"strings"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/hostdb"
)

const (
	// blocksPerDay defines the amount of blocks that are mined in a day (one
	// block every 10 minutes roughly)
	blocksPerDay = 144
)

var (
	// ErrMaxDowntimeHoursTooHigh is returned if the autopilot config is updated
	// with a value that exceeds the maximum of 99 years.
	ErrMaxDowntimeHoursTooHigh = errors.New("MaxDowntimeHours is too high, exceeds max value of 99 years")
)

type (
	// An Action is an autopilot operation.
	Action struct {
		Timestamp time.Time
		Type      string
		Action    interface{ isAction() }
	}

	// AutopilotConfig contains all autopilot configuration parameters.
	AutopilotConfig struct {
		Wallet    WalletConfig    `json:"wallet"`
		Hosts     HostsConfig     `json:"hosts"`
		Contracts ContractsConfig `json:"contracts"`
	}

	AutopilotTriggeredPOST struct {
		Triggered bool `json:"triggered"`
	}

	// WalletConfig contains all wallet configuration parameters.
	WalletConfig struct {
		DefragThreshold uint64 `json:"defragThreshold"`
	}

	HostHandlerGET struct {
		Host hostdb.Host `json:"host"`

		Gouging          bool                 `json:"gouging"`
		GougingBreakdown HostGougingBreakdown `json:"gougingBreakdown"`
		Score            float64              `json:"score"`
		ScoreBreakdown   HostScoreBreakdown   `json:"scoreBreakdown"`
		Usable           bool                 `json:"usable"`
		UnusableReasons  []string             `json:"unusableReasons"`
	}

	HostGougingBreakdown struct {
		V2 GougingChecks `json:"v2"`
		V3 GougingChecks `json:"v3"`
	}

	GougingChecks struct {
		ContractErr string `json:"contractErr"`
		DownloadErr string `json:"downloadErr"`
		GougingErr  string `json:"gougingErr"`
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

	// HostsConfig contains all hosts configuration parameters.
	HostsConfig struct {
		AllowRedundantIPs bool                        `json:"allowRedundantIPs"`
		MaxDowntimeHours  uint64                      `json:"maxDowntimeHours"`
		ScoreOverrides    map[types.PublicKey]float64 `json:"scoreOverrides"`
	}

	// ContractsConfig contains all contracts configuration parameters.
	ContractsConfig struct {
		Set         string         `json:"set"`
		Amount      uint64         `json:"amount"`
		Allowance   types.Currency `json:"allowance"`
		Period      uint64         `json:"period"`
		RenewWindow uint64         `json:"renewWindow"`
		Download    uint64         `json:"download"`
		Upload      uint64         `json:"upload"`
		Storage     uint64         `json:"storage"`
	}

	// AutopilotStatusResponseGET is the response type for the /autopilot/status
	// endpoint.
	AutopilotStatusResponseGET struct {
		CurrentPeriod uint64 `json:"currentPeriod"`
	}
)

func (hgb HostGougingBreakdown) Gouging() bool {
	return hgb.V2.Gouging() || hgb.V3.Gouging()
}

func (gc GougingChecks) Gouging() bool {
	for _, err := range []string{
		gc.ContractErr,
		gc.DownloadErr,
		gc.GougingErr,
		gc.UploadErr,
	} {
		if err != "" {
			return true
		}
	}
	return false
}

func (gc GougingChecks) Errors() (errs []string) {
	for _, err := range []string{
		gc.ContractErr,
		gc.DownloadErr,
		gc.GougingErr,
		gc.UploadErr,
	} {
		if err != "" {
			errs = append(errs, err)
		}
	}
	return
}

func (hgb HostGougingBreakdown) Reasons() string {
	var reasons []string
	for _, err := range append(hgb.V2.Errors(), hgb.V3.Errors()...) {
		if err != "" {
			reasons = append(reasons, err)
		}
	}
	if len(reasons) == 0 {
		return ""
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

// DefaultAutopilotConfig returns a configuration with sane default values.
func DefaultAutopilotConfig() (c AutopilotConfig) {
	c.Wallet.DefragThreshold = 1000
	c.Hosts.MaxDowntimeHours = 24 * 7 * 2 // 2 weeks
	c.Hosts.ScoreOverrides = make(map[types.PublicKey]float64)
	c.Contracts.Set = "autopilot"
	c.Contracts.Allowance = types.Siacoins(1000)
	c.Contracts.Amount = 50
	c.Contracts.Period = blocksPerDay * 7 * 6      // 6 weeks
	c.Contracts.RenewWindow = blocksPerDay * 7 * 2 // 2 weeks
	c.Contracts.Upload = 1 << 40                   // 1 TiB
	c.Contracts.Download = 1 << 40                 // 1 TiB
	c.Contracts.Storage = 1 << 42                  // 4 TiB
	return
}
