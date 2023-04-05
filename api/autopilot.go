package api

import (
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

	// WalletConfig contains all wallet configuration parameters.
	WalletConfig struct {
		DefragThreshold uint64 `json:"defragThreshold"`
	}

	HostHandlerGET struct {
		Host hostdb.Host `json:"host"`

		Gouging          bool               `json:"gouging"`
		GougingBreakdown GougingBreakdown   `json:"gougingBreakdown"`
		Score            float64            `json:"score"`
		ScoreBreakdown   HostScoreBreakdown `json:"scoreBreakdown"`
		Usable           bool               `json:"usable"`
		UnusableReasons  []string           `json:"unusableReasons"`
	}

	GougingBreakdown struct {
		RHPv2ContractErr error `json:"rhpv2ContractErr"`
		RHPv2DownloadErr error `json:"rhpv2DownloadErr"`
		RHPv2GougingErr  error `json:"rhpv2GougingErr"`
		RHPv2UploadErr   error `json:"rhpv2UploadErr"`

		RHPv3ContractErr error `json:"rhpv3ContractErr"`
		RHPv3DownloadErr error `json:"rhpv3DownloadErr"`
		RHPv3GougingErr  error `json:"rhpv3GougingErr"`
		RHPv3UploadErr   error `json:"rhpv3UploadErr"`
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
		IgnoreRedundantIPs bool                        `json:"ignoreRedundantIPs"`
		MaxDowntimeHours   uint64                      `json:"maxDowntimeHours"`
		ScoreOverrides     map[types.PublicKey]float64 `json:"scoreOverrides"`
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

func (gb GougingBreakdown) Gouging() bool {
	for _, err := range []error{
		gb.RHPv2ContractErr,
		gb.RHPv2DownloadErr,
		gb.RHPv2GougingErr,
		gb.RHPv2UploadErr,
		gb.RHPv3ContractErr,
		gb.RHPv3DownloadErr,
		gb.RHPv3GougingErr,
		gb.RHPv3UploadErr,
	} {
		if err != nil {
			return true
		}
	}
	return false
}

func (gb GougingBreakdown) Reasons() string {
	var reasons []string
	for _, err := range []error{
		gb.RHPv2ContractErr,
		gb.RHPv2DownloadErr,
		gb.RHPv2GougingErr,
		gb.RHPv2UploadErr,
		gb.RHPv3ContractErr,
		gb.RHPv3DownloadErr,
		gb.RHPv3GougingErr,
		gb.RHPv3UploadErr,
	} {
		if err != nil {
			reasons = append(reasons, err.Error())
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
