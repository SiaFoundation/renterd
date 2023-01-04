package api

import (
	"time"

	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/siad/types"
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

	// HostsConfig contains all hosts configuration parameters.
	HostsConfig struct {
		Blacklist          []string                        `json:"blacklist"`
		IgnoreRedundantIPs bool                            `json:"ignoreRedundantIPs"`
		ScoreOverrides     map[consensus.PublicKey]float64 `json:"scoreOverrides"`
		Whitelist          []string                        `json:"whitelist"`
	}

	// ContractsConfig contains all contracts configuration parameters.
	ContractsConfig struct {
		Allowance   types.Currency `json:"allowance"`
		Hosts       uint64         `json:"hosts"`
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

// DefaultAutopilotConfig returns a configuration with sane default values.
func DefaultAutopilotConfig() (c AutopilotConfig) {
	c.Wallet.DefragThreshold = 1000
	c.Hosts.ScoreOverrides = make(map[consensus.PublicKey]float64)
	c.Contracts.Allowance = types.SiacoinPrecision.Mul64(1000)
	c.Contracts.Hosts = 50
	c.Contracts.Period = 144 * 7 * 6      // 6 weeks
	c.Contracts.RenewWindow = 144 * 7 * 2 // 2 weeks
	c.Contracts.Upload = 1 << 40          // 1 TiB
	c.Contracts.Download = 1 << 40        // 1 TiB
	c.Contracts.Storage = 1 << 42         // 4 TiB
	return
}
