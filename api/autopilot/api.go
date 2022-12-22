package autopilot

import (
	"time"

	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/siad/types"
)

type (
	WalletConfig struct {
		DefragThreshold uint64 `json:"defragThreshold"`
	}
	HostsConfig struct {
		Blacklist          []string                        `json:"blacklist"`
		IgnoreRedundantIPs bool                            `json:"ignoreRedundantIPs"`
		ScoreOverrides     map[consensus.PublicKey]float64 `json:"scoreOverrides"`
		Whitelist          []string                        `json:"whitelist"`
	}
	ContractsConfig struct {
		Allowance   types.Currency `json:"allowance"`
		Hosts       uint64         `json:"hosts"`
		Period      uint64         `json:"period"`
		RenewWindow uint64         `json:"renewWindow"`
		Download    uint64         `json:"download"`
		Upload      uint64         `json:"upload"`
		Storage     uint64         `json:"storage"`
	}
)

type AutopilotStatusResponseGET struct {
	CurrentPeriod uint64 `json:"currentPeriod"`
}

// Config contains all autopilot configuration parameters.
type Config struct {
	Wallet    WalletConfig    `json:"wallet"`
	Hosts     HostsConfig     `json:"hosts"`
	Contracts ContractsConfig `json:"contracts"`
}

func DefaultConfig() (c Config) {
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

// An Action is an autopilot operation.
type Action struct {
	Timestamp time.Time
	Type      string
	Action    interface{ isAction() }
}
