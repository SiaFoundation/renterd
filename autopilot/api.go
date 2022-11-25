package autopilot

import (
	"time"

	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/siad/types"
)

// Config contains all autopilot configuration parameters.
type Config struct {
	Wallet struct {
		DefragThreshold uint64
	}
	Hosts struct {
		Whitelist      []string
		Blacklist      []string
		ScoreOverrides map[consensus.PublicKey]float64
	}
	Contracts struct {
		Allowance   types.Currency
		Hosts       uint64
		Period      uint64
		RenewWindow uint64
		Download    uint64
		Upload      uint64
		Storage     uint64
	}
	Objects struct {
		MinShards   uint8
		TotalShards uint8
	}
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
	c.Objects.MinShards = 10
	c.Objects.TotalShards = 30
	return
}

// An Action is an autopilot operation.
type Action struct {
	Timestamp time.Time
	Type      string
	Action    interface{ isAction() }
}

// for encoding/decoding time.Time values in API params
type paramTime time.Time

func (t paramTime) String() string                { return (time.Time)(t).Format(time.RFC3339) }
func (t *paramTime) UnmarshalText(b []byte) error { return (*time.Time)(t).UnmarshalText(b) }
