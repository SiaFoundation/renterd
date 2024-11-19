package api

import (
	"fmt"
	"time"

	"go.sia.tech/core/types"
)

var (
	ErrMaxIntervalsExceeded = fmt.Errorf("max number of intervals exceeds maximum of %v", MetricMaxIntervals)
)

const (
	MetricMaxIntervals = 1000

	MetricContractPrune = "contractprune"
	MetricContract      = "contract"
	MetricPerformance   = "performance"
	MetricWallet        = "wallet"
)

type (
	PerformanceMetricsQueryOpts struct {
		Action  string
		HostKey types.PublicKey
		Origin  string
	}

	ContractMetric struct {
		Timestamp TimeRFC3339 `json:"timestamp"`

		ContractID types.FileContractID `json:"contractID"`
		HostKey    types.PublicKey      `json:"hostKey"`

		RemainingCollateral types.Currency `json:"remainingCollateral"`
		RemainingFunds      types.Currency `json:"remainingFunds"`
		RevisionNumber      uint64         `json:"revisionNumber"`

		DeleteSpending      types.Currency `json:"deleteSpending"`
		FundAccountSpending types.Currency `json:"fundAccountSpending"`
		SectorRootsSpending types.Currency `json:"sectorRootsSpending"`
		UploadSpending      types.Currency `json:"uploadSpending"`
	}

	ContractMetricsQueryOpts struct {
		ContractID types.FileContractID
		HostKey    types.PublicKey
	}

	ContractPruneMetric struct {
		Timestamp TimeRFC3339 `json:"timestamp"`

		ContractID  types.FileContractID `json:"contractID"`
		HostKey     types.PublicKey      `json:"hostKey"`
		HostVersion string               `json:"hostVersion"`

		Pruned    uint64        `json:"pruned"`
		Remaining uint64        `json:"remaining"`
		Duration  time.Duration `json:"duration"`
	}

	ContractPruneMetricsQueryOpts struct {
		ContractID  types.FileContractID
		HostKey     types.PublicKey
		HostVersion string
	}

	WalletMetric struct {
		Timestamp TimeRFC3339 `json:"timestamp"`

		Confirmed   types.Currency `json:"confirmed"`
		Spendable   types.Currency `json:"spendable"`
		Unconfirmed types.Currency `json:"unconfirmed"`
		Immature    types.Currency `json:"immature"`
	}

	WalletMetricsQueryOpts struct{}
)

type (
	ContractPruneMetricRequestPUT struct {
		Metrics []ContractPruneMetric `json:"metrics"`
	}

	ContractMetricRequestPUT struct {
		Metrics []ContractMetric `json:"metrics"`
	}
)
