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

	ChurnDirAdded   = "added"
	ChurnDirRemoved = "removed"

	MetricContractPrune    = "contractprune"
	MetricContractSet      = "contractset"
	MetricContractSetChurn = "churn"
	MetricContract         = "contract"
	MetricPerformance      = "performance"
	MetricWallet           = "wallet"
)

type (
	ContractSetMetric struct {
		Contracts int         `json:"contracts"`
		Name      string      `json:"name"`
		Timestamp TimeRFC3339 `json:"timestamp"`
	}

	ContractSetMetricsQueryOpts struct {
		Name string
	}

	ContractSetChurnMetric struct {
		Direction  string               `json:"direction"`
		ContractID types.FileContractID `json:"contractID"`
		Name       string               `json:"name"`
		Reason     string               `json:"reason,omitempty"`
		Timestamp  TimeRFC3339          `json:"timestamp"`
	}

	ContractSetChurnMetricsQueryOpts struct {
		Name      string
		Direction string
		Reason    string
	}

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

	ContractSetChurnMetricRequestPUT struct {
		Metrics []ContractSetChurnMetric `json:"metrics"`
	}

	ContractMetricRequestPUT struct {
		Metrics []ContractMetric `json:"metrics"`
	}
)
