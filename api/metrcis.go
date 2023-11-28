package api

import (
	"time"

	"go.sia.tech/core/types"
)

const (
	ChurnDirAdded   = "added"
	ChurnDirRemoved = "removed"

	MetricContractPrune    = "contractprune"
	MetricContractSet      = "contractset"
	MetricContractSetChurn = "churn"
	MetricContract         = "contract"
)

type (
	ContractSetMetric struct {
		Contracts int       `json:"contracts"`
		Name      string    `json:"name"`
		Timestamp time.Time `json:"timestamp"`
	}

	ContractSetMetricsQueryOpts struct {
		Name string
	}

	ContractSetChurnMetric struct {
		Direction  string               `json:"direction"`
		ContractID types.FileContractID `json:"contractID"`
		Name       string               `json:"name"`
		Reason     string               `json:"reason,omitempty"`
		Timestamp  time.Time            `json:"timestamp"`
	}

	ContractSetChurnMetricsQueryOpts struct {
		Name      string
		Direction string
		Reason    string
	}

	PerformanceMetric struct {
		Action    string          `json:"action"`
		HostKey   types.PublicKey `json:"hostKey"`
		Origin    string          `json:"origin"`
		Duration  time.Duration   `json:"duration"`
		Timestamp time.Time       `json:"timestamp"`
	}

	PerformanceMetricsQueryOpts struct {
		Action  string
		HostKey types.PublicKey
		Origin  string
	}

	ContractMetric struct {
		Timestamp time.Time `json:"timestamp"`

		ContractID types.FileContractID `json:"contractID"`
		HostKey    types.PublicKey      `json:"hostKey"`

		RemainingCollateral types.Currency `json:"remainingCollateral"`
		RemainingFunds      types.Currency `json:"remainingFunds"`
		RevisionNumber      uint64         `json:"revisionNumber"`

		UploadSpending      types.Currency `json:"uploadSpending"`
		DownloadSpending    types.Currency `json:"downloadSpending"`
		FundAccountSpending types.Currency `json:"fundAccountSpending"`
		DeleteSpending      types.Currency `json:"deleteSpending"`
		ListSpending        types.Currency `json:"listSpending"`
	}

	ContractMetricsQueryOpts struct {
		ContractID types.FileContractID
		HostKey    types.PublicKey
	}

	ContractPruneMetric struct {
		Timestamp time.Time `json:"timestamp"`

		ContractID  types.FileContractID `json:"contractID"`
		HostKey     types.PublicKey      `json:"hostKey"`
		HostVersion string               `json:"hostVersion"`

		Pruned    uint64        `json:"pruned"`
		Remaining uint64        `json:"remaining"`
		Duration  time.Duration `json:"duration"`

		Error string `json:"error,omitempty"`
	}

	ContractPruneMetricsQueryOpts struct {
		ContractID  types.FileContractID
		HostKey     types.PublicKey
		HostVersion string
	}
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
