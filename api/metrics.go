package api

import (
	"fmt"
	"strings"
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
	MetricSlab             = "slab"
	MetricWallet           = "wallet"
)

type SlabAction uint8

const (
	SlabActionUnknown SlabAction = iota
	SlabActionUpload
	SlabActionDownload
	SlabActionMigrate
)

func (sa SlabAction) String() string {
	switch sa {
	case SlabActionUpload:
		return "upload"
	case SlabActionDownload:
		return "download"
	case SlabActionMigrate:
		return "migrate"
	default:
		panic("unknown action") // developer error
	}
}

func ParseSlabAction(s string) SlabAction {
	switch strings.ToLower(s) {
	case "upload":
		return SlabActionUpload
	case "download":
		return SlabActionDownload
	case "migrate":
		return SlabActionMigrate
	default:
		return SlabActionUnknown
	}
}

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

	PerformanceMetric struct {
		Action    string          `json:"action"`
		HostKey   types.PublicKey `json:"hostKey"`
		Origin    string          `json:"origin"`
		Duration  time.Duration   `json:"duration"`
		Timestamp TimeRFC3339     `json:"timestamp"`
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
	}

	WalletMetricsQueryOpts struct{}

	SlabMetric struct {
		Timestamp TimeRFC3339 `json:"timestamp"`

		Action          SlabAction `json:"action"`
		SpeedBytesPerMS uint64     `json:"speedBytesPerMs"`

		MinShards    uint8  `json:"minShards"`
		TotalShards  uint8  `json:"totalShards"`
		NumMigrated  uint8  `json:"numMigrated"`
		NumOverdrive uint64 `json:"numOverdrive"`
	}

	SlabMetricsQueryOpts struct {
		Action string
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

	PerformanceMetricRequestPUT struct {
		Metrics []PerformanceMetric `json:"metrics"`
	}

	SlabMetricRequestPUT struct {
		Metrics []SlabMetric `json:"metrics"`
	}
)
