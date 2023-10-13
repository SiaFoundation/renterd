package stores

import (
	"context"
	"fmt"
	"time"

	"gorm.io/gorm"
)

type (
	// dbContractMetric tracks information about a contract's funds.  It is
	// supposed to be reported by a worker every time a contract is revised.
	dbContractMetric struct {
		Model

		Time unixTimeMS `gorm:"index;NOT NULL"`

		FCID fileContractID `gorm:"index;NOT NULL"`
		Host publicKey      `gorm:"index;NOT NULL"`

		RemainingCollateralLo uint64 `gorm:"index;NOT NULL"`
		RemainingCollateralHi uint64 `gorm:"index;NOT NULL"`
		RemainingFundsLo      uint64 `gorm:"index;NOT NULL"`
		RemainingFundsHi      uint64 `gorm:"index;NOT NULL"`
		RevisionNumber        uint64 `gorm:"index;NOT NULL"`

		UploadSpendingLo      uint64 `gorm:"index;NOT NULL"`
		UploadSpendingHi      uint64 `gorm:"index;NOT NULL"`
		DownloadSpendingLo    uint64 `gorm:"index;NOT NULL"`
		DownloadSpendingHi    uint64 `gorm:"index;NOT NULL"`
		FundAccountSpendingLo uint64 `gorm:"index;NOT NULL"`
		FundAccountSpendingHi uint64 `gorm:"index;NOT NULL"`
		DeleteSpendingLo      uint64 `gorm:"index;NOT NULL"`
		DeleteSpendingHi      uint64 `gorm:"index;NOT NULL"`
		ListSpendingLo        uint64 `gorm:"index;NOT NULL"`
		ListSpendingHi        uint64 `gorm:"index;NOT NULL"`
	}

	// dbContractSetMetric tracks information about a specific contract set.
	// Such as the number of contracts it contains. Intended to be reported by
	// the bus every time the set is updated.
	dbContractSetMetric struct {
		Model
		Time unixTimeMS `gorm:"index;NOT NULL"`

		Name      string `gorm:"index;NOT NULL"`
		Contracts int    `gorm:"index;NOT NULL"`
	}

	// dbContractSetChurnMetric contains information about contracts being added
	// to / removed from a contract set. Expected to be reported by the entity
	// updating the set. e.g. the autopilot.
	dbContractSetChurnMetric struct {
		Model
		Time unixTimeMS `gorm:"index;NOT NULL"`

		Name      string         `gorm:"index;NOT NULL"`
		FCID      fileContractID `gorm:"index;NOT NULL"`
		Direction string         `gorm:"index;NOT NULL"` // "added" or "removed"
		Reason    string         `gorm:"index;NOT NULL"`
	}

	// dbPerformanceMetric is a generic metric used to track the performance of
	// an action. Such an action could be a ReadSector operation. Expected to be
	// reported by workers.
	dbPerformanceMetric struct {
		Model
		Time unixTimeMS `gorm:"index;NOT NULL"`

		Action   string    `gorm:"index;NOT NULL"`
		Host     publicKey `gorm:"index;NOT NULL"`
		Reporter string    `gorm:"index;NOT NULL"`
		Duration float64   `gorm:"index;NOT NULL"`
	}
)

func (dbContractMetric) TableName() string         { return "contracts" }
func (dbContractSetMetric) TableName() string      { return "contract_sets" }
func (dbContractSetChurnMetric) TableName() string { return "contract_sets_churn" }
func (dbPerformanceMetric) TableName() string      { return "performance" }

func (s *SQLStore) RecordContractSetMetric(ctx context.Context, t time.Time, set string, contracts int) error {
	return s.dbMetrics.Create(&dbContractSetMetric{
		Contracts: contracts,
		Name:      set,
		Time:      unixTimeMS(t),
	}).Error
}

type ContractSetMetricsQueryOpts struct {
	Name   *string
	After  *time.Time
	Before *time.Time
}

func scopeTimeRange(tx *gorm.DB, after, before *time.Time) *gorm.DB {
	if after != nil {
		tx = tx.Where("time > ?", unixTimeMS(*after))
	}
	if before != nil {
		tx = tx.Where("time <= ?", unixTimeMS(*before))
	}
	return tx
}

func (s *SQLStore) contractSetMetrics(ctx context.Context, opts ContractSetMetricsQueryOpts) ([]dbContractSetMetric, error) {
	tx := s.dbMetrics
	if opts.Name != nil {
		tx = tx.Where("name = ?", *opts.Name)
	}
	var metrics []dbContractSetMetric
	err := tx.Scopes(func(tx *gorm.DB) *gorm.DB {
		return scopeTimeRange(tx, opts.After, opts.Before)
	}).
		Order("time ASC").
		Find(&metrics).
		Error
	if err != nil {
		return nil, fmt.Errorf("failed to fetch contract set metrics: %w", err)
	}
	return metrics, nil
}
