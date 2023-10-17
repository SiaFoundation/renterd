package stores

import (
	"context"
	"fmt"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"gorm.io/gorm"
)

type (
	// dbContractMetric tracks information about a contract's funds.  It is
	// supposed to be reported by a worker every time a contract is revised.
	dbContractMetric struct {
		Model

		Time unixTimeMS `gorm:"index;NOT NULL"`

		FCID fileContractID `gorm:"index;size:32;NOT NULL;column:fcid"`
		Host publicKey      `gorm:"index;size:32;NOT NULL"`

		RemainingCollateralLo unsigned64 `gorm:"index;NOT NULL"`
		RemainingCollateralHi unsigned64 `gorm:"index;NOT NULL"`
		RemainingFundsLo      unsigned64 `gorm:"index;NOT NULL"`
		RemainingFundsHi      unsigned64 `gorm:"index;NOT NULL"`
		RevisionNumber        unsigned64 `gorm:"index;NOT NULL"`

		UploadSpendingLo      unsigned64 `gorm:"index;NOT NULL"`
		UploadSpendingHi      unsigned64 `gorm:"index;NOT NULL"`
		DownloadSpendingLo    unsigned64 `gorm:"index;NOT NULL"`
		DownloadSpendingHi    unsigned64 `gorm:"index;NOT NULL"`
		FundAccountSpendingLo unsigned64 `gorm:"index;NOT NULL"`
		FundAccountSpendingHi unsigned64 `gorm:"index;NOT NULL"`
		DeleteSpendingLo      unsigned64 `gorm:"index;NOT NULL"`
		DeleteSpendingHi      unsigned64 `gorm:"index;NOT NULL"`
		ListSpendingLo        unsigned64 `gorm:"index;NOT NULL"`
		ListSpendingHi        unsigned64 `gorm:"index;NOT NULL"`
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
		FCID      fileContractID `gorm:"index;size:32;NOT NULL"`
		Direction string         `gorm:"index;NOT NULL"` // "added" or "removed"
		Reason    string         `gorm:"index;NOT NULL"`
	}

	// dbPerformanceMetric is a generic metric used to track the performance of
	// an action. Such an action could be a ReadSector operation. Expected to be
	// reported by workers.
	dbPerformanceMetric struct {
		Model
		Time unixTimeMS `gorm:"index;NOT NULL"`

		Action   string        `gorm:"index;NOT NULL"`
		Host     publicKey     `gorm:"index;size:32;NOT NULL"`
		Reporter string        `gorm:"index;NOT NULL"`
		Duration time.Duration `gorm:"index;NOT NULL"`
	}
)

func (dbContractMetric) TableName() string         { return "contracts" }
func (dbContractSetMetric) TableName() string      { return "contract_sets" }
func (dbContractSetChurnMetric) TableName() string { return "contract_sets_churn" }
func (dbPerformanceMetric) TableName() string      { return "performance" }

func scopeTimeRange(tx *gorm.DB, after, before time.Time) *gorm.DB {
	if after != (time.Time{}) {
		tx = tx.Where("time > ?", unixTimeMS(after))
	}
	if before != (time.Time{}) {
		tx = tx.Where("time <= ?", unixTimeMS(before))
	}
	return tx
}

func (s *SQLStore) contractSetMetrics(ctx context.Context, opts api.ContractSetMetricsQueryOpts) ([]dbContractSetMetric, error) {
	tx := s.dbMetrics
	if opts.Name != "" {
		tx = tx.Where("name", opts.Name)
	}
	if opts.Offset != 0 {
		tx = tx.Offset(opts.Offset)
	}
	if opts.Limit != 0 {
		tx = tx.Limit(opts.Limit)
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

func (s *SQLStore) ContractSetMetrics(ctx context.Context, opts api.ContractSetMetricsQueryOpts) ([]api.ContractSetMetric, error) {
	metrics, err := s.contractSetMetrics(ctx, opts)
	if err != nil {
		return nil, err
	}
	resp := make([]api.ContractSetMetric, len(metrics))
	for i := range resp {
		resp[i] = api.ContractSetMetric{
			Contracts: metrics[i].Contracts,
			Name:      metrics[i].Name,
			Time:      time.Time(metrics[i].Time).UTC(),
		}
	}
	return resp, nil
}

func (s *SQLStore) RecordContractSetMetric(ctx context.Context, metrics ...api.ContractSetMetric) error {
	dbMetrics := make([]dbContractSetMetric, len(metrics))
	for i, metric := range metrics {
		dbMetrics[i] = dbContractSetMetric{
			Contracts: metric.Contracts,
			Name:      metric.Name,
			Time:      unixTimeMS(metric.Time),
		}
	}
	return s.dbMetrics.Create(&dbMetrics).Error
}

func (s *SQLStore) contractSetChurnMetrics(ctx context.Context, opts api.ContractSetChurnMetricsQueryOpts) ([]dbContractSetChurnMetric, error) {
	tx := s.dbMetrics
	if opts.Name != "" {
		tx = tx.Where("name", opts.Name)
	}
	if opts.Direction != "" {
		tx = tx.Where("direction", opts.Direction)
	}
	if opts.Reason != "" {
		tx = tx.Where("reason", opts.Reason)
	}
	if opts.Offset != 0 {
		tx = tx.Offset(opts.Offset)
	}
	if opts.Limit != 0 {
		tx = tx.Limit(opts.Limit)
	}
	var metrics []dbContractSetChurnMetric
	err := tx.Scopes(func(tx *gorm.DB) *gorm.DB {
		return scopeTimeRange(tx, opts.After, opts.Before)
	}).
		Order("time ASC").
		Find(&metrics).
		Error
	if err != nil {
		return nil, fmt.Errorf("failed to fetch contract set churn metrics: %w", err)
	}
	return metrics, nil
}

func (s *SQLStore) ContractSetChurnMetrics(ctx context.Context, opts api.ContractSetChurnMetricsQueryOpts) ([]api.ContractSetChurnMetric, error) {
	metrics, err := s.contractSetChurnMetrics(ctx, opts)
	if err != nil {
		return nil, err
	}
	resp := make([]api.ContractSetChurnMetric, len(metrics))
	for i := range resp {
		resp[i] = api.ContractSetChurnMetric{
			Direction: metrics[i].Direction,
			FCID:      types.FileContractID(metrics[i].FCID),
			Name:      metrics[i].Name,
			Reason:    metrics[i].Reason,
			Time:      time.Time(metrics[i].Time).UTC(),
		}
	}
	return resp, nil
}

func (s *SQLStore) RecordContractSetChurnMetric(ctx context.Context, metrics ...api.ContractSetChurnMetric) error {
	dbMetrics := make([]dbContractSetChurnMetric, len(metrics))
	for i, metric := range metrics {
		dbMetrics[i] = dbContractSetChurnMetric{
			Direction: string(metric.Direction),
			FCID:      fileContractID(metric.FCID),
			Name:      metric.Name,
			Reason:    metric.Reason,
			Time:      unixTimeMS(metric.Time),
		}
	}
	return s.dbMetrics.Create(dbMetrics).Error
}

func (s *SQLStore) performanceMetrics(ctx context.Context, opts api.PerformanceMetricsQueryOpts) ([]dbPerformanceMetric, error) {
	tx := s.dbMetrics
	if opts.Action != "" {
		tx = tx.Where("action", opts.Action)
	}
	if opts.Host != (types.PublicKey{}) {
		tx = tx.Where("host", publicKey(opts.Host))
	}
	if opts.Reporter != "" {
		tx = tx.Where("reporter", opts.Reporter)
	}
	if opts.Duration != 0 {
		tx = tx.Where("duration", opts.Duration)
	}
	if opts.Offset != 0 {
		tx = tx.Offset(opts.Offset)
	}
	if opts.Limit != 0 {
		tx = tx.Limit(opts.Limit)
	}

	var metrics []dbPerformanceMetric
	err := tx.Scopes(func(tx *gorm.DB) *gorm.DB {
		return scopeTimeRange(tx, opts.After, opts.Before)
	}).
		Order("time ASC").
		Find(&metrics).
		Error
	if err != nil {
		return nil, fmt.Errorf("failed to fetch performance metrics: %w", err)
	}
	return metrics, nil
}

func (s *SQLStore) PerformanceMetrics(ctx context.Context, opts api.PerformanceMetricsQueryOpts) ([]api.PerformanceMetric, error) {
	metrics, err := s.performanceMetrics(ctx, opts)
	if err != nil {
		return nil, err
	}
	resp := make([]api.PerformanceMetric, len(metrics))
	for i := range resp {
		resp[i] = api.PerformanceMetric{
			Action:   metrics[i].Action,
			Host:     types.PublicKey(metrics[i].Host),
			Reporter: metrics[i].Reporter,
			Duration: metrics[i].Duration,
			Time:     time.Time(metrics[i].Time).UTC(),
		}
	}
	return resp, nil
}

func (s *SQLStore) RecordPerformanceMetric(ctx context.Context, metrics ...api.PerformanceMetric) error {
	dbMetrics := make([]dbPerformanceMetric, len(metrics))
	for i, metric := range metrics {
		dbMetrics[i] = dbPerformanceMetric{
			Action:   metric.Action,
			Duration: metric.Duration,
			Host:     publicKey(metric.Host),
			Reporter: metric.Reporter,
			Time:     unixTimeMS(metric.Time),
		}
	}
	return s.dbMetrics.Create(dbMetrics).Error
}

func (s *SQLStore) contractMetrics(ctx context.Context, opts api.ContractMetricsQueryOpts) ([]dbContractMetric, error) {
	tx := s.dbMetrics
	if opts.FCID != (types.FileContractID{}) {
		tx = tx.Where("fcid", fileContractID(opts.FCID))
	}
	if opts.Host != (types.PublicKey{}) {
		tx = tx.Where("host", publicKey(opts.Host))
	}
	if opts.Offset != 0 {
		tx = tx.Offset(opts.Offset)
	}
	if opts.Limit != 0 {
		tx = tx.Limit(opts.Limit)
	}

	var metrics []dbContractMetric
	err := tx.Scopes(func(tx *gorm.DB) *gorm.DB {
		return scopeTimeRange(tx, opts.After, opts.Before)
	}).
		Order("time ASC").
		Find(&metrics).
		Error
	if err != nil {
		return nil, fmt.Errorf("failed to fetch contract metrics: %w", err)
	}
	return metrics, nil
}

func (s *SQLStore) ContractMetrics(ctx context.Context, opts api.ContractMetricsQueryOpts) ([]api.ContractMetric, error) {
	metrics, err := s.contractMetrics(ctx, opts)
	if err != nil {
		return nil, err
	}
	resp := make([]api.ContractMetric, len(metrics))
	toCurr := func(lo, hi unsigned64) types.Currency {
		return types.NewCurrency(uint64(lo), uint64(hi))
	}
	for i := range resp {
		resp[i] = api.ContractMetric{
			Time:                time.Time(metrics[i].Time).UTC(),
			FCID:                types.FileContractID(metrics[i].FCID),
			Host:                types.PublicKey(metrics[i].Host),
			RemainingCollateral: toCurr(metrics[i].RemainingCollateralLo, metrics[i].RemainingCollateralHi),
			RemainingFunds:      toCurr(metrics[i].RemainingFundsLo, metrics[i].RemainingFundsHi),
			RevisionNumber:      uint64(metrics[i].RevisionNumber),
			UploadSpending:      toCurr(metrics[i].UploadSpendingLo, metrics[i].UploadSpendingHi),
			DownloadSpending:    toCurr(metrics[i].DownloadSpendingLo, metrics[i].DownloadSpendingHi),
			FundAccountSpending: toCurr(metrics[i].FundAccountSpendingLo, metrics[i].FundAccountSpendingHi),
			DeleteSpending:      toCurr(metrics[i].DeleteSpendingLo, metrics[i].DeleteSpendingHi),
			ListSpending:        toCurr(metrics[i].ListSpendingLo, metrics[i].ListSpendingHi),
		}
	}
	return resp, nil
}

func (s *SQLStore) RecordContractMetric(ctx context.Context, metrics ...api.ContractMetric) error {
	dbMetrics := make([]dbContractMetric, len(metrics))
	for i, metric := range metrics {
		dbMetrics[i] = dbContractMetric{
			Time:                  unixTimeMS(metric.Time),
			FCID:                  fileContractID(metric.FCID),
			Host:                  publicKey(metric.Host),
			RemainingCollateralLo: unsigned64(metric.RemainingCollateral.Lo),
			RemainingCollateralHi: unsigned64(metric.RemainingCollateral.Hi),
			RemainingFundsLo:      unsigned64(metric.RemainingFunds.Lo),
			RemainingFundsHi:      unsigned64(metric.RemainingFunds.Hi),
			RevisionNumber:        unsigned64(metric.RevisionNumber),
			UploadSpendingLo:      unsigned64(metric.UploadSpending.Lo),
			UploadSpendingHi:      unsigned64(metric.UploadSpending.Hi),
			DownloadSpendingLo:    unsigned64(metric.DownloadSpending.Lo),
			DownloadSpendingHi:    unsigned64(metric.DownloadSpending.Hi),
			FundAccountSpendingLo: unsigned64(metric.FundAccountSpending.Lo),
			FundAccountSpendingHi: unsigned64(metric.FundAccountSpending.Hi),
			DeleteSpendingLo:      unsigned64(metric.DeleteSpending.Lo),
			DeleteSpendingHi:      unsigned64(metric.DeleteSpending.Hi),
			ListSpendingLo:        unsigned64(metric.ListSpending.Lo),
			ListSpendingHi:        unsigned64(metric.ListSpending.Hi),
		}
	}
	return s.dbMetrics.Create(&dbMetrics).Error
}
