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

		Timestamp unixTimeMS `gorm:"index;NOT NULL"`

		FCID fileContractID `gorm:"index;size:32;NOT NULL;column:fcid"`
		Host publicKey      `gorm:"index;size:32;NOT NULL"`

		RemainingCollateralLo unsigned64 `gorm:"index:idx_remaining_collateral;NOT NULL"`
		RemainingCollateralHi unsigned64 `gorm:"index:idx_remaining_collateral;NOT NULL"`
		RemainingFundsLo      unsigned64 `gorm:"index:idx_remaining_funds;NOT NULL"`
		RemainingFundsHi      unsigned64 `gorm:"index:idx_remaining_funds;NOT NULL"`
		RevisionNumber        unsigned64 `gorm:"index;NOT NULL"`

		UploadSpendingLo      unsigned64 `gorm:"index:idx_upload_spending;NOT NULL"`
		UploadSpendingHi      unsigned64 `gorm:"index:idx_upload_spending;NOT NULL"`
		DownloadSpendingLo    unsigned64 `gorm:"index:idx_download_spending;NOT NULL"`
		DownloadSpendingHi    unsigned64 `gorm:"index:idx_download_spending;NOT NULL"`
		FundAccountSpendingLo unsigned64 `gorm:"index:idx_fund_account_spending;NOT NULL"`
		FundAccountSpendingHi unsigned64 `gorm:"index:idx_fund_account_spending;NOT NULL"`
		DeleteSpendingLo      unsigned64 `gorm:"index:idx_delete_spending;NOT NULL"`
		DeleteSpendingHi      unsigned64 `gorm:"index:idx_delete_spending;NOT NULL"`
		ListSpendingLo        unsigned64 `gorm:"index:idx_list_spending;NOT NULL"`
		ListSpendingHi        unsigned64 `gorm:"index:idx_list_spending;NOT NULL"`
	}

	// dbContractSetMetric tracks information about a specific contract set.
	// Such as the number of contracts it contains. Intended to be reported by
	// the bus every time the set is updated.
	dbContractSetMetric struct {
		Model
		Timestamp unixTimeMS `gorm:"index;NOT NULL"`

		Name      string `gorm:"index;NOT NULL"`
		Contracts int    `gorm:"index;NOT NULL"`
	}

	// dbContractSetChurnMetric contains information about contracts being added
	// to / removed from a contract set. Expected to be reported by the entity
	// updating the set. e.g. the autopilot.
	dbContractSetChurnMetric struct {
		Model
		Timestamp unixTimeMS `gorm:"index;NOT NULL"`

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
		Timestamp unixTimeMS `gorm:"index;NOT NULL"`

		Action   string        `gorm:"index;NOT NULL"`
		Host     publicKey     `gorm:"index;size:32;NOT NULL"`
		Origin   string        `gorm:"index;NOT NULL"`
		Duration time.Duration `gorm:"index;NOT NULL"`
	}

	// dbWalletMetric tracks information about a specific wallet.
	dbWalletMetric struct {
		Model
		Timestamp unixTimeMS `gorm:"index;NOT NULL"`

		ConfirmedLo   unsigned64 `gorm:"index:idx_confirmed;NOT NULL"`
		ConfirmedHi   unsigned64 `gorm:"index:idx_confirmed;NOT NULL"`
		SpendableLo   unsigned64 `gorm:"index:idx_spendable;NOT NULL"`
		SpendableHi   unsigned64 `gorm:"index:idx_spendable;NOT NULL"`
		UnconfirmedLo unsigned64 `gorm:"index:idx_unconfirmed;NOT NULL"`
		UnconfirmedHi unsigned64 `gorm:"index:idx_unconfirmed;NOT NULL"`
	}
)

func (dbContractMetric) TableName() string         { return "contracts" }
func (dbContractSetMetric) TableName() string      { return "contract_sets" }
func (dbContractSetChurnMetric) TableName() string { return "contract_sets_churn" }
func (dbPerformanceMetric) TableName() string      { return "performance" }
func (dbWalletMetric) TableName() string           { return "wallets" }

func (s *SQLStore) ContractMetrics(ctx context.Context, start time.Time, n uint64, interval time.Duration, opts api.ContractMetricsQueryOpts) ([]api.ContractMetric, error) {
	metrics, err := s.contractMetrics(ctx, start, n, interval, opts)
	if err != nil {
		return nil, err
	}
	resp := make([]api.ContractMetric, len(metrics))
	toCurr := func(lo, hi unsigned64) types.Currency {
		return types.NewCurrency(uint64(lo), uint64(hi))
	}
	for i := range resp {
		resp[i] = api.ContractMetric{
			Timestamp:           api.TimeRFC3339(time.Time(metrics[i].Timestamp).UTC()),
			ContractID:          types.FileContractID(metrics[i].FCID),
			HostKey:             types.PublicKey(metrics[i].Host),
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

func (s *SQLStore) ContractSetChurnMetrics(ctx context.Context, start time.Time, n uint64, interval time.Duration, opts api.ContractSetChurnMetricsQueryOpts) ([]api.ContractSetChurnMetric, error) {
	metrics, err := s.contractSetChurnMetrics(ctx, start, n, interval, opts)
	if err != nil {
		return nil, err
	}
	resp := make([]api.ContractSetChurnMetric, len(metrics))
	for i := range resp {
		resp[i] = api.ContractSetChurnMetric{
			Direction:  metrics[i].Direction,
			ContractID: types.FileContractID(metrics[i].FCID),
			Name:       metrics[i].Name,
			Reason:     metrics[i].Reason,
			Timestamp:  api.TimeRFC3339(time.Time(metrics[i].Timestamp).UTC()),
		}
	}
	return resp, nil
}

func (s *SQLStore) ContractSetMetrics(ctx context.Context, start time.Time, n uint64, interval time.Duration, opts api.ContractSetMetricsQueryOpts) ([]api.ContractSetMetric, error) {
	metrics, err := s.contractSetMetrics(ctx, start, n, interval, opts)
	if err != nil {
		return nil, err
	}
	resp := make([]api.ContractSetMetric, len(metrics))
	for i := range resp {
		resp[i] = api.ContractSetMetric{
			Contracts: metrics[i].Contracts,
			Name:      metrics[i].Name,
			Timestamp: api.TimeRFC3339(time.Time(metrics[i].Timestamp).UTC()),
		}
	}
	return resp, nil
}

func (s *SQLStore) PerformanceMetrics(ctx context.Context, start time.Time, n uint64, interval time.Duration, opts api.PerformanceMetricsQueryOpts) ([]api.PerformanceMetric, error) {
	metrics, err := s.performanceMetrics(ctx, start, n, interval, opts)
	if err != nil {
		return nil, err
	}
	resp := make([]api.PerformanceMetric, len(metrics))
	for i := range resp {
		resp[i] = api.PerformanceMetric{
			Action:    metrics[i].Action,
			HostKey:   types.PublicKey(metrics[i].Host),
			Origin:    metrics[i].Origin,
			Duration:  metrics[i].Duration,
			Timestamp: api.TimeRFC3339(time.Time(metrics[i].Timestamp).UTC()),
		}
	}
	return resp, nil
}

func (s *SQLStore) RecordContractMetric(ctx context.Context, metrics ...api.ContractMetric) error {
	dbMetrics := make([]dbContractMetric, len(metrics))
	for i, metric := range metrics {
		dbMetrics[i] = dbContractMetric{
			Timestamp:             unixTimeMS(metric.Timestamp),
			FCID:                  fileContractID(metric.ContractID),
			Host:                  publicKey(metric.HostKey),
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

func (s *SQLStore) RecordContractSetChurnMetric(ctx context.Context, metrics ...api.ContractSetChurnMetric) error {
	dbMetrics := make([]dbContractSetChurnMetric, len(metrics))
	for i, metric := range metrics {
		dbMetrics[i] = dbContractSetChurnMetric{
			Direction: string(metric.Direction),
			FCID:      fileContractID(metric.ContractID),
			Name:      metric.Name,
			Reason:    metric.Reason,
			Timestamp: unixTimeMS(metric.Timestamp),
		}
	}
	return s.dbMetrics.Create(&dbMetrics).Error
}

func (s *SQLStore) RecordContractSetMetric(ctx context.Context, metrics ...api.ContractSetMetric) error {
	dbMetrics := make([]dbContractSetMetric, len(metrics))
	for i, metric := range metrics {
		dbMetrics[i] = dbContractSetMetric{
			Contracts: metric.Contracts,
			Name:      metric.Name,
			Timestamp: unixTimeMS(metric.Timestamp),
		}
	}
	return s.dbMetrics.Create(&dbMetrics).Error
}

func (s *SQLStore) RecordWalletMetric(ctx context.Context, metrics ...api.WalletMetric) error {
	dbMetrics := make([]dbWalletMetric, len(metrics))
	for i, metric := range metrics {
		dbMetrics[i] = dbWalletMetric{
			Timestamp:     unixTimeMS(metric.Timestamp),
			ConfirmedLo:   unsigned64(metric.Confirmed.Lo),
			ConfirmedHi:   unsigned64(metric.Confirmed.Hi),
			SpendableLo:   unsigned64(metric.Spendable.Lo),
			SpendableHi:   unsigned64(metric.Spendable.Hi),
			UnconfirmedLo: unsigned64(metric.Unconfirmed.Lo),
			UnconfirmedHi: unsigned64(metric.Unconfirmed.Hi),
		}
	}
	return s.dbMetrics.Create(&dbMetrics).Error
}

func (s *SQLStore) RecordPerformanceMetric(ctx context.Context, metrics ...api.PerformanceMetric) error {
	dbMetrics := make([]dbPerformanceMetric, len(metrics))
	for i, metric := range metrics {
		dbMetrics[i] = dbPerformanceMetric{
			Action:    metric.Action,
			Duration:  metric.Duration,
			Host:      publicKey(metric.HostKey),
			Origin:    metric.Origin,
			Timestamp: unixTimeMS(metric.Timestamp),
		}
	}
	return s.dbMetrics.Create(dbMetrics).Error
}

func (s *SQLStore) WalletMetrics(ctx context.Context, start time.Time, n uint64, interval time.Duration, opts api.WalletMetricsQueryOpts) ([]api.WalletMetric, error) {
	metrics, err := s.walletMetrics(ctx, start, n, interval, opts)
	if err != nil {
		return nil, err
	}
	resp := make([]api.WalletMetric, len(metrics))
	toCurr := func(lo, hi unsigned64) types.Currency {
		return types.NewCurrency(uint64(lo), uint64(hi))
	}
	for i := range resp {
		resp[i] = api.WalletMetric{
			Timestamp:   api.TimeRFC3339(time.Time(metrics[i].Timestamp).UTC()),
			Confirmed:   toCurr(metrics[i].ConfirmedLo, metrics[i].ConfirmedHi),
			Spendable:   toCurr(metrics[i].SpendableLo, metrics[i].SpendableHi),
			Unconfirmed: toCurr(metrics[i].UnconfirmedLo, metrics[i].UnconfirmedHi),
		}
	}
	return resp, nil
}

func (s *SQLStore) contractMetrics(ctx context.Context, start time.Time, n uint64, interval time.Duration, opts api.ContractMetricsQueryOpts) ([]dbContractMetric, error) {
	tx := s.dbMetrics
	if opts.ContractID != (types.FileContractID{}) {
		tx = tx.Where("fcid", fileContractID(opts.ContractID))
	}
	if opts.HostKey != (types.PublicKey{}) {
		tx = tx.Where("host", publicKey(opts.HostKey))
	}

	var metrics []dbContractMetric
	err := s.findPeriods(tx, &metrics, start, n, interval)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch contract metrics: %w", err)
	}

	return metrics, nil
}

func (s *SQLStore) contractSetChurnMetrics(ctx context.Context, start time.Time, n uint64, interval time.Duration, opts api.ContractSetChurnMetricsQueryOpts) ([]dbContractSetChurnMetric, error) {
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
	var metrics []dbContractSetChurnMetric
	err := s.findPeriods(tx, &metrics, start, n, interval)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch contract set churn metrics: %w", err)
	}

	return metrics, nil
}

func (s *SQLStore) contractSetMetrics(ctx context.Context, start time.Time, n uint64, interval time.Duration, opts api.ContractSetMetricsQueryOpts) ([]dbContractSetMetric, error) {
	tx := s.dbMetrics
	if opts.Name != "" {
		tx = tx.Where("name", opts.Name)
	}

	var metrics []dbContractSetMetric
	err := s.findPeriods(tx, &metrics, start, n, interval)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch contract set metrics: %w", err)
	}

	return metrics, nil
}

// findPeriods is the core of all methods retrieving metrics. By using integer
// division rounding combined with a GROUP BY operation, all rows of a table are
// split into intervals and the row with the lowest timestamp for each interval
// is returned. The result is then joined with the original table to retrieve
// only the metrics we want.
func (s *SQLStore) findPeriods(tx *gorm.DB, dst interface{}, start time.Time, n uint64, interval time.Duration) error {
	end := start.Add(time.Duration(n) * interval)
	// inner groups all metrics within the requested time range into periods of
	// 'interval' length and gives us the min timestamp of each period.
	inner := tx.Model(dst).
		Select("MIN(timestamp) AS min_time, (timestamp - ?) / ? * ? AS period", unixTimeMS(start), interval.Milliseconds(), interval.Milliseconds()).
		Where("timestamp >= ? AND timestamp < ?", unixTimeMS(start), unixTimeMS(end)).
		Group("period")
	// mid then joins the result with the original table. This might yield
	// duplicates if multiple rows have the same timestamp so we attach a
	// row number. We order the rows by id to make the result deterministic.
	mid := s.dbMetrics.Model(dst).
		Joins("INNER JOIN (?) periods ON timestamp = periods.min_time", inner).
		Select("*, ROW_NUMBER() OVER (PARTITION BY periods.min_time ORDER BY id) AS row_num")
	// lastly we select all metrics with row number 1
	return s.dbMetrics.Table("(?) numbered", mid).
		Where("numbered.row_num = 1").
		Find(dst).
		Error
}

func (s *SQLStore) walletMetrics(ctx context.Context, start time.Time, n uint64, interval time.Duration, opts api.WalletMetricsQueryOpts) (metrics []dbWalletMetric, err error) {
	err = s.findPeriods(s.dbMetrics, &metrics, start, n, interval)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch wallet metrics: %w", err)
	}
	return
}

func (s *SQLStore) performanceMetrics(ctx context.Context, start time.Time, n uint64, interval time.Duration, opts api.PerformanceMetricsQueryOpts) ([]dbPerformanceMetric, error) {
	tx := s.dbMetrics
	if opts.Action != "" {
		tx = tx.Where("action", opts.Action)
	}
	if opts.HostKey != (types.PublicKey{}) {
		tx = tx.Where("host", publicKey(opts.HostKey))
	}
	if opts.Origin != "" {
		tx = tx.Where("origin", opts.Origin)
	}

	var metrics []dbPerformanceMetric
	err := s.findPeriods(tx, &metrics, start, n, interval)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch performance metrics: %w", err)
	}

	return metrics, nil
}
