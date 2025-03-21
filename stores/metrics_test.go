package stores

import (
	"context"
	"math"
	"sort"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/v2/api"
	"go.sia.tech/renterd/v2/stores/sql"
	"lukechampine.com/frand"
)

func TestContractMetrics(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	// Create metrics to query.
	hosts := []types.PublicKey{types.GeneratePrivateKey().PublicKey(), types.GeneratePrivateKey().PublicKey()}
	times := []time.Time{time.UnixMilli(3), time.UnixMilli(1), time.UnixMilli(2)}
	var i byte
	fcid2Metric := make(map[types.FileContractID]api.ContractMetric)
	var metricsTimeAsc []api.ContractMetric
	for _, host := range hosts {
		for _, recordedTime := range times {
			metric := api.ContractMetric{
				Timestamp:           api.TimeRFC3339(recordedTime),
				ContractID:          types.FileContractID{i},
				HostKey:             host,
				RemainingCollateral: types.MaxCurrency,
				RemainingFunds:      types.NewCurrency(frand.Uint64n(math.MaxUint64), frand.Uint64n(math.MaxUint64)),
				RevisionNumber:      math.MaxUint64,
				UploadSpending:      types.NewCurrency(frand.Uint64n(math.MaxUint64), frand.Uint64n(math.MaxUint64)),
				FundAccountSpending: types.NewCurrency(frand.Uint64n(math.MaxUint64), frand.Uint64n(math.MaxUint64)),
				DeleteSpending:      types.NewCurrency(frand.Uint64n(math.MaxUint64), frand.Uint64n(math.MaxUint64)),
				SectorRootsSpending: types.NewCurrency64(1),
			}
			fcid2Metric[metric.ContractID] = metric
			metricsTimeAsc = append(metricsTimeAsc, metric)
			if err := ss.RecordContractMetric(context.Background(), metric); err != nil {
				t.Fatal(err)
			}
			i++
		}
	}
	sort.SliceStable(metricsTimeAsc, func(i, j int) bool {
		return metricsTimeAsc[i].Timestamp.Std().UnixMilli() < metricsTimeAsc[j].Timestamp.Std().UnixMilli()
	})

	assertMetrics := func(start time.Time, n uint64, interval time.Duration, opts api.ContractMetricsQueryOpts, expected int, cmpFn func(api.ContractMetric)) {
		t.Helper()
		metrics, err := ss.ContractMetrics(context.Background(), start, n, interval, opts)
		if err != nil {
			t.Fatal(err)
		}
		if len(metrics) != expected {
			t.Fatalf("expected %v metrics, got %v", expected, len(metrics))
		} else if !sort.SliceIsSorted(metrics, func(i, j int) bool {
			return time.Time(metrics[i].Timestamp).Before(time.Time(metrics[j].Timestamp))
		}) {
			t.Fatal("expected metrics to be sorted by time")
		}
		for _, m := range metrics {
			expectedMetric := fcid2Metric[m.ContractID]
			expectedMetric.Timestamp = api.TimeRFC3339(normaliseTimestamp(start, interval, sql.UnixTimeMS(expectedMetric.Timestamp)))
			if !cmp.Equal(m, expectedMetric, cmp.Comparer(api.CompareTimeRFC3339)) {
				t.Fatal("unexpected metric", cmp.Diff(m, expectedMetric, cmp.Comparer(api.CompareTimeRFC3339)))
			}
			cmpFn(m)
		}
	}

	// Query by host.
	start := time.UnixMilli(1)
	assertMetrics(start, 3, time.Millisecond, api.ContractMetricsQueryOpts{HostKey: hosts[0]}, 3, func(m api.ContractMetric) {
		if m.HostKey != hosts[0] {
			t.Fatalf("expected host to be %v, got %v", hosts[0], m.HostKey)
		}
	})

	// Query by fcid.
	fcid := types.FileContractID{2}
	assertMetrics(start, 3, time.Millisecond, api.ContractMetricsQueryOpts{ContractID: fcid}, 1, func(m api.ContractMetric) {
		if m.ContractID != fcid {
			t.Fatalf("expected fcid to be %v, got %v", fcid, m.ContractID)
		}
	})
	// Query without any filters. This will cause aggregate values to be returned.
	metrics, err := ss.ContractMetrics(context.Background(), start, 3, time.Millisecond, api.ContractMetricsQueryOpts{})
	if err != nil {
		t.Fatal(err)
	} else if len(metrics) != 3 {
		t.Fatalf("expected 3 metrics, got %v", len(metrics))
	}
	for i, m := range metrics {
		var expectedMetric api.ContractMetric
		expectedMetric.Timestamp = api.TimeRFC3339(normaliseTimestamp(start, time.Millisecond, sql.UnixTimeMS(metricsTimeAsc[2*i].Timestamp)))
		expectedMetric.ContractID = types.FileContractID{}
		expectedMetric.HostKey = types.PublicKey{}
		expectedMetric.RemainingCollateral, _ = metricsTimeAsc[2*i].RemainingCollateral.AddWithOverflow(metricsTimeAsc[2*i+1].RemainingCollateral)
		expectedMetric.RemainingFunds, _ = metricsTimeAsc[2*i].RemainingFunds.AddWithOverflow(metricsTimeAsc[2*i+1].RemainingFunds)
		expectedMetric.RevisionNumber = 0
		expectedMetric.UploadSpending, _ = metricsTimeAsc[2*i].UploadSpending.AddWithOverflow(metricsTimeAsc[2*i+1].UploadSpending)
		expectedMetric.FundAccountSpending, _ = metricsTimeAsc[2*i].FundAccountSpending.AddWithOverflow(metricsTimeAsc[2*i+1].FundAccountSpending)
		expectedMetric.DeleteSpending, _ = metricsTimeAsc[2*i].DeleteSpending.AddWithOverflow(metricsTimeAsc[2*i+1].DeleteSpending)
		expectedMetric.SectorRootsSpending, _ = metricsTimeAsc[2*i].SectorRootsSpending.AddWithOverflow(metricsTimeAsc[2*i+1].SectorRootsSpending)
		if !cmp.Equal(m, expectedMetric, cmp.Comparer(api.CompareTimeRFC3339)) {
			t.Fatal(i, "unexpected metric", cmp.Diff(m, expectedMetric, cmp.Comparer(api.CompareTimeRFC3339)))
		}
	}

	// Prune metrics
	if err := ss.PruneMetrics(context.Background(), api.MetricContract, time.UnixMilli(3)); err != nil {
		t.Fatal(err)
	} else if metrics, err := ss.ContractMetrics(context.Background(), time.UnixMilli(1), 3, time.Millisecond, api.ContractMetricsQueryOpts{}); err != nil {
		t.Fatal(err)
	} else if len(metrics) != 1 {
		t.Fatalf("expected 1 metric, got %v", len(metrics))
	}

	// Drop all metrics.
	if err := ss.PruneMetrics(context.Background(), api.MetricContract, time.Now()); err != nil {
		t.Fatal(err)
	}

	// Record multiple metrics for the same contract - one per second over 10 minutes
	for i := int64(0); i < 600; i++ {
		err := ss.RecordContractMetric(context.Background(), api.ContractMetric{
			ContractID: types.FileContractID{1},
			Timestamp:  api.TimeRFC3339(time.Unix(i, 0)),
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Check how many metrics were recorded.
	var n int64
	if err := ss.DBMetrics().QueryRow(context.Background(), "SELECT COUNT(*) FROM contracts").Scan(&n); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatalf("expected 2 metrics, got %v", n)
	}
}

func TestContractPruneMetrics(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	// add some metrics
	hosts := []types.PublicKey{{1}, {2}}
	hostVersions := []string{"1.5.9", "1.6.0"}
	times := []time.Time{time.UnixMilli(3), time.UnixMilli(1), time.UnixMilli(2)}
	var i byte
	fcid2Metric := make(map[types.FileContractID]api.ContractPruneMetric)
	for hi, host := range hosts {
		for _, recordedTime := range times {
			metric := api.ContractPruneMetric{
				Timestamp: api.TimeRFC3339(recordedTime),

				ContractID:  types.FileContractID{i},
				HostKey:     host,
				HostVersion: hostVersions[hi],

				Pruned:    math.MaxUint64,
				Remaining: math.MaxUint64,
				Duration:  time.Second,
			}
			fcid2Metric[metric.ContractID] = metric
			if err := ss.RecordContractPruneMetric(context.Background(), metric); err != nil {
				t.Fatal(err)
			}
			i++
		}
	}

	assertMetrics := func(start time.Time, n uint64, interval time.Duration, opts api.ContractPruneMetricsQueryOpts, expected int, cmpFn func(api.ContractPruneMetric)) {
		t.Helper()
		metrics, err := ss.ContractPruneMetrics(context.Background(), start, n, interval, opts)
		if err != nil {
			t.Fatal(err)
		}
		if len(metrics) != expected {
			t.Fatalf("expected %v metrics, got %v", expected, len(metrics))
		} else if !sort.SliceIsSorted(metrics, func(i, j int) bool {
			return time.Time(metrics[i].Timestamp).Before(time.Time(metrics[j].Timestamp))
		}) {
			t.Fatal("expected metrics to be sorted by time")
		}
		for _, m := range metrics {
			if !cmp.Equal(m, fcid2Metric[m.ContractID], cmpopts.IgnoreUnexported(api.ContractPruneMetric{}), cmp.Comparer(api.CompareTimeRFC3339)) {
				t.Fatal("unexpected metric", m, fcid2Metric[m.ContractID])
			}
			cmpFn(m)
		}
	}

	// Query without any filters.
	start := time.UnixMilli(1)
	assertMetrics(start, 3, time.Millisecond, api.ContractPruneMetricsQueryOpts{}, 3, func(m api.ContractPruneMetric) {})

	// Query by host.
	assertMetrics(start, 3, time.Millisecond, api.ContractPruneMetricsQueryOpts{HostKey: hosts[0]}, 3, func(m api.ContractPruneMetric) {
		if m.HostKey != hosts[0] {
			t.Fatalf("expected host to be %v, got %v", hosts[0], m.HostKey)
		}
	})

	// Query by host version.
	assertMetrics(start, 3, time.Millisecond, api.ContractPruneMetricsQueryOpts{HostVersion: hostVersions[0]}, 3, func(m api.ContractPruneMetric) {
		if m.HostKey != hosts[0] {
			t.Fatalf("expected host to be %v, got %v", hosts[0], m.HostKey)
		}
	})

	// Query by fcid.
	fcid := types.FileContractID{2}
	assertMetrics(start, 3, time.Millisecond, api.ContractPruneMetricsQueryOpts{ContractID: fcid}, 1, func(m api.ContractPruneMetric) {
		if m.ContractID != fcid {
			t.Fatalf("expected fcid to be %v, got %v", fcid, m.ContractID)
		}
	})

	// Prune metrics
	if err := ss.PruneMetrics(context.Background(), api.MetricContractPrune, time.UnixMilli(3)); err != nil {
		t.Fatal(err)
	} else if metrics, err := ss.ContractPruneMetrics(context.Background(), time.UnixMilli(1), 3, time.Millisecond, api.ContractPruneMetricsQueryOpts{}); err != nil {
		t.Fatal(err)
	} else if len(metrics) != 1 {
		t.Fatalf("expected 1 metric, got %v", len(metrics))
	}
}

func TestNormaliseTimestamp(t *testing.T) {
	tests := []struct {
		start    time.Time
		interval time.Duration
		ti       time.Time
		result   time.Time
	}{
		{
			start:    time.UnixMilli(100),
			interval: 10 * time.Millisecond,
			ti:       time.UnixMilli(105),
			result:   time.UnixMilli(100),
		},
		{
			start:    time.UnixMilli(100),
			interval: 10 * time.Millisecond,
			ti:       time.UnixMilli(115),
			result:   time.UnixMilli(110),
		},
		{
			start:    time.UnixMilli(100),
			interval: 10 * time.Millisecond,
			ti:       time.UnixMilli(125),
			result:   time.UnixMilli(120),
		},
	}

	for _, test := range tests {
		if result := time.Time(normaliseTimestamp(test.start, test.interval, sql.UnixTimeMS(test.ti))); !result.Equal(test.result) {
			t.Fatalf("expected %v, got %v", test.result, result)
		}
	}
}

func TestWalletMetrics(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	// Create metrics to query.
	times := []time.Time{time.UnixMilli(3), time.UnixMilli(1), time.UnixMilli(2)}
	for _, recordedTime := range times {
		metric := api.WalletMetric{
			Timestamp:   api.TimeRFC3339(recordedTime),
			Confirmed:   types.NewCurrency(frand.Uint64n(math.MaxUint64), frand.Uint64n(math.MaxUint64)),
			Unconfirmed: types.NewCurrency(frand.Uint64n(math.MaxUint64), frand.Uint64n(math.MaxUint64)),
			Spendable:   types.NewCurrency(frand.Uint64n(math.MaxUint64), frand.Uint64n(math.MaxUint64)),
			Immature:    types.NewCurrency(frand.Uint64n(math.MaxUint64), frand.Uint64n(math.MaxUint64)),
		}
		if err := ss.RecordWalletMetric(context.Background(), metric); err != nil {
			t.Fatal(err)
		}
	}

	// Fetch all metrcis
	metrics, err := ss.WalletMetrics(context.Background(), time.UnixMilli(1), 3, time.Millisecond, api.WalletMetricsQueryOpts{})
	if err != nil {
		t.Fatal(err)
	} else if len(metrics) != 3 {
		t.Fatalf("expected 3 metrics, got %v", len(metrics))
	} else if !sort.SliceIsSorted(metrics, func(i, j int) bool {
		return time.Time(metrics[i].Timestamp).Before(time.Time(metrics[j].Timestamp))
	}) {
		t.Fatalf("expected metrics to be sorted by time, %+v", metrics)
	}

	// Prune metrics
	if err := ss.PruneMetrics(context.Background(), api.MetricWallet, time.UnixMilli(3)); err != nil {
		t.Fatal(err)
	} else if metrics, err := ss.WalletMetrics(context.Background(), time.UnixMilli(1), 3, time.Millisecond, api.WalletMetricsQueryOpts{}); err != nil {
		t.Fatal(err)
	} else if len(metrics) != 1 {
		t.Fatalf("expected 1 metric, got %v", len(metrics))
	}
}

func normaliseTimestamp(start time.Time, interval time.Duration, t sql.UnixTimeMS) sql.UnixTimeMS {
	startMS := start.UnixMilli()
	toNormaliseMS := time.Time(t).UnixMilli()
	intervalMS := interval.Milliseconds()
	if startMS > toNormaliseMS {
		return sql.UnixTimeMS(start)
	}
	normalizedMS := (toNormaliseMS-startMS)/intervalMS*intervalMS + start.UnixMilli()
	return sql.UnixTimeMS(time.UnixMilli(normalizedMS))
}
