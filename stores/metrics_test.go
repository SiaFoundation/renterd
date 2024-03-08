package stores

import (
	"context"
	"math"
	"sort"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"lukechampine.com/frand"
)

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
		if result := time.Time(normaliseTimestamp(test.start, test.interval, unixTimeMS(test.ti))); !result.Equal(test.result) {
			t.Fatalf("expected %v, got %v", test.result, result)
		}
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
			if !cmp.Equal(m, fcid2Metric[m.ContractID], cmp.Comparer(api.CompareTimeRFC3339)) {
				t.Fatal("unexpected metric", cmp.Diff(m, fcid2Metric[m.ContractID]))
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

func TestContractSetMetrics(t *testing.T) {
	testStart := time.Now().Round(time.Millisecond).UTC()
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	metrics, err := ss.ContractSetMetrics(context.Background(), testStart, 1, time.Minute, api.ContractSetMetricsQueryOpts{})
	if err != nil {
		t.Fatal(err)
	} else if len(metrics) != 1 {
		t.Fatal(err)
	} else if m := metrics[0]; m.Contracts != 0 {
		t.Fatalf("expected 0 contracts, got %v", m.Contracts)
	} else if ti := time.Time(m.Timestamp); !ti.Equal(testStart) {
		t.Fatal("expected time to match start time")
	} else if m.Name != testContractSet {
		t.Fatalf("expected name to be %v, got %v", testContractSet, m.Name)
	}

	// Record 3 more contract set metrics some time apart from each other.
	// The recorded timestamps are out of order to test that the query ordery by
	// time.
	cs := t.Name()
	times := []time.Time{time.UnixMilli(200), time.UnixMilli(100), time.UnixMilli(150)}
	for i, recordedTime := range times {
		if err := ss.RecordContractSetMetric(context.Background(), api.ContractSetMetric{
			Contracts: i + 1,
			Name:      cs,
			Timestamp: api.TimeRFC3339(recordedTime),
		}); err != nil {
			t.Fatal(err)
		}
	}

	assertMetrics := func(start time.Time, n uint64, interval time.Duration, opts api.ContractSetMetricsQueryOpts, expected int, contracts []int) {
		t.Helper()
		metrics, err := ss.ContractSetMetrics(context.Background(), start, n, interval, opts)
		if err != nil {
			t.Fatal(err)
		}
		if len(metrics) != expected {
			t.Fatalf("expected %v metrics, got %v: %v", expected, len(metrics), metrics)
		} else if !sort.SliceIsSorted(metrics, func(i, j int) bool {
			return time.Time(metrics[i].Timestamp).Before(time.Time(metrics[j].Timestamp))
		}) {
			t.Fatal("expected metrics to be sorted by time")
		}
		for i, m := range metrics {
			if m.Contracts != contracts[i] {
				t.Fatalf("expected %v contracts, got %v", contracts[i], m.Contracts)
			}
		}
	}

	// Start at unix timestamp 100ms and fetch 3 period at a 50ms interval. This
	// should return 3 metrics.
	start := time.UnixMilli(100)
	assertMetrics(start, 3, 50*time.Millisecond, api.ContractSetMetricsQueryOpts{}, 3, []int{2, 3, 1})

	// Start at the same timestamp but fetch 2 period at 100ms each.
	assertMetrics(start, 2, 51*time.Millisecond, api.ContractSetMetricsQueryOpts{}, 2, []int{2, 1})

	// Query all metrics by contract set.
	assertMetrics(start, 3, 50*time.Millisecond, api.ContractSetMetricsQueryOpts{Name: cs}, 3, []int{2, 3, 1})

	// Query the metric in the middle of the 3 we added.
	assertMetrics(time.UnixMilli(150), 1, 50*time.Millisecond, api.ContractSetMetricsQueryOpts{Name: cs}, 1, []int{3})

	// Prune metrics
	if err := ss.PruneMetrics(context.Background(), api.MetricContractSet, time.UnixMilli(200)); err != nil {
		t.Fatal(err)
	} else if metrics, err := ss.ContractSetMetrics(context.Background(), time.UnixMilli(100), 3, 50*time.Millisecond, api.ContractSetMetricsQueryOpts{}); err != nil {
		t.Fatal(err)
	} else if len(metrics) != 1 {
		t.Fatalf("expected 1 metric, got %v", len(metrics))
	}
}

func TestContractChurnSetMetrics(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	// Create metrics to query.
	sets := []string{"foo", "bar"}
	directions := []string{api.ChurnDirAdded, api.ChurnDirRemoved}
	reasons := []string{"reasonA", "reasonB"}
	times := []time.Time{time.UnixMilli(3), time.UnixMilli(1), time.UnixMilli(2)}
	var i byte
	for _, set := range sets {
		for _, dir := range directions {
			for _, reason := range reasons {
				for _, recordedTime := range times {
					fcid := types.FileContractID{i}
					if err := ss.RecordContractSetChurnMetric(context.Background(), api.ContractSetChurnMetric{
						Timestamp:  api.TimeRFC3339(recordedTime),
						Name:       set,
						Direction:  dir,
						Reason:     reason,
						ContractID: fcid,
					}); err != nil {
						t.Fatal(err)
					}
					i++
				}
			}
		}
	}

	assertMetrics := func(start time.Time, n uint64, interval time.Duration, opts api.ContractSetChurnMetricsQueryOpts, expected int, cmp func(api.ContractSetChurnMetric)) {
		t.Helper()
		metrics, err := ss.ContractSetChurnMetrics(context.Background(), start, n, interval, opts)
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
			cmp(m)
		}
	}

	// Query without any filters.
	start := time.UnixMilli(1)
	assertMetrics(start, 3, time.Millisecond, api.ContractSetChurnMetricsQueryOpts{}, 3, func(m api.ContractSetChurnMetric) {})

	// Query by set name.
	assertMetrics(start, 3, time.Millisecond, api.ContractSetChurnMetricsQueryOpts{Name: sets[0]}, 3, func(m api.ContractSetChurnMetric) {
		if m.Name != sets[0] {
			t.Fatalf("expected name to be %v, got %v", sets[0], m.Name)
		}
	})

	// Query by direction.
	assertMetrics(start, 3, time.Millisecond, api.ContractSetChurnMetricsQueryOpts{Direction: directions[0]}, 3, func(m api.ContractSetChurnMetric) {
		if m.Direction != directions[0] {
			t.Fatalf("expected direction to be %v, got %v", directions[1], m.Direction)
		}
	})

	// Query by reason.
	assertMetrics(start, 3, time.Millisecond, api.ContractSetChurnMetricsQueryOpts{Reason: reasons[0]}, 3, func(m api.ContractSetChurnMetric) {
		if m.Reason != reasons[0] {
			t.Fatalf("expected reason to be %v, got %v", reasons[0], m.Reason)
		}
	})

	// Prune metrics
	if err := ss.PruneMetrics(context.Background(), api.MetricContractSetChurn, time.UnixMilli(3)); err != nil {
		t.Fatal(err)
	} else if metrics, err := ss.ContractSetChurnMetrics(context.Background(), time.UnixMilli(1), 3, time.Millisecond, api.ContractSetChurnMetricsQueryOpts{}); err != nil {
		t.Fatal(err)
	} else if len(metrics) != 1 {
		t.Fatalf("expected 1 metric, got %v", len(metrics))
	}
}

func TestPerformanceMetrics(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	// Create metrics to query.
	actions := []string{"download", "upload"}
	hosts := []types.PublicKey{types.GeneratePrivateKey().PublicKey(), types.GeneratePrivateKey().PublicKey()}
	origins := []string{"worker1", "worker2"}
	durations := []time.Duration{time.Second, time.Hour}
	times := []time.Time{time.UnixMilli(3), time.UnixMilli(1), time.UnixMilli(2)}
	var i byte
	for _, action := range actions {
		for _, host := range hosts {
			for _, origin := range origins {
				for _, duration := range durations {
					for _, recordedTime := range times {
						if err := ss.RecordPerformanceMetric(context.Background(), api.PerformanceMetric{
							Action:    action,
							Timestamp: api.TimeRFC3339(recordedTime),
							Duration:  duration,
							HostKey:   host,
							Origin:    origin,
						}); err != nil {
							t.Fatal(err)
						}
						i++
					}
				}
			}
		}
	}

	assertMetrics := func(start time.Time, n uint64, interval time.Duration, opts api.PerformanceMetricsQueryOpts, expected int, cmp func(api.PerformanceMetric)) {
		t.Helper()
		metrics, err := ss.PerformanceMetrics(context.Background(), start, n, interval, opts)
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
			cmp(m)
		}
	}

	// Query without any filters.
	start := time.UnixMilli(1)
	assertMetrics(start, 3, time.Millisecond, api.PerformanceMetricsQueryOpts{}, 3, func(m api.PerformanceMetric) {})

	// Filter by actions.
	assertMetrics(start, 3, time.Millisecond, api.PerformanceMetricsQueryOpts{Action: actions[0]}, 3, func(m api.PerformanceMetric) {
		if m.Action != actions[0] {
			t.Fatalf("expected action to be %v, got %v", actions[0], m.Action)
		}
	})

	// Filter by hosts.
	assertMetrics(start, 3, time.Millisecond, api.PerformanceMetricsQueryOpts{HostKey: hosts[0]}, 3, func(m api.PerformanceMetric) {
		if m.HostKey != hosts[0] {
			t.Fatalf("expected hosts to be %v, got %v", hosts[0], m.HostKey)
		}
	})

	// Filter by reporters.
	assertMetrics(start, 3, time.Millisecond, api.PerformanceMetricsQueryOpts{Origin: origins[0]}, 3, func(m api.PerformanceMetric) {
		if m.Origin != origins[0] {
			t.Fatalf("expected origin to be %v, got %v", origins[0], m.Origin)
		}
	})

	// Prune metrics
	if err := ss.PruneMetrics(context.Background(), api.MetricPerformance, time.UnixMilli(3)); err != nil {
		t.Fatal(err)
	} else if metrics, err := ss.PerformanceMetrics(context.Background(), time.UnixMilli(1), 3, time.Millisecond, api.PerformanceMetricsQueryOpts{}); err != nil {
		t.Fatal(err)
	} else if len(metrics) != 1 {
		t.Fatalf("expected 1 metric, got %v", len(metrics))
	}
}

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
				DownloadSpending:    types.NewCurrency(frand.Uint64n(math.MaxUint64), frand.Uint64n(math.MaxUint64)),
				FundAccountSpending: types.NewCurrency(frand.Uint64n(math.MaxUint64), frand.Uint64n(math.MaxUint64)),
				DeleteSpending:      types.NewCurrency(frand.Uint64n(math.MaxUint64), frand.Uint64n(math.MaxUint64)),
				ListSpending:        types.NewCurrency64(1),
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
			expectedMetric.Timestamp = api.TimeRFC3339(normaliseTimestamp(start, interval, unixTimeMS(expectedMetric.Timestamp)))
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
		expectedMetric.Timestamp = api.TimeRFC3339(normaliseTimestamp(start, time.Millisecond, unixTimeMS(metricsTimeAsc[2*i].Timestamp)))
		expectedMetric.ContractID = types.FileContractID{}
		expectedMetric.HostKey = types.PublicKey{}
		expectedMetric.RemainingCollateral, _ = metricsTimeAsc[2*i].RemainingCollateral.AddWithOverflow(metricsTimeAsc[2*i+1].RemainingCollateral)
		expectedMetric.RemainingFunds, _ = metricsTimeAsc[2*i].RemainingFunds.AddWithOverflow(metricsTimeAsc[2*i+1].RemainingFunds)
		expectedMetric.RevisionNumber = 0
		expectedMetric.UploadSpending, _ = metricsTimeAsc[2*i].UploadSpending.AddWithOverflow(metricsTimeAsc[2*i+1].UploadSpending)
		expectedMetric.DownloadSpending, _ = metricsTimeAsc[2*i].DownloadSpending.AddWithOverflow(metricsTimeAsc[2*i+1].DownloadSpending)
		expectedMetric.FundAccountSpending, _ = metricsTimeAsc[2*i].FundAccountSpending.AddWithOverflow(metricsTimeAsc[2*i+1].FundAccountSpending)
		expectedMetric.DeleteSpending, _ = metricsTimeAsc[2*i].DeleteSpending.AddWithOverflow(metricsTimeAsc[2*i+1].DeleteSpending)
		expectedMetric.ListSpending, _ = metricsTimeAsc[2*i].ListSpending.AddWithOverflow(metricsTimeAsc[2*i+1].ListSpending)
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
	if err := ss.dbMetrics.Where("TRUE").Delete(&dbContractMetric{}).Error; err != nil {
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
	if err := ss.dbMetrics.Model(&dbContractMetric{}).Count(&n).Error; err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatalf("expected 2 metrics, got %v", n)
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
