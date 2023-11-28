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
				Timestamp: recordedTime,

				ContractID:  types.FileContractID{i},
				HostKey:     host,
				HostVersion: hostVersions[hi],

				Pruned:    math.MaxUint64,
				Remaining: math.MaxUint64,
				Duration:  time.Second,

				Error: "a",
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
			if !cmp.Equal(m, fcid2Metric[m.ContractID]) {
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
}

func TestContractSetMetrics(t *testing.T) {
	testStart := time.Now()
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	metrics, err := ss.ContractSetMetrics(context.Background(), testStart, 1, time.Minute, api.ContractSetMetricsQueryOpts{})
	if err != nil {
		t.Fatal(err)
	} else if len(metrics) != 1 {
		t.Fatal(err)
	} else if m := metrics[0]; m.Contracts != 0 {
		t.Fatalf("expected 0 contracts, got %v", m.Contracts)
	} else if !time.Time(m.Timestamp).After(testStart) {
		t.Fatal("expected time to be after test start")
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
			Timestamp: recordedTime,
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
						Timestamp:  recordedTime,
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
							Timestamp: recordedTime,
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
}

func TestContractMetrics(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	// Create metrics to query.
	hosts := []types.PublicKey{types.GeneratePrivateKey().PublicKey(), types.GeneratePrivateKey().PublicKey()}
	times := []time.Time{time.UnixMilli(3), time.UnixMilli(1), time.UnixMilli(2)}
	var i byte
	fcid2Metric := make(map[types.FileContractID]api.ContractMetric)
	for _, host := range hosts {
		for _, recordedTime := range times {
			metric := api.ContractMetric{
				Timestamp:           recordedTime,
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
			if err := ss.RecordContractMetric(context.Background(), metric); err != nil {
				t.Fatal(err)
			}
			i++
		}
	}

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
			if !cmp.Equal(m, fcid2Metric[m.ContractID]) {
				t.Fatal("unexpected metric", cmp.Diff(m, fcid2Metric[m.ContractID]))
			}
			cmpFn(m)
		}
	}

	// Query without any filters.
	start := time.UnixMilli(1)
	assertMetrics(start, 3, time.Millisecond, api.ContractMetricsQueryOpts{}, 3, func(m api.ContractMetric) {})

	// Query by host.
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
}
