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

func TestContractSetMetrics(t *testing.T) {
	testStart := time.Now()
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	metrics, err := ss.ContractSetMetrics(context.Background(), api.ContractSetMetricsQueryOpts{})
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
	times := []time.Time{time.UnixMilli(3), time.UnixMilli(1), time.UnixMilli(2)}
	for i, recordedTime := range times {
		if err := ss.RecordContractSetMetric(context.Background(), api.ContractSetMetric{
			Contracts: i + 1,
			Name:      cs,
			Timestamp: recordedTime,
		}); err != nil {
			t.Fatal(err)
		}
	}

	assertMetrics := func(opts api.ContractSetMetricsQueryOpts, expected int, contracts []int) {
		t.Helper()
		metrics, err := ss.ContractSetMetrics(context.Background(), opts)
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
		for i, m := range metrics {
			if m.Contracts != contracts[i] {
				t.Fatalf("expected %v contracts, got %v", contracts[i], m.Contracts)
			}
		}
	}

	// Check that we have 4 metrics now.
	assertMetrics(api.ContractSetMetricsQueryOpts{}, 4, []int{2, 3, 1, 0})

	// Query all metrics by contract set.
	assertMetrics(api.ContractSetMetricsQueryOpts{Name: cs}, 3, []int{2, 3, 1})

	// Query the metric in the middle of the 3 we added.
	after := time.UnixMilli(1)  // 'after' is exclusive
	before := time.UnixMilli(2) // 'before' is inclusive
	assertMetrics(api.ContractSetMetricsQueryOpts{After: after, Before: before}, 1, []int{3})
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
					if err := ss.RecordContractSetChurnMetrics(context.Background(), api.ContractSetChurnMetric{
						Timestamp: recordedTime,
						Name:      set,
						Direction: dir,
						Reason:    reason,
						FCID:      fcid,
					}); err != nil {
						t.Fatal(err)
					}
					i++
				}
			}
		}
	}

	assertMetrics := func(opts api.ContractSetChurnMetricsQueryOpts, expected int, cmp func(api.ContractSetChurnMetric)) {
		t.Helper()
		metrics, err := ss.ContractSetChurnMetrics(context.Background(), opts)
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
	assertMetrics(api.ContractSetChurnMetricsQueryOpts{}, 24, func(m api.ContractSetChurnMetric) {})

	// Query by set name.
	assertMetrics(api.ContractSetChurnMetricsQueryOpts{Name: sets[0]}, 12, func(m api.ContractSetChurnMetric) {
		if m.Name != sets[0] {
			t.Fatalf("expected name to be %v, got %v", sets[0], m.Name)
		}
	})

	// Query by time.
	after := time.UnixMilli(2)  // 'after' is exclusive
	before := time.UnixMilli(3) // 'before' is inclusive
	assertMetrics(api.ContractSetChurnMetricsQueryOpts{After: after, Before: before}, 8, func(m api.ContractSetChurnMetric) {
		if !m.Timestamp.Equal(before) {
			t.Fatalf("expected time to be %v, got %v", before, time.Time(m.Timestamp).UnixMilli())
		}
	})

	// Query by direction.
	assertMetrics(api.ContractSetChurnMetricsQueryOpts{Direction: directions[0]}, 12, func(m api.ContractSetChurnMetric) {
		if m.Direction != directions[0] {
			t.Fatalf("expected direction to be %v, got %v", directions[1], m.Direction)
		}
	})

	// Query by reason.
	assertMetrics(api.ContractSetChurnMetricsQueryOpts{Reason: reasons[0]}, 12, func(m api.ContractSetChurnMetric) {
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
							Host:      host,
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

	assertMetrics := func(opts api.PerformanceMetricsQueryOpts, expected int, cmp func(api.PerformanceMetric)) {
		t.Helper()
		metrics, err := ss.PerformanceMetrics(context.Background(), opts)
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
	assertMetrics(api.PerformanceMetricsQueryOpts{}, 48, func(m api.PerformanceMetric) {})

	// Filter by actions.
	assertMetrics(api.PerformanceMetricsQueryOpts{Action: actions[0]}, 24, func(m api.PerformanceMetric) {
		if m.Action != actions[0] {
			t.Fatalf("expected action to be %v, got %v", actions[0], m.Action)
		}
	})

	// Filter by hosts.
	assertMetrics(api.PerformanceMetricsQueryOpts{Host: hosts[0]}, 24, func(m api.PerformanceMetric) {
		if m.Host != hosts[0] {
			t.Fatalf("expected hosts to be %v, got %v", hosts[0], m.Host)
		}
	})

	// Filter by reporters.
	assertMetrics(api.PerformanceMetricsQueryOpts{Origin: origins[0]}, 24, func(m api.PerformanceMetric) {
		if m.Origin != origins[0] {
			t.Fatalf("expected origin to be %v, got %v", origins[0], m.Origin)
		}
	})

	// Filter by duration.
	assertMetrics(api.PerformanceMetricsQueryOpts{Duration: durations[0]}, 24, func(m api.PerformanceMetric) {
		if m.Duration != durations[0] {
			t.Fatalf("expected duration to be %v, got %v", durations[0], m.Duration)
		}
	})

	// Filter by time.
	after := time.UnixMilli(2)  // 'after' is exclusive
	before := time.UnixMilli(3) // 'before' is inclusive
	assertMetrics(api.PerformanceMetricsQueryOpts{After: after, Before: before}, 16, func(m api.PerformanceMetric) {
		if !m.Timestamp.Equal(before) {
			t.Fatalf("expected time to be %v, got %v", before, m.Timestamp)
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
				FCID:                types.FileContractID{i},
				Host:                host,
				RemainingCollateral: types.MaxCurrency,
				RemainingFunds:      types.NewCurrency(frand.Uint64n(math.MaxUint64), frand.Uint64n(math.MaxUint64)),
				RevisionNumber:      math.MaxUint64,
				UploadSpending:      types.NewCurrency(frand.Uint64n(math.MaxUint64), frand.Uint64n(math.MaxUint64)),
				DownloadSpending:    types.NewCurrency(frand.Uint64n(math.MaxUint64), frand.Uint64n(math.MaxUint64)),
				FundAccountSpending: types.NewCurrency(frand.Uint64n(math.MaxUint64), frand.Uint64n(math.MaxUint64)),
				DeleteSpending:      types.NewCurrency(frand.Uint64n(math.MaxUint64), frand.Uint64n(math.MaxUint64)),
				ListSpending:        types.NewCurrency64(1),
			}
			fcid2Metric[metric.FCID] = metric
			if err := ss.RecordContractMetric(context.Background(), metric); err != nil {
				t.Fatal(err)
			}
			i++
		}
	}

	assertMetrics := func(opts api.ContractMetricsQueryOpts, expected int, cmpFn func(api.ContractMetric)) {
		t.Helper()
		metrics, err := ss.ContractMetrics(context.Background(), opts)
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
			if !cmp.Equal(m, fcid2Metric[m.FCID]) {
				t.Fatal("unexpected metric", cmp.Diff(m, fcid2Metric[m.FCID]))
			}
			cmpFn(m)
		}
	}

	// Query without any filters.
	assertMetrics(api.ContractMetricsQueryOpts{}, 6, func(m api.ContractMetric) {})

	// Query by host.
	assertMetrics(api.ContractMetricsQueryOpts{Host: hosts[0]}, 3, func(m api.ContractMetric) {
		if m.Host != hosts[0] {
			t.Fatalf("expected host to be %v, got %v", hosts[0], m.Host)
		}
	})

	// Query by fcid.
	fcid := types.FileContractID{2}
	assertMetrics(api.ContractMetricsQueryOpts{FCID: fcid}, 1, func(m api.ContractMetric) {
		if m.FCID != fcid {
			t.Fatalf("expected fcid to be %v, got %v", fcid, m.FCID)
		}
	})

	// Query by time.
	after := time.UnixMilli(2)  // 'after' is exclusive
	before := time.UnixMilli(3) // 'before' is inclusive
	assertMetrics(api.ContractMetricsQueryOpts{After: after, Before: before}, 2, func(m api.ContractMetric) {
		if !m.Timestamp.Equal(before) {
			t.Fatalf("expected time to be %v, got %v", before, m.Timestamp)
		}
	})
}
