package stores

import (
	"context"
	"sort"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
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
		if err := ss.RecordContractSetMetric(context.Background(), recordedTime, cs, i+1); err != nil {
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
					if err := ss.RecordContractSetChurnMetric(context.Background(), recordedTime, set, dir, reason, fcid); err != nil {
						t.Fatal(err)
					}
					i++
				}
			}
		}
	}

	// Query without any filters.
	metrics, err := ss.contractSetChurnMetrics(context.Background(), api.ContractSetChurnMetricsQueryOpts{})
	if err != nil {
		t.Fatal(err)
	} else if len(metrics) != 24 {
		t.Fatalf("expected 24 metrics, got %v", len(metrics))
	} else if !sort.SliceIsSorted(metrics, func(i, j int) bool {
		return time.Time(metrics[i].Timestamp).Before(time.Time(metrics[j].Timestamp))
	}) {
		t.Fatal("expected metrics to be sorted by time")
	}

	// Query by set name.
	metrics, err = ss.contractSetChurnMetrics(context.Background(), api.ContractSetChurnMetricsQueryOpts{
		Name: sets[0],
	})
	if err != nil {
		t.Fatal(err)
	} else if len(metrics) != 12 {
		t.Fatalf("expected 12 metrics, got %v", len(metrics))
	}
	for _, m := range metrics {
		if m.Name != sets[0] {
			t.Fatalf("expected name to be %v, got %v", sets[0], m.Name)
		}
	}

	// Query by time.
	after := time.UnixMilli(2)  // 'after' is exclusive
	before := time.UnixMilli(3) // 'before' is inclusive
	metrics, err = ss.contractSetChurnMetrics(context.Background(), api.ContractSetChurnMetricsQueryOpts{
		After:  after,
		Before: before,
	})
	if err != nil {
		t.Fatal(err)
	} else if len(metrics) != 8 {
		t.Fatalf("expected 8 metrics, got %v", len(metrics))
	}
	for _, m := range metrics {
		if m.Timestamp != unixTimeMS(before) {
			t.Fatalf("expected time to be %v, got %v", before, time.Time(m.Timestamp).UnixMilli())
		}
	}

	// Query by direction.
	metrics, err = ss.contractSetChurnMetrics(context.Background(), api.ContractSetChurnMetricsQueryOpts{
		Direction: directions[1],
	})
	if err != nil {
		t.Fatal(err)
	} else if len(metrics) != 12 {
		t.Fatalf("expected 12 metrics, got %v", len(metrics))
	}
	for _, m := range metrics {
		if m.Direction != directions[1] {
			t.Fatalf("expected direction to be %v, got %v", directions[1], m.Direction)
		}
	}

	// Query by reason.
	metrics, err = ss.contractSetChurnMetrics(context.Background(), api.ContractSetChurnMetricsQueryOpts{
		Reason: reasons[1],
	})
	if err != nil {
		t.Fatal(err)
	} else if len(metrics) != 12 {
		t.Fatalf("expected 12 metrics, got %v", len(metrics))
	}
	for _, m := range metrics {
		if m.Reason != reasons[1] {
			t.Fatalf("expected reason to be %v, got %v", reasons[1], m.Reason)
		}
	}
}
