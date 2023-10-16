package stores

import (
	"context"
	"sort"
	"testing"
	"time"

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
	} else if !time.Time(m.Time).After(testStart) {
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
			return time.Time(metrics[i].Time).Before(time.Time(metrics[j].Time))
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
