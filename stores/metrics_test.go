package stores

import (
	"context"
	"fmt"
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

	// Check that we have 4 metrics now.
	metrics, err = ss.ContractSetMetrics(context.Background(), api.ContractSetMetricsQueryOpts{})
	if err != nil {
		t.Fatal(err)
	} else if len(metrics) != 4 {
		t.Fatalf("expected 4 metrics, got %v", len(metrics))
	} else if !sort.SliceIsSorted(metrics, func(i, j int) bool {
		return time.Time(metrics[i].Time).Before(time.Time(metrics[j].Time))
	}) {
		for _, m := range metrics {
			fmt.Println(time.Time(m.Time).UnixMilli())
		}
		t.Fatal("expected metrics to be sorted by time")
	}

	// Query all metrics by contract set.
	metrics, err = ss.ContractSetMetrics(context.Background(), api.ContractSetMetricsQueryOpts{
		Name: &cs,
	})
	if err != nil {
		t.Fatal(err)
	} else if len(metrics) != 3 {
		t.Fatalf("expected 3 metrics, got %v", len(metrics))
	} else if metrics[0].Contracts != 2 {
		t.Fatalf("expected 2 contracts, got %v", metrics[0].Contracts)
	} else if metrics[1].Contracts != 3 {
		t.Fatalf("expected 3 contracts, got %v", metrics[1].Contracts)
	} else if metrics[2].Contracts != 1 {
		t.Fatalf("expected 1 contracts, got %v", metrics[2].Contracts)
	}

	// Query the metric in the middle of the 3 we added.
	after := time.UnixMilli(1)  // 'after' is exclusive
	before := time.UnixMilli(2) // 'before' is inclusive
	metrics, err = ss.ContractSetMetrics(context.Background(), api.ContractSetMetricsQueryOpts{
		After:  &after,
		Before: &before,
	})
	if err != nil {
		t.Fatal(err)
	} else if len(metrics) != 1 {
		t.Fatalf("expected 1 metrics, got %v", len(metrics))
	} else if metrics[0].Contracts != 3 {
		t.Fatalf("expected 3 contracts, got %v", metrics[1].Contracts)
	}
}
