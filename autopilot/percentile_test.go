// Copyright (c) 2014-2020 Montana Flynn (https://montanaflynn.com)
package autopilot

import (
	"testing"
)

// TestPercentile is copied from Montana Flynn's stats library since the
// implementation of percentile is more or less copied. Copying this test
// asserts both implementations behave the same way.
func TestPercentile(t *testing.T) {
	m, _ := percentile([]float64{43, 54, 56, 61, 62, 66}, 90)
	if m != 64.0 {
		t.Errorf("%.1f != %.1f", m, 64.0)
	}
	m, _ = percentile([]float64{43}, 90)
	if m != 43.0 {
		t.Errorf("%.1f != %.1f", m, 43.0)
	}
	m, _ = percentile([]float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 50)
	if m != 5.0 {
		t.Errorf("%.1f != %.1f", m, 5.0)
	}
	m, _ = percentile([]float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 99.9)
	if m != 9.5 {
		t.Errorf("%.1f != %.1f", m, 9.5)
	}
	m, _ = percentile([]float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 100)
	if m != 10.0 {
		t.Errorf("%.1f != %.1f", m, 10.0)
	}
	_, err := percentile([]float64{}, 99.9)
	if err != errEmptyInput {
		t.Errorf("Empty slice didn't return expected error; got %v", err)
	}
	_, err = percentile([]float64{1, 2, 3, 4, 5}, 0)
	if err != errOutOfBounds {
		t.Errorf("Zero percent didn't return expected error; got %v", err)
	}
	_, err = percentile([]float64{1, 2, 3, 4, 5}, 0.13)
	if err != errOutOfBounds {
		t.Errorf("Too low percent didn't return expected error; got %v", err)
	}
	_, err = percentile([]float64{1, 2, 3, 4, 5}, 101)
	if err != errOutOfBounds {
		t.Errorf("Too high percent didn't return expected error; got %v", err)
	}
}
