package autopilot

import (
	"testing"
)

func TestAddLeeway(t *testing.T) {
	tests := []struct {
		name   string
		n      uint64
		pct    float64
		result uint64
	}{
		{name: "min 10% more", n: 14, pct: .1, result: 16},
		{name: "min 10% more, min 1", n: 1, pct: .1, result: 2},
		{name: "max 10% less", n: 12, pct: -.1, result: 11},
		{name: "max 10% less, min 1", n: 1, pct: -.1, result: 1},
		{name: "100% less", n: 1, pct: -1, result: 0},
		{name: "100% more", n: 1, pct: 1, result: 2},
		{name: "0% less", n: 1, pct: 0, result: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if result := addLeeway(tt.n, tt.pct); result != tt.result {
				t.Errorf("%s failed, %v != %v", tt.name, result, tt.result)
			}
		})
	}
}
