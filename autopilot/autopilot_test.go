package autopilot

import "testing"

func TestComputeNextPeriod(t *testing.T) {
	currentPeriod := uint64(100)
	period := uint64(100)
	tests := []struct {
		blockHeight uint64
		nextPeriod  uint64
	}{
		{
			blockHeight: 100,
			nextPeriod:  100,
		},
		{
			blockHeight: 150,
			nextPeriod:  100,
		},
		{
			blockHeight: 200,
			nextPeriod:  200,
		},
		{
			blockHeight: 400,
			nextPeriod:  400,
		},
	}
	for _, test := range tests {
		nextPeriod := computeNextPeriod(test.blockHeight, currentPeriod, period)
		if nextPeriod != test.nextPeriod {
			t.Fatalf("expected next period to be %d, got %d", test.nextPeriod, nextPeriod)
		}
	}
}
