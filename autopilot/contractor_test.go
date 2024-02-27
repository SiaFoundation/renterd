package autopilot

import (
	"context"
	"math"
	"testing"

	"go.uber.org/zap"
)

func TestCalculateMinScore(t *testing.T) {
	c := &contractor{
		logger: zap.NewNop().Sugar(),
	}

	var candidates []scoredHost
	for i := 0; i < 250; i++ {
		candidates = append(candidates, scoredHost{score: float64(i + 1)})
	}

	// Test with 100 hosts which makes for a random set size of 250
	minScore := c.calculateMinScore(context.Background(), candidates, 100)
	if minScore != 0.002 {
		t.Fatalf("expected minScore to be 0.002 but was %v", minScore)
	}

	// Test with 0 hosts
	minScore = c.calculateMinScore(context.Background(), []scoredHost{}, 100)
	if minScore != math.SmallestNonzeroFloat64 {
		t.Fatalf("expected minScore to be math.SmallestNonzeroFLoat64 but was %v", minScore)
	}

	// Test with 300 hosts which is 50 more than we have
	minScore = c.calculateMinScore(context.Background(), candidates, 300)
	if minScore != math.SmallestNonzeroFloat64 {
		t.Fatalf("expected minScore to be math.SmallestNonzeroFLoat64 but was %v", minScore)
	}
}
