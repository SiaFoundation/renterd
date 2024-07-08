package contractor

import (
	"math"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

func TestCalculateMinScore(t *testing.T) {
	var candidates []scoredHost
	for i := 0; i < 250; i++ {
		candidates = append(candidates, scoredHost{score: float64(i + 1)})
	}

	// Test with 100 hosts which makes for a random set size of 250
	minScore := calculateMinScore(candidates, 100, zap.NewNop().Sugar())
	if minScore != 0.002 {
		t.Fatalf("expected minScore to be 0.002 but was %v", minScore)
	}

	// Test with 0 hosts
	minScore = calculateMinScore([]scoredHost{}, 100, zap.NewNop().Sugar())
	if minScore != math.SmallestNonzeroFloat64 {
		t.Fatalf("expected minScore to be math.SmallestNonzeroFLoat64 but was %v", minScore)
	}

	// Test with 300 hosts which is 50 more than we have
	minScore = calculateMinScore(candidates, 300, zap.NewNop().Sugar())
	if minScore != math.SmallestNonzeroFloat64 {
		t.Fatalf("expected minScore to be math.SmallestNonzeroFLoat64 but was %v", minScore)
	}
}

func TestRenewFundingEstimate(t *testing.T) {
	tests := []struct {
		name                 string
		minRenterFunds       uint64
		initRenterFunds      uint64
		remainingRenterFunds uint64
		expected             uint64
	}{
		{
			name:                 "UnusedAboveMinAboveInit",
			minRenterFunds:       40,
			initRenterFunds:      100,
			remainingRenterFunds: 100,
			expected:             50,
		},
		{
			name:                 "UnusedAboveMinBelowInit",
			minRenterFunds:       80,
			initRenterFunds:      100,
			remainingRenterFunds: 100,
			expected:             80,
		},
		{
			name:                 "UnusedBelowMin",
			minRenterFunds:       0,
			initRenterFunds:      100,
			remainingRenterFunds: 100,
			expected:             50,
		},
		{
			name:                 "UsedUnderMin",
			minRenterFunds:       50,
			initRenterFunds:      10,
			remainingRenterFunds: 0,
			expected:             50,
		},
		{
			name:                 "UsedAboveMin",
			minRenterFunds:       50,
			initRenterFunds:      100,
			remainingRenterFunds: 90,
			expected:             90,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			renterFunds := renewFundingEstimate(types.NewCurrency64(test.minRenterFunds), types.NewCurrency64(test.initRenterFunds), types.NewCurrency64(test.remainingRenterFunds), zap.NewNop().Sugar())
			if !renterFunds.Equals(types.NewCurrency64(test.expected)) {
				t.Errorf("expected %v but got %v", test.expected, renterFunds)
			}
		})
	}
}

func TestShouldForgiveFailedRenewal(t *testing.T) {
	var fcid types.FileContractID
	frand.Read(fcid[:])
	c := &Contractor{
		firstRefreshFailure: make(map[types.FileContractID]time.Time),
	}

	// try twice since the first time will set the failure time
	if !c.shouldForgiveFailedRefresh(fcid) {
		t.Fatal("should forgive")
	} else if !c.shouldForgiveFailedRefresh(fcid) {
		t.Fatal("should forgive")
	}

	// set failure to be a full period in the past
	c.firstRefreshFailure[fcid] = time.Now().Add(-failedRefreshForgivenessPeriod - time.Second)
	if c.shouldForgiveFailedRefresh(fcid) {
		t.Fatal("should not forgive")
	}

	// prune map
	c.pruneContractRefreshFailures([]api.ContractMetadata{})
	if len(c.firstRefreshFailure) != 0 {
		t.Fatal("expected no failures")
	}
}
