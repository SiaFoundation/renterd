package autopilot

import (
	"testing"

	"go.sia.tech/core/types"
)

func TestMinNewCollateral(t *testing.T) {
	t.Parallel()

	// The collateral threshold is 10% meaning that we expect 10 times the
	// remaining collateral to be the minimum to trigger a renewal.
	if min := minNewCollateral(types.Siacoins(1)); !min.Equals(types.Siacoins(10)) {
		t.Fatalf("expected 10, got %v", min)
	}
}

func TestIsBelowCollateralThreshold(t *testing.T) {
	t.Parallel()

	tests := []struct {
		newCollateral       types.Currency
		remainingCollateral types.Currency
		isBelow             bool
	}{
		{
			remainingCollateral: types.NewCurrency64(1),
			newCollateral:       types.NewCurrency64(10),
			isBelow:             true,
		},
		{
			remainingCollateral: types.NewCurrency64(1),
			newCollateral:       types.NewCurrency64(9),
			isBelow:             false,
		},
		{
			remainingCollateral: types.NewCurrency64(1),
			newCollateral:       types.NewCurrency64(11),
			isBelow:             true,
		},
		{
			remainingCollateral: types.NewCurrency64(1),
			newCollateral:       types.ZeroCurrency,
			isBelow:             false,
		},
	}
	for i, test := range tests {
		if isBelow := isBelowCollateralThreshold(test.newCollateral, test.remainingCollateral); isBelow != test.isBelow {
			t.Fatalf("%v: expected %v, got %v", i+1, test.isBelow, isBelow)
		}
	}
}
