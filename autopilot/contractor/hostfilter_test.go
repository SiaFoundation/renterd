package contractor

import (
	"math"
	"testing"

	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
)

func TestMinRemainingCollateral(t *testing.T) {
	t.Parallel()

	// consts
	rs := api.RedundancySettings{MinShards: 1, TotalShards: 2} // 2x redundancy
	cfg := api.AutopilotConfig{
		Contracts: api.ContractsConfig{
			Amount: 5,
			Period: 100,
		},
	}
	one := types.NewCurrency64(1)
	pt := rhpv3.HostPriceTable{
		CollateralCost:        one,
		InitBaseCost:          one,
		WriteBaseCost:         one,
		ReadBaseCost:          one,
		WriteLengthCost:       one,
		WriteStoreCost:        one,
		ReadLengthCost:        one,
		UploadBandwidthCost:   one,
		DownloadBandwidthCost: one,
	}
	s := rhpv2.HostSettings{}
	_, sectorCollateral := pt.BaseCost().
		Add(pt.AppendSectorCost(cfg.Contracts.Period)).
		Add(pt.ReadSectorCost(rhpv2.SectorSize)).
		Total()

		// testcases
	tests := []struct {
		expectedStorage  uint64
		remainingStorage uint64
		renterFunds      types.Currency
		expected         types.Currency
	}{
		{
			// lots of funds but no remaining storage
			expected:         types.ZeroCurrency,
			expectedStorage:  100,
			remainingStorage: 0,
			renterFunds:      types.Siacoins(1000),
		},
		{
			// lots of funds but only 1 byte of remaining storage
			expected:         sectorCollateral.Mul64(3),
			expectedStorage:  100,
			remainingStorage: 1,
			renterFunds:      types.Siacoins(1000),
		},
		{
			// ideal data is capping the collateral
			// 100 sectors * 2 (redundancy) * 2 (buffer) / 5 (hosts) / 20 (denom) = 4 sectors of collateral
			expected:         sectorCollateral.Mul64(4),                               // 100 sectors * 2 (redundancy) * 2 (buffer)
			expectedStorage:  5 * rhpv2.SectorSize * minContractCollateralDenominator, // 100 sectors
			remainingStorage: math.MaxUint64,
			renterFunds:      types.Siacoins(1000),
		},
		{
			// nothing is capping the expected storage
			expected:         types.NewCurrency64(17175674880), // ~13.65 x the previous 'expected'
			expectedStorage:  math.MaxUint32,
			remainingStorage: math.MaxUint64,
			renterFunds:      types.Siacoins(1000),
		},
	}

	for i, test := range tests {
		cfg.Contracts.Storage = test.expectedStorage
		s.RemainingStorage = test.remainingStorage
		min := minRemainingCollateral(cfg, rs, test.renterFunds, s, pt)
		if min.Cmp(test.expected) != 0 {
			t.Fatalf("%v: expected %v, got %v", i+1, test.expected, min)
		}
	}
}
