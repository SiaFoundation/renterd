package contractor

import (
	"fmt"
	"math"
	"testing"
	"time"

	"go.sia.tech/core/consensus"
	rhpv4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/v2/api"
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

func TestContractFunding(t *testing.T) {
	defaultSettings := rhpv4.HostSettings{
		MaxCollateral: types.Siacoins(1), // unattainable
		Prices: rhpv4.HostPrices{
			StoragePrice: types.NewCurrency64(1), // 1 H/byte/block
			IngressPrice: types.NewCurrency64(1), // 1 H/byte/block
			EgressPrice:  types.NewCurrency64(1), // 1 H/byte/block
			Collateral:   types.NewCurrency64(2), // 2 H/byte/block
		},
	}

	tests := []struct {
		initialDataSize uint64
		modSettings     func(settings *rhpv4.HostSettings)
		calc            func(settings rhpv4.HostSettings) (expectedAllowance types.Currency, expectedCollateral types.Currency)
	}{
		{
			initialDataSize: 0,
			calc: func(settings rhpv4.HostSettings) (expectedAllowance types.Currency, expectedCollateral types.Currency) {
				const sectors = minContractGrowthRate / rhpv4.SectorSize
				uploadCost := settings.Prices.RPCWriteSectorCost(rhpv4.SectorSize).RenterCost().Mul64(sectors)
				downloadCost := settings.Prices.RPCReadSectorCost(rhpv4.SectorSize).RenterCost().Mul64(sectors)
				storeCost := settings.Prices.RPCAppendSectorsCost(sectors, 1).RenterCost()
				expectedAllowance = storeCost.Add(uploadCost).Add(downloadCost)
				expectedCollateral = rhpv4.MaxHostCollateral(settings.Prices, storeCost)
				return
			},
		},
		{
			initialDataSize: 100,
			calc: func(settings rhpv4.HostSettings) (expectedAllowance types.Currency, expectedCollateral types.Currency) {
				const sectors = minContractGrowthRate / rhpv4.SectorSize // value should still be minimum growth rate
				uploadCost := settings.Prices.RPCWriteSectorCost(rhpv4.SectorSize).RenterCost().Mul64(sectors)
				downloadCost := settings.Prices.RPCReadSectorCost(rhpv4.SectorSize).RenterCost().Mul64(sectors)
				storeCost := settings.Prices.RPCAppendSectorsCost(sectors, 1).RenterCost()
				expectedAllowance = storeCost.Add(uploadCost).Add(downloadCost)
				expectedCollateral = rhpv4.MaxHostCollateral(settings.Prices, storeCost)
				return
			},
		},
		{
			initialDataSize: 500 << 30, // 500 GiB
			calc: func(settings rhpv4.HostSettings) (expectedAllowance types.Currency, expectedCollateral types.Currency) {
				const (
					additionalData = 500 << 30
					sectors        = additionalData / rhpv4.SectorSize
				)
				uploadCost := settings.Prices.RPCWriteSectorCost(rhpv4.SectorSize).RenterCost().Mul64(sectors)
				downloadCost := settings.Prices.RPCReadSectorCost(rhpv4.SectorSize).RenterCost().Mul64(sectors)
				storeCost := settings.Prices.RPCAppendSectorsCost(sectors, 1).RenterCost()
				expectedAllowance = storeCost.Add(uploadCost).Add(downloadCost)
				expectedCollateral = rhpv4.MaxHostCollateral(settings.Prices, storeCost)
				return
			},
		},
		{
			initialDataSize: 3 << 40, // 3 TiB
			calc: func(settings rhpv4.HostSettings) (expectedAllowance types.Currency, expectedCollateral types.Currency) {
				const sectors = maxContractGrowthRate / rhpv4.SectorSize // clamped to 512 GiB
				uploadCost := settings.Prices.RPCWriteSectorCost(rhpv4.SectorSize).RenterCost().Mul64(sectors)
				downloadCost := settings.Prices.RPCReadSectorCost(rhpv4.SectorSize).RenterCost().Mul64(sectors)
				storeCost := settings.Prices.RPCAppendSectorsCost(sectors, 1).RenterCost()
				expectedAllowance = storeCost.Add(uploadCost).Add(downloadCost)
				expectedCollateral = rhpv4.MaxHostCollateral(settings.Prices, storeCost)
				return
			},
		},
		{
			initialDataSize: 3 << 40, // 3 TiB
			modSettings: func(settings *rhpv4.HostSettings) {
				settings.MaxCollateral = types.NewCurrency64(10) // want to test that collateral is clamped to the host's max
			},
			calc: func(settings rhpv4.HostSettings) (expectedAllowance types.Currency, expectedCollateral types.Currency) {
				const sectors = maxContractGrowthRate / rhpv4.SectorSize // clamped to 512 GiB
				uploadCost := settings.Prices.RPCWriteSectorCost(rhpv4.SectorSize).RenterCost().Mul64(sectors)
				downloadCost := settings.Prices.RPCReadSectorCost(rhpv4.SectorSize).RenterCost().Mul64(sectors)
				storeCost := settings.Prices.RPCAppendSectorsCost(sectors, 1).RenterCost()
				expectedAllowance = storeCost.Add(uploadCost).Add(downloadCost)
				expectedCollateral = types.NewCurrency64(10) // clamped to the host's max collateral)
				return
			},
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("data size %d", test.initialDataSize), func(t *testing.T) {
			settings := defaultSettings
			if test.modSettings != nil {
				test.modSettings(&settings)
			}
			expectedAllowance, expectedCollateral := test.calc(settings)
			allowance, collateral := contractFunding(settings, test.initialDataSize, types.ZeroCurrency, types.ZeroCurrency, 1)
			if !allowance.Equals(expectedAllowance) {
				t.Errorf("expected allowance %v but got %v", expectedAllowance, allowance)
			}
			if !collateral.Equals(expectedCollateral) {
				t.Errorf("expected collateral %v but got %v", expectedCollateral, collateral)
			}
		})
	}
}

func TestShouldArchive(t *testing.T) {
	c := Contractor{revisionSubmissionBuffer: 5}

	// dummy network
	var n consensus.Network
	n.HardforkV2.AllowHeight = 10
	n.HardforkV2.RequireHeight = 20

	// dummy contract
	c1 := contract{
		ContractMetadata: api.ContractMetadata{
			State:          api.ContractStateActive,
			StartHeight:    0,
			WindowStart:    30,
			WindowEnd:      35,
			RevisionNumber: 1,
		},
		Revision: &api.Revision{
			RevisionNumber: 1,
		},
	}

	err := c.shouldArchive(c1, 25, n)
	if err != nil {
		t.Fatal("unexpected error", err)
	}
	err = c.shouldArchive(c1, 26, n)
	if err != errContractExpired {
		t.Fatal("unexpected error", err)
	}

	// max revision number
	c1.Revision.RevisionNumber = math.MaxUint64
	err = c.shouldArchive(c1, 2, n)
	if err != errContractRenewed {
		t.Fatal("unexpected error", err)
	}
	c1.Revision.RevisionNumber = 1

	// max revision number
	c1.RevisionNumber = math.MaxUint64
	err = c.shouldArchive(c1, 2, n)
	if err != errContractRenewed {
		t.Fatal("unexpected error", err)
	}
	c1.RevisionNumber = 1

	// renewed
	c1.RenewedTo = types.FileContractID{1}
	err = c.shouldArchive(c1, 2, n)
	if err != errContractRenewed {
		t.Fatal("unexpected error", err)
	}
	c1.RenewedTo = types.FileContractID{}

	// not confirmed
	c1.State = api.ContractStatePending
	err = c.shouldArchive(c1, ContractConfirmationDeadline, n)
	if err != nil {
		t.Fatal("unexpected error", err)
	}
	err = c.shouldArchive(c1, ContractConfirmationDeadline+1, n)
	if err != errContractNotConfirmed {
		t.Fatal("unexpected error", err)
	}
	c1.State = api.ContractStateActive
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
