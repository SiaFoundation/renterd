package contractor

import (
	"math"
	"testing"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/gouging"
	"go.sia.tech/renterd/internal/test"
)

var cfg = api.AutopilotConfig{
	Contracts: api.ContractsConfig{
		Amount:      50,
		Period:      144 * 7 * 6,
		RenewWindow: 144 * 7 * 2,

		Download: 1e12, // 1 TB
		Upload:   1e12, // 1 TB
		Storage:  4e12, // 4 TB
	},
	Hosts: api.HostsConfig{
		MaxDowntimeHours:           24 * 7 * 2,
		MaxConsecutiveScanFailures: 10,
	},
}

func TestClampScore(t *testing.T) {
	tests := []struct {
		in  float64
		out float64
	}{
		{
			in:  -1,
			out: 0,
		},
		{
			in:  0,
			out: 0,
		},
		{
			in:  1,
			out: 1,
		},
		{
			in:  1.1,
			out: 1,
		},
		{
			in:  0.05,
			out: minSubScore,
		},
	}
	for _, test := range tests {
		if out := clampScore(test.in); out != test.out {
			t.Errorf("expected %v, got %v", test.out, out)
		}
	}
}

func TestHostScore(t *testing.T) {
	day := 24 * time.Hour

	newHost := func(s rhpv2.HostSettings) api.Host {
		return test.NewHost(test.RandomHostKey(), test.NewHostPriceTable(), s)
	}
	h1 := newHost(test.NewHostSettings())
	h2 := newHost(test.NewHostSettings())
	gs := api.GougingSettings{
		MaxUploadPrice:   types.NewCurrency64(1000000000000),
		MaxStoragePrice:  types.NewCurrency64(3000000000),
		MaxDownloadPrice: types.NewCurrency64(100000000000000),
	}

	// assert both hosts score equal
	redundancy := 3.0
	if hostScore(cfg, gs, h1, redundancy) != hostScore(cfg, gs, h2, redundancy) {
		t.Fatal("unexpected")
	}

	//	// assert age affects the score
	h1.KnownSince = time.Now().Add(-100 * day)
	if hostScore(cfg, gs, h1, redundancy).Score() <= hostScore(cfg, gs, h2, redundancy).Score() {
		t.Fatal("unexpected")
	}

	// assert collateral affects the score
	h1 = newHost(test.NewHostSettings()) // reset
	h1.PriceTable.CollateralCost = h1.PriceTable.CollateralCost.Div64(1000)
	if hostScore(cfg, gs, h1, redundancy).Score() >= hostScore(cfg, gs, h2, redundancy).Score() {
		t.Fatal("unexpected")
	}

	// assert interactions affect the score
	h1 = newHost(test.NewHostSettings()) // reset
	h1.Interactions.SuccessfulInteractions++
	if hostScore(cfg, gs, h1, redundancy).Score() <= hostScore(cfg, gs, h2, redundancy).Score() {
		t.Fatal("unexpected")
	}

	// assert uptime affects the score
	h2 = newHost(test.NewHostSettings()) // reset
	h2.Interactions.SecondToLastScanSuccess = false
	if hostScore(cfg, gs, h1, redundancy).Score() <= hostScore(cfg, gs, h2, redundancy).Score() || ageScore(h1) != ageScore(h2) {
		t.Fatal("unexpected")
	}

	// assert version affects the score
	h2Settings := test.NewHostSettings()
	h2Settings.Version = "1.5.6" // lower
	h2 = newHost(h2Settings)     // reset
	if hostScore(cfg, gs, h1, redundancy).Score() <= hostScore(cfg, gs, h2, redundancy).Score() {
		t.Fatal("unexpected")
	}

	// asseret remaining storage affects the score.
	h1 = newHost(test.NewHostSettings()) // reset
	h2.Settings.RemainingStorage = 100
	if hostScore(cfg, gs, h1, redundancy).Score() <= hostScore(cfg, gs, h2, redundancy).Score() {
		t.Fatal("unexpected")
	}

	// assert MaxCollateral affects the score.
	h2 = newHost(test.NewHostSettings()) // reset
	h2.PriceTable.MaxCollateral = types.ZeroCurrency
	if hostScore(cfg, gs, h1, redundancy).Score() <= hostScore(cfg, gs, h2, redundancy).Score() {
		t.Fatal("unexpected")
	}

	// assert price affects the score.
	h2 = newHost(test.NewHostSettings()) // reset
	h2.PriceTable.WriteBaseCost = types.Siacoins(1)
	if hostScore(cfg, gs, h1, redundancy).Score() <= hostScore(cfg, gs, h2, redundancy).Score() {
		t.Fatal("unexpected")
	}
}

func TestPriceAdjustmentScore(t *testing.T) {
	score := func(mdp, mup, msp uint64) float64 {
		t.Helper()
		pt := rhpv3.HostPriceTable{
			WriteStoreCost:        types.NewCurrency64(50),
			DownloadBandwidthCost: types.NewCurrency64(50),
			UploadBandwidthCost:   types.NewCurrency64(50),
		}
		dppb, _ := gouging.DownloadPricePerByte(pt)
		uppb, _ := gouging.UploadPricePerByte(pt)
		sppb := pt.WriteStoreCost
		return priceAdjustmentScore(dppb, uppb, sppb, api.GougingSettings{
			MaxDownloadPrice: types.NewCurrency64(mdp),
			MaxUploadPrice:   types.NewCurrency64(mup),
			MaxStoragePrice:  types.NewCurrency64(msp),
		})
	}

	// Cost matches budges.

	if s := score(100, 100, 100); s != 0.5 {
		t.Errorf("expected %v but got %v", 0.5, s)
	}

	// Test increasing gouging values. Score should go from 0.5 to 1 and
	// be capped at 1.

	round := func(f float64) float64 {
		i := uint64(f * 100.0)
		return float64(i) / 100.0
	}

	if s := round(score(125, 125, 125)); s != 0.62 {
		t.Fatalf("expected %v but got %v", 0.62, s)
	}

	if s := round(score(150, 150, 150)); s != 0.75 {
		t.Fatalf("expected %v but got %v", 0.75, s)
	}

	if s := round(score(175, 175, 175)); s != 0.87 {
		t.Fatalf("expected %v but got %v", 0.87, s)
	}

	if s := round(score(190, 190, 190)); s != 0.95 {
		t.Fatalf("expected %v but got %v", 0.95, s)
	}

	if s := round(score(200, 200, 200)); s != 1 {
		t.Fatalf("expected %v but got %v", 1, s)
	}

	if s := round(score(1000, 1000, 1000)); s != 1 {
		t.Fatalf("expected %v but got %v", 1, s)
	}

	// Test decreasing gouging values. Score should go from 0.5 towards 0.

	if s := round(score(99, 99, 99)); s != 0.48 {
		t.Fatalf("expected %v but got %v", 0.48, s)
	}

	if s := round(score(90, 90, 90)); s != 0.44 {
		t.Fatalf("expected %v but got %v", 0.44, s)
	}

	if s := round(score(75, 75, 75)); s != 0.33 {
		t.Fatalf("expected %v but got %v", 0.33, s)
	}

	if s := round(score(50, 50, 50)); s != 0.16 {
		t.Fatalf("expected %v but got %v", 0.16, s)
	}

	if s := round(score(25, 25, 25)); s != 0.01 {
		t.Fatalf("expected %v but got %v", 0.01, s)
	}

	if s := round(score(10, 10, 10)); s != 0 {
		t.Fatalf("expected %v but got %v", 0, s)
	}

	// Edge case where gouging is disabled
	if s := round(score(0, 0, 0)); s != 1.0 {
		t.Fatalf("expected %v but got %v", 1.0, s)
	}
}

func TestCollateralScore(t *testing.T) {
	period := uint64(5)
	storageCost := uint64(100)
	score := func(collateral, maxCollateral uint64) float64 {
		t.Helper()
		pt := rhpv3.HostPriceTable{
			CollateralCost: types.NewCurrency64(collateral),
			MaxCollateral:  types.NewCurrency64(maxCollateral),
			WriteStoreCost: types.NewCurrency64(storageCost),
		}
		appendSectorCost := pt.AppendSectorCost(period).Storage
		return collateralScore(appendSectorCost, pt.MaxCollateral, pt.CollateralCost, rhpv2.SectorSize, period)
	}

	round := func(f float64) float64 {
		i := uint64(f * 100.0)
		return float64(i) / 100.0
	}

	// NOTE: with the above settings, the cutoff is at 7500H.
	cutoff := uint64(storageCost * rhpv2.SectorSize * period * 3 / 2)
	cutoffCollateral := storageCost * 3 / 2

	// Collateral is exactly at cutoff.
	if s := round(score(math.MaxInt64, cutoff)); s != 0.24 {
		t.Errorf("expected %v but got %v", 0.24, s)
	}
	if s := round(score(cutoffCollateral, math.MaxInt64)); s != 0.24 {
		t.Errorf("expected %v but got %v", 0.24, s)
	}

	// Increase collateral with linear steps. Score should approach linearly as
	// well.
	// 1.5 times cutoff
	if s := round(score(cutoffCollateral*3/2, math.MaxInt64)); s != 0.37 {
		t.Errorf("expected %v but got %v", 0.37, s)
	}
	// 2 times cutoff
	if s := round(score(2*cutoffCollateral, math.MaxInt64)); s != 0.49 {
		t.Errorf("expected %v but got %v", 0.49, s)
	}
	// 2.5 times cutoff
	if s := round(score(cutoffCollateral*5/2, math.MaxInt64)); s != 0.62 {
		t.Errorf("expected %v but got %v", 0.62, s)
	}
	// 3 times cutoff
	if s := round(score(3*cutoffCollateral, math.MaxInt64)); s != 0.74 {
		t.Errorf("expected %v but got %v", 0.74, s)
	}
	// 3.5 times cutoff
	if s := round(score(cutoffCollateral*7/2, math.MaxInt64)); s != 0.87 {
		t.Errorf("expected %v but got %v", 0.87, s)
	}
	// 4 times cutoff
	if s := round(score(4*cutoffCollateral, math.MaxInt64)); s != 1 {
		t.Errorf("expected %v but got %v", 1, s)
	}

	// Going below the cutoff should result in a score of 0.
	if s := round(score(cutoffCollateral-1, math.MaxInt64)); s != 0 {
		t.Errorf("expected %v but got %v", 0, s)
	}
}
