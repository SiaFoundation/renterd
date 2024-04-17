package contractor

import (
	"math"
	"testing"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/test"
)

var cfg = api.AutopilotConfig{
	Contracts: api.ContractsConfig{
		Allowance:   types.Siacoins(1000),
		Amount:      50,
		Period:      144 * 7 * 6,
		RenewWindow: 144 * 7 * 2,

		Download: 1 << 40, // 1 TiB
		Upload:   1 << 40, // 1 TiB
		Storage:  1 << 42, // 4 TiB

		Set: api.DefaultAutopilotID,
	},
	Hosts: api.HostsConfig{
		MaxDowntimeHours:      24 * 7 * 2,
		MinRecentScanFailures: 10,
	},
}

func TestHostScore(t *testing.T) {
	day := 24 * time.Hour

	newHost := func(s rhpv2.HostSettings) api.Host {
		return test.NewHost(test.RandomHostKey(), test.NewHostPriceTable(), s)
	}
	h1 := newHost(test.NewHostSettings())
	h2 := newHost(test.NewHostSettings())

	// assert both hosts score equal
	redundancy := 3.0
	if hostScore(cfg, h1, redundancy) != hostScore(cfg, h2, redundancy) {
		t.Fatal("unexpected")
	}

	// assert age affects the score
	h1.KnownSince = time.Now().Add(-1 * day)
	if hostScore(cfg, h1, redundancy).Score() <= hostScore(cfg, h2, redundancy).Score() {
		t.Fatal("unexpected")
	}

	// assert collateral affects the score
	settings := test.NewHostSettings()
	settings.Collateral = settings.Collateral.Div64(2)
	settings.MaxCollateral = settings.MaxCollateral.Div64(2)
	h1 = newHost(settings) // reset
	if hostScore(cfg, h1, redundancy).Score() <= hostScore(cfg, h2, redundancy).Score() {
		t.Fatal("unexpected")
	}

	// assert interactions affect the score
	h1 = newHost(test.NewHostSettings()) // reset
	h1.Interactions.SuccessfulInteractions++
	if hostScore(cfg, h1, redundancy).Score() <= hostScore(cfg, h2, redundancy).Score() {
		t.Fatal("unexpected")
	}

	// assert uptime affects the score
	h2 = newHost(test.NewHostSettings()) // reset
	h2.Interactions.SecondToLastScanSuccess = false
	if hostScore(cfg, h1, redundancy).Score() <= hostScore(cfg, h2, redundancy).Score() || ageScore(h1) != ageScore(h2) {
		t.Fatal("unexpected")
	}

	// assert version affects the score
	h2Settings := test.NewHostSettings()
	h2Settings.Version = "1.5.6" // lower
	h2 = newHost(h2Settings)     // reset
	if hostScore(cfg, h1, redundancy).Score() <= hostScore(cfg, h2, redundancy).Score() {
		t.Fatal("unexpected")
	}

	// asseret remaining storage affects the score.
	h1 = newHost(test.NewHostSettings()) // reset
	h2.Settings.RemainingStorage = 100
	if hostScore(cfg, h1, redundancy).Score() <= hostScore(cfg, h2, redundancy).Score() {
		t.Fatal("unexpected")
	}

	// assert MaxCollateral affects the score.
	h2 = newHost(test.NewHostSettings()) // reset
	h2.PriceTable.MaxCollateral = types.ZeroCurrency
	if hostScore(cfg, h1, redundancy).Score() <= hostScore(cfg, h2, redundancy).Score() {
		t.Fatal("unexpected")
	}

	// assert price affects the score.
	h2 = newHost(test.NewHostSettings()) // reset
	h2.PriceTable.WriteBaseCost = types.Siacoins(1)
	if hostScore(cfg, h1, redundancy).Score() <= hostScore(cfg, h2, redundancy).Score() {
		t.Fatal("unexpected")
	}
}

func TestPriceAdjustmentScore(t *testing.T) {
	score := func(cpp uint32) float64 {
		t.Helper()
		cfg := api.ContractsConfig{
			Allowance: types.Siacoins(5000),
			Amount:    50,
		}
		return priceAdjustmentScore(types.Siacoins(cpp), cfg)
	}

	// Cost matches budges.
	if s := score(100); s != 0.5 {
		t.Errorf("expected %v but got %v", 0.5, s)
	}

	// Test decreasing values for host cost. Score should go from 0.5 to 1 and
	// be capped at 1.
	round := func(f float64) float64 {
		i := uint64(f * 100.0)
		return float64(i) / 100.0
	}
	if s := round(score(50)); s != 0.56 {
		t.Errorf("expected %v but got %v", 0.56, s)
	}
	if s := round(score(25)); s != 0.68 {
		t.Errorf("expected %v but got %v", 0.64, s)
	}
	if s := round(score(15)); s != 0.84 {
		t.Errorf("expected %v but got %v", 0.84, s)
	}
	if s := round(score(10)); s != 1 {
		t.Errorf("expected %v but got %v", 1, s)
	}
	if s := round(score(1)); s != 1 {
		t.Errorf("expected %v but got %v", 1, s)
	}

	// Test increasing values for host cost. Score should go from 1 towards 0.
	if s := round(score(101)); s != 0.49 {
		t.Errorf("expected %v but got %v", 0.49, s)
	}
	if s := round(score(110)); s != 0.44 {
		t.Errorf("expected %v but got %v", 0.44, s)
	}
	if s := round(score(125)); s != 0.37 {
		t.Errorf("expected %v but got %v", 0.37, s)
	}
	if s := round(score(150)); s != 0.28 {
		t.Errorf("expected %v but got %v", 0.28, s)
	}
	if s := round(score(200)); s != 0.16 {
		t.Errorf("expected %v but got %v", 0.16, s)
	}
	if s := round(score(250)); s != 0.09 {
		t.Errorf("expected %v but got %v", 0.09, s)
	}
	if s := round(score(300)); s != 0.05 {
		t.Errorf("expected %v but got %v", 0.05, s)
	}
}

func TestCollateralScore(t *testing.T) {
	period := uint64(5)
	storageCost := uint64(100)
	score := func(collateral, maxCollateral uint64) float64 {
		t.Helper()
		cfg := api.ContractsConfig{
			Period: period,
		}
		pt := rhpv3.HostPriceTable{
			CollateralCost: types.NewCurrency64(collateral),
			MaxCollateral:  types.NewCurrency64(maxCollateral),
			WriteStoreCost: types.NewCurrency64(storageCost),
		}
		return collateralScore(cfg, pt, rhpv2.SectorSize)
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

func absDiffInt(x, y int) int {
	if x < y {
		return y - x
	}
	return x - y
}
