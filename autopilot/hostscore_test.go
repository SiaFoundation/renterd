package autopilot

import (
	"encoding/json"
	"math"
	"testing"
	"time"

	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/internal/consensus"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"go.sia.tech/siad/types"
	"lukechampine.com/frand"
)

func TestHostScore(t *testing.T) {
	cfg := DefaultConfig()
	day := 24 * time.Hour

	h1 := newTestHost(newTestHostSettings())
	h2 := newTestHost(newTestHostSettings())

	// assert both hosts score equal
	if hostScore(cfg, h1) != hostScore(cfg, h2) {
		t.Fatal("unexpected")
	}

	// assert age affects the score
	h1.Announcements[0].Timestamp = time.Now().Add(-1 * day)
	if hostScore(cfg, h1) <= hostScore(cfg, h2) {
		t.Fatal("unexpected")
	}

	// assert collateral affects the score
	settings := newTestHostSettings()
	settings.Collateral = types.NewCurrency64(1)
	settings.MaxCollateral = types.NewCurrency64(10)
	h1 = newTestHost(settings) // reset
	if hostScore(cfg, h1) <= hostScore(cfg, h2) {
		t.Fatal("unexpected")
	}

	// assert interactions affect the score
	h1 = newTestHost(newTestHostSettings()) // reset
	h1.Interactions = append(h1.Interactions, newTestScan(newTestHostSettings()))
	if hostScore(cfg, h1) <= hostScore(cfg, h2) {
		t.Fatal("unexpected")
	}

	// assert uptime affects the score
	h2 = newTestHost(newTestHostSettings())
	h2.Interactions[0].Success = false
	if hostScore(cfg, h1) <= hostScore(cfg, h2) || ageScore(h1) != ageScore(h2) {
		t.Fatal("unexpected")
	}

	// assert version affects the score
	h2Settings := newTestHostSettings()
	h2Settings.Version = "1.5.6" // lower
	h2 = newTestHost(h2Settings) // reset
	if hostScore(cfg, h1) <= hostScore(cfg, h2) {
		t.Fatal("unexpected")
	}
}

func TestRandSelectByWeight(t *testing.T) {
	// assert min float is never selected
	weights := []float64{.1, .2, math.SmallestNonzeroFloat64}
	for i := 0; i < 100; i++ {
		frand.Shuffle(len(weights), func(i, j int) { weights[i], weights[j] = weights[j], weights[i] })
		if weights[randSelectByWeight(weights)] == math.SmallestNonzeroFloat64 {
			t.Fatal("unexpected")
		}
	}

	// assert select is random on equal inputs
	counts := make([]int, 2)
	weights = []float64{.1, .1}
	for i := 0; i < 100; i++ {
		counts[randSelectByWeight(weights)]++
	}
	if diff := absDiffInt(counts[0], counts[1]); diff > 40 {
		t.Fatal("unexpected", counts[0], counts[1], diff)
	}
}

func newTestHost(settings *rhpv2.HostSettings) Host {
	return Host{
		hostdb.Host{
			Announcements: []hostdb.Announcement{{Timestamp: time.Now()}},
			Interactions:  []hostdb.Interaction{newTestScan(settings)},
			PublicKey:     consensus.PublicKey{1},
		},
	}
}

func newTestHostSettings() *rhpv2.HostSettings {
	return &rhpv2.HostSettings{
		AcceptingContracts: true,
		MaxDuration:        144 * 7 * 12, // 12w
		Version:            "1.5.10",
	}
}

func newTestScan(settings *rhpv2.HostSettings) hostdb.Interaction {
	js, err := json.Marshal(settings)
	if err != nil {
		panic(err)
	}
	return hostdb.Interaction{
		Timestamp: time.Now(),
		Type:      "scan",
		Success:   true,
		Result:    json.RawMessage(js),
	}
}

func absDiffInt(x, y int) int {
	if x < y {
		return y - x
	}
	return x - y
}
