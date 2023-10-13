package autopilot

import (
	"math"
	"testing"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/hostdb"
	"lukechampine.com/frand"
)

func TestScoredHostsRandSelectByScore(t *testing.T) {
	hosts := scoredHosts{
		{score: math.SmallestNonzeroFloat64, host: hostdb.Host{PublicKey: types.PublicKey{1}}},
		{score: .1, host: hostdb.Host{PublicKey: types.PublicKey{2}}},
		{score: .2, host: hostdb.Host{PublicKey: types.PublicKey{3}}},
	}

	// assert we can pass any value for n
	if len(hosts.randSelectByScore(0)) != 0 {
		t.Fatal("unexpected")
	} else if len(hosts.randSelectByScore(-1)) != 0 {
		t.Fatal("unexpected")
	} else if len(hosts.randSelectByScore(4)) != 3 {
		t.Fatal("unexpected")
	}

	// assert we never select the same host twice
	for i := 0; i < 100; i++ {
		seen := make(map[types.PublicKey]struct{})
		selected := hosts.randSelectByScore(3)
		for _, h := range selected {
			if _, ok := seen[h.host.PublicKey]; ok {
				for _, s := range selected {
					t.Log(s.host.PublicKey)
				}
				t.Fatal("unexpected", i)
			}
			seen[h.host.PublicKey] = struct{}{}
		}
	}

	// assert input is deep copied
	if hosts[0].score != math.SmallestNonzeroFloat64 || hosts[1].score != .1 || hosts[2].score != .2 {
		t.Fatal("unexpected")
	}

	// assert min float is never selected
	for i := 0; i < 100; i++ {
		frand.Shuffle(len(hosts), func(i, j int) { hosts[i], hosts[j] = hosts[j], hosts[i] })
		if hosts.randSelectByScore(1)[0].score == math.SmallestNonzeroFloat64 {
			t.Fatal("unexpected")
		}
	}

	// assert select is random on equal inputs
	counts := make([]int, 2)
	hosts = scoredHosts{
		{score: .1, host: hostdb.Host{PublicKey: types.PublicKey{1}}},
		{score: .1, host: hostdb.Host{PublicKey: types.PublicKey{2}}},
	}
	for i := 0; i < 100; i++ {
		if hosts.randSelectByScore(1)[0].host.PublicKey == (types.PublicKey{1}) {
			counts[0]++
		} else {
			counts[1]++
		}
	}
	if diff := absDiffInt(counts[0], counts[1]); diff > 40 {
		t.Fatal("unexpected", counts[0], counts[1], diff)
	}
}
