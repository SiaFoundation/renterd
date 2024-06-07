package contractor

import (
	"math"
	"testing"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"lukechampine.com/frand"
)

func TestScoredHostsRandSelectByScore(t *testing.T) {
	hostToScores := map[types.PublicKey]float64{
		{1}: math.SmallestNonzeroFloat64,
		{2}: .1,
		{3}: .2,
	}

	var hosts scoredHosts
	for hk, score := range hostToScores {
		hosts = append(hosts, scoredHost{score: score, host: api.Host{PublicKey: hk}})
	}

	for i := 0; i < 1000; i++ {
		seen := make(map[types.PublicKey]struct{})
		for _, h := range hosts.randSelectByScore(3) {
			// assert we get non-normalized scores
			if hostToScores[h.host.PublicKey] != h.score {
				t.Fatal("unexpected")
			}

			// assert we never select the same host twice
			if _, seen := seen[h.host.PublicKey]; seen {
				t.Fatal("unexpected")
			}
			seen[h.host.PublicKey] = struct{}{}
		}

		// assert min float is never selected
		frand.Shuffle(len(hosts), func(i, j int) { hosts[i], hosts[j] = hosts[j], hosts[i] })
		if hosts.randSelectByScore(1)[0].score == math.SmallestNonzeroFloat64 {
			t.Fatal("unexpected")
		}
	}

	// assert we can pass any value for n
	if len(hosts.randSelectByScore(0)) != 0 {
		t.Fatal("unexpected")
	} else if len(hosts.randSelectByScore(-1)) != 0 {
		t.Fatal("unexpected")
	} else if len(hosts.randSelectByScore(4)) != 3 {
		t.Fatal("unexpected")
	}

	// assert select is random on equal inputs, we calculate the chi-square
	// statistic and assert it's less than critical value of 6.635 (1 degree of
	// freedom, using alpha of .01)
	var counts [2]int
	hosts = scoredHosts{
		{score: .1, host: api.Host{PublicKey: types.PublicKey{1}}},
		{score: .1, host: api.Host{PublicKey: types.PublicKey{2}}},
	}
	nRuns := 1e5
	for i := 0; i < int(nRuns); i++ {
		if hosts.randSelectByScore(1)[0].host.PublicKey == (types.PublicKey{1}) {
			counts[0]++
		} else {
			counts[1]++
		}
	}
	var chi2 float64
	for i := 0; i < 2; i++ {
		chi2 += math.Pow(float64(counts[i])-nRuns/2, 2) / (nRuns / 2)
	}
	if chi2 > 6.635 {
		t.Fatal("unexpected", counts[0], counts[1], chi2)
	}
}
