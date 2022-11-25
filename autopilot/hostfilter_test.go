package autopilot

import (
	"encoding/json"
	"math"
	"net"
	"testing"
	"time"

	"gitlab.com/NebulousLabs/fastrand"
	"go.sia.tech/renterd/bus"
	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/internal/consensus"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"go.sia.tech/siad/types"
)

type testResolver struct {
	cache map[string][]net.IP
}

func (r testResolver) LookupIP(host string) ([]net.IP, error) {
	if _, exists := r.cache[host]; !exists {
		rawIP := make([]byte, 16)
		fastrand.Read(rawIP)
		r.cache[host] = []net.IP{net.IP(rawIP)}
	}

	return r.cache[host], nil
}

func TestHostFilter(t *testing.T) {
	r := testResolver{cache: make(map[string][]net.IP)}
	m := newTestMetadata()
	h1 := newTestHost("foo.bar:5882")
	h2 := newTestHost("bar.baz:5882")
	cfg := DefaultConfig()
	ipf := newTestIPFilter(&r)

	isGFU := func(m bus.ContractMetadata) bool { return m.GoodForUpload }
	isGFR := func(m bus.ContractMetadata) bool { return m.GoodForRenew }

	// base case
	md, reasons, filtered := newHostFilter(h1, m).finalize()
	if !isGFU(md) || !isGFR(md) || filtered || len(reasons) != 0 {
		t.Fatal("unexpected")
	}

	// whitelist filter - on list
	cfg.Hosts.Whitelist = []string{h1.NetAddress()}
	md, _, filtered = newHostFilter(h1, m).withWhiteListFilter(cfg).finalize()
	if !isGFU(md) || !isGFR(md) || filtered {
		t.Fatal("unexpected")
	}

	// whitelist filter - not on list
	cfg.Hosts.Whitelist = []string{"somehost"}
	md, _, filtered = newHostFilter(h1, m).withWhiteListFilter(cfg).finalize()
	if isGFU(md) || isGFR(md) || !filtered {
		t.Fatal("unexpected")
	}

	// blacklist filter - not on list
	cfg.Hosts.Blacklist = []string{"somehost"}
	md, _, filtered = newHostFilter(h1, m).withBlackListFilter(cfg).finalize()
	if !isGFU(md) || !isGFR(md) || filtered {
		t.Fatal("unexpected")
	}

	// blacklist filter - on list
	cfg.Hosts.Blacklist = append(cfg.Hosts.Blacklist, h1.NetAddress())
	md, _, filtered = newHostFilter(h1, m).withBlackListFilter(cfg).finalize()
	if isGFU(md) || isGFR(md) || !filtered {
		t.Fatal("unexpected")
	}

	// max revision filter - not max
	md, _, filtered = newHostFilter(h1, m).withMaxRevisionFilter().finalize()
	if !isGFU(md) || !isGFR(md) || filtered {
		t.Fatal("unexpected")
	}

	// max revision filter - max
	h1.Contract.Revision.NewRevisionNumber = math.MaxUint64
	md, _, filtered = newHostFilter(h1, m).withMaxRevisionFilter().finalize()
	if isGFU(md) || isGFR(md) || !filtered {
		t.Fatal("unexpected")
	}

	// offline filter - host online
	md, _, filtered = newHostFilter(h1, m).withOfflineFilter().finalize()
	if !isGFU(md) || !isGFR(md) || filtered {
		t.Fatal("unexpected")
	}

	// offline filter - host offline
	h1.Interactions = append(h1.Interactions, newTestScan(false), newTestScan(false))
	md, _, filtered = newHostFilter(h1, m).withOfflineFilter().finalize()
	if isGFU(md) || isGFR(md) || !filtered {
		t.Fatal("unexpected")
	}

	// redundant IP filter - IP not redundant
	md, _, filtered = newHostFilter(h1, m).withRedundantIPFilter(ipf).finalize()
	if !isGFU(md) || !isGFR(md) || filtered {
		t.Fatal("unexpected")
	}

	// redundant IP filter - IP redundant
	host1, _, _ := net.SplitHostPort(h1.NetAddress())
	host2, _, _ := net.SplitHostPort(h2.NetAddress())
	r.cache[host2] = r.cache[host1] // have h2 use h1's IP by manipulating the resolver cache
	md, _, filtered = newHostFilter(h2, m).withRedundantIPFilter(ipf).finalize()
	if isGFU(md) || isGFR(md) || !filtered {
		t.Fatal("unexpected")
	}

	// remaining funds filter - enough remaining
	m.TotalCost = types.SiacoinPrecision
	h1.Revision.NewValidProofOutputs = []types.SiacoinOutput{{Value: m.TotalCost}}
	md, _, filtered = newHostFilter(h1, m).withRemainingFundsFilter(cfg).finalize()
	if !isGFU(md) || !isGFR(md) || filtered {
		t.Fatal("unexpected")
	}

	// remaining funds filter - out of funds
	h1.Revision.NewValidProofOutputs = []types.SiacoinOutput{{Value: m.TotalCost.MulFloat(minContractFundUploadThreshold - .01)}}
	md, _, filtered = newHostFilter(h1, m).withRemainingFundsFilter(cfg).finalize()
	if isGFU(md) || !isGFR(md) || !filtered {
		t.Fatal("unexpected")
	}

	// score filter - good score
	md, _, filtered = newHostFilter(h1, m).withScoreFilter(cfg, 0).finalize()
	if !isGFU(md) || !isGFR(md) || filtered {
		t.Fatal("unexpected")
	}

	// score filter - bad score
	md, _, filtered = newHostFilter(h1, m).withScoreFilter(cfg, 0.1).finalize()
	if isGFU(md) || isGFR(md) || !filtered {
		t.Fatal("unexpected")
	}

	// up for renewal filter - not up for renewal
	blockHeight := uint64(0)
	h1.Contract.Revision.NewWindowStart = (144 * 7) + 1
	md, _, filtered = newHostFilter(h1, m).withUpForRenewalFilter(cfg, blockHeight).finalize()
	if !isGFU(md) || !isGFR(md) || filtered {
		t.Fatal("unexpected")
	}

	// up for renewal filter - up for renewal
	h1.Contract.Revision.NewWindowStart -= 1
	md, _, filtered = newHostFilter(h1, m).withUpForRenewalFilter(cfg, blockHeight).finalize()
	if isGFU(md) || !isGFR(md) || !filtered {
		t.Fatal("unexpected")
	}

	// apply all filters and assert reasons
	md, reasons, _ = newHostFilter(h1, m).
		withWhiteListFilter(cfg).
		withBlackListFilter(cfg).
		withMaxRevisionFilter().
		withOfflineFilter().
		withRedundantIPFilter(ipf).
		withRemainingFundsFilter(cfg).
		withScoreFilter(cfg, .1).
		withUpForRenewalFilter(cfg, blockHeight).
		finalize()

	if isGFU(md) || isGFR(md) || len(reasons) != 8 {
		t.Fatal("unexpected")
	}
}

func newTestHost(address string) host {
	return host{
		hostdb.Host{
			PublicKey: consensus.PublicKey{1},
			Announcements: []hostdb.Announcement{{
				Index:      consensus.ChainIndex{},
				Timestamp:  time.Now(),
				NetAddress: address,
			}},
			Interactions: []hostdb.Interaction{newTestScan(true)},
		},
		rhpv2.Contract{},
	}
}

func newTestMetadata() bus.ContractMetadata {
	return bus.ContractMetadata{
		GoodForUpload: true,
		GoodForRenew:  true,
	}
}

func newTestScan(success bool) hostdb.Interaction {
	js, _ := json.Marshal(rhpv2.HostSettings{})
	return hostdb.Interaction{
		Timestamp: time.Now(),
		Type:      hostdb.InteractionTypeScan,
		Success:   success,
		Result:    json.RawMessage(js),
	}
}

func newTestIPFilter(r resolver) *ipFilter {
	return &ipFilter{
		subnets:  make(map[string]string),
		resolver: r,
	}
}
