package autopilot

import (
	"encoding/json"
	"net"
	"testing"
	"time"

	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/internal/consensus"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"lukechampine.com/frand"
)

func TestHost(t *testing.T) {
	hk := randomHostKey()
	h := Host{newTestHost(hk, newTestHostSettings())}

	// assert host is online
	if !h.IsOnline() {
		t.Fatal("unexpected")
	}

	// add two failed scans and assert it's offline
	scan1 := newTestScan(newTestHostSettings(), false)
	scan2 := newTestScan(newTestHostSettings(), false)
	h.Interactions = append(h.Interactions, scan1, scan2)
	if h.IsOnline() {
		t.Fatal("unexpected")
	}

	// assert we get the last known settings
	s, _, found := h.LastKnownSettings()
	if !found || !s.AcceptingContracts {
		t.Fatal("unexpected")
	}

	// add new scan and assert we get the latest settings
	settings := newTestHostSettings()
	settings.AcceptingContracts = false
	scan3 := newTestScan(settings, true)
	h.Interactions = append(h.Interactions, scan3)
	s, _, _ = h.LastKnownSettings()
	if s.AcceptingContracts {
		t.Fatal("unexpected")
	}

	// fetch latest host scans without limit
	var latestSettings rhpv2.HostSettings
	if scans := h.LatestHostScans(0); len(scans) != 4 {
		t.Fatal("unexpected", len(scans))
	}
	if scans := h.LatestHostScans(1); len(scans) != 1 {
		t.Fatal("unexpected", len(scans))
	} else if err := json.Unmarshal(scans[0].Result, &latestSettings); err != nil {
		t.Fatal("unexpected err", err)
	} else if latestSettings.AcceptingContracts {
		t.Fatal("unexpected scan", scans[0])
	}

	// assert is host returns expected outcome
	if h.IsHost("foo") || !h.IsHost(hk.String()) || !h.IsHost(h.NetAddress()) {
		t.Fatal("unexpected")
	}
}

func newTestHost(hk consensus.PublicKey, settings *rhpv2.HostSettings) hostdb.Host {
	return hostdb.Host{
		Announcements: []hostdb.Announcement{{Timestamp: time.Now(), NetAddress: randomIP().String()}},
		Interactions:  []hostdb.Interaction{newTestScan(settings, true)},
		PublicKey:     hk,
	}
}

func newTestHostSettings() *rhpv2.HostSettings {
	return &rhpv2.HostSettings{
		AcceptingContracts: true,
		MaxDuration:        144 * 7 * 12, // 12w
		Version:            "1.5.10",
	}
}

func randomIP() net.IP {
	rawIP := make([]byte, 16)
	frand.Read(rawIP)
	return net.IP(rawIP)
}

func randomHostKey() consensus.PublicKey {
	var hk consensus.PublicKey
	frand.Read(hk[:])
	return hk
}
