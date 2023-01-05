package autopilot

import (
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

	// Mark last scan failed. Should still be online.
	h.Interactions.LastScanSuccess = false
	if !h.IsOnline() {
		t.Fatal("unexpected")
	}

	// Mark previous scan failed as well. Should be offline.
	h.Interactions.PreviousScanSuccess = false
	if h.IsOnline() {
		t.Fatal("unexpected")
	}

	// Mark last scan successful again. Should be online.
	h.Interactions.LastScanSuccess = true
	if !h.IsOnline() {
		t.Fatal("unexpected")
	}
}

func newTestHost(hk consensus.PublicKey, settings *rhpv2.HostSettings) hostdb.Host {
	return hostdb.Host{
		NetAddress: randomIP().String(),
		KnownSince: time.Now(),
		Interactions: hostdb.Interactions{
			TotalScans:          2,
			LastScan:            time.Now().Add(-time.Minute),
			LastScanSuccess:     true,
			PreviousScanSuccess: true,
			Uptime:              10 * time.Minute,
			Downtime:            10 * time.Minute,

			SuccessfulInteractions: 2,
			FailedInteractions:     0,
		},
		PublicKey: hk,
		Settings:  settings,
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
