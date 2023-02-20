package autopilot

import (
	"net"
	"testing"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/hostdb"
	"lukechampine.com/frand"
)

func TestHost(t *testing.T) {
	hk := randomHostKey()
	h := newTestHost(hk, newTestHostSettings())

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
	h.Interactions.SecondToLastScanSuccess = false
	if h.IsOnline() {
		t.Fatal("unexpected")
	}

	// Mark last scan successful again. Should be online.
	h.Interactions.LastScanSuccess = true
	if !h.IsOnline() {
		t.Fatal("unexpected")
	}
}

func newTestHosts(n int) []hostdb.Host {
	hosts := make([]hostdb.Host, n)
	for i := 0; i < n; i++ {
		hosts[i] = newTestHost(randomHostKey(), newTestHostSettings())
	}
	return hosts
}

func newTestHost(hk types.PublicKey, settings *rhpv2.HostSettings) hostdb.Host {
	return hostdb.Host{
		NetAddress: randomIP().String(),
		KnownSince: time.Now(),
		Interactions: hostdb.Interactions{
			TotalScans:              2,
			LastScan:                time.Now().Add(-time.Minute),
			LastScanSuccess:         true,
			SecondToLastScanSuccess: true,
			Uptime:                  10 * time.Minute,
			Downtime:                10 * time.Minute,

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
		MaxCollateral:      types.Siacoins(10000),
		MaxDuration:        144 * 7 * 12, // 12w
		Version:            "1.5.10",
		RemainingStorage:   1 << 42, // 4 TiB
	}
}

func randomIP() net.IP {
	rawIP := make([]byte, 16)
	frand.Read(rawIP)
	return net.IP(rawIP)
}

func randomHostKey() types.PublicKey {
	var hk types.PublicKey
	frand.Read(hk[:])
	return hk
}
