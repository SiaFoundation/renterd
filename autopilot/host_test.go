package autopilot

import (
	"net"
	"testing"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/hostdb"
	"lukechampine.com/frand"
)

func TestHost(t *testing.T) {
	hk := randomHostKey()
	h := newTestHost(hk, newTestHostPriceTable(), newTestHostSettings())

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
		hosts[i] = newTestHost(randomHostKey(), newTestHostPriceTable(), newTestHostSettings())
	}
	return hosts
}

func newTestHost(hk types.PublicKey, pt *rhpv3.HostPriceTable, settings *rhpv2.HostSettings) hostdb.Host {
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
		PublicKey:  hk,
		PriceTable: pt,
		Settings:   settings,
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

func newTestHostPriceTable() *rhpv3.HostPriceTable {
	oneSC := types.Siacoins(1)

	dlbwPrice := oneSC.Mul64(25).Div64(1 << 40) // 25 SC / TiB
	ulbwPrice := oneSC.Div64(1 << 40)           // 1 SC / TiB

	return &rhpv3.HostPriceTable{
		Validity:             time.Minute,
		FundAccountCost:      oneSC,
		UpdatePriceTableCost: oneSC,

		HasSectorBaseCost:  oneSC,
		InitBaseCost:       oneSC,
		MemoryTimeCost:     oneSC,
		ReadBaseCost:       oneSC,
		ReadLengthCost:     oneSC,
		SwapSectorBaseCost: oneSC,

		DownloadBandwidthCost: dlbwPrice,
		UploadBandwidthCost:   ulbwPrice,

		WriteBaseCost:   oneSC,
		WriteLengthCost: oneSC,
		WriteStoreCost:  oneSC,
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
