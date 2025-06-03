package test

import (
	"net"
	"time"

	rhpv4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/v2/api"
	rhp4 "go.sia.tech/renterd/v2/internal/rhp/v4"
	"lukechampine.com/frand"
)

func NewHosts(n int) []api.Host {
	hosts := make([]api.Host, n)
	for i := 0; i < n; i++ {
		hosts[i] = NewV2Host(RandomHostKey(), NewV2HostSettings())
	}
	return hosts
}

func NewV2Host(hk types.PublicKey, settings rhp4.HostSettings) api.Host {
	return api.Host{
		V2SiamuxAddresses: []string{randomIP().String()},
		KnownSince:        time.Now(),
		LastAnnouncement:  time.Now(),
		Interactions: api.HostInteractions{
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
		V2Settings: settings,
		Scanned:    true,
	}
}

func NewV2HostSettings() rhp4.HostSettings {
	oneSC := types.Siacoins(1)

	return rhp4.HostSettings{
		HostSettings: rhpv4.HostSettings{
			AcceptingContracts:  true,
			MaxCollateral:       types.Siacoins(10000),
			MaxContractDuration: 144 * 7 * 12, // 12w
			Prices: rhpv4.HostPrices{
				Collateral:      types.Siacoins(1).Div64(1 << 40),
				ContractPrice:   types.Siacoins(1),
				StoragePrice:    oneSC.Div64(4032).Div64(1 << 40), // 1 SC / TiB / month
				EgressPrice:     oneSC.Mul64(25).Div64(1 << 40),   // 25 SC / TiB
				IngressPrice:    oneSC.Div64(1 << 40),             // 1 SC / TiB
				FreeSectorPrice: types.NewCurrency64(1),
				TipHeight:       0,
				ValidUntil:      time.Now().Add(time.Minute),
			},
			ProtocolVersion:  [3]uint8{1, 0, 0},
			RemainingStorage: 1 << 42 / uint64(rhpv4.SectorSize), // 4 TiB
		},
		Validity: time.Minute,
	}
}

func RandomHostKey() types.PublicKey {
	var hk types.PublicKey
	frand.Read(hk[:])
	return hk
}

func randomIP() net.IP {
	rawIP := make([]byte, 16)
	frand.Read(rawIP)
	return net.IP(rawIP)
}
