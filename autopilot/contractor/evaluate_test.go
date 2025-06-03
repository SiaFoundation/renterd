package contractor

import (
	"math"
	"testing"
	"time"

	rhpv4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/v2/api"
	"go.sia.tech/renterd/v2/internal/rhp/v4"
)

func TestOptimiseGougingSetting(t *testing.T) {
	// create 10 hosts that should all be usable
	var hosts []api.Host
	for i := 0; i < 10; i++ {
		hosts = append(hosts, api.Host{
			KnownSince: time.Unix(0, 0),
			V2Settings: rhp.HostSettings{
				HostSettings: rhpv4.HostSettings{
					AcceptingContracts: true,
					MaxCollateral:      types.Siacoins(1000),
					Prices: rhpv4.HostPrices{
						Collateral: types.Siacoins(1),
					},
					ProtocolVersion: [3]uint8{2, 0, 0},
				},
			},
			Interactions: api.HostInteractions{
				Uptime:                  time.Hour * 1000,
				LastScan:                time.Now(),
				LastScanSuccess:         true,
				SecondToLastScanSuccess: true,
				TotalScans:              100,
			},
			LastAnnouncement: time.Unix(0, 0),
			Scanned:          true,
			Blocked:          false,
		})
	}

	// prepare settings that result in all hosts being usable
	cfg := api.AutopilotConfig{
		Contracts: api.ContractsConfig{
			Amount: 10,
		},
	}
	cs := api.ConsensusState{
		BlockHeight:   100,
		LastBlockTime: api.TimeNow(),
		Synced:        true,
	}
	rs := api.RedundancySettings{MinShards: 10, TotalShards: 30}
	gs := api.GougingSettings{
		MaxRPCPrice:           types.Siacoins(1),
		MaxContractPrice:      types.Siacoins(1),
		MaxDownloadPrice:      types.Siacoins(1),
		MaxUploadPrice:        types.Siacoins(1),
		MaxStoragePrice:       types.Siacoins(1),
		HostBlockHeightLeeway: math.MaxInt32,
	}

	// confirm all hosts are usable
	assertUsable := func(n int) {
		t.Helper()
		nUsable := countUsableHosts(cfg, cs, 0, rs, gs, hosts)
		if nUsable != uint64(n) {
			t.Fatalf("expected %v usable hosts, got %v", len(hosts), nUsable)
		}
	}
	assertUsable(len(hosts))

	// Case1: test optimising a field which gets us back to a full set of hosts
	for i := range hosts {
		hosts[i].V2Settings.Prices.StoragePrice = types.Siacoins(uint32(i + 1))
	}
	assertUsable(1)
	if !optimiseGougingSetting(&gs, &gs.MaxStoragePrice, cfg, cs, 0, rs, hosts) {
		t.Fatal("optimising failed")
	}
	assertUsable(len(hosts))
	if gs.MaxStoragePrice.ExactString() != "10164000000000000000000000" { // 10.164 SC
		t.Fatal("unexpected storage price", gs.MaxStoragePrice.ExactString())
	}

	// Case2: test optimising a field where we can't get back to a full set of
	// hosts
	hosts[0].V2Settings.Prices.StoragePrice = types.Siacoins(100000)
	assertUsable(9)
	if optimiseGougingSetting(&gs, &gs.MaxStoragePrice, cfg, cs, 0, rs, hosts) {
		t.Fatal("optimising succeeded")
	}
	if gs.MaxStoragePrice.ExactString() != "41631744000000000000000000000" { // ~41.63 KS
		t.Fatal("unexpected storage price", gs.MaxStoragePrice.ExactString())
	}

	// Case3: force overflow
	for i := range hosts {
		hosts[i].V2Settings.Prices.StoragePrice = types.MaxCurrency
	}
	gs.MaxStoragePrice = types.MaxCurrency.Sub(types.Siacoins(1))
	assertUsable(0)
	if optimiseGougingSetting(&gs, &gs.MaxStoragePrice, cfg, cs, 0, rs, hosts) {
		t.Fatal("optimising succeeded")
	}
	if gs.MaxStoragePrice.ExactString() != "340282366920937463463374607431768211455" { // ~340.3 TS
		t.Fatal("unexpected storage price", gs.MaxStoragePrice.ExactString())
	}
}
