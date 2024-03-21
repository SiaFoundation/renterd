package autopilot

import (
	"math"
	"testing"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/hostdb"
)

func TestOptimiseGougingSetting(t *testing.T) {
	// create 10 hosts that should all be usable
	var hosts []api.Host
	for i := 0; i < 10; i++ {
		hosts = append(hosts, api.Host{

			Host: hostdb.Host{
				KnownSince: time.Unix(0, 0),
				PriceTable: hostdb.HostPriceTable{
					HostPriceTable: rhpv3.HostPriceTable{
						CollateralCost: types.Siacoins(1),
						MaxCollateral:  types.Siacoins(1000),
					},
				},
				Settings: rhpv2.HostSettings{
					AcceptingContracts: true,
					Collateral:         types.Siacoins(1),
					MaxCollateral:      types.Siacoins(1000),
					Version:            "1.6.0",
				},
				Interactions: hostdb.Interactions{
					Uptime:                  time.Hour * 1000,
					LastScan:                time.Now(),
					LastScanSuccess:         true,
					SecondToLastScanSuccess: true,
					TotalScans:              100,
				},
				LastAnnouncement: time.Unix(0, 0),
				Scanned:          true,
			},
			Blocked: false,
			Checks:  nil,
		})
	}

	// prepare settings that result in all hosts being usable
	cfg := api.AutopilotConfig{
		Contracts: api.ContractsConfig{
			Allowance: types.Siacoins(100000),
			Amount:    10,
		},
		Hosts: api.HostsConfig{},
	}
	cs := api.ConsensusState{
		BlockHeight:   100,
		LastBlockTime: api.TimeNow(),
		Synced:        true,
	}
	fee := types.ZeroCurrency
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
		nUsable := countUsableHosts(cfg, cs, fee, 0, rs, gs, hosts)
		if nUsable != uint64(n) {
			t.Fatalf("expected %v usable hosts, got %v", len(hosts), nUsable)
		}
	}
	assertUsable(len(hosts))

	// Case1: test optimising a field which gets us back to a full set of hosts
	for i := range hosts {
		hosts[i].Settings.StoragePrice = types.Siacoins(uint32(i + 1))
	}
	assertUsable(1)
	if !optimiseGougingSetting(&gs, &gs.MaxStoragePrice, cfg, cs, fee, 0, rs, hosts) {
		t.Fatal("optimising failed")
	}
	assertUsable(len(hosts))
	if gs.MaxStoragePrice.ExactString() != "10164000000000000000000000" { // 10.164 SC
		t.Fatal("unexpected storage price", gs.MaxStoragePrice.ExactString())
	}

	// Case2: test optimising a field where we can't get back to a full set of
	// hosts
	hosts[0].Settings.StoragePrice = types.Siacoins(100000)
	assertUsable(9)
	if optimiseGougingSetting(&gs, &gs.MaxStoragePrice, cfg, cs, fee, 0, rs, hosts) {
		t.Fatal("optimising succeeded")
	}
	if gs.MaxStoragePrice.ExactString() != "41631744000000000000000000000" { // ~41.63 KS
		t.Fatal("unexpected storage price", gs.MaxStoragePrice.ExactString())
	}

	// Case3: force overflow
	for i := range hosts {
		hosts[i].Settings.StoragePrice = types.MaxCurrency
	}
	gs.MaxStoragePrice = types.MaxCurrency.Sub(types.Siacoins(1))
	assertUsable(0)
	if optimiseGougingSetting(&gs, &gs.MaxStoragePrice, cfg, cs, fee, 0, rs, hosts) {
		t.Fatal("optimising succeeded")
	}
	if gs.MaxStoragePrice.ExactString() != "340282366920937463463374607431768211455" { // ~340.3 TS
		t.Fatal("unexpected storage price", gs.MaxStoragePrice.ExactString())
	}
}
