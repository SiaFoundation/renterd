package stores

import (
	"context"
	"reflect"
	"testing"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
)

func TestAutopilotStore(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	// assert we have no autopilots
	autopilots, err := ss.Autopilots(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if len(autopilots) != 0 {
		t.Fatal("expected number of autopilots", len(autopilots))
	}

	// create a cfg
	cfg := api.AutopilotConfig{
		Contracts: api.ContractsConfig{
			Allowance:   types.Siacoins(1).Mul64(1e3),
			Amount:      3,
			Period:      144,
			RenewWindow: 72,

			Download: rhpv2.SectorSize * 500,
			Upload:   rhpv2.SectorSize * 500,
			Storage:  rhpv2.SectorSize * 5e3,

			Set: testContractSet,
		},
		Hosts: api.HostsConfig{
			MaxDowntimeHours:  10,
			AllowRedundantIPs: true, // allow for integration tests by default
		},
		Wallet: api.WalletConfig{
			DefragThreshold: 1234,
		},
	}

	// add an autopilot with that config
	err = ss.UpdateAutopilot(context.Background(), api.Autopilot{ID: t.Name(), Config: cfg})
	if err != nil {
		t.Fatal(err)
	}

	// assert we have one autopilot
	autopilots, err = ss.Autopilots(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if len(autopilots) != 1 {
		t.Fatal("expected number of autopilots", len(autopilots))
	}
	autopilot := autopilots[0]

	// assert config
	if !reflect.DeepEqual(autopilot.Config, cfg) {
		t.Fatal("expected autopilot config to be default config")
	}
	if autopilot.CurrentPeriod != 0 {
		t.Fatal("expected current period to be 0")
	}

	// update the autopilot and set a new current period and update the config
	autopilot.CurrentPeriod = 1
	autopilot.Config.Contracts.Amount = 99
	err = ss.UpdateAutopilot(context.Background(), autopilot)
	if err != nil {
		t.Fatal(err)
	}

	// fetch it and assert it was updated
	updated, err := ss.Autopilot(context.Background(), t.Name())
	if err != nil {
		t.Fatal(err)
	}
	if updated.CurrentPeriod != 1 {
		t.Fatal("expected current period to be 1")
	}
	if updated.Config.Contracts.Amount != 99 {
		t.Fatal("expected amount to be 99")
	}
}
