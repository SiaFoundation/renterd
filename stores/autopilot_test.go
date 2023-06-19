package stores

import (
	"context"
	"reflect"
	"testing"

	"go.sia.tech/renterd/api"
)

func TestAutopilotStore(t *testing.T) {
	db, _, _, err := newTestSQLStore()
	if err != nil {
		t.Fatal(err)
	}

	// assert we have no autopilots
	autopilots, err := db.Autopilots(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if len(autopilots) != 0 {
		t.Fatal("expected number of autopilots", len(autopilots))
	}

	// add an autopilot with default config
	err = db.UpdateAutopilot(context.Background(), api.Autopilot{ID: "autopilot", Config: api.DefaultAutopilotConfig()})
	if err != nil {
		t.Fatal(err)
	}

	// assert we have one autopilot
	autopilots, err = db.Autopilots(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if len(autopilots) != 1 {
		t.Fatal("expected number of autopilots", len(autopilots))
	}
	autopilot := autopilots[0]

	// assert config
	if !reflect.DeepEqual(autopilot.Config, api.DefaultAutopilotConfig()) {
		t.Fatal("expected autopilot config to be default config")
	}
	if autopilot.CurrentPeriod != 0 {
		t.Fatal("expected current period to be 0")
	}

	// update the autopilot and set a new current period and update the config
	autopilot.CurrentPeriod = 1
	autopilot.Config.Contracts.Amount = 99
	err = db.UpdateAutopilot(context.Background(), autopilot)
	if err != nil {
		t.Fatal(err)
	}

	// fetch it and assert it was updated
	updated, err := db.Autopilot(context.Background(), "autopilot")
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
