package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/bus"
	"go.sia.tech/renterd/internal/test"
	"go.sia.tech/renterd/webhooks"
)

// TestEvents is a test that verifies the bus sends webhooks for certain events,
// providing an event webhook was registered.
func TestEvents(t *testing.T) {
	// list all events
	allEvents := []bus.Event{
		api.EventConsensusUpdate{},
		api.EventContractArchived{},
		api.EventContractRenewed{},
		api.EventContractSetUpdate{},
		api.EventSettingUpdate{},
	}

	// define helper to check if the event is known
	isKnownEvent := func(e webhooks.Event) bool {
		for _, known := range allEvents {
			module, event := known.Kind()
			if module == e.Module && event == e.Event {
				return true
			}
		}
		return false
	}

	// define a small helper to keep track of received events
	var mu sync.Mutex
	received := make(map[string]webhooks.Event)
	receiveEvent := func(event webhooks.Event) error {
		// ignore pings
		if event.Event == webhooks.WebhookEventPing {
			return nil
		}

		// check if the event is expected
		if !isKnownEvent(event) {
			return fmt.Errorf("unexpected event %+v", event)
		}

		// keep track of the event
		mu.Lock()
		defer mu.Unlock()
		key := event.Module + "_" + event.Event
		if _, ok := received[key]; !ok {
			received[key] = event
		}
		return nil
	}

	// setup test server to receive webhooks
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var webhook webhooks.Event
		if err := json.NewDecoder(r.Body).Decode(&webhook); err != nil {
			t.Fatal(err)
		} else if err := receiveEvent(webhook); err != nil {
			t.Fatal(err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// setup test cluster
	cluster := newTestCluster(t, testClusterOptions{hosts: 1})
	defer cluster.Shutdown()
	b := cluster.Bus
	tt := cluster.tt

	// register webhooks
	for _, e := range allEvents {
		tt.OK(b.RegisterWebhook(context.Background(), bus.NewEventWebhook(server.URL, e)))
	}

	// fetch our contract
	contracts, err := b.Contracts(context.Background(), api.ContractsOpts{})
	tt.OK(err)
	if len(contracts) != 1 {
		tt.Fatalf("expected 1 contract, got %v", len(contracts))
	}
	c := contracts[0]

	// mine blocks to update consensus & to renew
	cluster.MineToRenewWindow()

	// wait until our contract got renewed
	var renewed api.ContractMetadata
	tt.Retry(10, time.Second, func() (err error) {
		renewed, err = b.RenewedContract(context.Background(), c.ID)
		return err
	})

	// archive the renewal
	tt.OK(b.ArchiveContracts(context.Background(), map[types.FileContractID]string{renewed.ID: t.Name()}))

	// fetch current gouging params
	gp, err := b.GougingParams(context.Background())
	tt.OK(err)

	// update settings
	gs := gp.GougingSettings
	gs.HostBlockHeightLeeway = 100
	b.UpdateSetting(context.Background(), api.SettingGouging, gs)

	// wait until we received the events
	tt.Retry(10, time.Second, func() error {
		mu.Lock()
		defer mu.Unlock()
		if len(received) < len(allEvents) {
			cluster.MineBlocks(1)
			return fmt.Errorf("expected %d unique events, got %v", len(allEvents), len(received))
		}
		return nil
	})

	// assert the events we received contain the expected information
	for _, r := range received {
		event, err := parseEvent(r)
		tt.OK(err)
		switch e := event.(type) {
		case api.EventContractRenewed:
			if e.ContractID != renewed.ID || e.RenewedFromID != c.ID || e.Timestamp.IsZero() {
				t.Fatalf("unexpected event %+v", e)
			}
		case api.EventContractArchived:
			if e.ContractID != renewed.ID || e.Reason != t.Name() || e.Timestamp.IsZero() {
				t.Fatalf("unexpected event %+v", e)
			}
		case api.EventContractSetUpdate:
			if e.Name != test.ContractSet || len(e.ContractIDs) != 1 || e.ContractIDs[0] != c.ID || e.Timestamp.IsZero() {
				t.Fatalf("unexpected event %+v", e)
			}
		case api.EventConsensusUpdate:
			if e.TransactionFee.IsZero() || e.BlockHeight == 0 || e.Timestamp.IsZero() || !e.Synced {
				t.Fatalf("unexpected event %+v", e)
			}
		case api.EventSettingUpdate:
			if e.Key != api.SettingGouging || e.Timestamp.IsZero() {
				t.Fatalf("unexpected event %+v", e)
			}
			var update api.GougingSettings
			bytes, _ := json.Marshal(e.Update)
			tt.OK(json.Unmarshal(bytes, &update))
			if update.HostBlockHeightLeeway != 100 {
				t.Fatalf("unexpected update %+v", update)
			}
		}
	}
}

func parseEvent(event webhooks.Event) (interface{}, error) {
	bytes, err := json.Marshal(event.Payload)
	if err != nil {
		return nil, err
	}
	switch event.Module {
	case api.ModuleContracts:
		if event.Event == api.EventArchived {
			var e api.EventContractArchived
			if err := json.Unmarshal(bytes, &e); err != nil {
				return nil, err
			}
			return e, nil
		} else if event.Event == api.EventRenewed {
			var e api.EventContractRenewed
			if err := json.Unmarshal(bytes, &e); err != nil {
				return nil, err
			}
			return e, nil
		}
	case api.ModuleContractSet:
		if event.Event == api.EventUpdated {
			var e api.EventContractSetUpdate
			if err := json.Unmarshal(bytes, &e); err != nil {
				return nil, err
			}
			return e, nil
		}
	case api.ModuleConsensus:
		if event.Event == api.EventUpdated {
			var e api.EventConsensusUpdate
			if err := json.Unmarshal(bytes, &e); err != nil {
				return nil, err
			}
			return e, nil
		}
	case api.ModuleSettings:
		if event.Event == api.EventUpdated {
			var e api.EventSettingUpdate
			if err := json.Unmarshal(bytes, &e); err != nil {
				return nil, err
			}
			return e, nil
		}
	}
	return nil, fmt.Errorf("unexpected event %+v", event)
}
