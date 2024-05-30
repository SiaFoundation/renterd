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
	"go.sia.tech/renterd/events"
	"go.sia.tech/renterd/internal/test"
	"go.sia.tech/renterd/webhooks"
)

// TestEventWebhooks is a test that verifies the bus sends webhooks for certain
// events, providing an event webhook was registered.
func TestEventWebhooks(t *testing.T) {
	// define all events
	allEvents := map[string]struct{}{
		events.WebhookEventConsensusUpdate:   {},
		events.WebhookEventContractArchived:  {},
		events.WebhookEventContractRenewal:   {},
		events.WebhookEventContractSetUpdate: {},
		events.WebhookEventSettingUpdate:     {},
	}

	// define a small helper to keep track of received events
	var mu sync.Mutex
	received := make(map[string]webhooks.Event)
	receiveEvent := func(event webhooks.Event) error {
		_, ok := allEvents[event.Event]
		if !ok && event.Event == webhooks.WebhookEventPing {
			return nil
		} else if !ok {
			return fmt.Errorf("unexpected event %v", event.Event)
		} else if _, ok := received[event.Event]; !ok {
			mu.Lock()
			received[event.Event] = event
			mu.Unlock()
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

	// register webhooks for certain events
	for we := range allEvents {
		if err := b.RegisterWebhook(context.Background(), events.NewEventWebhook(server.URL, we)); err != nil {
			t.Fatal(err)
		}
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

	mu.Lock()
	defer mu.Unlock()

	// assert contract renewal
	var ecr events.EventContractRenewal
	bytes, _ := json.Marshal(received[events.WebhookEventContractRenewal].Payload)
	tt.OK(json.Unmarshal(bytes, &ecr))
	if ecr.Renewal.ID != renewed.ID || ecr.Renewal.RenewedFrom != c.ID || ecr.Timestamp.IsZero() {
		t.Fatalf("unexpected event %+v", ecr)
	}

	// assert contract archived event
	var eca events.EventContractArchived
	bytes, _ = json.Marshal(received[events.WebhookEventContractArchived].Payload)
	tt.OK(json.Unmarshal(bytes, &eca))
	if eca.ContractID != renewed.ID || eca.Reason != t.Name() || eca.Timestamp.IsZero() {
		t.Fatalf("unexpected event %+v", ecr)
	}

	// assert contract set update event
	var ecsu events.EventContractSetUpdate
	bytes, _ = json.Marshal(received[events.WebhookEventContractSetUpdate].Payload)
	tt.OK(json.Unmarshal(bytes, &ecsu))
	if ecsu.Name != test.ContractSet || len(ecsu.ContractIDs) != 1 || ecsu.ContractIDs[0] != c.ID || ecsu.Timestamp.IsZero() {
		t.Fatalf("unexpected event %+v", ecsu)
	}

	// assert consensus update
	var ecu events.EventConsensusUpdate
	bytes, _ = json.Marshal(received[events.WebhookEventConsensusUpdate].Payload)
	tt.OK(json.Unmarshal(bytes, &ecu))
	if ecu.TransactionFee.IsZero() || ecu.BlockHeight == 0 || ecu.Timestamp.IsZero() || !ecu.Synced {
		t.Fatalf("unexpected event %+v", ecu)
	}

	// assert setting update
	var esu events.EventSettingUpdate
	bytes, _ = json.Marshal(received[events.WebhookEventSettingUpdate].Payload)
	tt.OK(json.Unmarshal(bytes, &esu))
	if esu.Key != api.SettingGouging || esu.Timestamp.IsZero() {
		t.Fatalf("unexpected event %+v", esu)
	}

	// assert setting update payload
	var update api.GougingSettings
	bytes, _ = json.Marshal(esu.Update)
	tt.OK(json.Unmarshal(bytes, &update))
	if update.HostBlockHeightLeeway != 100 {
		t.Fatalf("unexpected update %+v", update)
	}
}
