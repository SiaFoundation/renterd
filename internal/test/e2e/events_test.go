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
	"go.sia.tech/renterd/internal/test"
	"go.sia.tech/renterd/webhooks"
)

// TestEvents is a test that verifies the bus sends webhooks for certain events,
// providing an event webhook was registered.
func TestEvents(t *testing.T) {
	// list all webhooks
	allEvents := []func(string, map[string]string) webhooks.Webhook{
		api.WebhookConsensusUpdate,
		api.WebhookContractArchive,
		api.WebhookContractRenew,
		api.WebhookContractSetUpdate,
		api.WebhookHostUpdate,
		api.WebhookSettingUpdate,
	}

	// define helper to check if the event is known
	isKnownEvent := func(e webhooks.Event) bool {
		for _, eFn := range allEvents {
			known := eFn("", nil)
			if known.Module == e.Module && known.Event == e.Event {
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

		// ignore host updates with net address diff. from the update we assert to receive
		if event.Module == api.ModuleHost && event.Event == api.EventUpdate {
			if parsed, err := api.ParseEventWebhook(event); err != nil {
				t.Fatal(err)
			} else if parsed.(api.EventHostUpdate).NetAddr != "127.0.0.1:0" {
				return nil
			}
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
		tt.OK(b.RegisterWebhook(context.Background(), e(server.URL, nil)))
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
	tt.OK(b.UpdateGougingSettings(context.Background(), gs))

	// update host setting
	h := cluster.hosts[0]
	settings := h.settings.Settings()
	settings.NetAddress = "127.0.0.1:0"
	tt.OK(h.UpdateSettings(settings))

	// wait until we received the events
	tt.Retry(100, 100*time.Millisecond, func() error {
		mu.Lock()
		defer mu.Unlock()
		if len(received) < len(allEvents) {
			cluster.MineBlocks(1)
			return fmt.Errorf("expected %d unique events, got %+v (%d)", len(allEvents), received, len(received))
		}
		return nil
	})

	// assert the events we received contain the expected information
	for _, r := range received {
		event, err := api.ParseEventWebhook(r)
		tt.OK(err)
		switch e := event.(type) {
		case api.EventContractRenew:
			if e.Renewal.ID != renewed.ID || e.Renewal.RenewedFrom != c.ID || e.Timestamp.IsZero() {
				t.Fatalf("unexpected event %+v", e)
			}
		case api.EventContractArchive:
			if e.ContractID != renewed.ID || e.Reason != t.Name() || e.Timestamp.IsZero() {
				t.Fatalf("unexpected event %+v", e)
			}
		case api.EventContractSetUpdate:
			if e.Name != test.ContractSet || len(e.ToAdd) != 1 || (e.ToAdd[0] != c.ID && e.ToAdd[0] != renewed.ID) || len(e.ToRemove) != 0 || e.Timestamp.IsZero() {
				t.Fatalf("unexpected event %+v", e)
			}
		case api.EventConsensusUpdate:
			if e.TransactionFee.IsZero() || e.BlockHeight == 0 || e.Timestamp.IsZero() || !e.Synced {
				t.Fatalf("unexpected event %+v", e)
			}
		case api.EventHostUpdate:
			if e.HostKey != h.PublicKey() || e.NetAddr != "127.0.0.1:0" || e.Timestamp.IsZero() {
				t.Fatalf("unexpected event %+v", e)
			}
		case api.EventSettingUpdate:
			if e.GougingSettings == nil || e.GougingSettings.HostBlockHeightLeeway != 100 || e.Timestamp.IsZero() {
				t.Fatalf("unexpected event %+v", e)
			}
		}
	}
}
