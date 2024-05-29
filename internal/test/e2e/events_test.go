package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/events"
	"go.sia.tech/renterd/webhooks"
)

// TestEventWebhooks is a test that verifies the bus sends webhooks for certain
// events, providing an event webhook was registered.
func TestEventWebhooks(t *testing.T) {
	// setup test cluster
	cluster := newTestCluster(t, testClusterOptions{hosts: 1})
	defer cluster.Shutdown()
	b := cluster.Bus
	tt := cluster.tt

	// setup test server to receive webhooks
	var mu sync.Mutex
	var received []webhooks.Event
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var webhook webhooks.Event
		if err := json.NewDecoder(r.Body).Decode(&webhook); err != nil {
			t.Fatal(err)
		}

		mu.Lock()
		defer mu.Unlock()
		received = append(received, webhook)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// register webhooks for certain events
	for _, wh := range []webhooks.Webhook{
		events.NewEventWebhook(server.URL, events.WebhookEventSettingUpdate),
		events.NewEventWebhook(server.URL, events.WebhookEventConsensusUpdate),
		events.NewEventWebhook(server.URL, events.WebhookEventContractSetUpdate),
	} {
		if err := b.RegisterWebhook(context.Background(), wh); err != nil {
			t.Fatal(err)
		}
	}

	// mine block to update consensus
	cluster.MineBlocks(1)

	// fetch current gouging params
	gp, err := b.GougingParams(context.Background())
	tt.OK(err)

	// update settings
	gs := gp.GougingSettings
	gs.HostBlockHeightLeeway = 100
	b.UpdateSetting(context.Background(), api.SettingGouging, gs)

	rs := gp.RedundancySettings
	rs.MinShards = 1
	rs.TotalShards = 1
	b.UpdateSetting(context.Background(), api.SettingRedundancy, rs)

	// wait until we received the events
	var uniqueEvents []string
	tt.Retry(10, time.Second, func() error {
		mu.Lock()
		defer mu.Unlock()

		unique := make(map[string]struct{})
		for _, wh := range received {
			unique[wh.Event] = struct{}{}
		}
		if len(unique) < 4 {
			return fmt.Errorf("expected 4 unique events, got %v", len(unique))
		}

		for event := range unique {
			uniqueEvents = append(uniqueEvents, event)
		}
		sort.Slice(uniqueEvents, func(i, j int) bool {
			return uniqueEvents[i] < uniqueEvents[j]
		})
		return nil
	})

	// assert we received the events we expected

	if !reflect.DeepEqual(uniqueEvents, []string{
		events.WebhookEventConsensusUpdate,
		events.WebhookEventContractSetUpdate,
		webhooks.WebhookEventPing,
		events.WebhookEventSettingUpdate,
	}) {
		tt.Fatalf("expected events %v, got %v", []string{
			events.WebhookEventConsensusUpdate,
			events.WebhookEventContractSetUpdate,
			webhooks.WebhookEventPing,
			events.WebhookEventSettingUpdate,
		}, uniqueEvents)
	}
}
