package worker

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/test"
	"go.sia.tech/renterd/webhooks"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

type mockBus struct {
	contracts     []api.ContractMetadata
	gougingParams api.GougingParams
}

func (m *mockBus) GougingParams(ctx context.Context) (api.GougingParams, error) {
	return m.gougingParams, nil
}

func (m *mockBus) UsableHosts(ctx context.Context) (hosts []api.HostInfo, _ error) {
	for _, c := range m.contracts {
		hosts = append(hosts, c.HostInfo())
	}
	return
}

type mockEventSubscriber struct {
	readyChan chan struct{}
}

func (m *mockEventSubscriber) AddEventHandler(id string, h EventHandler) (chan struct{}, error) {
	return m.readyChan, nil
}

func (m *mockEventSubscriber) ProcessEvent(event webhooks.Event) {}

func (m *mockEventSubscriber) Register(ctx context.Context, eventURL string, opts ...webhooks.HeaderOption) error {
	return nil
}

func (m *mockEventSubscriber) Shutdown(ctx context.Context) error {
	return nil
}

func newMockBus() *mockBus {
	return &mockBus{
		contracts: []api.ContractMetadata{
			testContractMetadata(1),
			testContractMetadata(2),
			testContractMetadata(3),
		},
		gougingParams: api.GougingParams{
			RedundancySettings: test.RedundancySettings,
			GougingSettings:    test.GougingSettings,
			ConsensusState: api.ConsensusState{
				BlockHeight:   1,
				LastBlockTime: api.TimeRFC3339{},
				Synced:        true,
			},
		},
	}
}

func TestWorkerCache(t *testing.T) {
	// observe logs
	observedZapCore, observedLogs := observer.New(zap.DebugLevel)

	// create mock bus and cache
	c, b, mc := newTestCache(zap.New(observedZapCore))

	// create mock event subscriber
	m := &mockEventSubscriber{readyChan: make(chan struct{})}

	// subscribe cache to event subscriber
	c.Subscribe(m)

	// assert using cache before it's ready prints a warning
	hosts, err := c.UsableHosts(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if len(hosts) != 3 {
		t.Fatal("expected 3 hosts, got", len(hosts))
	}
	gp, err := c.GougingParams(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if gp.RedundancySettings != test.RedundancySettings {
		t.Fatal("expected redundancy settings to match", gp.RedundancySettings, test.RedundancySettings)
	} else if gp.GougingSettings != test.GougingSettings {
		t.Fatal("expected gouging settings to match", gp.GougingSettings, test.GougingSettings)
	}

	// assert warnings are printed when the cache is not ready yet
	if logs := observedLogs.FilterLevelExact(zap.WarnLevel); logs.Len() != 2 {
		t.Fatal("expected 2 warnings, got", logs.Len())
	} else if lines := observedLogs.TakeAll(); lines[0].Message != lines[1].Message {
		t.Fatal("expected same message, got", lines[0].Message, lines[1].Message)
	} else if !strings.Contains(lines[0].Message, errCacheNotReady.Error()) {
		t.Fatal("expected error message to contain 'cache is not ready yet', got", lines[0].Message)
	}

	// close the ready channel
	close(m.readyChan)

	// fetch usable hosts & gouging params so they're cached
	_, err = c.UsableHosts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.GougingParams(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	// update bus contracts & expire cache entry manually
	b.contracts = append(b.contracts, testContractMetadata(4))
	hosts, err = c.UsableHosts(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if len(hosts) != 3 {
		t.Fatal("expected 3 hosts, got", len(hosts))
	}
	mc.mu.Lock()
	mc.items[cacheKeyUsableHosts].expiry = time.Now().Add(-1 * time.Minute)
	mc.mu.Unlock()

	// fetch hosts again, assert we have 4 now and we printed a warning to indicate the cache entry was invalid
	hosts, err = c.UsableHosts(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if len(hosts) != 4 {
		t.Fatal("expected 4 hosts, got", len(hosts))
	} else if logs := observedLogs.FilterLevelExact(zap.WarnLevel); logs.Len() != 1 {
		t.Fatal("expected 1 warning, got", logs.Len(), logs.All())
	} else if lines := observedLogs.TakeAll(); !strings.Contains(lines[0].Message, errCacheOutdated.Error()) || !strings.Contains(lines[0].Message, cacheKeyUsableHosts) {
		t.Fatal("expected error message to contain 'cache is outdated', got", lines[0].Message)
	}

	// update gouging params & expire cache entry manually
	b.gougingParams.ConsensusState.BlockHeight += 1

	// expire cache entry manually
	mc.mu.Lock()
	mc.items[cacheKeyGougingParams].expiry = time.Now().Add(-1 * time.Minute)
	mc.mu.Unlock()

	// fetch contracts again, assert we have 4 now and we printed a warning to indicate the cache entry was invalid
	gp, err = c.GougingParams(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if logs := observedLogs.FilterLevelExact(zap.WarnLevel); logs.Len() != 1 {
		t.Fatal("expected 1 warning, got", logs.Len(), logs.All())
	} else if lines := observedLogs.TakeAll(); !strings.Contains(lines[0].Message, errCacheOutdated.Error()) || !strings.Contains(lines[0].Message, cacheKeyGougingParams) {
		t.Fatal("expected error message to contain 'cache is outdated', got", lines[0].Message)
	}

	// assert the worker cache handles every event
	_ = observedLogs.TakeAll() // clear logs
	for _, event := range []webhooks.Event{
		{Module: api.ModuleConsensus, Event: api.EventUpdate, Payload: nil},
		{Module: api.ModuleContract, Event: api.EventArchive, Payload: nil},
		{Module: api.ModuleContract, Event: api.EventRenew, Payload: nil},
		{Module: api.ModuleHost, Event: api.EventUpdate, Payload: nil},
		{Module: api.ModuleSetting, Event: api.EventUpdate, Payload: nil},
	} {
		if err := c.HandleEvent(event); err != nil {
			t.Fatal(err)
		}
	}
	for _, entry := range observedLogs.TakeAll() {
		if strings.Contains(entry.Message, "unhandled event") {
			t.Fatal("expected no unhandled event, got", entry)
		}
	}
}

func newTestCache(logger *zap.Logger) (WorkerCache, *mockBus, *memoryCache) {
	b := newMockBus()
	c := newMemoryCache()
	return &cache{
		b:      b,
		cache:  c,
		logger: logger.Sugar(),
	}, b, c
}

func testContractMetadata(n int) api.ContractMetadata {
	return api.ContractMetadata{
		ID:          types.FileContractID{byte(n)},
		HostKey:     types.PublicKey{byte(n)},
		WindowStart: 0,
		WindowEnd:   10,
	}
}
