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

func (m *mockBus) Contracts(ctx context.Context, opts api.ContractsOpts) ([]api.ContractMetadata, error) {
	return m.contracts, nil
}
func (m *mockBus) GougingParams(ctx context.Context) (api.GougingParams, error) {
	return m.gougingParams, nil
}
func (m *mockBus) RegisterWebhook(ctx context.Context, wh webhooks.Webhook) error {
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
			TransactionFee:     types.Siacoins(1),
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

	// assert using cache before it's initialized prints a warning
	contracts, err := c.DownloadContracts(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if len(contracts) != 3 {
		t.Fatal("expected 3 contracts, got", len(contracts))
	}
	gp, err := c.GougingParams(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if gp.RedundancySettings != test.RedundancySettings {
		t.Fatal("expected redundancy settings to match", gp.RedundancySettings, test.RedundancySettings)
	} else if gp.GougingSettings != test.GougingSettings {
		t.Fatal("expected gouging settings to match", gp.GougingSettings, test.GougingSettings)
	} else if !gp.TransactionFee.Equals(types.Siacoins(1)) {
		t.Fatal("expected transaction fee to match", gp.TransactionFee, types.Siacoins(1))
	}

	// assert warnings are printed when the cache is not ready yet
	if logs := observedLogs.FilterLevelExact(zap.WarnLevel); logs.Len() != 2 {
		t.Fatal("expected 2 warnings, got", logs.Len())
	} else if lines := observedLogs.TakeAll(); lines[0].Message != lines[1].Message {
		t.Fatal("expected same message, got", lines[0].Message, lines[1].Message)
	} else if !strings.Contains(lines[0].Message, errCacheNotReady.Error()) {
		t.Fatal("expected error message to contain 'cache is not ready yet', got", lines[0].Message)
	}

	// initialize the cache
	if err := c.Initialize(context.Background(), ""); err != nil {
		t.Fatal(err)
	}

	// fetch contracts & gouging params so they're cached
	_, err = c.DownloadContracts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.GougingParams(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	// update bus contracts & expire cache entry manually
	b.contracts = append(b.contracts, testContractMetadata(4))
	contracts, err = c.DownloadContracts(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if len(contracts) != 3 {
		t.Fatal("expected 3 contracts, got", len(contracts))
	}
	mc.mu.Lock()
	mc.items[cacheKeyDownloadContracts].expiry = time.Now().Add(-1 * time.Minute)
	mc.mu.Unlock()

	// fetch contracts again, assert we have 4 now and we printed a warning to indicate the cache entry was invalid
	contracts, err = c.DownloadContracts(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if len(contracts) != 4 {
		t.Fatal("expected 4 contracts, got", len(contracts))
	} else if logs := observedLogs.FilterLevelExact(zap.WarnLevel); logs.Len() != 1 {
		t.Fatal("expected 1 warning, got", logs.Len(), logs.All())
	} else if lines := observedLogs.TakeAll(); !strings.Contains(lines[0].Message, errCacheOutdated.Error()) || !strings.Contains(lines[0].Message, cacheKeyDownloadContracts) {
		t.Fatal("expected error message to contain 'cache is outdated', got", lines[0].Message)
	}

	// update gouging params & expire cache entry manually
	b.gougingParams.TransactionFee = b.gougingParams.TransactionFee.Mul64(2)

	// expire cache entry manually
	mc.mu.Lock()
	mc.items[cacheKeyGougingParams].expiry = time.Now().Add(-1 * time.Minute)
	mc.mu.Unlock()

	// fetch contracts again, assert we have 4 now and we printed a warning to indicate the cache entry was invalid
	gp, err = c.GougingParams(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if !gp.TransactionFee.Equals(b.gougingParams.TransactionFee) {
		t.Fatal("expected transaction fee to be updated, got", gp.TransactionFee)
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
		{Module: api.ModuleSetting, Event: api.EventUpdate, Payload: nil},
		{Module: api.ModuleSetting, Event: api.EventDelete, Payload: nil},
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
