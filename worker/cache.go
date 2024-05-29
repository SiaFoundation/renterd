package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/events"
)

const (
	cacheKeyDownloadContracts = "downloadcontracts"
	cacheKeyGougingParams     = "gougingparams"
)

var (
	errUnhandledEvent = errors.New("unhandled event")
)

type memoryCache struct {
	items map[string]interface{}
	mu    sync.RWMutex
}

func newMemoryCache() *memoryCache {
	return &memoryCache{
		items: make(map[string]interface{}),
	}
}

func (c *memoryCache) Get(key string) (interface{}, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	value, _ := c.items[key]
	return value, false // TODO: remove this hardcoded false rendering the cache useless
}

func (c *memoryCache) Set(key string, value interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items[key] = value
}

func (c *memoryCache) Invalidate(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.items, key)
}

type cache struct {
	b     Bus
	cache *memoryCache
}

func (w *worker) initCache() {
	if w.cache != nil {
		panic("cache already initialized") // developer error
	}
	w.cache = newCache(w.bus)
}

func newCache(b Bus) *cache {
	return &cache{
		b:     b,
		cache: newMemoryCache(),
	}
}

func (c *cache) DownloadContracts(ctx context.Context) ([]api.ContractMetadata, error) {
	value, ok := c.cache.Get(cacheKeyDownloadContracts)
	if ok {
		return value.([]api.ContractMetadata), nil
	}
	contracts, err := c.b.Contracts(ctx, api.ContractsOpts{})
	if err != nil {
		return nil, err
	}
	c.cache.Set(cacheKeyDownloadContracts, contracts)
	return contracts, nil
}

func (c *cache) GougingParams(ctx context.Context) (api.GougingParams, error) {
	value, ok := c.cache.Get(cacheKeyGougingParams)
	if ok {
		return value.(api.GougingParams), nil
	}
	gp, err := c.b.GougingParams(ctx)
	if err != nil {
		return api.GougingParams{}, err
	}
	c.cache.Set(cacheKeyGougingParams, gp)
	return gp, nil
}

func (c *cache) handleEventWebhook(event string, payload interface{}) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	switch event {
	case events.WebhookEventSettingUpdate:
		var update events.EventSettingUpdate
		if err := json.Unmarshal(b, &update); err != nil {
			return err
		}
		return c.handleSettingUpdate(update)
	case events.WebhookEventContractSetUpdate:
		var update events.EventContractSetUpdate
		if err := json.Unmarshal(b, &update); err != nil {
			return err
		}
		return c.handleContractSetUpdate(update)
	case events.WebhookEventConsensusUpdate:
		var update events.EventConsensusUpdate
		if err := json.Unmarshal(b, &update); err != nil {
			return err
		}
		return c.handleConsensusUpdate(update)
	default:
	}

	return errUnhandledEvent
}

func (c *cache) handleSettingUpdate(e events.EventSettingUpdate) (err error) {
	// return early if the cache doesn't have gouging params to update
	value, found := c.cache.Get(cacheKeyGougingParams)
	if !found {
		return nil
	}
	gp := value.(api.GougingParams)

	// invalidate the cache on error to be safe
	defer func() {
		if err != nil {
			c.cache.Invalidate(cacheKeyGougingParams)
		}
	}()

	// no updated setting, simply invalidate
	if e.Update == nil {
		c.cache.Invalidate(cacheKeyGougingParams)
		return nil
	}

	// marshal the updated value
	data, err := json.Marshal(e.Update)
	if err != nil {
		return fmt.Errorf("couldn't marshal the given value, error: %v", err)
	}

	// unmarshal into the appropriated setting and update the cache
	switch e.Key {
	case api.SettingGouging:
		var gs api.GougingSettings
		if err := json.Unmarshal(data, &gs); err != nil {
			return fmt.Errorf("couldn't update gouging settings, invalid request body, %t", e.Update)
		} else if err := gs.Validate(); err != nil {
			return fmt.Errorf("couldn't update gouging settings, error: %v", err)
		}

		gp.GougingSettings = gs
		c.cache.Set(cacheKeyGougingParams, gp)
	case api.SettingRedundancy:
		var rs api.RedundancySettings
		if err := json.Unmarshal(data, &rs); err != nil {
			return fmt.Errorf("couldn't update redundancy settings, invalid request body, %t", e.Update)
		} else if err := rs.Validate(); err != nil {
			return fmt.Errorf("couldn't update redundancy settings, error: %v", err)
		}

		gp.RedundancySettings = rs
		c.cache.Set(cacheKeyGougingParams, gp)
	default:
	}
	return nil
}

func (c *cache) handleContractSetUpdate(_ events.EventContractSetUpdate) error {
	// return early if the cache doesn't have contracts
	value, found := c.cache.Get(cacheKeyDownloadContracts)
	if !found {
		return nil
	}

	return nil // TODO: handle contract set update
}

func (c *cache) handleConsensusUpdate(event events.EventConsensusUpdate) error {
	// return early if the doesn't have gouging params to update
	value, found := c.cache.Get(cacheKeyGougingParams)
	if !found {
		return nil
	}
	gp := value.(api.GougingParams)

	gp.ConsensusState = event.ConsensusState
	gp.TransactionFee = event.TransactionFee
	c.cache.Set(cacheKeyGougingParams, gp)
	return nil
}
