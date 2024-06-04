package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/webhooks"
)

const (
	cacheKeyDownloadContracts = "downloadcontracts"
	cacheKeyGougingParams     = "gougingparams"

	cacheEntryExpiry = 5 * time.Minute
)

var (
	errCacheNotReady = errors.New("cache is not ready yet, required webhooks have not been registered")
)

type memoryCache struct {
	items map[string]cacheEntry
	mu    sync.RWMutex
}

type cacheEntry struct {
	value  interface{}
	expiry time.Time
}

func newMemoryCache() *memoryCache {
	return &memoryCache{
		items: make(map[string]cacheEntry),
	}
}

func (c *memoryCache) Get(key string) (interface{}, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	entry, ok := c.items[key]
	if !ok || time.Now().After(entry.expiry) {
		return nil, false
	}
	return entry.value, ok
}

func (c *memoryCache) Set(key string, value interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items[key] = cacheEntry{
		value:  value,
		expiry: time.Now().Add(cacheEntryExpiry),
	}
}

func (c *memoryCache) Invalidate(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.items, key)
}

type (
	Bus interface {
		Contracts(ctx context.Context, opts api.ContractsOpts) ([]api.ContractMetadata, error)
		GougingParams(ctx context.Context) (api.GougingParams, error)
		RegisterWebhook(ctx context.Context, wh webhooks.Webhook) error
	}

	WorkerCache interface {
		DownloadContracts(ctx context.Context) ([]api.ContractMetadata, error)
		GougingParams(ctx context.Context) (api.GougingParams, error)
		HandleEvent(event webhooks.Event) error
		Initialize(ctx context.Context) error
	}
)

type cache struct {
	b         Bus
	eventsURL string

	cache  *memoryCache
	logger *zap.SugaredLogger

	mu    sync.Mutex
	ready bool
}

func NewCache(b Bus, workerAddr string, logger *zap.Logger) WorkerCache {
	return &cache{
		b:         b,
		eventsURL: workerAddr,

		cache:  newMemoryCache(),
		logger: logger.Sugar().Named("workercache"),
	}
}

func (c *cache) DownloadContracts(ctx context.Context) ([]api.ContractMetadata, error) {
	if c.isReady() {
		value, ok := c.cache.Get(cacheKeyDownloadContracts)
		if ok {
			return value.([]api.ContractMetadata), nil
		}
	} else {
		c.logger.Warn(errCacheNotReady)
	}

	contracts, err := c.b.Contracts(ctx, api.ContractsOpts{})
	if err != nil {
		return nil, err
	}
	c.cache.Set(cacheKeyDownloadContracts, contracts)
	return contracts, nil
}

func (c *cache) GougingParams(ctx context.Context) (api.GougingParams, error) {
	if c.isReady() {
		value, ok := c.cache.Get(cacheKeyGougingParams)
		if ok {
			return value.(api.GougingParams), nil
		}
	} else {
		c.logger.Warn(errCacheNotReady)
	}

	gp, err := c.b.GougingParams(ctx)
	if err != nil {
		return api.GougingParams{}, err
	}
	c.cache.Set(cacheKeyGougingParams, gp)
	return gp, nil
}

func (c *cache) HandleEvent(event webhooks.Event) error {
	parsed, err := api.ParseEvent(event)
	if err != nil {
		return err
	}

	switch e := parsed.(type) {
	case api.EventConsensusUpdate:
		return c.handleConsensusUpdate(e)
	case api.EventContractArchive:
		return c.handleContractArchive(e)
	case api.EventContractRenew:
		return c.handleContractRenew(e)
	case api.EventSettingUpdate:
		return c.handleSettingUpdate(e)
	case api.EventSettingDelete:
		return c.handleSettingDelete(e)
	default:
	}
	return nil
}

func (c *cache) Initialize(ctx context.Context) error {
	for _, wh := range []webhooks.Webhook{
		webhooks.NewEventWebhook(c.eventsURL, api.EventConsensusUpdate{}),
		webhooks.NewEventWebhook(c.eventsURL, api.EventContractArchive{}),
		webhooks.NewEventWebhook(c.eventsURL, api.EventContractRenew{}),
		webhooks.NewEventWebhook(c.eventsURL, api.EventSettingUpdate{}),
	} {
		if err := c.b.RegisterWebhook(ctx, wh); err != nil {
			return fmt.Errorf("failed to register webhook '%s', err: %v", wh, err)
		}
	}
	c.mu.Lock()
	c.ready = true
	c.mu.Unlock()
	return nil
}

func (c *cache) isReady() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ready
}

func (c *cache) handleConsensusUpdate(event api.EventConsensusUpdate) error {
	// return early if the doesn't have gouging params to update
	value, found := c.cache.Get(cacheKeyGougingParams)
	if !found {
		return nil
	}

	// update gouging params
	gp := value.(api.GougingParams)
	gp.ConsensusState = event.ConsensusState
	gp.TransactionFee = event.TransactionFee
	c.cache.Set(cacheKeyGougingParams, gp)
	return nil
}

func (c *cache) handleContractArchive(event api.EventContractArchive) error {
	// return early if the cache doesn't have contracts
	value, found := c.cache.Get(cacheKeyDownloadContracts)
	if !found {
		return nil
	}
	contracts := value.([]api.ContractMetadata)

	// remove the contract from the cache
	for i, contract := range contracts {
		if contract.ID == event.ContractID {
			contracts = append(contracts[:i], contracts[i+1:]...)
			break
		}
	}
	c.cache.Set(cacheKeyDownloadContracts, contracts)
	return nil
}

func (c *cache) handleContractRenew(event api.EventContractRenew) error {
	// return early if the cache doesn't have contracts
	value, found := c.cache.Get(cacheKeyDownloadContracts)
	if !found {
		return nil
	}
	contracts := value.([]api.ContractMetadata)

	// update the renewed contract in the cache
	for i, contract := range contracts {
		if contract.ID == event.Renewal.RenewedFrom {
			contracts[i] = event.Renewal
			break
		}
	}

	c.cache.Set(cacheKeyDownloadContracts, contracts)
	return nil
}

func (c *cache) handleSettingDelete(e api.EventSettingDelete) (err error) {
	if e.Key == api.SettingGouging || e.Key == api.SettingRedundancy {
		c.cache.Invalidate(cacheKeyGougingParams)
	}
	return nil
}

func (c *cache) handleSettingUpdate(e api.EventSettingUpdate) (err error) {
	// return early if the cache doesn't have gouging params to update
	value, found := c.cache.Get(cacheKeyGougingParams)
	if !found {
		return nil
	}
	gp := value.(api.GougingParams)

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
