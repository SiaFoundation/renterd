package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"time"

	"go.uber.org/zap"

	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/webhooks"
)

const (
	cacheKeyGougingParams = "gougingparams"
	cacheKeyUsableHosts   = "usablehosts"

	cacheEntryExpiry = 5 * time.Minute
)

var (
	errCacheNotReady = errors.New("cache is not ready yet, required webhooks have not been registered")
	errCacheOutdated = errors.New("cache is outdated, the value fetched from the bus does not match the cached value")
)

type memoryCache struct {
	items map[string]*cacheEntry
	mu    sync.RWMutex
}

type cacheEntry struct {
	value  interface{}
	expiry time.Time
}

func newMemoryCache() *memoryCache {
	return &memoryCache{
		items: make(map[string]*cacheEntry),
	}
}

func (c *memoryCache) Get(key string) (value interface{}, found bool, expired bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	entry, ok := c.items[key]
	if !ok {
		return nil, false, false
	} else if time.Now().After(entry.expiry) {
		return entry.value, true, true
	}

	t := reflect.TypeOf(entry.value)
	if t.Kind() == reflect.Slice {
		v := reflect.ValueOf(entry.value)
		copied := reflect.MakeSlice(t, v.Len(), v.Cap())
		reflect.Copy(copied, v)
		return copied.Interface(), true, false
	}

	return entry.value, true, false
}

func (c *memoryCache) Set(key string, value interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items[key] = &cacheEntry{
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
		UsableHosts(ctx context.Context) ([]api.HostInfo, error)
		GougingParams(ctx context.Context) (api.GougingParams, error)
	}

	WorkerCache interface {
		UsableHosts(ctx context.Context) ([]api.HostInfo, error)
		GougingParams(ctx context.Context) (api.GougingParams, error)
		HandleEvent(event webhooks.Event) error
		Subscribe(e EventSubscriber) error
	}
)

type cache struct {
	b Bus

	cache  *memoryCache
	logger *zap.SugaredLogger

	mu        sync.Mutex
	readyChan chan struct{}
}

func NewCache(b Bus, logger *zap.Logger) WorkerCache {
	logger = logger.Named("workercache")
	return &cache{
		b: b,

		cache:  newMemoryCache(),
		logger: logger.Sugar(),
	}
}

func (c *cache) UsableHosts(ctx context.Context) (hosts []api.HostInfo, err error) {
	// fetch directly from bus if the cache is not ready
	if !c.isReady() {
		c.logger.Warn(errCacheNotReady)
		hosts, err = c.b.UsableHosts(ctx)
		return
	}

	// fetch from bus if it's not cached or expired
	value, found, expired := c.cache.Get(cacheKeyUsableHosts)
	if !found || expired {
		hosts, err = c.b.UsableHosts(ctx)
		if err == nil {
			c.cache.Set(cacheKeyUsableHosts, hosts)
		}
		if expired && !hostsEqual(value.([]api.HostInfo), hosts) {
			c.logger.Warn(fmt.Errorf("%w: key %v", errCacheOutdated, cacheKeyUsableHosts))
		}
		return
	}
	return value.([]api.HostInfo), nil
}

func (c *cache) GougingParams(ctx context.Context) (gp api.GougingParams, err error) {
	// fetch directly from bus if the cache is not ready
	if !c.isReady() {
		c.logger.Warn(errCacheNotReady)
		gp, err = c.b.GougingParams(ctx)
		return
	}

	// fetch from bus if it's not cached or expired
	value, found, expired := c.cache.Get(cacheKeyGougingParams)
	if !found || expired {
		gp, err = c.b.GougingParams(ctx)
		if err == nil {
			c.cache.Set(cacheKeyGougingParams, gp)
		}
		if expired && !gougingParamsEqual(value.(api.GougingParams), gp) {
			c.logger.Warn(fmt.Errorf("%w: key %v", errCacheOutdated, cacheKeyGougingParams))
		}
		return
	}

	return value.(api.GougingParams), nil
}

func (c *cache) HandleEvent(event webhooks.Event) (err error) {
	log := c.logger.With("module", event.Module, "event", event.Event)

	// parse the event
	parsed, err := api.ParseEventWebhook(event)
	if err != nil {
		log.Errorw("failed to parse event", "error", err)
		return err
	}

	// handle the event
	switch e := parsed.(type) {
	case api.EventConsensusUpdate:
		log = log.With("bh", e.BlockHeight, "ts", e.Timestamp)
		c.handleConsensusUpdate(e)
	case api.EventSettingUpdate:
		log = log.With("gouging", e.GougingSettings != nil, "pinned", e.PinnedSettings != nil, "upload", e.UploadSettings != nil, "ts", e.Timestamp)
		c.handleSettingUpdate(e)
	case api.EventContractAdd:
	case api.EventContractArchive:
	case api.EventContractRenew:
	case api.EventHostUpdate:
		c.cache.Invalidate(cacheKeyUsableHosts)
	default:
		log.Info("unhandled event", e)
		return
	}

	// log the outcome
	if err != nil {
		log.Errorw("failed to handle event", "error", err)
	} else {
		log.Info("handled event")
	}
	return
}

func (c *cache) Subscribe(e EventSubscriber) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.readyChan != nil {
		return fmt.Errorf("already subscribed")
	}

	c.readyChan, err = e.AddEventHandler(c.logger.Desugar().Name(), c)
	if err != nil {
		return fmt.Errorf("failed to subscribe the worker cache, error: %v", err)
	}
	return nil
}

func (c *cache) isReady() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	select {
	case <-c.readyChan:
		return true
	default:
	}
	return false
}

func (c *cache) handleConsensusUpdate(event api.EventConsensusUpdate) {
	// return early if the doesn't have gouging params to update
	value, found, _ := c.cache.Get(cacheKeyGougingParams)
	if !found {
		return
	}

	// update gouging params
	gp := value.(api.GougingParams)
	gp.ConsensusState = event.ConsensusState
	c.cache.Set(cacheKeyGougingParams, gp)
}

func (c *cache) handleSettingUpdate(e api.EventSettingUpdate) {
	// return early if the cache doesn't have gouging params to update
	value, found, _ := c.cache.Get(cacheKeyGougingParams)
	if !found {
		return
	}

	// update the cache
	gp := value.(api.GougingParams)
	if e.GougingSettings != nil {
		gp.GougingSettings = *e.GougingSettings
	}
	if e.UploadSettings != nil {
		gp.RedundancySettings = e.UploadSettings.Redundancy
	}
	c.cache.Set(cacheKeyGougingParams, gp)
}

func hostsEqual(x, y []api.HostInfo) bool {
	if len(x) != len(y) {
		return false
	}
	sort.Slice(x, func(i, j int) bool { return x[i].PublicKey.String() < x[j].PublicKey.String() })
	sort.Slice(y, func(i, j int) bool { return y[i].PublicKey.String() < y[j].PublicKey.String() })
	for i, c := range x {
		if c.PublicKey.String() != y[i].PublicKey.String() || c.SiamuxAddr != y[i].SiamuxAddr {
			return false
		}
	}
	return true
}

func gougingParamsEqual(x, y api.GougingParams) bool {
	var xb bytes.Buffer
	var yb bytes.Buffer
	json.NewEncoder(&xb).Encode(x)
	json.NewEncoder(&yb).Encode(y)
	return bytes.Equal(xb.Bytes(), yb.Bytes())
}
