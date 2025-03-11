package worker

import (
	"context"
	"reflect"
	"sync"
	"time"

	"go.uber.org/zap"

	"go.sia.tech/renterd/v2/api"
)

const (
	cacheKeyUsableHosts = "usablehosts"
)

type memoryCache struct {
	cacheEntryExpiry time.Duration
	items            map[string]*cacheEntry
	mu               sync.RWMutex
}

type cacheEntry struct {
	value  interface{}
	expiry time.Time
}

func newMemoryCache(expiry time.Duration) *memoryCache {
	return &memoryCache{
		cacheEntryExpiry: expiry,
		items:            make(map[string]*cacheEntry),
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
		expiry: time.Now().Add(c.cacheEntryExpiry),
	}
}

type (
	Bus interface {
		UsableHosts(ctx context.Context) ([]api.HostInfo, error)
	}

	WorkerCache interface {
		UsableHosts(ctx context.Context) ([]api.HostInfo, error)
	}
)

type cache struct {
	b      Bus
	cache  *memoryCache
	logger *zap.SugaredLogger
}

func NewCache(b Bus, expiry time.Duration, logger *zap.Logger) WorkerCache {
	logger = logger.Named("workercache")
	return &cache{
		b: b,

		cache:  newMemoryCache(expiry),
		logger: logger.Sugar(),
	}
}

func (c *cache) UsableHosts(ctx context.Context) (hosts []api.HostInfo, err error) {
	value, found, expired := c.cache.Get(cacheKeyUsableHosts)
	if !found || expired {
		hosts, err = c.b.UsableHosts(ctx)
		if err == nil {
			c.cache.Set(cacheKeyUsableHosts, hosts)
		}
		return
	}
	return value.([]api.HostInfo), nil
}
