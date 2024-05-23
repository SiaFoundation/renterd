package worker

import "sync"

const (
	cacheKeyDownloadContracts = "downloadcontracts"
	cacheKeyGougingParams     = "gougingparams"
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
