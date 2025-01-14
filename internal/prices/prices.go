package prices

import (
	"context"
	"fmt"
	"sync"
	"time"

	rhpv4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
)

const (
	// priceRefreshInterval is the max interval at which we refresh price tables.
	// It is set to 20 minutes since that equals 2 blocks on average which keeps
	// prices reasonably up-to-date. If the price table has an expiry shorter
	// than this, it will be refreshed sooner.
	priceRefreshInterval = 20 * time.Minute
)

type (
	PricesFetcher interface {
		Prices(ctx context.Context) (rhpv4.HostPrices, error)
		PublicKey() types.PublicKey
	}

	PricesCache struct {
		mu    sync.Mutex
		cache map[types.PublicKey]*cachedPrices
	}

	cachedPrices struct {
		hk types.PublicKey

		mu        sync.Mutex
		prices    rhpv4.HostPrices
		renewTime time.Time
		update    *pricesUpdate
	}

	pricesUpdate struct {
		err    error
		done   chan struct{}
		prices rhpv4.HostPrices
	}
)

func NewPricesCache() *PricesCache {
	return &PricesCache{
		cache: make(map[types.PublicKey]*cachedPrices),
	}
}

// Fetch returns a price table for the given host
func (c *PricesCache) Fetch(ctx context.Context, h PricesFetcher) (rhpv4.HostPrices, error) {
	c.mu.Lock()
	prices, exists := c.cache[h.PublicKey()]
	if !exists {
		prices = &cachedPrices{
			hk: h.PublicKey(),
		}
		c.cache[h.PublicKey()] = prices
	}
	c.mu.Unlock()

	return prices.fetch(ctx, h)
}

func (p *cachedPrices) fetch(ctx context.Context, h PricesFetcher) (rhpv4.HostPrices, error) {
	// grab the current price table
	p.mu.Lock()
	prices := p.prices
	renewTime := p.renewTime

	// figure out whether we should update the price table, if not we can return
	if !renewTime.IsZero() && time.Now().Before(renewTime) {
		p.mu.Unlock()
		return prices, nil
	}

	// figure out whether an update is ongoing and register an ongoing update if
	// not
	ongoing := p.update != nil
	if p.update == nil {
		p.update = &pricesUpdate{done: make(chan struct{})}
	}
	update := p.update
	p.mu.Unlock()

	// if there's one ongoing we can either wait or return early depending on
	// whether the price table we have is still usable
	if ongoing && time.Now().Add(priceTableValidityLeeway).Before(prices.ValidUntil) {
		return prices, nil
	} else if ongoing {
		select {
		case <-ctx.Done():
			return rhpv4.HostPrices{}, fmt.Errorf("%w; %w", errPriceTableUpdateTimedOut, context.Cause(ctx))
		case <-update.done:
		}
		return update.prices, update.err
	}

	// this thread is updating the price table
	prices, err := h.Prices(ctx)

	// signal the other threads that the update is done
	update.prices = prices
	update.err = err
	close(update.done)

	p.mu.Lock()
	defer p.mu.Unlock()

	if err == nil {
		p.prices = prices

		// renew the prices 2 blocks after receiving them or 30 seconds
		// before expiry, whatever comes first
		p.renewTime = time.Now().Add(priceRefreshInterval)
		if expiry := p.prices.ValidUntil.Add(-priceTableValidityLeeway); expiry.Before(p.renewTime) {
			p.renewTime = expiry
		}
	}
	p.update = nil

	return prices, err
}
