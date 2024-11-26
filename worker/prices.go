package worker

import (
	"context"
	"fmt"
	"sync"
	"time"

	rhpv4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
)

type (
	PricesFetcher interface {
		Prices(ctx context.Context) (rhpv4.HostPrices, error)
		PublicKey() types.PublicKey
	}

	pricesCache struct {
		mu    sync.Mutex
		cache map[types.PublicKey]*cachedPrices
	}

	cachedPrices struct {
		hk types.PublicKey

		mu     sync.Mutex
		prices rhpv4.HostPrices
		update *pricesUpdate
	}

	pricesUpdate struct {
		err    error
		done   chan struct{}
		prices rhpv4.HostPrices
	}
)

func newPricesCache() *pricesCache {
	return &pricesCache{
		cache: make(map[types.PublicKey]*cachedPrices),
	}
}

// fetch returns a price table for the given host
func (c *pricesCache) fetch(ctx context.Context, h PricesFetcher) (rhpv4.HostPrices, types.Currency, error) {
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

func (p *cachedPrices) ongoingUpdate() (bool, *pricesUpdate) {
	p.mu.Lock()
	defer p.mu.Unlock()

	var ongoing bool
	if p.update == nil {
		p.update = &pricesUpdate{done: make(chan struct{})}
	} else {
		ongoing = true
	}

	return ongoing, p.update
}

func (p *cachedPrices) fetch(ctx context.Context, h PricesFetcher) (prices rhpv4.HostPrices, cost types.Currency, err error) {
	// grab the current price table
	p.mu.Lock()
	prices = p.prices
	p.mu.Unlock()

	// get gouging checker to figure out how many blocks we have left before the
	// current price table is considered to gouge on the block height
	gc, err := GougingCheckerFromContext(ctx, false)
	if err != nil {
		return rhpv4.HostPrices{}, types.ZeroCurrency, err
	}

	// figure out whether we should update the price table, if not we can return
	if !prices.ValidUntil.IsZero() {
		closeToGouging := gc.BlocksUntilBlockHeightGouging(prices.TipHeight) <= priceTableBlockHeightLeeway
		closeToExpiring := time.Now().Add(priceTableValidityLeeway).After(prices.ValidUntil)
		if !closeToExpiring && !closeToGouging {
			return
		}
	}

	// figure out whether an update is already ongoing, if there's one ongoing
	// we can either wait or return early depending on whether the price table
	// we have is still usable
	ongoing, update := p.ongoingUpdate()
	if ongoing && time.Now().Add(priceTableValidityLeeway).Before(prices.ValidUntil) {
		return
	} else if ongoing {
		select {
		case <-ctx.Done():
			return rhpv4.HostPrices{}, types.ZeroCurrency, fmt.Errorf("%w; %w", errPriceTableUpdateTimedOut, context.Cause(ctx))
		case <-update.done:
		}
		return update.prices, types.ZeroCurrency, update.err
	}

	// this thread is updating the price table
	defer func() {
		update.prices = prices
		update.err = err
		close(update.done)

		p.mu.Lock()
		if err == nil {
			p.prices = prices
		}
		p.update = nil
		p.mu.Unlock()
	}()

	// otherwise fetch it
	prices, err = h.Prices(ctx)

	// handle error after recording
	if err != nil {
		return rhpv4.HostPrices{}, types.ZeroCurrency, fmt.Errorf("failed to update pricetable, err %v", err)
	}
	return
}
