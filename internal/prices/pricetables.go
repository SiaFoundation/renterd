package prices

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/v2/api"
)

const (
	// priceTableValidityLeeway is a leeway we apply when deeming a price table safe
	// for use, we essentially add 60 seconds to the current time when checking
	// whether we are still before a pricetable's expiry time
	priceTableValidityLeeway = 60 * time.Second
)

var (
	// errPriceTableUpdateTimedOut is returned when we timeout waiting for
	// another thread that's performing a price table update
	errPriceTableUpdateTimedOut = errors.New("timeout while blocking for pricetable update")
)

type (
	priceTableFetcher interface {
		PriceTable(ctx context.Context, rev *types.FileContractRevision) (hpt api.HostPriceTable, cost types.Currency, err error)
		PublicKey() types.PublicKey
	}

	PriceTables struct {
		mu          sync.Mutex
		priceTables map[types.PublicKey]*priceTable
	}

	priceTable struct {
		hk types.PublicKey

		mu        sync.Mutex
		hpt       api.HostPriceTable
		renewTime time.Time
		update    *priceTableUpdate
	}

	priceTableUpdate struct {
		err  error
		done chan struct{}
		hpt  api.HostPriceTable
	}
)

func NewPriceTables() *PriceTables {
	return &PriceTables{
		priceTables: make(map[types.PublicKey]*priceTable),
	}
}

// Fetch returns a price table for the given host
func (pts *PriceTables) Fetch(ctx context.Context, h priceTableFetcher, rev *types.FileContractRevision) (api.HostPriceTable, types.Currency, error) {
	pts.mu.Lock()
	pt, exists := pts.priceTables[h.PublicKey()]
	if !exists {
		pt = &priceTable{
			hk: h.PublicKey(),
		}
		pts.priceTables[h.PublicKey()] = pt
	}
	pts.mu.Unlock()

	return pt.fetch(ctx, h, rev)
}

func (pt *priceTable) ongoingUpdate() (bool, *priceTableUpdate) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	var ongoing bool
	if pt.update == nil {
		pt.update = &priceTableUpdate{done: make(chan struct{})}
	} else {
		ongoing = true
	}

	return ongoing, pt.update
}

func (p *priceTable) fetch(ctx context.Context, h priceTableFetcher, rev *types.FileContractRevision) (api.HostPriceTable, types.Currency, error) {
	// grab the current price table
	p.mu.Lock()
	hpt := p.hpt
	p.mu.Unlock()

	// figure out whether we should update the price table, if not we can return
	if !p.renewTime.IsZero() && time.Now().Before(p.renewTime) {
		return hpt, types.ZeroCurrency, nil
	}

	// figure out whether an update is already ongoing, if there's one ongoing
	// we can either wait or return early depending on whether the price table
	// we have is still usable
	ongoing, update := p.ongoingUpdate()
	if ongoing && time.Now().Add(priceTableValidityLeeway).Before(hpt.Expiry) {
		return hpt, types.ZeroCurrency, nil
	} else if ongoing {
		select {
		case <-ctx.Done():
			return api.HostPriceTable{}, types.ZeroCurrency, fmt.Errorf("%w; %w", errPriceTableUpdateTimedOut, context.Cause(ctx))
		case <-update.done:
		}
		return update.hpt, types.ZeroCurrency, update.err
	}

	// this thread is updating the price table
	hpt, cost, err := h.PriceTable(ctx, rev)

	// signal the other threads that the update is done
	update.hpt = hpt
	update.err = err
	close(update.done)

	p.mu.Lock()
	defer p.mu.Unlock()

	if err == nil {
		p.hpt = hpt

		// renew the prices 2 blocks after receiving them or 30 seconds
		// before expiry, whatever comes first
		p.renewTime = time.Now().Add(priceRefreshInterval) // 2 blocks
		if expiry := p.hpt.Expiry.Add(-priceTableValidityLeeway); expiry.Before(p.renewTime) {
			p.renewTime = expiry
		}
	}
	p.update = nil

	return hpt, cost, err
}
