package worker

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"lukechampine.com/frand"
)

const (
	// priceTableValidityLeeway is a leeway we apply when deeming a price table safe
	// for use, we essentially add 30 seconds to the current time when checking
	// whether we are still before a pricetable's expiry time
	priceTableValidityLeeway = 30 * time.Second

	// priceTableBlockHeightLeeway is the amount of blocks before a price table
	// is considered gouging on the block height when we renew it even if it is
	// still valid
	priceTableBlockHeightLeeway = 2
)

var (
	// errPriceTableUpdateTimedOut is returned when we timeout waiting for
	// another thread that's performing a price table update
	errPriceTableUpdateTimedOut = errors.New("timeout while blocking for pricetable update")
)

type (
	priceTables struct {
		hm HostManager
		hs HostStore

		mu          sync.Mutex
		priceTables map[types.PublicKey]*priceTable
	}

	priceTable struct {
		hm HostManager
		hs HostStore
		hk types.PublicKey

		mu     sync.Mutex
		hpt    api.HostPriceTable
		update *priceTableUpdate
	}

	priceTableUpdate struct {
		err  error
		done chan struct{}
		hpt  api.HostPriceTable
	}
)

func (w *worker) initPriceTables() {
	if w.priceTables != nil {
		panic("priceTables already initialized") // developer error
	}
	w.priceTables = newPriceTables(w, w.bus)
}

func newPriceTables(hm HostManager, hs HostStore) *priceTables {
	return &priceTables{
		hm: hm,
		hs: hs,

		priceTables: make(map[types.PublicKey]*priceTable),
	}
}

// fetch returns a price table for the given host
func (pts *priceTables) fetch(ctx context.Context, hk types.PublicKey, rev *types.FileContractRevision) (api.HostPriceTable, error) {
	pts.mu.Lock()
	pt, exists := pts.priceTables[hk]
	if !exists {
		pt = &priceTable{
			hm: pts.hm,
			hs: pts.hs,
			hk: hk,
		}
		pts.priceTables[hk] = pt
	}
	pts.mu.Unlock()

	return pt.fetch(ctx, rev)
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

func (p *priceTable) fetch(ctx context.Context, rev *types.FileContractRevision) (hpt api.HostPriceTable, err error) {
	// grab the current price table
	p.mu.Lock()
	hpt = p.hpt
	p.mu.Unlock()

	// get gouging checker to figure out how many blocks we have left before the
	// current price table is considered to gouge on the block height
	gc, err := GougingCheckerFromContext(ctx, false)
	if err != nil {
		return api.HostPriceTable{}, err
	}

	// figure out whether we should update the price table, if not we can return
	if hpt.UID != (rhpv3.SettingsID{}) {
		randomUpdateLeeway := frand.Intn(int(math.Floor(hpt.HostPriceTable.Validity.Seconds() * 0.1)))
		closeToGouging := gc.BlocksUntilBlockHeightGouging(hpt.HostBlockHeight) <= priceTableBlockHeightLeeway
		closeToExpiring := time.Now().Add(priceTableValidityLeeway).Add(time.Duration(randomUpdateLeeway) * time.Second).After(hpt.Expiry)
		if !closeToExpiring && !closeToGouging {
			return
		}
	}

	// figure out whether an update is already ongoing, if there's one ongoing
	// we can either wait or return early depending on whether the price table
	// we have is still usable
	ongoing, update := p.ongoingUpdate()
	if ongoing && time.Now().Add(priceTableValidityLeeway).Before(hpt.Expiry) {
		return
	} else if ongoing {
		select {
		case <-ctx.Done():
			return api.HostPriceTable{}, fmt.Errorf("%w; %w", errPriceTableUpdateTimedOut, context.Cause(ctx))
		case <-update.done:
		}
		return update.hpt, update.err
	}

	// this thread is updating the price table
	defer func() {
		update.hpt = hpt
		update.err = err
		close(update.done)

		p.mu.Lock()
		if err == nil {
			p.hpt = hpt
		}
		p.update = nil
		p.mu.Unlock()
	}()

	// fetch the host, return early if it has a valid price table
	host, err := p.hs.Host(ctx, p.hk)
	if err == nil && host.Scanned && time.Now().Add(priceTableValidityLeeway).Before(host.PriceTable.Expiry) {
		hpt = host.PriceTable
		return
	}

	// sanity check the host has been scanned before fetching the price table
	if !host.Scanned {
		return api.HostPriceTable{}, fmt.Errorf("host %v was not scanned", p.hk)
	}

	// otherwise fetch it
	h := p.hm.Host(p.hk, types.FileContractID{}, host.Settings.SiamuxAddr())
	hpt, err = h.FetchPriceTable(ctx, rev)
	if err != nil {
		return api.HostPriceTable{}, fmt.Errorf("failed to update pricetable, err %v", err)
	}

	return
}
