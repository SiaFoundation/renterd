package worker

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/renterd/rhp/v3"
	rhpv3 "go.sia.tech/renterd/rhp/v3"
	"go.sia.tech/siad/types"
)

type priceTables struct {
	mu          sync.Mutex
	priceTables map[consensus.PublicKey]*priceTable
}

type priceTable struct {
	pt     *rhp.HostPriceTable
	hk     consensus.PublicKey
	expiry time.Time

	w *worker

	mu            sync.Mutex
	ongoingUpdate *priceTableUpdate
}

type priceTableUpdate struct {
	err  error
	done chan struct{}
	pt   *rhp.HostPriceTable
}

// PriceTable returns a price table for the give host and an bool to indicate
// whether it is valid or not.
func (pts *priceTables) PriceTable(hk consensus.PublicKey) (rhp.HostPriceTable, bool) {
	// TODO: Check if valid price table exists and handle the following edge cases.
	// - If no price table exists, we fetch a new one.
	// - If a price table exists that is valid but about to expire use it but launch an update.
	// - If a price table exists that is valid and doesn't expire soon, use it.
	pt := pts.priceTable(hk)

	// TODO: If the pricetable is valid, kick off a non-blocking update
	// using an EA.

	return pt.convert()
}

// Update updates a price table with the given host. If a revision is provided,
// the table will be paid for using that revision. Otherwise an ephemeral
// account will be used. The new table is returned.
func (pts *priceTables) Update(ctx context.Context, f rhpv3.PriceTablePaymentFunc, hostIP string, hk consensus.PublicKey) (rhp.HostPriceTable, error) {
	// Fetch the price table to update.
	pt := pts.priceTable(hk)

	// Check if there is some update going on already. If not, create one.
	pt.mu.Lock()
	ongoing := pt.ongoingUpdate
	var performUpdate bool
	if ongoing == nil {
		ongoing = &priceTableUpdate{
			done: make(chan struct{}),
		}
		pt.ongoingUpdate = ongoing
		performUpdate = true
	}
	pt.mu.Unlock()

	// If this thread is not supposed to perform the update, just block and
	// return the result.
	if !performUpdate {
		select {
		case <-ctx.Done():
			return rhp.HostPriceTable{}, errors.New("timeout while blocking for pricetable update")
		case <-ongoing.done:
		}
		if ongoing.err == nil {
			return *ongoing.pt, nil
		} else {
			return rhp.HostPriceTable{}, ongoing.err
		}
	}

	// Update price table.
	var hpt rhpv3.HostPriceTable
	err := pt.w.withTransportV3(ctx, hostIP, hk, func(t *rhpv3.Transport) (err error) {
		hpt, err = rhpv3.RPCPriceTable(t, f)
		return err
	})

	pt.mu.Lock()
	// On success we update the pt.
	if err == nil {
		pt.pt = &hpt
		pt.expiry = time.Now().Add(hpt.Validity)
	}
	// Signal that the update is over.
	ongoing.err = err
	close(ongoing.done)
	pt.ongoingUpdate = nil
	pt.mu.Unlock()
	return hpt, err
}

// priceTable returns a priceTable from priceTables for the given host or
// creates a new one.
func (pts *priceTables) priceTable(hk consensus.PublicKey) *priceTable {
	pts.mu.Lock()
	defer pts.mu.Unlock()
	pt, exists := pts.priceTables[hk]
	if !exists {
		pt = &priceTable{
			hk: hk,
		}
		pts.priceTables[hk] = pt
	}
	return pt
}

// convert turn the priceTable into a rhp.HostPriceTable and also returns
// whether or not the price table is valid at the given time.
func (pt *priceTable) convert() (rhp.HostPriceTable, bool) {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	if pt.pt == nil {
		return rhp.HostPriceTable{}, false
	}
	return *pt.pt, time.Now().Before(pt.expiry)
}

// preparePriceTableContractPayment prepare a payment function to pay for a
// price table from the given host using the provided revision.
func (w *worker) preparePriceTableContractPayment(hk consensus.PublicKey, revision *types.FileContractRevision) rhpv3.PriceTablePaymentFunc {
	return func(pt rhpv3.HostPriceTable) (rhpv3.PaymentMethod, error) {
		// TODO: gouging check on price table

		refundAccount := rhp.Account(w.deriveAccountKey(hk).PublicKey())
		rk := w.deriveRenterKey(hk)
		payment, ok := rhpv3.PayByContract(revision, pt.UpdatePriceTableCost, refundAccount, rk)
		if !ok {
			return nil, errors.New("insufficient funds")
		}
		return &payment, nil
	}
}

// preparePriceTableAccountPayment prepare a payment function to pay for a
// price table from the given host using the provided revision.
func (w *worker) preparePriceTableAccountPayment(hk consensus.PublicKey, revision *types.FileContractRevision) rhpv3.PriceTablePaymentFunc {
	return func(pt rhpv3.HostPriceTable) (rhpv3.PaymentMethod, error) {
		// TODO: gouging check on price table

		accountKey := w.deriveAccountKey(hk)
		account := rhpv3.Account(accountKey.PublicKey())
		payment := rhpv3.PayByEphemeralAccount(account, pt.UpdatePriceTableCost, math.MaxUint64, accountKey)
		return &payment, nil
	}
}
