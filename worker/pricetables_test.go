package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.sia.tech/renterd/hostdb"
)

func TestPriceTables(t *testing.T) {
	// create host & contract stores
	hs := newHostStoreMock()
	cs := newContractStoreMock()

	// create host manager & price table
	hm := newTestHostManager(t)
	pts := newPriceTables(hm, hs)

	// create host & contract mock
	h := hs.addHost()
	c := cs.addContract(h.hk)

	// expire its price table
	expiredPT := newTestHostPriceTable()
	expiredPT.Expiry = time.Now()
	h.hi.PriceTable = expiredPT

	// manage the host, make sure fetching the price table blocks
	fetchPTBlockChan := make(chan struct{})
	validPT := newTestHostPriceTable()
	hm.addHost(newTestHostCustom(h, c, func() hostdb.HostPriceTable {
		<-fetchPTBlockChan
		return validPT
	}))

	// trigger a fetch to make it block
	go pts.fetch(context.Background(), h.hk, nil)
	time.Sleep(50 * time.Millisecond)

	// fetch it again but with a canceled context to avoid blocking
	// indefinitely, the error will indicate we were blocking on a price table
	// update
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := pts.fetch(ctx, h.hk, nil)
	if !errors.Is(err, errPriceTableUpdateTimedOut) {
		t.Fatal("expected errPriceTableUpdateTimedOut, got", err)
	}

	// unblock and assert we receive a valid price table
	close(fetchPTBlockChan)
	update, err := pts.fetch(context.Background(), h.hk, nil)
	if err != nil {
		t.Fatal(err)
	} else if update.UID != validPT.UID {
		t.Fatal("price table mismatch")
	}

	// refresh the price table on the host, update again, assert we receive the
	// same price table as it hasn't expired yet
	h.hi.PriceTable = newTestHostPriceTable()
	update, err = pts.fetch(context.Background(), h.hk, nil)
	if err != nil {
		t.Fatal(err)
	} else if update.UID != validPT.UID {
		t.Fatal("price table mismatch")
	}

	// manually expire the price table
	pts.priceTables[h.hk].hpt.Expiry = time.Now()

	// fetch it again and assert we updated the price table
	update, err = pts.fetch(context.Background(), h.hk, nil)
	if err != nil {
		t.Fatal(err)
	} else if update.UID != h.hi.PriceTable.UID {
		t.Fatal("price table mismatch")
	}
}
