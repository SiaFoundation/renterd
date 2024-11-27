package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	rhpv4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/test/mocks"
)

func TestPricesCache(t *testing.T) {
	cache := newPricesCache()
	hk := types.PublicKey{1}
	hostMock := mocks.NewHost(hk)
	c := mocks.NewContract(hk, types.FileContractID{1})

	// expire its prices
	expiredPT := newTestHostPriceTable()
	expiredPT.Expiry = time.Now()
	hostMock.UpdatePriceTable(expiredPT)

	// manage the host, make sure fetching the prices blocks
	fetchPTBlockChan := make(chan struct{})
	validPrices := newTestHostPrices()
	h := newTestHostCustom(hostMock, c, func() api.HostPriceTable {
		t.Fatal("shouldn't be called")
		return api.HostPriceTable{}
	}, func() rhpv4.HostPrices {
		<-fetchPTBlockChan
		return validPrices
	})

	// trigger a fetch to make it block
	go cache.fetch(context.Background(), h)
	time.Sleep(50 * time.Millisecond)

	// fetch it again but with a canceled context to avoid blocking
	// indefinitely, the error will indicate we were blocking on a prices
	// update
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := cache.fetch(ctx, h)
	if !errors.Is(err, errPriceTableUpdateTimedOut) {
		t.Fatal("expected errPriceTableUpdateTimedOut, got", err)
	}

	// unblock and assert we paid for the prices
	close(fetchPTBlockChan)
	update, err := cache.fetch(context.Background(), h)
	if err != nil {
		t.Fatal(err)
	} else if update.Signature != validPrices.Signature {
		t.Fatal("prices mismatch")
	}

	// refresh the prices on the host, update again, assert we receive the
	// same prices as it hasn't expired yet
	oldValidPrices := validPrices
	validPrices = newTestHostPrices()
	h.UpdatePrices(validPrices)
	update, err = cache.fetch(context.Background(), h)
	if err != nil {
		t.Fatal(err)
	} else if update.Signature != oldValidPrices.Signature {
		t.Fatal("prices mismatch")
	}

	// manually expire the prices
	cache.cache[h.PublicKey()].renewTime = time.Now().Add(-time.Second)
	update, err = cache.fetch(context.Background(), h)
	if err != nil {
		t.Fatal(err)
	} else if update.Signature != validPrices.Signature {
		t.Fatal("prices mismatch")
	}
}
