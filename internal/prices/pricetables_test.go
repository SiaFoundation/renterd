package prices

import (
	"context"
	"errors"
	"testing"
	"time"

	rhpv3 "go.sia.tech/core/rhp/v3"
	rhpv4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/v2/api"
	"go.sia.tech/renterd/v2/internal/test/mocks"
	"lukechampine.com/frand"
)

func newTestHostPriceTable() api.HostPriceTable {
	var uid rhpv3.SettingsID
	frand.Read(uid[:])

	return api.HostPriceTable{
		HostPriceTable: rhpv3.HostPriceTable{UID: uid, HostBlockHeight: 100, Validity: time.Minute},
		Expiry:         time.Now().Add(70 * time.Second),
	}
}

func TestPriceTables(t *testing.T) {
	// create host manager & price table
	pts := NewPriceTables()

	// create host & contract mock
	hostMock := mocks.NewHost(types.PublicKey{1})

	// expire its price table
	expiredPT := newTestHostPriceTable()
	expiredPT.Expiry = time.Now()
	hostMock.UpdatePriceTable(expiredPT)

	// manage the host, make sure fetching the price table blocks
	fetchPTBlockChan := make(chan struct{})
	validPT := newTestHostPriceTable()
	h := &pricesFetcher{
		hk: types.PublicKey{1},
		hptFn: func() api.HostPriceTable {
			<-fetchPTBlockChan
			return validPT
		},
		pFn: func() rhpv4.HostPrices {
			t.Fatal("shouldn't be called")
			return rhpv4.HostPrices{}
		},
	}

	// trigger a fetch to make it block
	go pts.Fetch(context.Background(), h, nil)
	time.Sleep(50 * time.Millisecond)

	// fetch it again but with a canceled context to avoid blocking
	// indefinitely, the error will indicate we were blocking on a price table
	// update
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err := pts.Fetch(ctx, h, nil)
	if !errors.Is(err, errPriceTableUpdateTimedOut) {
		t.Fatal("expected errPriceTableUpdateTimedOut, got", err)
	}

	// unblock and assert we paid for the price table
	close(fetchPTBlockChan)
	update, _, err := pts.Fetch(context.Background(), h, nil)
	if err != nil {
		t.Fatal(err)
	} else if update.UID != validPT.UID {
		t.Fatal("price table mismatch")
	}

	// refresh the price table on the host, update again, assert we receive the
	// same price table as it hasn't expired yet
	oldValidPT := validPT
	validPT = newTestHostPriceTable()
	update, _, err = pts.Fetch(context.Background(), h, nil)
	if err != nil {
		t.Fatal(err)
	} else if update.UID != oldValidPT.UID {
		t.Fatal("price table mismatch")
	}

	// manually expire the price table
	pts.priceTables[h.PublicKey()].renewTime = time.Now().Add(-time.Second)
	update, _, err = pts.Fetch(context.Background(), h, nil)
	if err != nil {
		t.Fatal(err)
	} else if update.UID != validPT.UID {
		t.Fatal("price table mismatch")
	}
}
