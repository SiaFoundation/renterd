package worker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/hostdb"
)

var (
	errHostNotFound = errors.New("host not found")
)

var (
	_ HostStore = (*mockHostStore)(nil)
)

type mockHostStore struct {
	mu    sync.Mutex
	hosts map[types.PublicKey]hostdb.HostInfo
}

func (mhs *mockHostStore) Host(ctx context.Context, hostKey types.PublicKey) (hostdb.HostInfo, error) {
	mhs.mu.Lock()
	defer mhs.mu.Unlock()

	h, ok := mhs.hosts[hostKey]
	if !ok {
		return hostdb.HostInfo{}, errHostNotFound
	}
	return h, nil
}

func newMockHostStore(hosts []*hostdb.HostInfo) *mockHostStore {
	hs := &mockHostStore{hosts: make(map[types.PublicKey]hostdb.HostInfo)}
	for _, h := range hosts {
		hs.hosts[h.PublicKey] = *h
	}
	return hs
}

func TestPriceTables(t *testing.T) {
	// create two price tables, a valid one and one that expired
	expiredPT := newTestHostPriceTable(time.Now())
	validPT := newTestHostPriceTable(time.Now().Add(time.Minute))

	// create a mock host that has a valid price table
	hk1 := types.PublicKey{1}
	h1 := newMockHost(hk1)
	h1.hpt = validPT

	// create host manager
	hm := newMockHostManager()
	hm.addHost(h1)

	// create a hostdb entry for that host that returns the expired price table
	hdb1 := &hostdb.HostInfo{
		Host: hostdb.Host{
			PublicKey:  hk1,
			PriceTable: expiredPT,
			Scanned:    true,
		},
	}

	// create host store
	hs := newMockHostStore([]*hostdb.HostInfo{hdb1})

	// create price tables
	pts := newPriceTables(hm, hs)

	// fetch the price table in a goroutine, make it blocking
	h1.hptBlockChan = make(chan struct{})
	go pts.fetch(context.Background(), hk1, nil)
	time.Sleep(50 * time.Millisecond)

	// fetch it again but with a canceled context to avoid blocking indefinitely, the error will indicate we were blocking on a price table update
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := pts.fetch(ctx, hk1, nil)
	if !errors.Is(err, errPriceTableUpdateTimedOut) {
		t.Fatal("expected errPriceTableUpdateTimedOut, got", err)
	}

	// unblock and assert we receive a valid price table
	close(h1.hptBlockChan)
	update, err := pts.fetch(context.Background(), hk1, nil)
	if err != nil {
		t.Fatal(err)
	} else if update.UID != validPT.UID {
		t.Fatal("price table mismatch")
	}

	// refresh the price table on the host, update again, assert we receive the
	// same price table as it hasn't expired yet
	h1.hpt = newTestHostPriceTable(time.Now().Add(time.Minute))
	update, err = pts.fetch(context.Background(), hk1, nil)
	if err != nil {
		t.Fatal(err)
	} else if update.UID != validPT.UID {
		t.Fatal("price table mismatch")
	}

	// manually expire the price table
	pts.priceTables[hk1].hpt.Expiry = time.Now()

	// fetch it again and assert we updated the price table
	update, err = pts.fetch(context.Background(), hk1, nil)
	if err != nil {
		t.Fatal(err)
	} else if update.UID != h1.hpt.UID {
		t.Fatal("price table mismatch")
	}
}
