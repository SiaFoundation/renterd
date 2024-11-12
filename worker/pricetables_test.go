package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/test/mocks"
)

func TestPriceTables(t *testing.T) {
	// create host & contract stores
	hs := mocks.NewHostStore()
	cs := mocks.NewContractStore()

	// create host manager & price table
	hm := newTestHostManager(t)
	pts := newPriceTables(hm, hs)

	// create host & contract mock
	h := hs.AddHost()
	c := cs.AddContract(h.PublicKey())

	cm := mocks.NewChain(api.ConsensusState{
		BlockHeight: 1,
	})
	cState, _ := cm.ConsensusState(context.Background())

	blockHeightLeeway := 10
	gCtx := WithGougingChecker(context.Background(), cm, api.GougingParams{
		ConsensusState: cState,
		GougingSettings: api.GougingSettings{
			HostBlockHeightLeeway: blockHeightLeeway,
		},
	})

	// expire its price table
	expiredPT := newTestHostPriceTable()
	expiredPT.Expiry = time.Now()
	h.UpdatePriceTable(expiredPT)

	// manage the host, make sure fetching the price table blocks
	fetchPTBlockChan := make(chan struct{})
	validPT := newTestHostPriceTable()
	hm.addHost(newTestHostCustom(h, c, func() api.HostPriceTable {
		<-fetchPTBlockChan
		return validPT
	}))

	// trigger a fetch to make it block
	go pts.fetch(gCtx, h.PublicKey(), nil)
	time.Sleep(50 * time.Millisecond)

	// fetch it again but with a canceled context to avoid blocking
	// indefinitely, the error will indicate we were blocking on a price table
	// update
	ctx, cancel := context.WithCancel(gCtx)
	cancel()
	_, _, err := pts.fetch(ctx, h.PublicKey(), nil)
	if !errors.Is(err, errPriceTableUpdateTimedOut) {
		t.Fatal("expected errPriceTableUpdateTimedOut, got", err)
	}

	// unblock and assert we paid for the price table
	close(fetchPTBlockChan)
	update, _, err := pts.fetch(gCtx, h.PublicKey(), nil)
	if err != nil {
		t.Fatal(err)
	} else if update.UID != validPT.UID {
		t.Fatal("price table mismatch")
	}

	// refresh the price table on the host, update again, assert we receive the
	// same price table as it hasn't expired yet
	h.UpdatePriceTable(newTestHostPriceTable())
	update, _, err = pts.fetch(gCtx, h.PublicKey(), nil)
	if err != nil {
		t.Fatal(err)
	} else if update.UID != validPT.UID {
		t.Fatal("price table mismatch")
	}

	// manually expire the price table
	pts.priceTables[h.PublicKey()].hpt.Expiry = time.Now()

	// fetch it again and assert we updated the price table
	update, _, err = pts.fetch(gCtx, h.PublicKey(), nil)
	if err != nil {
		t.Fatal(err)
	} else if update.UID != h.PriceTable().UID {
		t.Fatal("price table mismatch")
	}

	// refresh the price table on the host and make sure fetching doesn't update
	// the price table since it's not expired
	validPT = h.PriceTable()
	h.UpdatePriceTable(newTestHostPriceTable())
	update, _, err = pts.fetch(gCtx, h.PublicKey(), nil)
	if err != nil {
		t.Fatal(err)
	} else if update.UID != validPT.UID {
		t.Fatal("price table mismatch")
	}

	// increase the current block height to be exactly
	// 'priceTableBlockHeightLeeway' blocks before the leeway of the gouging
	// settings
	cm.UpdateHeight(validPT.HostBlockHeight + uint64(blockHeightLeeway) - priceTableBlockHeightLeeway)

	// fetch it again and assert we updated the price table
	update, _, err = pts.fetch(gCtx, h.PublicKey(), nil)
	if err != nil {
		t.Fatal(err)
	} else if update.UID != h.PriceTable().UID {
		t.Fatal("price table mismatch")
	}
}
