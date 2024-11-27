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

	cache := newPricesCache()
	hk := types.PublicKey{1}
	hostMock := mocks.NewHost(hk)
	c := mocks.NewContract(hk, types.FileContractID{1})

	// expire its price table
	expiredPT := newTestHostPriceTable()
	expiredPT.Expiry = time.Now()
	hostMock.UpdatePriceTable(expiredPT)

	// manage the host, make sure fetching the price table blocks
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
	go cache.fetch(gCtx, h)
	time.Sleep(50 * time.Millisecond)

	// fetch it again but with a canceled context to avoid blocking
	// indefinitely, the error will indicate we were blocking on a price table
	// update
	ctx, cancel := context.WithCancel(gCtx)
	cancel()
	_, err := cache.fetch(ctx, h)
	if !errors.Is(err, errPriceTableUpdateTimedOut) {
		t.Fatal("expected errPriceTableUpdateTimedOut, got", err)
	}

	// unblock and assert we paid for the price table
	close(fetchPTBlockChan)
	update, err := cache.fetch(gCtx, h)
	if err != nil {
		t.Fatal(err)
	} else if update.Signature != validPrices.Signature {
		t.Fatal("price table mismatch")
	}

	// refresh the price table on the host, update again, assert we receive the
	// same price table as it hasn't expired yet
	h.UpdatePriceTable(newTestHostPriceTable())
	update, err = cache.fetch(gCtx, h)
	if err != nil {
		t.Fatal(err)
	} else if update.Signature != validPrices.Signature {
		t.Fatal("price table mismatch")
	}

	// increase the current block height to be exactly
	// 'priceTableBlockHeightLeeway' blocks before the leeway of the gouging
	// settings
	h.UpdatePrices(newTestHostPrices())
	validPrices = h.HostPrices()
	cm.UpdateHeight(validPrices.TipHeight + uint64(blockHeightLeeway) - priceTableBlockHeightLeeway)

	// fetch it again and assert we updated the price table
	update, err = cache.fetch(gCtx, h)
	if err != nil {
		t.Fatal(err)
	} else if update.Signature != h.HostPrices().Signature {
		t.Fatal("price table mismatch")
	}
}
