package hosts

import (
	"context"
	"errors"
	"testing"
	"time"

	rhpv4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/v2/api"
	"go.sia.tech/renterd/v2/internal/gouging"
	rhp4 "go.sia.tech/renterd/v2/internal/rhp/v4"
)

type mockGougingStore struct {
	gp  api.GougingParams
	err error
}

func (m mockGougingStore) GougingParams(ctx context.Context) (api.GougingParams, error) {
	return m.gp, m.err
}

type mockSettingsFetcher struct {
	settings rhp4.HostSettings
	err      error
}

func (m mockSettingsFetcher) Settings(ctx context.Context, hk types.PublicKey, addr string) (rhp4.HostSettings, error) {
	return m.settings, m.err
}

func TestFetchPrices(t *testing.T) {
	gp := api.GougingParams{
		GougingSettings: api.GougingSettings{
			MaxContractPrice:      types.Siacoins(1),
			MaxDownloadPrice:      types.Siacoins(1),
			MaxUploadPrice:        types.Siacoins(1),
			MaxStoragePrice:       types.Siacoins(1),
			HostBlockHeightLeeway: 1,
			MinPriceTableValidity: api.DurationMS(time.Minute),
		},
		ConsensusState: api.ConsensusState{
			BlockHeight:   10,
			Synced:        true,
			LastBlockTime: api.TimeRFC3339(time.Now()),
		},
	}
	goodSettings := func() rhp4.HostSettings {
		return rhp4.HostSettings{
			HostSettings: rhpv4.HostSettings{
				MaxCollateral: types.Siacoins(1),
				Prices: rhpv4.HostPrices{
					ContractPrice:   types.Siacoins(1),
					Collateral:      types.Siacoins(1),
					StoragePrice:    types.Siacoins(1),
					IngressPrice:    types.Siacoins(1),
					EgressPrice:     types.Siacoins(1),
					FreeSectorPrice: types.Siacoins(1).Div64((1 << 40) / rhpv4.SectorSize),
					TipHeight:       10,
				},
			},
			Validity: time.Minute,
		}
	}

	gs := mockGougingStore{gp: gp}
	hi := api.HostInfo{PublicKey: types.PublicKey{1}, V2SiamuxAddresses: []string{"host:1234"}}

	// good settings -> returns prices, no error
	prices, err := fetchPrices(context.Background(), mockSettingsFetcher{settings: goodSettings()}, gs, hi)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	} else if prices != goodSettings().Prices {
		t.Fatal("expected prices to match")
	}

	// gouging storage price -> ErrHostSettingsGouging
	bad := goodSettings()
	bad.Prices.StoragePrice = bad.Prices.StoragePrice.Add(types.NewCurrency64(1))
	if _, err := fetchPrices(context.Background(), mockSettingsFetcher{settings: bad}, gs, hi); !errors.Is(err, gouging.ErrHostSettingsGouging) {
		t.Fatalf("expected ErrHostSettingsGouging, got %v", err)
	}

	// gouging download price -> ErrHostSettingsGouging
	bad = goodSettings()
	bad.Prices.EgressPrice = bad.Prices.EgressPrice.Add(types.NewCurrency64(1))
	if _, err := fetchPrices(context.Background(), mockSettingsFetcher{settings: bad}, gs, hi); !errors.Is(err, gouging.ErrHostSettingsGouging) {
		t.Fatalf("expected ErrHostSettingsGouging, got %v", err)
	}

	// gouging upload price -> ErrHostSettingsGouging
	bad = goodSettings()
	bad.Prices.IngressPrice = bad.Prices.IngressPrice.Add(types.NewCurrency64(1))
	if _, err := fetchPrices(context.Background(), mockSettingsFetcher{settings: bad}, gs, hi); !errors.Is(err, gouging.ErrHostSettingsGouging) {
		t.Fatalf("expected ErrHostSettingsGouging, got %v", err)
	}

	// settings fetcher error propagates (and gouging check is skipped)
	fetchErr := errors.New("host unreachable")
	if _, err := fetchPrices(context.Background(), mockSettingsFetcher{err: fetchErr}, gs, hi); !errors.Is(err, fetchErr) {
		t.Fatalf("expected fetch error to propagate, got %v", err)
	}

	// GougingStore error propagates
	storeErr := errors.New("bus is down")
	if _, err := fetchPrices(context.Background(), mockSettingsFetcher{settings: goodSettings()}, mockGougingStore{err: storeErr}, hi); !errors.Is(err, storeErr) {
		t.Fatalf("expected store error to propagate, got %v", err)
	}
}
