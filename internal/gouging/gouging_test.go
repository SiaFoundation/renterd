package gouging

import (
	"testing"
	"time"

	"go.sia.tech/renterd/v2/api"
	"go.sia.tech/renterd/v2/internal/rhp/v4"

	rhpv4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
)

func TestCheck(t *testing.T) {
	goodSettings := func() rhp.HostSettings {
		return rhp.HostSettings{
			HostSettings: rhpv4.HostSettings{
				MaxCollateral: types.Siacoins(1),
				Prices: rhpv4.HostPrices{
					ContractPrice:   types.Siacoins(1),
					Collateral:      types.Siacoins(1),
					StoragePrice:    types.Siacoins(1),
					IngressPrice:    types.Siacoins(1),
					EgressPrice:     types.Siacoins(1),
					FreeSectorPrice: types.Siacoins(1).Div64((1 << 40) / rhpv4.SectorSize), // 1 SC / TiB
					TipHeight:       10,
				},
			},
			Validity: time.Minute,
		}
	}
	gc := NewChecker(api.GougingSettings{
		MaxContractPrice:      types.Siacoins(1),
		MaxDownloadPrice:      types.Siacoins(1),
		MaxUploadPrice:        types.Siacoins(1),
		MaxStoragePrice:       types.Siacoins(1),
		HostBlockHeightLeeway: 1,
		MinPriceTableValidity: api.DurationMS(time.Minute),
	}, api.ConsensusState{
		BlockHeight:   10,
		Synced:        true,
		LastBlockTime: api.TimeRFC3339(time.Now()),
	})

	// good settings
	if gb := gc.Check(goodSettings()); gb.Gouging() {
		t.Fatal("shouldn't be gouging", gb)
	}

	// gouging on 'MaxContractPrice'
	settings := goodSettings()
	settings.Prices.ContractPrice = settings.Prices.ContractPrice.Add(types.NewCurrency64(1))
	if gb := gc.Check(settings); !gb.Gouging() || gb.GougingErr == "" {
		t.Fatal("should be gouging", gb)
	}

	// gouging on 'MaxDownloadPrice'
	settings = goodSettings()
	settings.Prices.EgressPrice = settings.Prices.EgressPrice.Add(types.NewCurrency64(1))
	if gb := gc.Check(settings); !gb.Gouging() || gb.DownloadErr == "" {
		t.Fatal("should be gouging", gb)
	}

	// gouging on 'MaxUploadPrice'
	settings = goodSettings()
	settings.Prices.IngressPrice = settings.Prices.IngressPrice.Add(types.NewCurrency64(1))
	if gb := gc.Check(settings); !gb.Gouging() || gb.UploadErr == "" {
		t.Fatal("should be gouging", gb)
	}

	// gouging on 'MaxStoragePrice'
	settings = goodSettings()
	settings.Prices.StoragePrice = settings.Prices.StoragePrice.Add(types.NewCurrency64(1))
	if gb := gc.Check(settings); !gb.Gouging() || gb.UploadErr == "" {
		t.Fatal("should be gouging", gb)
	}

	// gouging on 'MinBlockHeightLeeway'
	settings = goodSettings()
	settings.Prices.TipHeight += uint64(gc.(checker).settings.HostBlockHeightLeeway) + 1
	if gb := gc.Check(settings); !gb.Gouging() || gb.GougingErr == "" {
		t.Fatal("should be gouging", gb)
	}

	// gouging on 'MinPriceTableValidity'
	settings = goodSettings()
	settings.Validity -= time.Millisecond
	if gb := gc.Check(settings); !gb.Gouging() || gb.GougingErr == "" {
		t.Fatal("should be gouging", gb)
	}
}
