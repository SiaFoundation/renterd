package e2e

import (
	"context"
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/bus"
	"go.sia.tech/renterd/internal/test"
	"go.sia.tech/renterd/internal/utils"
)

// TestSettings is a test for bus settings.
func TestSettings(t *testing.T) {
	// create cluster
	cluster := newTestCluster(t, clusterOptsDefault)
	defer cluster.Shutdown()

	// convenience variables
	b := cluster.Bus

	// build default pinned settings
	dps := api.DefaultPinnedSettings

	// build default S3 settings
	ds3 := api.S3Settings{
		Authentication: api.S3AuthenticationSettings{
			V4Keypairs: map[string]string{test.S3AccessKeyID: test.S3SecretAccessKey},
		},
	}

	// build default upload settings
	dus := test.UploadSettings
	dus.Packing = api.UploadPackingSettings{
		Enabled:               false,
		SlabBufferMaxSizeSoft: 1 << 32, // 4 GiB,
	}

	// assert gouging settings
	gs, err := b.GougingSettings(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(gs, test.GougingSettings) {
		t.Fatal("expected default test gouging settings", cmp.Diff(gs, test.GougingSettings))
	}

	// perform update
	gs.MaxRPCPrice = gs.MaxRPCPrice.Mul64(2)
	if err := b.UpdateGougingSettings(context.Background(), gs); err != nil {
		t.Fatal(err)
	}

	// perform patch
	if updated, err := b.PatchGougingSettings(context.Background(), map[string]any{"maxDownloadPrice": gs.MaxDownloadPrice.Mul64(2)}); err != nil {
		t.Fatal(err)
	} else if updated.MaxDownloadPrice.Cmp(test.GougingSettings.MaxDownloadPrice.Mul64(2)) != 0 {
		t.Fatal("expected updated max download price to be doubled")
	} else if updated.MaxRPCPrice.Cmp(test.GougingSettings.MaxRPCPrice.Mul64(2)) != 0 {
		t.Fatal("expected updated max RPC price to be unchanged")
	} else if updated.MaxRPCPrice.Add(updated.MaxDownloadPrice).IsZero() {
		t.Fatal("expected test settings to have non-zero max dl and rpc price")
	} else if _, err := b.PatchGougingSettings(context.Background(), map[string]any{"iDoNotExist": types.ZeroCurrency}); !utils.IsErr(err, bus.ErrSettingFieldNotFound) {
		t.Fatal("unexpected error", err)
	}

	// assert pinned settings
	ps, err := b.PinnedSettings(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(ps, dps) {
		t.Fatal("expected default pinned settings", cmp.Diff(ps, dps))
	}

	// perform update
	ps.Currency = "yen"
	if err := b.UpdatePinnedSettings(context.Background(), ps); err != nil {
		t.Fatal(err)
	}

	// perform patch
	if updated, err := b.PatchPinnedSettings(context.Background(), map[string]any{"threshold": 0.666}); err != nil {
		t.Fatal(err)
	} else if updated.Threshold != 0.666 {
		t.Fatal("expected updated threshold to be 0.666")
	} else if updated.Currency != "yen" {
		t.Fatal("expected updated currency to be 'yen'")
	} else if _, err := b.PatchPinnedSettings(context.Background(), map[string]any{"iDoNotExist": types.ZeroCurrency}); !utils.IsErr(err, bus.ErrSettingFieldNotFound) {
		t.Fatal("unexpected error", err)
	}

	// assert upload settings
	us, err := b.UploadSettings(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(us, dus) {
		t.Fatalf("expected default test upload settings, got %v", cmp.Diff(us, dus))
	}

	// perform update
	us.Packing.Enabled = true
	if err := b.UpdateUploadSettings(context.Background(), us); err != nil {
		t.Fatal(err)
	}

	// perform patch
	if updated, err := b.PatchUploadSettings(context.Background(), map[string]any{"redundancy": api.RedundancySettings{MinShards: 10, TotalShards: 30}}); err != nil {
		t.Fatal(err)
	} else if !updated.Packing.Enabled {
		t.Fatal("expected packing to be enabled")
	} else if updated.Redundancy.MinShards != 10 || updated.Redundancy.TotalShards != 30 {
		t.Fatal("expected updated redundancy to be 30/10")
	} else if _, err := b.PatchUploadSettings(context.Background(), map[string]any{"iDoNotExist": types.ZeroCurrency}); !utils.IsErr(err, bus.ErrSettingFieldNotFound) {
		t.Fatal("unexpected error", err)
	}

	// assert success
	us, err = b.UploadSettings(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if !us.Packing.Enabled {
		t.Fatalf("expected upload packing to be disabled by default, got %v", us.Packing.Enabled)
	}

	// assert S3 settings
	s3, err := b.S3Settings(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(s3, ds3) {
		t.Fatalf("expected default test S3 settings, got %v", cmp.Diff(s3, ds3))
	}

	// perform update
	s3AccessKeyFoo := "FOOFOOFOOFOOFOOFOOFOO"
	s3.Authentication.V4Keypairs[s3AccessKeyFoo] = test.S3SecretAccessKey
	if err := b.UpdateS3Settings(context.Background(), s3); err != nil {
		t.Fatal(err)
	}

	// perform patch
	if updated, err := b.PatchS3Settings(context.Background(), map[string]any{"authentication": map[string]any{"v4Keypairs": map[string]string{test.S3AccessKeyID: "mykey"}}}); err != nil {
		t.Fatal(err)
	} else if value, ok := updated.Authentication.V4Keypairs[s3AccessKeyFoo]; !ok || value != test.S3SecretAccessKey {
		t.Fatal("unexpected value", ok, value)
	} else if value, ok := updated.Authentication.V4Keypairs[test.S3AccessKeyID]; !ok || value != "mykey" {
		t.Fatal("unexpected value", ok, value)
	} else if _, err := b.PatchS3Settings(context.Background(), map[string]any{"iDoNotExist": types.ZeroCurrency}); !utils.IsErr(err, bus.ErrSettingFieldNotFound) {
		t.Fatal("unexpected error", err)
	}
}
