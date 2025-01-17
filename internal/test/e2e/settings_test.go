package e2e

import (
	"context"
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/test"
)

// TestSettings is a test for bus settings.
func TestE2ESettings(t *testing.T) {
	// create cluster
	cluster := newTestCluster(t, clusterOptsDefault)
	defer cluster.Shutdown()

	// convenience variables
	b := cluster.Bus

	// assert gouging settings match defaults
	gs, err := b.GougingSettings(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(gs, test.GougingSettings) {
		t.Fatal("expected default test gouging settings", cmp.Diff(gs, test.GougingSettings))
	}

	// assert update gouging settings
	gs.MaxRPCPrice = types.Siacoins(123)
	if err := b.UpdateGougingSettings(context.Background(), gs); err != nil {
		t.Fatal(err)
	} else if gs, err := b.GougingSettings(context.Background()); err != nil {
		t.Fatal(err)
	} else if gs.MaxRPCPrice.Cmp(types.Siacoins(123)) != 0 {
		t.Fatal("expected updated max RPC price to be 123 SC")
	}

	// assert pinned settings match defaults
	ps, err := b.PinnedSettings(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(ps, api.DefaultPinnedSettings) {
		t.Fatal("expected default pinned settings", cmp.Diff(ps, api.DefaultPinnedSettings))
	}

	// assert update pinned settings
	ps.Currency = "yen"
	if err := b.UpdatePinnedSettings(context.Background(), ps); err != nil {
		t.Fatal(err)
	} else if ps, err := b.PinnedSettings(context.Background()); err != nil {
		t.Fatal(err)
	} else if ps.Currency != "yen" {
		t.Fatal("expected updated currency to be yen")
	}

	// assert upload settings match defaults
	dus := test.UploadSettings
	dus.Packing = api.UploadPackingSettings{
		Enabled:               false,
		SlabBufferMaxSizeSoft: 1 << 32, // 4 GiB,
	}
	us, err := b.UploadSettings(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(us, dus) {
		t.Fatalf("expected default test upload settings, got %v", cmp.Diff(us, dus))
	}

	// assert update upload settings
	us.Packing.Enabled = true
	if err := b.UpdateUploadSettings(context.Background(), us); err != nil {
		t.Fatal(err)
	} else if us, err = b.UploadSettings(context.Background()); err != nil {
		t.Fatal(err)
	} else if !us.Packing.Enabled {
		t.Fatalf("expected upload packing to be disabled by default, got %v", us.Packing.Enabled)
	}

	// assert S3 settings
	ds3 := api.S3Settings{
		Authentication: api.S3AuthenticationSettings{
			V4Keypairs: map[string]string{test.S3AccessKeyID: test.S3SecretAccessKey},
		},
	}
	s3, err := b.S3Settings(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(s3, ds3) {
		t.Fatalf("expected default test S3 settings, got %v", cmp.Diff(s3, ds3))
	}

	// assert update S3 settings
	s3AccessKeyFoo := "FOOFOOFOOFOOFOOFOOFOO"
	s3.Authentication.V4Keypairs[s3AccessKeyFoo] = test.S3SecretAccessKey
	if err := b.UpdateS3Settings(context.Background(), s3); err != nil {
		t.Fatal(err)
	} else if s3, err = b.S3Settings(context.Background()); err != nil {
		t.Fatal(err)
	} else if s3.Authentication.V4Keypairs[s3AccessKeyFoo] != test.S3SecretAccessKey {
		t.Fatalf("expected updated S3 access key to be %v", s3AccessKeyFoo)
	}
}
