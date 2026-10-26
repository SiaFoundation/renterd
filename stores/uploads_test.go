package stores

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/v2/api"
	"go.sia.tech/renterd/v2/config"
)

func TestUploadingSectors(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()
	ctx := context.Background()

	uID1 := api.UploadID{1}
	uID2 := api.UploadID{2}

	assertSectors := func(expected ...types.Hash256) {
		t.Helper()
		roots, err := ss.UploadingSectors(ctx)
		if err != nil {
			t.Fatal(err)
		} else if len(roots) != len(expected) {
			t.Fatalf("unexpected number of sectors: %v != %v", len(roots), len(expected))
		}
		got := make(map[types.Hash256]struct{})
		for _, root := range roots {
			got[root] = struct{}{}
		}
		for _, root := range expected {
			if _, ok := got[root]; !ok {
				t.Fatalf("missing sector %v", root)
			}
		}
	}

	// adding sectors to an unknown upload fails
	if err := ss.AddUploadingSectors(ctx, uID1, []types.Hash256{{1}}); !errors.Is(err, api.ErrUnknownUpload) {
		t.Fatalf("unexpected error: %v", err)
	}

	// track uploads, tracking twice fails
	if err := ss.TrackUpload(ctx, uID1); err != nil {
		t.Fatal(err)
	} else if err := ss.TrackUpload(ctx, uID2); err != nil {
		t.Fatal(err)
	} else if err := ss.TrackUpload(ctx, uID1); !errors.Is(err, api.ErrUploadAlreadyExists) {
		t.Fatalf("unexpected error: %v", err)
	}
	assertSectors()

	// add sectors
	if err := ss.AddUploadingSectors(ctx, uID1, []types.Hash256{{1}, {2}}); err != nil {
		t.Fatal(err)
	} else if err := ss.AddUploadingSectors(ctx, uID1, []types.Hash256{{3}}); err != nil {
		t.Fatal(err)
	} else if err := ss.AddUploadingSectors(ctx, uID2, []types.Hash256{{4}}); err != nil {
		t.Fatal(err)
	} else if err := ss.AddUploadingSectors(ctx, uID2, nil); err != nil {
		t.Fatal(err)
	}
	assertSectors(types.Hash256{1}, types.Hash256{2}, types.Hash256{3}, types.Hash256{4})

	// finish the first upload
	if err := ss.FinishUpload(ctx, uID1); err != nil {
		t.Fatal(err)
	}
	assertSectors(types.Hash256{4})
	if err := ss.AddUploadingSectors(ctx, uID1, []types.Hash256{{1}}); !errors.Is(err, api.ErrUnknownUpload) {
		t.Fatalf("unexpected error: %v", err)
	}

	// finishing an unknown upload is a no-op
	if err := ss.FinishUpload(ctx, uID1); err != nil {
		t.Fatal(err)
	}
	assertSectors(types.Hash256{4})

	// expire the second upload, fetching the uploading sectors prunes it
	if _, err := ss.DB().Exec(ctx, "UPDATE uploads SET created_at = ? WHERE upload_id = ?", time.Now().Add(-2*uploadExpiry), uID2[:]); err != nil {
		t.Fatal(err)
	}
	assertSectors()
	if err := ss.TrackUpload(ctx, uID2); err != nil {
		t.Fatal(err)
	}

	// finishing an upload prunes expired uploads too
	if _, err := ss.DB().Exec(ctx, "UPDATE uploads SET created_at = ? WHERE upload_id = ?", time.Now().Add(-2*uploadExpiry), uID2[:]); err != nil {
		t.Fatal(err)
	} else if err := ss.TrackUpload(ctx, uID1); err != nil {
		t.Fatal(err)
	} else if err := ss.FinishUpload(ctx, uID1); err != nil {
		t.Fatal(err)
	} else if err := ss.TrackUpload(ctx, uID2); err != nil {
		t.Fatal(err)
	}
}

func TestUploadingSectorsRestart(t *testing.T) {
	// the default SQLite store is in-memory so an on-disk store is used to
	// restart the store, MySQL is persistent by nature
	cfg := defaultTestSQLStoreConfig
	if config.MySQLConfigFromEnv().URI == "" {
		cfg.persistent = true
	}
	ss := newTestSQLStore(t, cfg)
	ctx := context.Background()

	uID := api.UploadID{1}
	if err := ss.TrackUpload(ctx, uID); err != nil {
		t.Fatal(err)
	} else if err := ss.AddUploadingSectors(ctx, uID, []types.Hash256{{1}, {2}}); err != nil {
		t.Fatal(err)
	} else if err := ss.Close(); err != nil {
		t.Fatal(err)
	}

	// reopen the store and check the sectors are still there
	ss = ss.Reopen()
	defer ss.Close()
	if roots, err := ss.UploadingSectors(ctx); err != nil {
		t.Fatal(err)
	} else if len(roots) != 2 {
		t.Fatalf("expected 2 roots, got %v", len(roots))
	} else if err := ss.TrackUpload(ctx, uID); !errors.Is(err, api.ErrUploadAlreadyExists) {
		t.Fatalf("unexpected error: %v", err)
	}
}
