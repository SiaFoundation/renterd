package stores

import (
	"context"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"lukechampine.com/frand"
)

func TestWalletLockUnlock(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	expectedLocked := make(map[types.SiacoinOutputID]bool)
	lockedIDs := make([]types.SiacoinOutputID, 10)
	for i := range lockedIDs {
		lockedIDs[i] = frand.Entropy256()
		expectedLocked[lockedIDs[i]] = true
	}
	if err := ss.LockUTXOs(lockedIDs, time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	ids, err := ss.LockedUTXOs(time.Now())
	if err != nil {
		t.Fatal(err)
	} else if len(ids) != len(lockedIDs) {
		t.Fatalf("expected %d locked outputs, got %d", len(lockedIDs), len(ids))
	}
	for _, id := range ids {
		if _, ok := expectedLocked[id]; !ok {
			t.Fatalf("unexpected locked output %s", id)
		}
	}

	if err := ss.ReleaseUTXOs(lockedIDs); err != nil {
		t.Fatal(err)
	}

	ids, err = ss.LockedUTXOs(time.Now())
	if err != nil {
		t.Fatal(err)
	} else if len(ids) != 0 {
		t.Fatalf("expected 0 locked outputs, got %d", len(ids))
	}

	// lock the ids, but set the unlock time to the past
	if err := ss.LockUTXOs(lockedIDs, time.Now().Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	ids, err = ss.LockedUTXOs(time.Now())
	if err != nil {
		t.Fatal(err)
	} else if len(ids) != 0 {
		t.Fatalf("expected 0 locked outputs, got %d", len(ids))
	}

	// assert the outputs were cleaned up
	var count int
	err = ss.DB().QueryRow(context.Background(), `SELECT COUNT(*) FROM wallet_locked_outputs`).Scan(&count)
	if err != nil {
		t.Fatal(err)
	} else if count != 0 {
		t.Fatalf("expected 0 locked outputs, got %d", count)
	}
}
