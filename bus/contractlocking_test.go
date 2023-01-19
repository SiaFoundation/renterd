package bus

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go.sia.tech/siad/types"
)

// TestContractAcquire is a unit test for contractLocks.Acquire.
func TestContractAcquire(t *testing.T) {
	locks := newContractLocks()

	verify := func(fcid types.FileContractID, references, lockID uint64, lockedDuration time.Duration, delta time.Duration) {
		t.Helper()
		if lockID == 0 {
			t.Fatal("invalid lock id")
		}
		lock := locks.lockForContractID(fcid)
		if lock.heldBy != lockID {
			t.Fatal("heldBy not set")
		}
		lockedUntil := time.Now().Add(lockedDuration)
		if lock.lockedUntil.Before(lockedUntil.Add(-delta)) || lock.lockedUntil.After(lockedUntil.Add(delta)) {
			t.Fatal("locked_until not set correctly")
		}
		if lock.references != references {
			t.Fatal("wrong references")
		}
	}

	// Acquire contract.
	fcid := types.FileContractID{1}
	lockID, err := locks.Acquire(context.Background(), fcid, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	verify(fcid, 1, lockID, time.Minute, 3*time.Second)

	// Acquire another contract but this time it has been acquired already
	// and the lock expired.
	fcid = types.FileContractID{2}
	_, err = locks.Acquire(context.Background(), fcid, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond) // wait for lock to expire

	lockID, err = locks.Acquire(context.Background(), fcid, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	verify(fcid, 1, lockID, time.Minute, 3*time.Second)

	// Same thing again but with multiple locks that expire.
	fcid = types.FileContractID{3}
	var lockIDs []uint64
	var lockIDsMu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lockID, err = locks.Acquire(context.Background(), fcid, 100*time.Millisecond)
			if err != nil {
				t.Error(err)
				return
			}
			lockIDsMu.Lock()
			lockIDs = append(lockIDs, lockID)
			lockIDsMu.Unlock()
		}()
	}
	wg.Wait()
	if len(lockIDs) != 10 {
		t.Fatal("wrong number of lock ids")
	}
	verify(fcid, 1, lockIDs[len(lockIDs)-1], 100*time.Millisecond, 50*time.Millisecond)

	// Test timing out while trying to acquire a lock.
	fcid = types.FileContractID{4}
	lockID, err = locks.Acquire(context.Background(), fcid, time.Hour)
	if err != nil {
		t.Error(err)
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = locks.Acquire(ctx, fcid, 100*time.Millisecond)
	if !errors.Is(err, ErrAcquireContractTimeout) {
		t.Fatal("acquire should time out", err)
		return
	}
	verify(fcid, 1, lockID, time.Hour, time.Second)
}
