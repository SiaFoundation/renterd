package bus

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"go.sia.tech/siad/types"
)

// TestContractAcquire is a unit test for contractLocks.Acquire.
func TestContractAcquire(t *testing.T) {
	locks := newContractLocks()

	verify := func(fcid types.FileContractID, lockID uint64, lockedDuration time.Duration, delta time.Duration) {
		t.Helper()
		if lockID == 0 {
			t.Fatal("invalid lock id")
		}
		lock := locks.lockForContractID(fcid, false)
		if lock.heldBy != lockID {
			t.Fatal("heldBy not set")
		}
	}

	// Acquire contract.
	fcid := types.FileContractID{1}
	lockID, err := locks.Acquire(context.Background(), 0, fcid, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	verify(fcid, lockID, time.Minute, 3*time.Second)

	// Acquire another contract but this time it has been acquired already
	// and the lock expired.
	fcid = types.FileContractID{2}
	_, err = locks.Acquire(context.Background(), 0, fcid, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond) // wait for lock to expire

	lockID, err = locks.Acquire(context.Background(), 0, fcid, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	verify(fcid, lockID, time.Minute, 3*time.Second)

	// Same thing again but with multiple locks that expire. The first lock
	// is acquired in the same thread to guarantee it's the first and the
	// remaining ones are launched simultaneously in goroutines.
	// The index of the launch is the priority so the first goroutine should
	// acquire the lock last.
	fcid = types.FileContractID{3}
	var mu sync.Mutex
	var wg sync.WaitGroup
	threadIndices := []int{}
	lockIDs := []uint64{}
	start := time.Now()
	_, err = locks.Acquire(context.Background(), 0, fcid, 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(threadIndex int) {
			defer wg.Done()
			lockID, err := locks.Acquire(context.Background(), threadIndex, fcid, 100*time.Millisecond)
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			lockIDs = append(lockIDs, lockID)
			threadIndices = append(threadIndices, threadIndex)
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	if len(lockIDs) != 10 {
		t.Fatal("wrong number of lock ids")
	}
	if !sort.IsSorted(sort.Reverse(sort.IntSlice(threadIndices))) {
		t.Fatal("threads didn't finish in order or priority", threadIndices)
	}
	verify(fcid, lockIDs[len(lockIDs)-1], 100*time.Millisecond, 50*time.Millisecond)

	// Acquiring the lock should take 10 threads with a 100ms lock duration
	// a total of at least 900ms.
	if time.Since(start) < 900*time.Millisecond {
		t.Fatal("not enough time has passed")
	}

	// Test timing out while trying to acquire a lock.
	fcid = types.FileContractID{4}
	lockID, err = locks.Acquire(context.Background(), 0, fcid, time.Hour)
	if err != nil {
		t.Error(err)
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = locks.Acquire(ctx, 0, fcid, 100*time.Millisecond)
	if !errors.Is(err, ErrAcquireContractTimeout) {
		t.Fatal("acquire should time out", err)
		return
	}
	verify(fcid, lockID, time.Hour, time.Second)
}

// TestContractRelease is a unit test for contractLocks.Release.
func TestContractRelease(t *testing.T) {
	locks := newContractLocks()

	verify := func(fcid types.FileContractID, lockID uint64, lockedUntil time.Time, delta time.Duration) {
		t.Helper()
		lock := locks.lockForContractID(fcid, false)
		if lock.heldBy != lockID {
			t.Fatalf("heldBy not set")
		}
	}

	// Acquire contract.
	fcid := types.FileContractID{1}
	lockID, err := locks.Acquire(context.Background(), 0, fcid, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	verify(fcid, lockID, time.Now().Add(time.Minute), 3*time.Second)

	// Acquire it again but release the contract within a second.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(time.Second)
		if err := locks.Release(fcid, lockID); err != nil {
			t.Error(err)
		}
	}()

	lockID, err = locks.Acquire(context.Background(), 0, fcid, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	verify(fcid, lockID, time.Now().Add(time.Minute), 3*time.Second)

	// Release one more time. Should decrease the references to 0 and reset
	// fields.
	if err := locks.Release(fcid, lockID); err != nil {
		t.Error(err)
	}
	verify(fcid, 0, time.Time{}, 0)

	// Try to release lock again. Is a no-op.
	if err := locks.Release(fcid, lockID); err != nil {
		t.Fatal(err)
	}

	// Try to release lock for another contract. Should fail.
	if err := locks.Release(types.FileContractID{2}, lockID); err != nil {
		t.Fatal(err)
	}
}
