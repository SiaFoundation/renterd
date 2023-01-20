package bus

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"go.sia.tech/siad/types"
	"lukechampine.com/frand"
)

// ErrAcquireContractTimeout is returned when the context passed in to
// contractLocks.Acquire is closed before the lock can be acquired.
var ErrAcquireContractTimeout = errors.New("acquiring the lock timed out")

type contractLocks struct {
	mu    sync.Mutex
	locks map[types.FileContractID]*contractLock
}

type contractLock struct {
	mu          sync.Mutex    // locks contractLock fields
	lockChan    chan struct{} // locks the contract
	lockedUntil time.Time
	heldBy      uint64
	queue       []*lockCandidate
}

type lockCandidate struct {
	lockID       uint64
	lockDuration time.Duration
	c            chan struct{}
}

func newContractLocks() *contractLocks {
	return &contractLocks{
		locks: make(map[types.FileContractID]*contractLock),
	}
}

func (l *contractLocks) lockForContractID(id types.FileContractID, create bool) *contractLock {
	l.mu.Lock()
	defer l.mu.Unlock()
	lock, exists := l.locks[id]
	if !exists && create {
		c := make(chan struct{})
		close(c)
		lock = &contractLock{
			lockChan:    c,
			lockedUntil: time.Now().Add(time.Hour),
		}
		l.locks[id] = lock
	}
	return lock
}

// Acquire acquires a contract lock for the given id and provided duration. If
// acquiring the lock doesn't finish before the context is closed,
// ErrAcquireContractTimeout is returned. Upon success an identifier is returned
// which can be used to release the lock before its lock duration has passed.
// TODO: Extend this with some sort of priority. e.g. migrations would acquire a
// lock with a low priority but contract maintenance would have a very high one
// to avoid being starved by low prio tasks.
func (l *contractLocks) Acquire(ctx context.Context, id types.FileContractID, d time.Duration) (uint64, error) {
	lock := l.lockForContractID(id, true)

	// Prepare a random lockID for ourselves.
	ourLockID := frand.Uint64n(math.MaxUint64)

	// Prepare a channel to indicate to other candidates whether we are
	// still waiting to acquire the lock.
	done := make(chan struct{})
	defer close(done)

	// Add ourselves to the queue of candidates for locking, grab the
	// lockChan to be notified when the lock is released prematurely and
	// prepare a timer to be notified when the current lock expires.
	lock.mu.Lock()
	lockChan := lock.lockChan
	lock.queue = append(lock.queue, &lockCandidate{
		lockID:       ourLockID,
		lockDuration: d,
		c:            done,
	})
	t := time.NewTimer(time.Until(lock.lockedUntil)) // prepare a timer for expiring locks
	lock.mu.Unlock()

	// Drain timer at the end if necessary.
	drainTimer := func() {
		if !t.Stop() {
			select {
			case <-t.C:
			default:
			}
		}
	}
	defer drainTimer()

	// tryAcquire is a helper function that tries to acquire a lock
	// whenenver waiting threads are either woken by the lock timing out or
	// the lock being released.
	tryAcquire := func() (uint64, bool, error) {
		lock.mu.Lock()

		// Fetch the next candidate for locking. Drop any that
		// have timed out in the meantime.
		var nextCandidate *lockCandidate
		for {
			if len(lock.queue) == 0 {
				panic("queue is empty - should never happen")
			}
			select {
			case <-lock.queue[0].c:
				lock.queue = lock.queue[1:]
				continue
			default:
			}
			nextCandidate = lock.queue[0]
			break
		}

		// Check if it is safe to acquire the lock for the next
		// candidate. That's the case if either lock.heldBy == 0 or we
		// are beyond the lock's expiry.
		if lock.heldBy != 0 && time.Until(lock.lockedUntil) > 0 {
			drainTimer()
			t.Reset(time.Until(lock.lockedUntil))
			lock.mu.Unlock()
			return 0, false, nil
		}

		// It is safe to acquire the lock. Check whether we are the next
		// one to acquire it or not.
		if nextCandidate.lockID == ourLockID {
			// If we are the next candidate in the queue, acquire
			// the lock and return.
			lock.lockedUntil = time.Now().Add(d)
			lock.heldBy = ourLockID
			lock.lockChan = make(chan struct{})
			lock.queue = lock.queue[1:]
			heldBy := lock.heldBy
			lock.mu.Unlock()
			return heldBy, true, nil
		} else if nextCandidate.lockID != ourLockID {
			// We are not the next candidate to acquire the lock.
			// Wait until the next candidate is done acquiring it.
			lock.lockChan = nextCandidate.c
			lockChan = lock.lockChan
			lock.mu.Unlock()
		}
		return 0, false, nil
	}

	// Begin loop.
	for {
		var heldBy uint64
		var acquired bool
		var err error
		select {
		case <-ctx.Done():
			err = ErrAcquireContractTimeout
		case <-t.C:
			heldBy, acquired, err = tryAcquire()
		case <-lockChan:
			heldBy, acquired, err = tryAcquire()
		}
		if err != nil {
			return 0, err
		}
		if acquired {
			return heldBy, nil
		}
	}
}

// Release releases the contract lock for a given contract and lock id.
func (l *contractLocks) Release(id types.FileContractID, lockID uint64) error {
	if lockID == 0 {
		return errors.New("can't release lock with id 0")
	}
	lock := l.lockForContractID(id, false)
	if lock == nil {
		return fmt.Errorf("no active contract lock found for contract %v and lockID %v", id, lockID)
	}

	lock.mu.Lock()
	defer lock.mu.Unlock()
	if lock.heldBy != lockID {
		return fmt.Errorf("can't unlock lock due to id mismatch %v != %v", lockID, lock.heldBy)
	}
	lock.heldBy = 0
	lock.lockedUntil = time.Time{}
	close(lock.lockChan)

	// Unlock channel. This should never block.
	select {
	case <-lock.lockChan:
	default:
		panic("trying to unlock an unlocked contract lock")
	}
	return nil
}
