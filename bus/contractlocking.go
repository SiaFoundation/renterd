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

var ErrAcquireContractTimeout = errors.New("acquiring the lock timed out")

// Acquire acquires a contract lock for the given id and provided duration. If
// acquiring the lock doesn't finish before the context is closed,
// ErrAcquireContractTimeout is returned. Upon success an identifier is returned
// which can be used to release the lock before its lock duration has passed.
// TODO: Extend this with some sort of priority. e.g. migrations would acquire a
// lock with a low priority but contract maintenance would have a very high one
// to avoid being starved by low prio tasks.
func (l *contractLocks) Acquire(ctx context.Context, id types.FileContractID, d time.Duration) (uint64, error) {
	lock := l.lockForContractID(id, true)
	ourLockID := frand.Uint64n(math.MaxUint64)
	done := make(chan struct{})
	defer close(done)

	setAcquired := func() {
		lock.lockedUntil = time.Now().Add(d)
		lock.heldBy = frand.Uint64n(math.MaxUint64)
	}

	lock.mu.Lock()
	lockChan := lock.lockChan // remember lockChan to wait on
	lock.queue = append(lock.queue, &lockCandidate{
		lockID: ourLockID,
		c:      done,
	})
	t := time.NewTimer(time.Until(lock.lockedUntil)) // prepare a timer for expiring locks
	lock.mu.Unlock()

	// TODO: Drain timer.

	// tryAcquire is a helper function that tries to acquire a lock
	// whenenver waiting threads are either woken by the lock timing out or
	// the lock being released.
	tryAcquire := func() (uint64, bool, error) {
		lock.mu.Lock()

		// Fetch the next candidate for locking. Drop any that
		// have timed out.
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

		if nextCandidate.lockID == ourLockID {
			// If this thread is the next candidate in the queue,
			// acquire the lock.
			setAcquired()
			heldBy := lock.heldBy
			lock.lockChan = make(chan struct{})
			lock.queue = lock.queue[1:]
			lock.mu.Unlock()
			return heldBy, true, nil
		} else if nextCandidate.lockID != ourLockID {
			// We are not the next candidate. Sleep for however long
			// the next candidate wants to acquire the lock.
			t.Reset(nextCandidate.lockDuration)
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
