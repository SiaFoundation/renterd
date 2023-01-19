package bus

import (
	"context"
	"errors"
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
	references  uint64
}

func newContractLocks() *contractLocks {
	return &contractLocks{
		locks: make(map[types.FileContractID]*contractLock),
	}
}

func (l *contractLocks) lockForContractID(id types.FileContractID) *contractLock {
	l.mu.Lock()
	defer l.mu.Unlock()
	lock, exists := l.locks[id]
	if !exists {
		lock = &contractLock{
			lockChan:    make(chan struct{}, 1),
			lockedUntil: time.Now().Add(time.Hour),
		}
		l.locks[id] = lock
	}
	return lock
}

var ErrAcquireContractTimeout = errors.New("acquiring the lock timed out")

// Acquire acquires a contract lock for the given id and provided duration. If
// acquiring the lock doesn't finish before the context is closed,
// ErrAcquireContractTimeout is returned.
func (l *contractLocks) Acquire(ctx context.Context, id types.FileContractID, d time.Duration) (uint64, error) {
	lock := l.lockForContractID(id)

	setAcquired := func() {
		lock.lockedUntil = time.Now().Add(d)
		lock.heldBy = frand.Uint64n(math.MaxUint64)
	}

	lock.mu.Lock()
	lockChan := lock.lockChan                        // remember lockChan to wait on
	previousHolder := lock.heldBy                    // remember previous holder
	lock.references++                                // update the number of threads waiting
	t := time.NewTimer(time.Until(lock.lockedUntil)) // prepare a timer for expiring locks
	lock.mu.Unlock()

	for {
		select {
		case <-ctx.Done():
			// We hit a timeout. Abort.
			lock.mu.Lock()
			lock.references--
			lock.mu.Unlock()
			return 0, ErrAcquireContractTimeout
		case <-t.C:
			// The lock has expired. That leaves us with 2 options
			// after acquiring the mutext.
			//   1. the holder hasn't changed. So we are the first
			//   thread that acquired the mutex after the lock has
			//   expired. We are free to acquire the lock in this
			//   case and have to swap out the channel and remove
			//   the reference of the original holder.
			//   2. the holder has changed. In that case we were too
			//   slow and another thread has acquired the lock and
			//   swapped out the lockChan in the meantime. In that
			//   case we grab the new channel, update the timer and
			//   retry.
			lock.mu.Lock()
			if lock.heldBy == previousHolder {
				setAcquired()
				// Create a new lockChan and acquire it right away.
				lockChan = make(chan struct{}, 1)
				lockChan <- struct{}{}
				lock.lockChan = lockChan
				heldBy := lock.heldBy
				// Remove the reference of the previous, expired
				// holder.
				lock.references--
				lock.mu.Unlock()
				return heldBy, nil
			} else {
				lockChan = lock.lockChan
				previousHolder = lock.heldBy
				t.Reset(time.Until(lock.lockedUntil))
			}
			lock.mu.Unlock()
			continue
			// The lock was acquired successfully.
		case lockChan <- struct{}{}: // lock acquired
			lock.mu.Lock()
			setAcquired()
			heldBy := lock.heldBy
			lock.mu.Unlock()
			return heldBy, nil

		}
	}
}

func (l *contractLock) Release(id types.FileContractID, lockID uint64) error {
	panic("not implemeneted")
}
