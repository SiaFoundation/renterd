package bus

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"go.sia.tech/core/types"
	"lukechampine.com/frand"
)

// ErrAcquireContractTimeout is returned when the context passed in to
// contractLocks.Acquire is closed before the lock can be acquired.
var ErrAcquireContractTimeout = errors.New("acquiring the lock timed out")

// lockCandidatePriorityHeap is a max-heap of lockCandidates.
type lockCandidatePriorityHeap []*lockCandidate

func (h lockCandidatePriorityHeap) Len() int           { return len(h) }
func (h lockCandidatePriorityHeap) Less(i, j int) bool { return h[i].priority > h[j].priority }
func (h lockCandidatePriorityHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h lockCandidatePriorityHeap) Peek() *lockCandidate {
	if h.Len() == 0 {
		return nil
	}
	return h[0]
}

func (h *lockCandidatePriorityHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(*lockCandidate))
}

func (h *lockCandidatePriorityHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	old[n-1] = &lockCandidate{}
	*h = old[0 : n-1]
	return x
}

type contractLocks struct {
	mu    sync.Mutex
	locks map[types.FileContractID]*contractLock
}

type contractLock struct {
	mu          sync.Mutex // locks contractLock fields
	heldByID    uint64
	wakeupTimer *time.Timer
	queue       *lockCandidatePriorityHeap
}

type lockCandidate struct {
	lockID   uint64
	wake     chan struct{}
	priority int
	timedOut <-chan struct{}
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
			queue: &lockCandidatePriorityHeap{},
		}
		l.locks[id] = lock
	}
	return lock
}

func (lock *contractLock) setTimer(l *contractLocks, lockID uint64, id types.FileContractID, d time.Duration) {
	lock.wakeupTimer = time.AfterFunc(d, func() {
		l.Release(id, lockID)
	})
}

func (l *contractLock) stopTimer() {
	if l.wakeupTimer == nil {
		return
	}
	if !l.wakeupTimer.Stop() {
		select {
		case <-l.wakeupTimer.C:
		default:
		}
	}
	l.wakeupTimer = nil
}

// Acquire acquires a contract lock for the given id and provided duration. If
// acquiring the lock doesn't finish before the context is closed,
// ErrAcquireContractTimeout is returned. Upon success an identifier is returned
// which can be used to release the lock before its lock duration has passed.
// TODO: Extend this with some sort of priority. e.g. migrations would acquire a
// lock with a low priority but contract maintenance would have a very high one
// to avoid being starved by low prio tasks.
func (l *contractLocks) Acquire(ctx context.Context, priority int, id types.FileContractID, d time.Duration) (uint64, error) {
	lock := l.lockForContractID(id, true)

	// Prepare a random lockID for ourselves.
	ourLockID := frand.Uint64n(math.MaxUint64) + 1

	lock.mu.Lock()

	// If nobody holds the lock, acquire it and launch a timer to release
	// the lock after the expiry.
	if lock.heldByID == 0 {
		lock.heldByID = ourLockID
		lock.setTimer(l, ourLockID, id, d)
		lock.mu.Unlock()
		return ourLockID, nil
	}

	// Someone is holding the lock. Add ourselves to the queue.
	wakeChan := make(chan struct{})
	heap.Push(lock.queue, &lockCandidate{
		lockID:   ourLockID,
		wake:     wakeChan,
		priority: priority,
		timedOut: ctx.Done(),
	})

	lock.mu.Unlock()
	select {
	case <-ctx.Done():
		return 0, ErrAcquireContractTimeout
	case <-wakeChan:
	}
	lock.mu.Lock()
	defer lock.mu.Unlock()

	if lock.heldByID != ourLockID {
		panic("lock should be acquired by us after being woken up")
	}
	lock.setTimer(l, ourLockID, id, d)
	return ourLockID, nil
}

// KeepAlive refreshes the timer on a contract lock for a given contract if the
// lockID matches the one on the lock.
func (l *contractLocks) KeepAlive(id types.FileContractID, lockID uint64, d time.Duration) error {
	lock := l.lockForContractID(id, false)
	if lock == nil {
		return errors.New("lock not found")
	}
	lock.mu.Lock()
	defer lock.mu.Unlock()
	if lock.heldByID != lockID {
		return fmt.Errorf("lockID doesn't match: %v != %v", lock.heldByID, lockID)
	}
	if !lock.wakeupTimer.Stop() {
		return errors.New("timer has fired already")
	}
	lock.setTimer(l, lockID, id, d)
	return nil
}

// Release releases the contract lock for a given contract and lock id.
func (l *contractLocks) Release(id types.FileContractID, lockID uint64) error {
	if lockID == 0 {
		return errors.New("can't release lock with id 0")
	}
	lock := l.lockForContractID(id, false)
	if lock == nil {
		return nil // nothing to do
	}

	lock.mu.Lock()
	defer lock.mu.Unlock()
	if lock.heldByID == 0 {
		return nil // nothing to do
	}
	if lock.heldByID != lockID {
		return fmt.Errorf("failed to unlock lock held by lockID %v with lockID %v - potentially due to a timeout", lock.heldByID, lockID)
	}

	// Stop the timer on the lock.
	lock.stopTimer()

	// Set holder to 0.
	lock.heldByID = 0

	// If there is no next candidate we are done.
	if lock.queue.Len() == 0 {
		return nil
	}

	// Wake the next candidate.
	for lock.queue.Len() > 0 {
		next := heap.Pop(lock.queue).(*lockCandidate)
		if func() bool {
			defer close(next.wake)
			select {
			case next.wake <- struct{}{}:
				return true // woken successfully
			case <-next.timedOut:
				return false // timed out already
			}
		}() {
			lock.heldByID = next.lockID // acquire lock for woken up thread
			return nil
		}
	}
	return nil
}
