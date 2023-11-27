package worker

import (
	"context"
	"sync"
)

type (
	// memoryManager helps regulate processes that use a lot of memory. Such as
	// uploads and downloads.
	memoryManager struct {
		totalAvailable uint64

		mu        sync.Mutex
		sigNewMem sync.Cond
		available uint64
	}

	acquiredMemory struct {
		mm *memoryManager

		mu        sync.Mutex
		remaining uint64
	}
)

func newMemoryManager() *memoryManager {
	mm := &memoryManager{
		totalAvailable: 1 << 30, // 1 GB
	}
	mm.available = mm.totalAvailable
	mm.sigNewMem = *sync.NewCond(&mm.mu)
	return mm
}

func (mm *memoryManager) AcquireMemory(ctx context.Context, amt uint64) <-chan *acquiredMemory {
	memChan := make(chan *acquiredMemory, 1)
	// block until enough memory is available
	mm.sigNewMem.L.Lock()
	for mm.available < amt {
		mm.sigNewMem.Wait()

		// check if the context was canceled in the meantime
		select {
		case <-ctx.Done():
			mm.sigNewMem.L.Unlock()
			close(memChan)
			return memChan
		default:
		}
	}
	mm.available -= amt
	mm.sigNewMem.L.Unlock()

	memChan <- &acquiredMemory{
		mm:        mm,
		remaining: amt,
	}
	close(memChan)

	mm.sigNewMem.Signal() // wake next goroutine
	return memChan
}

// release returns all the remaining memory to the memory manager. Should always
// be called on every acquiredMemory when done using it.
func (am *acquiredMemory) Release() {
	am.mm.sigNewMem.L.Lock()
	am.mm.available += am.remaining
	am.mm.sigNewMem.L.Unlock()

	am.mu.Lock()
	am.remaining = 0
	am.mu.Unlock()

	am.mm.sigNewMem.Signal() // wake next goroutine
}

// ReleaseSome releases some of the remaining memory to the memory manager.
// Panics if more memory is released than was acquired.
func (am *acquiredMemory) ReleaseSome(amt uint64) {
	am.mm.sigNewMem.L.Lock()
	if amt > am.remaining {
		panic("releasing more memory than remaining")
	}
	am.mm.available += amt
	am.mm.sigNewMem.L.Unlock()

	am.mu.Lock()
	am.remaining -= amt
	am.mu.Unlock()

	am.mm.sigNewMem.Signal() // wake next goroutine
}
