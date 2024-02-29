package worker

import (
	"context"
	"fmt"
	"sync"

	"go.sia.tech/renterd/api"
	"go.uber.org/zap"
)

type (
	// MemoryManager helps regulate processes that use a lot of memory. Such as
	// uploads and downloads.
	MemoryManager interface {
		Status() api.MemoryStatus
		AcquireMemory(ctx context.Context, amt uint64) Memory
		Limit(amt uint64) (MemoryManager, error)
	}

	Memory interface {
		Release()
		ReleaseSome(amt uint64)
	}

	memoryManager struct {
		totalAvailable uint64
		logger         *zap.SugaredLogger

		mu        sync.Mutex
		sigNewMem sync.Cond
		available uint64
	}

	acquiredMemory struct {
		mm *memoryManager

		remaining uint64
	}
)

var _ MemoryManager = (*memoryManager)(nil)

func newMemoryManager(logger *zap.SugaredLogger, maxMemory uint64) MemoryManager {
	mm := &memoryManager{
		logger:         logger,
		totalAvailable: maxMemory,
	}
	mm.available = mm.totalAvailable
	mm.sigNewMem = *sync.NewCond(&mm.mu)
	return mm
}

func (mm *memoryManager) Limit(amt uint64) (MemoryManager, error) {
	if amt > mm.totalAvailable {
		return nil, fmt.Errorf("cannot limit memory to %v when only %v is available", amt, mm.available)
	}
	return &limitMemoryManager{
		parent: mm,
		child:  newMemoryManager(mm.logger, amt),
	}, nil
}

func (mm *memoryManager) Status() api.MemoryStatus {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	return api.MemoryStatus{
		Available: mm.available,
		Total:     mm.totalAvailable,
	}
}

func (mm *memoryManager) AcquireMemory(ctx context.Context, amt uint64) Memory {
	if amt == 0 {
		mm.logger.Fatal("cannot acquire 0 memory")
	} else if mm.totalAvailable < amt {
		mm.logger.Errorf("cannot acquire %v memory with only %v available", amt, mm.totalAvailable)
		return nil
	}
	// block until enough memory is available
	mm.sigNewMem.L.Lock()
	for mm.available < amt {
		mm.sigNewMem.Wait()

		// check if the context was canceled in the meantime
		select {
		case <-ctx.Done():
			mm.sigNewMem.Broadcast() // flush out other cancelled goroutines
			mm.sigNewMem.L.Unlock()
			return nil
		default:
		}
	}
	mm.available -= amt
	mm.sigNewMem.Signal() // wake next goroutine
	mm.sigNewMem.L.Unlock()

	return &acquiredMemory{
		mm:        mm,
		remaining: amt,
	}
}

// release returns all the remaining memory to the memory manager. Should always
// be called on every acquiredMemory when done using it.
func (am *acquiredMemory) Release() {
	am.mm.sigNewMem.L.Lock()
	am.mm.available += am.remaining
	am.remaining = 0
	am.mm.sigNewMem.Signal() // wake next goroutine
	am.mm.sigNewMem.L.Unlock()
}

// ReleaseSome releases some of the remaining memory to the memory manager.
// Panics if more memory is released than was acquired.
func (am *acquiredMemory) ReleaseSome(amt uint64) {
	am.mm.sigNewMem.L.Lock()
	if amt > am.remaining {
		am.mm.sigNewMem.L.Unlock()
		panic("releasing more memory than remaining")
	}
	am.mm.available += amt
	am.remaining -= amt
	am.mm.sigNewMem.Signal() // wake next goroutine
	am.mm.sigNewMem.L.Unlock()
}

type (
	limitMemoryManager struct {
		parent MemoryManager
		child  MemoryManager
	}

	limitAcquiredMemory struct {
		parent Memory
		child  Memory
	}
)

func (lmm *limitMemoryManager) Status() api.MemoryStatus {
	return lmm.child.Status()
}

func (lmm *limitMemoryManager) AcquireMemory(ctx context.Context, amt uint64) Memory {
	childMem := lmm.child.AcquireMemory(ctx, amt)
	if childMem == nil {
		return nil
	}
	parentMem := lmm.parent.AcquireMemory(ctx, amt)
	if parentMem == nil {
		childMem.Release()
		return nil
	}
	return &limitAcquiredMemory{
		child:  childMem,
		parent: parentMem,
	}
}

func (lmm *limitMemoryManager) Limit(amt uint64) (MemoryManager, error) {
	return lmm.child.Limit(amt)
}

func (lam *limitAcquiredMemory) Release() {
	lam.child.Release()
	lam.parent.Release()
}

func (lam *limitAcquiredMemory) ReleaseSome(amt uint64) {
	lam.child.ReleaseSome(amt)
	lam.parent.ReleaseSome(amt)
}
