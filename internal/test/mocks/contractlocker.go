package mocks

import (
	"context"
	"sync"
	"time"

	"go.sia.tech/core/types"
)

type ContractLocker struct {
	mu    sync.Mutex
	locks map[types.FileContractID]*sync.Mutex
}

func NewContractLocker() *ContractLocker {
	return &ContractLocker{
		locks: make(map[types.FileContractID]*sync.Mutex),
	}
}

func (cs *ContractLocker) AcquireContract(_ context.Context, fcid types.FileContractID, _ int, _ time.Duration) (uint64, error) {
	cs.mu.Lock()
	lock, exists := cs.locks[fcid]
	if !exists {
		cs.locks[fcid] = new(sync.Mutex)
		lock = cs.locks[fcid]
	}
	cs.mu.Unlock()

	lock.Lock()
	return 0, nil
}

func (cs *ContractLocker) ReleaseContract(_ context.Context, fcid types.FileContractID, _ uint64) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	cs.locks[fcid].Unlock()
	return nil
}

func (*ContractLocker) KeepaliveContract(context.Context, types.FileContractID, uint64, time.Duration) error {
	return nil
}
