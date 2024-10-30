package autopilot

import (
	"context"
	"sync"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/object"
	"lukechampine.com/frand"
)

type Worker interface {
	Account(ctx context.Context, hostKey types.PublicKey) (api.Account, error)
	ID(ctx context.Context) (string, error)
	MigrateSlab(ctx context.Context, s object.Slab, set string) error

	RHPPriceTable(ctx context.Context, hostKey types.PublicKey, siamuxAddr string, timeout time.Duration) (api.HostPriceTable, error)
	RHPScan(ctx context.Context, hostKey types.PublicKey, hostIP string, timeout time.Duration) (api.RHPScanResponse, error)
}

// workerPool contains all workers known to the autopilot.  Users can call
// withWorker to execute a function with a worker of the pool or withWorkers to
// sequentially run a function on all workers.  Due to the RWMutex this will
// never block during normal operations. However, during an update of the
// workerpool, this allows us to guarantee that all workers have finished their
// tasks by calling  acquiring an exclusive lock on the pool before updating it.
// That way the caller who updated the pool can rely on the autopilot not using
// a worker that was removed during the update after the update operation
// returns.
type workerPool struct {
	mu      sync.RWMutex
	workers []Worker
}

func newWorkerPool(workers []Worker) *workerPool {
	return &workerPool{
		workers: workers,
	}
}

func (wp *workerPool) withWorker(workerFunc func(Worker)) {
	wp.mu.RLock()
	defer wp.mu.RUnlock()
	workerFunc(wp.workers[frand.Intn(len(wp.workers))])
}

func (wp *workerPool) withWorkers(workerFunc func([]Worker)) {
	wp.mu.RLock()
	defer wp.mu.RUnlock()
	workerFunc(wp.workers)
}
