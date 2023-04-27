package worker

import (
	"context"
	"errors"
	"math"
	"sort"
	"sync"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
)

var (
	errNoUploaderAvailable = errors.New("no uploader available")
)

type (
	Uploader interface {
		schedule(ctx context.Context, id uploadID, size int, lockPriority int, exclude map[types.PublicKey]struct{}) (contract api.ContractMetadata, signalChan chan error, doneChan chan error, err error)
		finish(uploadID)
		update([]api.ContractMetadata)
		total() int
	}

	uploaderPool struct {
		locker contractLocker

		poolMu sync.Mutex
		pool   []*uploader
	}

	uploadID [8]byte
	upload   struct {
		id           uploadID
		size         int
		lockPriority int

		ctx    context.Context // request context
		signal chan error      // signal caller upload can start
		done   chan error      // signal uploader upload is done
	}

	uploader struct {
		contract api.ContractMetadata
		locker   contractLocker

		queue   chan upload
		uploads map[uploadID]struct{}

		statsMu          sync.Mutex
		statsAvgSpeed    float64
		statsDatapoints  []float64
		statsIdx         int
		statsTotalQueued int
	}
)

func (w *worker) initUploaderPool() {
	if w.uploader != nil {
		panic("uploads already initialized") // developer error
	}
	w.uploader = newUploaderPool(w)
}

func newUploaderPool(locker contractLocker) *uploaderPool {
	return &uploaderPool{
		locker: locker,
		pool:   make([]*uploader, 0),
	}
}

func newUploader(contract api.ContractMetadata, locker contractLocker) *uploader {
	return &uploader{
		contract: contract,
		locker:   locker,

		queue:   make(chan upload, 1e3),
		uploads: make(map[uploadID]struct{}),

		statsDatapoints: make([]float64, 1e3),
	}
}

func (up *uploaderPool) schedule(ctx context.Context, id uploadID, size int, lockPriority int, exclude map[types.PublicKey]struct{}) (api.ContractMetadata, chan error, chan error, error) {
	up.poolMu.Lock()
	defer up.poolMu.Unlock()

	// prepare the upload item
	upload := upload{
		id:           id,
		size:         size,
		lockPriority: lockPriority,

		ctx:    ctx,
		signal: make(chan error, 1),
		done:   make(chan error, 1),
	}

	// sort the uploaders
	sort.SliceStable(up.pool, func(i, j int) bool {
		return up.pool[i].estimate(size) < up.pool[j].estimate(size)
	})

	// find the next best uploader
	for _, ul := range up.pool {
		_, excluded := exclude[ul.contract.HostKey]
		if excluded {
			continue // do not use an excluded host
		}
		_, used := ul.uploads[id]
		if used {
			continue // do not reuse a host
		}

		err := ul.schedule(ctx, upload, size)
		if err != nil {
			return api.ContractMetadata{}, nil, nil, err
		}

		return ul.contract, upload.signal, upload.done, nil
	}

	return api.ContractMetadata{}, nil, nil, errNoUploaderAvailable
}

func (up *uploaderPool) finish(id uploadID) {
	up.poolMu.Lock()
	defer up.poolMu.Unlock()
	for _, ul := range up.pool {
		delete(ul.uploads, id)
	}
}

func (up *uploaderPool) update(contracts []api.ContractMetadata) {
	up.poolMu.Lock()
	defer up.poolMu.Unlock()

	// build contracts map
	cmap := make(map[types.FileContractID]api.ContractMetadata)
	for _, c := range contracts {
		cmap[c.ID] = c
	}

	// recreate the pool in-place and close out uploader queues that we no
	// longer want and adjust the map so we are left with the missing uploaders
	var i int
	for _, u := range up.pool {
		if _, keep := cmap[u.contract.ID]; keep {
			delete(cmap, u.contract.ID)
			up.pool[i] = u
			i++
		} else {
			close(u.queue)
		}
	}
	for j := i; j < len(up.pool); j++ {
		up.pool[j] = nil
	}
	up.pool = up.pool[:i]

	// add missing uploaders
	for _, contract := range cmap {
		uploader := newUploader(contract, up.locker)
		up.pool = append(up.pool, uploader)
		go uploader.start()
	}
}

func (up *uploaderPool) total() int {
	up.poolMu.Lock()
	defer up.poolMu.Unlock()
	return len(up.pool)
}

func (u *uploader) estimate(size int) float64 {
	u.statsMu.Lock()
	defer u.statsMu.Unlock()

	avg := u.statsAvgSpeed
	if avg == 0 {
		avg = 1 // avoid division by zero
	}
	return float64(u.statsTotalQueued+size) / avg
}

func (u *uploader) track(size int, duration time.Duration) {
	u.statsMu.Lock()
	defer u.statsMu.Unlock()

	u.statsDatapoints[u.statsIdx%len(u.statsDatapoints)] = mbps(int64(size), duration.Seconds())
	u.statsIdx++

	var sum, pts float64
	for _, d := range u.statsDatapoints {
		if d == 0 {
			continue
		}
		sum += d
		pts++
	}
	u.statsAvgSpeed = sum / pts
}

func (u *uploader) schedule(ctx context.Context, ul upload, size int) error {
	select {
	case u.queue <- ul:
		u.uploads[ul.id] = struct{}{}

		u.statsMu.Lock()
		u.statsTotalQueued += size
		u.statsMu.Unlock()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (u *uploader) start() {
	for ul := range u.queue {
		// acquire contract lock
		lock, err := u.locker.AcquireContract(ul.ctx, u.contract.ID, ul.lockPriority)
		if err != nil {
			select {
			case ul.signal <- err:
			default:
			}
			return
		}

		// unblock the upload
		select {
		case ul.signal <- nil:
			start := time.Now()
			select {
			case err := <-ul.done:
				if errors.Is(err, errHostNotUsed) {
					delete(u.uploads, ul.id)
					err = nil // unset error so we track the speed
				}
				if err == nil {
					u.track(ul.size, time.Since(start))
				}
			case <-time.After(30 * time.Minute):
				// make sure we always release the lock
			}
		default:
		}

		// update the queued stats
		u.statsMu.Lock()
		u.statsTotalQueued -= ul.size
		u.statsMu.Unlock()

		// release the lock
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		_ = lock.Release(ctx) // nothing we can do if this fails
		cancel()
	}
}

func mbps(b int64, s float64) float64 {
	bps := float64(b) / s
	return math.Round(bps*0.000008*100) / 100
}
