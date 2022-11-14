package worker

import (
	"context"
	"io"

	"go.sia.tech/renterd/slab"
)

type slabMover struct {
	pool *slab.SessionPool
}

func (sm slabMover) withHosts(ctx context.Context, contracts []Contract, fn func([]slab.Host) error) (err error) {
	var hosts []slab.Host
	for _, c := range contracts {
		hosts = append(hosts, sm.pool.Session(ctx, c.HostKey, c.HostIP, c.ID, c.RenterKey))
	}
	done := make(chan struct{})
	go func() {
		select {
		case <-done:
			for _, h := range hosts {
				sm.pool.UnlockContract(h.(*slab.Session))
			}
		case <-ctx.Done():
			for _, h := range hosts {
				sm.pool.ForceClose(h.(*slab.Session))
			}
		}
	}()
	defer func() {
		close(done)
		if ctx.Err() != nil {
			err = ctx.Err()
		}
	}()
	err = fn(hosts)
	return err
}

func (sm slabMover) UploadSlab(ctx context.Context, r io.Reader, m, n uint8, currentHeight uint64, contracts []Contract) (s slab.Slab, err error) {
	sm.pool.SetCurrentHeight(currentHeight)
	err = sm.withHosts(ctx, contracts, func(hosts []slab.Host) error {
		s, err = slab.UploadSlab(ctx, r, m, n, hosts)
		return err
	})
	return
}

func (sm slabMover) DownloadSlab(ctx context.Context, w io.Writer, s slab.Slice, contracts []Contract) (err error) {
	return sm.withHosts(ctx, contracts, func(hosts []slab.Host) error {
		return slab.DownloadSlab(ctx, w, s, hosts)
	})
}

func (sm slabMover) DeleteSlabs(ctx context.Context, slabs []slab.Slab, contracts []Contract) (err error) {
	return sm.withHosts(ctx, contracts, func(hosts []slab.Host) error {
		return slab.DeleteSlabs(ctx, slabs, hosts)
	})
}

func (sm slabMover) MigrateSlab(ctx context.Context, s *slab.Slab, currentHeight uint64, from, to []Contract) (err error) {
	sm.pool.SetCurrentHeight(currentHeight)
	return sm.withHosts(ctx, append(from, to...), func(hosts []slab.Host) error {
		from, to := hosts[:len(from)], hosts[len(from):]
		return slab.MigrateSlab(ctx, s, from, to)
	})
}

func newSlabMover() slabMover {
	return slabMover{
		pool: slab.NewSessionPool(),
	}
}
