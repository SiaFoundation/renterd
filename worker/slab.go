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
		hosts = append(hosts, sm.pool.Session(c.HostKey, c.HostIP, c.ID, c.RenterKey))
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
	return fn(hosts)
}

func (sm slabMover) UploadSlab(ctx context.Context, r io.Reader, m, n uint8, currentHeight uint64, contracts []Contract) (s slab.Slab, err error) {
	sm.pool.SetCurrentHeight(currentHeight)
	err = sm.withHosts(ctx, contracts, func(hosts []slab.Host) error {
		s, err = slab.UploadSlab(ctx, r, m, n, hosts)
		return err
	})
	return
}

func (sm slabMover) DownloadSlab(ctx context.Context, s slab.Slice, contracts []Contract) (b []byte, err error) {
	err = sm.withHosts(ctx, contracts, func(hosts []slab.Host) error {
		b, err = slab.DownloadSlab(ctx, s, hosts)
		return err
	})
	return
}

func (sm slabMover) DeleteSlabs(ctx context.Context, slabs []slab.Slab, contracts []Contract) error {
	return sm.withHosts(ctx, contracts, func(hosts []slab.Host) error {
		return slab.DeleteSlabs(ctx, slabs, hosts)
	})
}

func (sm slabMover) MigrateSlab(ctx context.Context, s *slab.Slab, currentHeight uint64, from, to []Contract) (err error) {
	sm.pool.SetCurrentHeight(currentHeight)
	var fromHosts []slab.Host
	for _, c := range from {
		h := sm.pool.Session(c.HostKey, c.HostIP, c.ID, c.RenterKey)
		defer sm.pool.UnlockContract(h)
		fromHosts = append(fromHosts, h)
	}
	var toHosts []slab.Host
	for _, c := range to {
		h := sm.pool.Session(c.HostKey, c.HostIP, c.ID, c.RenterKey)
		defer sm.pool.UnlockContract(h)
		toHosts = append(toHosts, h)
	}
	done := make(chan struct{})
	go func() {
		select {
		case <-done:
		case <-ctx.Done():
			for _, h := range fromHosts {
				sm.pool.ForceClose(h.(*slab.Session))
			}
			for _, h := range toHosts {
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
	return slab.MigrateSlab(ctx, s, fromHosts, toHosts)
}

func newSlabMover() slabMover {
	return slabMover{
		pool: slab.NewSessionPool(),
	}
}
