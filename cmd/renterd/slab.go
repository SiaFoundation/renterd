package main

import (
	"context"
	"io"

	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/slab"
)

type slabMover struct {
	pool *slab.SessionPool
}

func (sm slabMover) withHosts(ctx context.Context, contracts []api.Contract, fn func([]slab.Host) error) (err error) {
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
				sm.pool.UnlockContract(h.(*slab.Session))
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

func (sm slabMover) UploadSlabs(ctx context.Context, r io.Reader, m, n uint8, currentHeight uint64, contracts []api.Contract) (slabs []slab.Slab, err error) {
	sm.pool.SetCurrentHeight(currentHeight)
	err = sm.withHosts(ctx, contracts, func(hosts []slab.Host) error {
		slabs, err = slab.UploadSlabs(r, m, n, hosts)
		return err
	})
	return
}

func (sm slabMover) DownloadSlabs(ctx context.Context, w io.Writer, slabs []slab.Slice, offset, length int64, contracts []api.Contract) error {
	return sm.withHosts(ctx, contracts, func(hosts []slab.Host) error {
		return slab.DownloadSlabs(w, slabs, offset, length, hosts)
	})
}

func (sm slabMover) DeleteSlabs(ctx context.Context, slabs []slab.Slab, contracts []api.Contract) error {
	return sm.withHosts(ctx, contracts, func(hosts []slab.Host) error {
		return slab.DeleteSlabs(slabs, hosts)
	})
}

func (sm slabMover) MigrateSlabs(ctx context.Context, slabs []slab.Slab, currentHeight uint64, from, to []api.Contract) (err error) {
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
	return slab.MigrateSlabs(slabs, fromHosts, toHosts)
}

func newSlabMover() slabMover {
	return slabMover{
		pool: slab.NewSessionPool(),
	}
}
