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

func (sm slabMover) UploadSlab(ctx context.Context, r io.Reader, m, n uint8, currentHeight uint64, contracts []api.Contract) (s slab.Slab, his []slab.HostInteraction, err error) {
	sm.pool.SetCurrentHeight(currentHeight)
	err = sm.withHosts(ctx, contracts, func(hosts []slab.Host) error {
		s, his, err = slab.UploadSlab(r, m, n, hosts)
		return err
	})
	return
}

func (sm slabMover) DownloadSlab(ctx context.Context, w io.Writer, s slab.Slice, contracts []api.Contract) (his []slab.HostInteraction, err error) {
	err = sm.withHosts(ctx, contracts, func(hosts []slab.Host) error {
		his, err = slab.DownloadSlab(w, s, hosts)
		return err
	})
	return
}

func (sm slabMover) DeleteSlabs(ctx context.Context, slabs []slab.Slab, contracts []api.Contract) (his []slab.HostInteraction, err error) {
	err = sm.withHosts(ctx, contracts, func(hosts []slab.Host) error {
		his, err = slab.DeleteSlabs(slabs, hosts)
		return err
	})
	return
}

func (sm slabMover) MigrateSlab(ctx context.Context, s *slab.Slab, currentHeight uint64, from, to []api.Contract) (his []slab.HostInteraction, err error) {
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
	return slab.MigrateSlab(s, fromHosts, toHosts)
}

func newSlabMover() slabMover {
	return slabMover{
		pool: slab.NewSessionPool(),
	}
}
