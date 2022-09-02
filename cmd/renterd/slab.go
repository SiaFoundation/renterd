package main

import (
	"context"
	"io"

	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/slab"
)

type slabMover struct{}

func (slabMover) withHostSet(ctx context.Context, contracts []api.Contract, fn func(*slab.HostSet) error) (err error) {
	hs := slab.NewHostSet()
	for _, c := range contracts {
		hs.AddHost(c.HostKey, c.HostIP, c.ID, c.RenterKey)
	}
	done := make(chan struct{})
	go func() {
		select {
		case <-done:
		case <-ctx.Done():
		}
		hs.Close()
	}()
	defer func() {
		close(done)
		if ctx.Err() != nil {
			err = ctx.Err()
		}
	}()
	return fn(hs)
}

func (sm slabMover) UploadSlabs(ctx context.Context, r io.Reader, m, n uint8, currentHeight uint64, contracts []api.Contract) (slabs []slab.Slab, err error) {
	err = sm.withHostSet(ctx, contracts, func(hs *slab.HostSet) error {
		slabs, err = slab.UploadSlabs(r, m, n, hs.Uploaders(currentHeight))
		return err
	})
	return
}

func (sm slabMover) DownloadSlabs(ctx context.Context, w io.Writer, slabs []slab.Slice, offset, length int64, contracts []api.Contract) error {
	return sm.withHostSet(ctx, contracts, func(hs *slab.HostSet) error {
		return slab.DownloadSlabs(w, slabs, offset, length, hs.Downloaders())
	})
}

func (sm slabMover) DeleteSlabs(ctx context.Context, slabs []slab.Slab, contracts []api.Contract) error {
	return sm.withHostSet(ctx, contracts, func(hs *slab.HostSet) error {
		return slab.DeleteSlabs(slabs, hs.Deleters())
	})
}

func (sm slabMover) MigrateSlabs(ctx context.Context, slabs []slab.Slab, currentHeight uint64, from, to []api.Contract) (err error) {
	fromHS := slab.NewHostSet()
	for _, c := range from {
		fromHS.AddHost(c.HostKey, c.HostIP, c.ID, c.RenterKey)
	}
	toHS := slab.NewHostSet()
	for _, c := range to {
		toHS.AddHost(c.HostKey, c.HostIP, c.ID, c.RenterKey)
	}
	done := make(chan struct{})
	go func() {
		select {
		case <-done:
		case <-ctx.Done():
		}
		fromHS.Close()
		toHS.Close()
	}()
	defer func() {
		close(done)
		if ctx.Err() != nil {
			err = ctx.Err()
		}
	}()
	return slab.MigrateSlabs(slabs, fromHS.Downloaders(), toHS.Uploaders(currentHeight))
}
