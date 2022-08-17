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
		hs.SetCurrentHeight(currentHeight)
		ssu := slab.SerialSlabsUploader{SlabUploader: slab.NewSerialSlabUploader(hs)}
		slabs, err = ssu.UploadSlabs(r, m, n)
		return err
	})
	return
}

func (sm slabMover) DownloadSlabs(ctx context.Context, w io.Writer, slabs []slab.Slice, offset, length int64, contracts []api.Contract) error {
	return sm.withHostSet(ctx, contracts, func(hs *slab.HostSet) error {
		ssd := slab.SerialSlabsDownloader{SlabDownloader: slab.NewSerialSlabDownloader(hs)}
		return ssd.DownloadSlabs(w, slabs, offset, length)
	})
}

func (sm slabMover) DeleteSlabs(ctx context.Context, slabs []slab.Slab, contracts []api.Contract) error {
	return sm.withHostSet(ctx, contracts, func(hs *slab.HostSet) error {
		ssd := slab.NewSerialSlabsDeleter(hs)
		return ssd.DeleteSlabs(slabs)
	})
}
