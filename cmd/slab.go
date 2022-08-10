package main

import (
	"context"
	"io"

	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/slab"
)

type slabMover struct{}

func (slabMover) UploadSlabs(ctx context.Context, r io.Reader, m, n uint8, currentHeight uint64, contracts []api.Contract) ([]slab.Slab, error) {
	hs := slab.NewHostSet(currentHeight)
	for _, c := range contracts {
		hs.AddHost(c.HostKey, c.HostIP, c.ID, c.RenterKey)
	}
	ssu := slab.SerialSlabsUploader{SlabUploader: slab.NewSerialSlabUploader(hs)}
	return ssu.UploadSlabs(ctx, slab.NewUniformSlabReader(r, m, n))
}

func (slabMover) DownloadSlabs(ctx context.Context, w io.Writer, slabs []slab.Slice, offset, length int64, currentHeight uint64, contracts []api.Contract) error {
	hs := slab.NewHostSet(currentHeight)
	for _, c := range contracts {
		hs.AddHost(c.HostKey, c.HostIP, c.ID, c.RenterKey)
	}
	ssd := slab.SerialSlabsDownloader{SlabDownloader: slab.NewSerialSlabDownloader(hs)}
	return ssd.DownloadSlabs(ctx, w, slabs, offset, length)
}

func (slabMover) DeleteSlabs(ctx context.Context, slabs []slab.Slab, currentHeight uint64, contracts []api.Contract) error {
	hs := slab.NewHostSet(currentHeight)
	for _, c := range contracts {
		hs.AddHost(c.HostKey, c.HostIP, c.ID, c.RenterKey)
	}
	ssd := slab.NewSerialSlabsDeleter(hs)
	return ssd.DeleteSlabs(ctx, slabs)
}
