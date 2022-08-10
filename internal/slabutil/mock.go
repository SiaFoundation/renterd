package slabutil

import (
	"context"
	"errors"
	"io"

	"go.sia.tech/renterd/internal/consensus"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"go.sia.tech/renterd/slab"
)

// A MockHost simulates a Sia host in memory.
type MockHost struct {
	sectors map[consensus.Hash256][]byte
}

// UploadSector implements slab.SectorUploader.
func (h *MockHost) UploadSector(ctx context.Context, sector *[rhpv2.SectorSize]byte) (consensus.Hash256, error) {
	root := rhpv2.SectorRoot(sector)
	h.sectors[root] = append([]byte(nil), sector[:]...)
	return root, nil
}

// DownloadSector implements slab.SectorDownloader.
func (h *MockHost) DownloadSector(ctx context.Context, w io.Writer, root consensus.Hash256, offset, length uint32) error {
	sector, ok := h.sectors[root]
	if !ok {
		return errors.New("unknown root")
	}
	_, err := w.Write(sector[offset:][:length])
	return err
}

// NewMockHost creates a new MockHost.
func NewMockHost() *MockHost {
	return &MockHost{
		sectors: make(map[consensus.Hash256][]byte),
	}
}

// A MockHostSet simulates a set of Sia hosts in memory.
type MockHostSet struct {
	Hosts map[consensus.PublicKey]*MockHost
}

// AddHost adds a host to the set, returning its public key.
func (hs *MockHostSet) AddHost() consensus.PublicKey {
	hostKey := consensus.GeneratePrivateKey().PublicKey()
	hs.Hosts[hostKey] = NewMockHost()
	return hostKey
}

// SlabUploader returns a slab.Uploader for the host set.
func (hs *MockHostSet) SlabUploader() slab.Uploader {
	hosts := make(map[consensus.PublicKey]slab.SectorUploader)
	for hostKey, sess := range hs.Hosts {
		hosts[hostKey] = sess
	}
	return &slab.SerialSlabUploader{
		Hosts: hosts,
	}
}

// SlabDownloader returns a slab.Downloader for the host set.
func (hs *MockHostSet) SlabDownloader() slab.Downloader {
	hosts := make(map[consensus.PublicKey]slab.SectorDownloader)
	for hostKey, sess := range hs.Hosts {
		hosts[hostKey] = sess
	}
	return &slab.SerialSlabDownloader{
		Hosts: hosts,
	}
}

// NewMockHostSet creates a new MockHostSet.
func NewMockHostSet() *MockHostSet {
	return &MockHostSet{
		Hosts: make(map[consensus.PublicKey]*MockHost),
	}
}
