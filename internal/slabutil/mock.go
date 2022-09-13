package slabutil

import (
	"errors"
	"io"

	"go.sia.tech/renterd/internal/consensus"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
)

// A MockHost simulates a Sia host in memory.
type MockHost struct {
	publicKey consensus.PublicKey
	sectors   map[consensus.Hash256][]byte
}

// PublicKey implements slab.Host.
func (h *MockHost) PublicKey() consensus.PublicKey {
	return h.publicKey
}

// UploadSector implements slab.Host.
func (h *MockHost) UploadSector(sector *[rhpv2.SectorSize]byte) (consensus.Hash256, error) {
	root := rhpv2.SectorRoot(sector)
	h.sectors[root] = append([]byte(nil), sector[:]...)
	return root, nil
}

// DownloadSector implements slab.Host.
func (h *MockHost) DownloadSector(w io.Writer, root consensus.Hash256, offset, length uint32) error {
	sector, ok := h.sectors[root]
	if !ok {
		return errors.New("unknown root")
	}
	_, err := w.Write(sector[offset:][:length])
	return err
}

// DeleteSectors implements slab.Host.
func (h *MockHost) DeleteSectors(roots []consensus.Hash256) error {
	for _, root := range roots {
		delete(h.sectors, root)
	}
	return nil
}

// NewMockHost creates a new MockHost.
func NewMockHost() *MockHost {
	return &MockHost{
		publicKey: consensus.GeneratePrivateKey().PublicKey(),
		sectors:   make(map[consensus.Hash256][]byte),
	}
}
