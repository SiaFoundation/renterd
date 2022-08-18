package slabutil

import (
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
func (h *MockHost) UploadSector(sector *[rhpv2.SectorSize]byte) (consensus.Hash256, error) {
	root := rhpv2.SectorRoot(sector)
	h.sectors[root] = append([]byte(nil), sector[:]...)
	return root, nil
}

// DownloadSector implements slab.SectorDownloader.
func (h *MockHost) DownloadSector(w io.Writer, root consensus.Hash256, offset, length uint32) error {
	sector, ok := h.sectors[root]
	if !ok {
		return errors.New("unknown root")
	}
	_, err := w.Write(sector[offset:][:length])
	return err
}

// DeleteSectors implements slab.SectorDeleter.
func (h *MockHost) DeleteSectors(roots []consensus.Hash256) error {
	for _, root := range roots {
		delete(h.sectors, root)
	}
	return nil
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

// Uploaders returns the hosts as a set of slab.SectorUploaders.
func (hs *MockHostSet) Uploaders() map[consensus.PublicKey]slab.SectorUploader {
	m := make(map[consensus.PublicKey]slab.SectorUploader)
	for hostKey, sess := range hs.Hosts {
		m[hostKey] = sess
	}
	return m
}

// Downloaders returns the hosts as a set of slab.SectorDownloaders.
func (hs *MockHostSet) Downloaders() map[consensus.PublicKey]slab.SectorDownloader {
	m := make(map[consensus.PublicKey]slab.SectorDownloader)
	for hostKey, sess := range hs.Hosts {
		m[hostKey] = sess
	}
	return m
}

// Deleters returns the hosts as a set of slab.SectorDeleters.
func (hs *MockHostSet) Deleters() map[consensus.PublicKey]slab.SectorDeleter {
	m := make(map[consensus.PublicKey]slab.SectorDeleter)
	for hostKey, sess := range hs.Hosts {
		m[hostKey] = sess
	}
	return m
}

// NewMockHostSet creates a new MockHostSet.
func NewMockHostSet() *MockHostSet {
	return &MockHostSet{
		Hosts: make(map[consensus.PublicKey]*MockHost),
	}
}
