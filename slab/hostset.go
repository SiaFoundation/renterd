package slab

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"go.sia.tech/renterd/internal/consensus"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"go.sia.tech/siad/types"
)

// A HostError associates an error with a given host.
type HostError struct {
	HostKey consensus.PublicKey
	Err     error
}

// Error implements error.
func (he HostError) Error() string {
	return fmt.Sprintf("%x: %v", he.HostKey[:4], he.Err.Error())
}

// Unwrap returns the underlying error.
func (he HostError) Unwrap() error {
	return he.Err
}

// A HostErrorSet is a collection of errors from various hosts.
type HostErrorSet []*HostError

// Error implements error.
func (hes HostErrorSet) Error() string {
	strs := make([]string, len(hes))
	for i := range strs {
		strs[i] = hes[i].Error()
	}
	// include a leading newline so that the first error isn't printed on the
	// same line as the error context
	return "\n" + strings.Join(strs, "\n")
}

// A Session wraps a RHPv2 session with useful metadata and methods.
type Session struct {
	*rhpv2.Session
	hostKey       consensus.PublicKey
	hostIP        string
	contractID    types.FileContractID
	renterKey     consensus.PrivateKey
	settings      rhpv2.HostSettings
	currentHeight uint64
	lastSeen      time.Time
}

func (s *Session) reconnect(ctx context.Context) error {
	if s.Session != nil {
		// if it hasn't been long since the last reconnect, assume the
		// connection is still open
		if time.Since(s.lastSeen) < 2*time.Minute {
			s.lastSeen = time.Now()
			return nil
		}
		// otherwise, the connection *might* still be open; test by sending
		// a "ping" RPC
		if _, err := rhpv2.RPCSettings(s.Transport()); err == nil {
			s.lastSeen = time.Now()
			return nil
		}
		// connection timed out, or some other error occurred; close our
		// end (just in case) and fallthrough to the reconnection logic
		s.Close()
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", s.hostIP)
	if err != nil {
		return err
	}
	t, err := rhpv2.NewRenterTransport(conn, s.hostKey)
	if err != nil {
		return err
	}
	s.settings, err = rhpv2.RPCSettings(t)
	if err != nil {
		t.Close()
		return err
	}
	s.Session, err = rhpv2.RPCLock(t, s.contractID, s.renterKey, 10*time.Second)
	if err != nil {
		t.Close()
		return err
	}
	s.lastSeen = time.Now()
	return nil
}

// UploadSector implements SectorUploader.
func (s *Session) UploadSector(sector *[rhpv2.SectorSize]byte) (consensus.Hash256, error) {
	if s.currentHeight == 0 {
		panic("cannot upload without knowing current height") // developer error
	}
	if err := s.reconnect(context.TODO()); err != nil {
		return consensus.Hash256{}, err
	}
	storageDuration := uint64(s.Contract().Revision.NewWindowStart) - s.currentHeight
	price, collateral := rhpv2.RPCAppendCost(s.settings, storageDuration)
	return s.Append(sector, price, collateral)
}

// DownloadSector implements SectorDownloader.
func (s *Session) DownloadSector(w io.Writer, root consensus.Hash256, offset, length uint32) error {
	if err := s.reconnect(context.TODO()); err != nil {
		return err
	}
	sections := []rhpv2.RPCReadRequestSection{{
		MerkleRoot: root,
		Offset:     uint64(offset),
		Length:     uint64(length),
	}}
	price := rhpv2.RPCReadCost(s.settings, sections)
	return s.Read(w, sections, price)
}

// DeleteSectors implements SectorDeleter.
func (s *Session) DeleteSectors(roots []consensus.Hash256) error {
	if err := s.reconnect(context.TODO()); err != nil {
		return err
	}
	// download the full set of SectorRoots
	contractSectors := s.Contract().NumSectors()
	rootIndices := make(map[consensus.Hash256]uint64, contractSectors)
	for offset := uint64(0); offset < contractSectors; {
		n := uint64(130000) // a little less than 4MiB of roots
		if offset+n > contractSectors {
			n = contractSectors - offset
		}
		price := rhpv2.RPCSectorRootsCost(s.settings, n)
		roots, err := s.SectorRoots(offset, n, price)
		if err != nil {
			return err
		}
		for i, root := range roots {
			rootIndices[root] = offset + uint64(i)
		}
		offset += n
	}

	// look up the index of each sector
	badIndices := make([]uint64, 0, len(roots))
	for _, r := range roots {
		if index, ok := rootIndices[r]; ok {
			badIndices = append(badIndices, index)
			delete(rootIndices, r) // prevent duplicates
		}
	}

	price := rhpv2.RPCDeleteCost(s.settings, len(badIndices))
	return s.Session.Delete(badIndices, price)
}

// A HostSet is a set of hosts that can be used for uploading and downloading.
type HostSet struct {
	hosts map[consensus.PublicKey]*Session
}

// Close closes all of the sessions in the set.
func (hs *HostSet) Close() error {
	for hostKey, sess := range hs.hosts {
		sess.Close()
		delete(hs.hosts, hostKey)
	}
	return nil
}

// AddHost adds a host to the set.
func (hs *HostSet) AddHost(hostKey consensus.PublicKey, hostIP string, contractID types.FileContractID, renterKey consensus.PrivateKey) {
	hs.hosts[hostKey] = &Session{
		hostKey:    hostKey,
		hostIP:     hostIP,
		contractID: contractID,
		renterKey:  renterKey,
	}
}

// Uploaders returns the hosts as a set of SectorUploaders.
func (hs *HostSet) Uploaders(currentHeight uint64) map[consensus.PublicKey]SectorUploader {
	m := make(map[consensus.PublicKey]SectorUploader)
	for hostKey, sess := range hs.hosts {
		sess.currentHeight = currentHeight
		m[hostKey] = sess
	}
	return m
}

// Downloaders returns the hosts as a set of SectorDownloaders.
func (hs *HostSet) Downloaders() map[consensus.PublicKey]SectorDownloader {
	m := make(map[consensus.PublicKey]SectorDownloader)
	for hostKey, sess := range hs.hosts {
		m[hostKey] = sess
	}
	return m
}

// Deleters returns the hosts as a set of SectorDeleters.
func (hs *HostSet) Deleters() map[consensus.PublicKey]SectorDeleter {
	m := make(map[consensus.PublicKey]SectorDeleter)
	for hostKey, sess := range hs.hosts {
		m[hostKey] = sess
	}
	return m
}

// NewHostSet creates a new HostSet.
func NewHostSet() *HostSet {
	return &HostSet{
		hosts: make(map[consensus.PublicKey]*Session),
	}
}
