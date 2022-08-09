package slab

import (
	"context"
	"errors"
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

func (s *Session) UploadSector(ctx context.Context, sector *[rhpv2.SectorSize]byte) (consensus.Hash256, error) {
	storageDuration := s.currentHeight - uint64(s.Contract().Revision.NewWindowStart)
	price, collateral := rhpv2.RPCAppendCost(s.settings, storageDuration)
	return s.Append(sector, price, collateral)
}

func (s *Session) DownloadSector(ctx context.Context, w io.Writer, root consensus.Hash256, offset, length uint32) error {
	sections := []rhpv2.RPCReadRequestSection{{
		MerkleRoot: root,
		Offset:     uint64(offset),
		Length:     uint64(length),
	}}
	price := rhpv2.RPCReadCost(s.settings, sections)
	return s.Read(w, sections, price)
}

type HostSet struct {
	hosts         map[consensus.PublicKey]*Session
	currentHeight uint64
}

// Close closes all of the sessions in the set.
func (hs *HostSet) Close() error {
	for hostKey, sess := range hs.hosts {
		sess.Close()
		delete(hs.hosts, hostKey)
	}
	return nil
}

func (hs *HostSet) Host(host consensus.PublicKey) (*Session, error) {
	sess, ok := hs.hosts[host]
	if !ok {
		return nil, errors.New("unknown host")
	}
	if err := sess.reconnect(context.TODO()); err != nil {
		return nil, err
	}
	return sess, nil
}

// AddHost adds a host to the set.
func (hs *HostSet) AddHost(hostKey consensus.PublicKey, hostIP string, contractID types.FileContractID, renterKey consensus.PrivateKey) {
	hs.hosts[hostKey] = &Session{
		hostKey:       hostKey,
		hostIP:        hostIP,
		contractID:    contractID,
		renterKey:     renterKey,
		currentHeight: hs.currentHeight,
	}
}

func NewHostSet(currentHeight uint64) *HostSet {
	return &HostSet{
		hosts:         make(map[consensus.PublicKey]*Session),
		currentHeight: currentHeight,
	}
}
