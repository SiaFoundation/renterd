package node

import (
	"time"

	"go.sia.tech/coreutils/syncer"
)

type (
	peerStore struct{}
)

var (
	_ syncer.PeerStore = (*peerStore)(nil)
)

func NewPeerStore() syncer.PeerStore {
	return &peerStore{}
}

// AddPeer adds a peer to the store. If the peer already exists, nil should
// be returned.
func (ps *peerStore) AddPeer(addr string) error { panic("implement me") }

// Peers returns the set of known peers.
func (ps *peerStore) Peers() ([]syncer.PeerInfo, error) { panic("implement me") }

// UpdatePeerInfo updates the metadata for the specified peer. If the peer is
// not found, the error should be ErrPeerNotFound.
func (ps *peerStore) UpdatePeerInfo(addr string, fn func(*syncer.PeerInfo)) error {
	panic("implement me")
}

// Ban temporarily bans one or more IPs. The addr should either be a single IP
// with port (e.g. 1.2.3.4:5678) or a CIDR subnet (e.g. 1.2.3.4/16).
func (ps *peerStore) Ban(addr string, duration time.Duration, reason string) error {
	panic("implement me")
}

// Banned returns true, nil if the peer is banned.
func (ps *peerStore) Banned(addr string) (bool, error) { panic("implement me") }
