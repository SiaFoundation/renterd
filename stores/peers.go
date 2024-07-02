package stores

import (
	"context"
	"time"

	"go.sia.tech/coreutils/syncer"
	"go.sia.tech/renterd/stores/sql"
)

var (
	_ syncer.PeerStore = (*SQLStore)(nil)
)

// AddPeer adds a peer to the store. If the peer already exists, nil should be
// returned.
func (s *SQLStore) AddPeer(addr string) error {
	return s.bMain.Transaction(context.Background(), func(tx sql.DatabaseTx) error {
		return tx.AddPeer(context.Background(), addr)
	})
}

// Peers returns the set of known peers.
func (s *SQLStore) Peers() (peers []syncer.PeerInfo, err error) {
	err = s.bMain.Transaction(context.Background(), func(tx sql.DatabaseTx) (txErr error) {
		peers, txErr = tx.Peers(context.Background())
		return
	})
	return
}

// PeerInfo returns the metadata for the specified peer or ErrPeerNotFound
// if the peer wasn't found in the store.
func (s *SQLStore) PeerInfo(addr string) (info syncer.PeerInfo, err error) {
	err = s.bMain.Transaction(context.Background(), func(tx sql.DatabaseTx) (txErr error) {
		info, txErr = tx.PeerInfo(context.Background(), addr)
		return
	})
	return
}

// UpdatePeerInfo updates the metadata for the specified peer. If the peer
// is not found, the error should be ErrPeerNotFound.
func (s *SQLStore) UpdatePeerInfo(addr string, fn func(*syncer.PeerInfo)) error {
	return s.bMain.Transaction(context.Background(), func(tx sql.DatabaseTx) error {
		return tx.UpdatePeerInfo(context.Background(), addr, fn)
	})
}

// Ban temporarily bans one or more IPs. The addr should either be a single
// IP with port (e.g. 1.2.3.4:5678) or a CIDR subnet (e.g. 1.2.3.4/16).
func (s *SQLStore) Ban(addr string, duration time.Duration, reason string) error {
	return s.bMain.Transaction(context.Background(), func(tx sql.DatabaseTx) error {
		return tx.BanPeer(context.Background(), addr, duration, reason)
	})
}

// Banned returns true, nil if the peer is banned.
func (s *SQLStore) Banned(addr string) (banned bool, err error) {
	err = s.bMain.Transaction(context.Background(), func(tx sql.DatabaseTx) (txErr error) {
		banned, txErr = tx.PeerBanned(context.Background(), addr)
		return
	})
	return
}
