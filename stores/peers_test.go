package stores

import (
	"testing"
	"time"

	"go.sia.tech/coreutils/syncer"
)

const (
	testPeer = "1.2.3.4:9981"
)

func TestPeers(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	// assert ErrPeerNotFound before we add it
	err := ss.UpdatePeerInfo(testPeer, func(info *syncer.PeerInfo) {})
	if err != ErrPeerNotFound {
		t.Fatal("expected peer not found")
	}

	// add peer
	err = ss.AddPeer(testPeer)
	if err != nil {
		t.Fatal(err)
	}

	// fetch peers
	var peer syncer.PeerInfo
	peers, err := ss.Peers()
	if err != nil {
		t.Fatal(err)
	} else if len(peers) != 1 {
		t.Fatal("expected 1 peer")
	} else {
		peer = peers[0]
	}

	// assert peer info
	if peer.Address != testPeer {
		t.Fatal("unexpected address")
	} else if peer.FirstSeen.IsZero() {
		t.Fatal("unexpected first seen")
	} else if !peer.LastConnect.IsZero() {
		t.Fatal("unexpected last connect")
	} else if peer.SyncedBlocks != 0 {
		t.Fatal("unexpected synced blocks")
	} else if peer.SyncDuration != 0 {
		t.Fatal("unexpected sync duration")
	}

	// prepare peer update
	lastConnect := time.Now().Truncate(time.Millisecond)
	syncedBlocks := uint64(15)
	syncDuration := 5 * time.Second

	// update peer
	err = ss.UpdatePeerInfo(testPeer, func(info *syncer.PeerInfo) {
		info.LastConnect = lastConnect
		info.SyncedBlocks = syncedBlocks
		info.SyncDuration = syncDuration
	})
	if err != nil {
		t.Fatal(err)
	}

	// refetch peer
	peers, err = ss.Peers()
	if err != nil {
		t.Fatal(err)
	} else if len(peers) != 1 {
		t.Fatal("expected 1 peer")
	} else {
		peer = peers[0]
	}

	// assert peer info
	if peer.Address != testPeer {
		t.Fatal("unexpected address")
	} else if peer.FirstSeen.IsZero() {
		t.Fatal("unexpected first seen")
	} else if !peer.LastConnect.Equal(lastConnect) {
		t.Fatal("unexpected last connect")
	} else if peer.SyncedBlocks != syncedBlocks {
		t.Fatal("unexpected synced blocks")
	} else if peer.SyncDuration != syncDuration {
		t.Fatal("unexpected sync duration")
	}

	// ban peer
	err = ss.Ban(testPeer, time.Hour, "too many hits")
	if err != nil {
		t.Fatal(err)
	}

	// assert the peer was banned
	banned, err := ss.Banned(testPeer)
	if err != nil {
		t.Fatal(err)
	} else if !banned {
		t.Fatal("expected banned")
	}

	// add another banned peer
	bannedPeer := "1.2.3.4:9982"
	err = ss.AddPeer(bannedPeer)
	if err != nil {
		t.Fatal(err)
	}

	// add another unbanned peer
	unbannedPeer := "1.2.3.5:9981"
	err = ss.AddPeer(unbannedPeer)
	if err != nil {
		t.Fatal(err)
	}

	// assert we have three peers
	peers, err = ss.Peers()
	if err != nil {
		t.Fatal(err)
	} else if len(peers) != 3 {
		t.Fatalf("expected 3 peers, got %d", len(peers))
	}

	// assert the peers are properly banned
	banned, err = ss.Banned(bannedPeer)
	if err != nil {
		t.Fatal(err)
	} else if !banned {
		t.Fatal("expected banned")
	}

	banned, err = ss.Banned(unbannedPeer)
	if err != nil {
		t.Fatal(err)
	} else if banned {
		t.Fatal("expected unbanned")
	}
}
