package stores

import (
	"encoding/hex"
	"errors"
	"reflect"
	"testing"
	"time"

	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/siad/modules"
	"lukechampine.com/frand"
)

// TestSQLHostDB tests the basic functionality of SQLHostDB using an in-memory
// SQLite DB.
func TestSQLHostDB(t *testing.T) {
	dbName := hex.EncodeToString(frand.Bytes(32)) // random name for db

	conn := NewEphemeralSQLiteConnection(dbName)
	hdb, ccid, err := NewSQLHostDB(conn, true)
	if err != nil {
		t.Fatal(err)
	}
	if ccid != modules.ConsensusChangeBeginning {
		t.Fatal("wrong ccid", ccid, modules.ConsensusChangeBeginning)
	}

	// Create a host and 2 interactions.
	hostKey := consensus.GeneratePrivateKey().PublicKey()
	hi1 := hostdb.Interaction{
		Timestamp: time.Now().Round(time.Second).UTC(),
		Type:      "foo1",
		Result:    []byte{1},
	}
	hi2 := hostdb.Interaction{
		Timestamp: time.Now().Round(time.Second).UTC(),
		Type:      "foo2",
		Result:    []byte{2},
	}

	// Try to fetch the host. Should fail.
	_, err = hdb.Host(hostKey)
	if !errors.Is(err, ErrHostNotFound) {
		t.Fatal(err)
	}

	// Add the interactions to the db.
	if err := hdb.RecordInteraction(hostKey, hi1); err != nil {
		t.Fatal(err)
	}
	if err := hdb.RecordInteraction(hostKey, hi2); err != nil {
		t.Fatal(err)
	}

	// Read the interactions and verify them.
	var interactions []interaction
	tx := hdb.staticDB.Find(&interactions)
	if tx.Error != nil {
		t.Fatal(err)
	}
	if tx.RowsAffected != 2 {
		t.Fatalf("expected %v rows but got %v", 2, tx.RowsAffected)
	}
	if !reflect.DeepEqual(interactions[0].Interaction(), hi1) {
		t.Fatal("interaction mismatch", interactions[0], hi1)
	}
	if !reflect.DeepEqual(interactions[1].Interaction(), hi2) {
		t.Fatal("interaction mismatch", interactions[1], hi2)
	}

	// Insert an announcement for the host.
	a := hostdb.Announcement{
		Index: consensus.ChainIndex{
			Height: 42,
			ID:     consensus.BlockID{1, 2, 3},
		},
		Timestamp:  time.Now().Round(time.Second).UTC(),
		NetAddress: "host.com",
	}

	// Read the announcement and verify it.
	err = insertAnnouncement(hdb.staticDB, hostKey, a)
	if err != nil {
		t.Fatal(err)
	}
	var announcements []announcement
	tx = hdb.staticDB.Find(&announcements)
	if tx.Error != nil {
		t.Fatal(err)
	}
	if len(announcements) != 1 {
		t.Fatalf("wrong number of announcements %v != %v", len(announcements), 1)
	}
	if !reflect.DeepEqual(announcements[0].Announcement(), a) {
		t.Fatal("announcement mismatch", announcements[0], a)
	}

	// Read the host using SelectHosts. Even without manually adding it
	// there should be an entry which was created upon inserting the first
	// interaction. We should also be able to preload the interactions.
	hosts, err := hdb.SelectHosts(10, func(hostdb.Host) bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	h := hosts[0]
	if len(hosts) != 1 {
		t.Fatalf("invalid number of hosts %v != %v", len(hosts), 1)
	}
	if len(h.Interactions) != 2 {
		t.Fatalf("wrong number of interactions %v != %v", len(h.Interactions), 2)
	}
	if len(h.Announcements) != 1 {
		t.Fatalf("wrong number of announcements %v != %v", len(h.Announcements), 1)
	}

	// Same thing again but with Host.
	h, err = hdb.Host(hostKey)
	if err != nil {
		t.Fatal(err)
	}
	if len(h.Interactions) != 2 {
		t.Fatalf("wrong number of interactions %v != %v", len(h.Interactions), 2)
	}
	if len(h.Announcements) != 1 {
		t.Fatalf("wrong number of announcements %v != %v", len(h.Announcements), 1)
	}

	// Apply a consensus change to make sure the ccid is updated.
	ccid2 := modules.ConsensusChangeID{1, 2, 3}
	hdb.ProcessConsensusChange(modules.ConsensusChange{ID: ccid2})

	// Connect to the same DB again.
	conn2 := NewEphemeralSQLiteConnection(dbName)
	hdb2, ccid, err := NewSQLHostDB(conn2, false)
	if err != nil {
		t.Fatal(err)
	}
	if ccid != ccid2 {
		t.Fatal("ccid wasn't updated", ccid, ccid2)
	}
	_, err = hdb2.Host(hostKey)
	if err != nil {
		t.Fatal(err)
	}
}
