package stores

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/siad/modules"
)

// TestSQLHostDB tests the basic functionality of SQLHostDB using an in-memory
// SQLite DB.
func TestSQLHostDB(t *testing.T) {
	hdb, dbName, ccid, err := newTestSQLStore()
	if err != nil {
		t.Fatal(err)
	}
	if ccid != modules.ConsensusChangeBeginning {
		t.Fatal("wrong ccid", ccid, modules.ConsensusChangeBeginning)
	}

	// Create a host and 2 interactions. One interaction is an hour in the
	// past and one is an hour in the future.
	hostKey := consensus.GeneratePrivateKey().PublicKey()
	currentTime := time.Now().UTC().Round(time.Second)
	hi1 := hostdb.Interaction{
		Timestamp: currentTime,
		Type:      "foo1",

		Result: []byte{1},
	}
	hi2 := hostdb.Interaction{
		Timestamp: currentTime,
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
	var interactions []dbInteraction
	tx := hdb.db.Find(&interactions)
	if tx.Error != nil {
		t.Fatal(err)
	}
	if len(interactions) != 2 {
		t.Fatalf("expected %v rows but got %v", 2, len(interactions))
	}
	if !reflect.DeepEqual(interactions[0].convert(), hi1) {
		t.Fatal("interaction mismatch", interactions[0], hi1)
	}
	if !reflect.DeepEqual(interactions[1].convert(), hi2) {
		t.Fatal("interaction mismatch", interactions[1], hi2)
	}

	// Insert an announcement for the host and another one for an unknown
	// host.
	a := hostdb.Announcement{
		Index: consensus.ChainIndex{
			Height: 42,
			ID:     consensus.BlockID{1, 2, 3},
		},
		Timestamp:  time.Now().UTC().Round(time.Second),
		NetAddress: "host.com",
	}
	err = insertAnnouncement(hdb.db, hostKey, a)
	if err != nil {
		t.Fatal(err)
	}

	// Read the announcement and verify it.
	var announcements []dbAnnouncement
	tx = hdb.db.Find(&announcements)
	if tx.Error != nil {
		t.Fatal(err)
	}
	if len(announcements) != 1 {
		t.Fatalf("wrong number of announcements %v != %v", len(announcements), 1)
	}
	if !reflect.DeepEqual(announcements[0].convert(), a) {
		t.Fatal("announcement mismatch", announcements[0], a)
	}

	// Read the host using SelectHosts. Even without manually adding it
	// there should be an entry which was created upon inserting the first
	// interaction. We should also be able to preload the interactions.
	hosts, err := hdb.hosts()
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
	h2, err := hdb.Host(hostKey)
	if err != nil {
		t.Fatal(err)
	}
	if len(h2.Interactions) != 2 {
		t.Fatalf("wrong number of interactions %v != %v", len(h2.Interactions), 2)
	}
	if len(h2.Announcements) != 1 {
		t.Fatalf("wrong number of announcements %v != %v", len(h2.Announcements), 1)
	}

	// Insert another announcement for an unknown host.
	unknownKey := consensus.PublicKey{1, 4, 7}
	err = insertAnnouncement(hdb.db, unknownKey, a)
	if err != nil {
		t.Fatal(err)
	}
	h3, err := hdb.Host(unknownKey)
	if err != nil {
		t.Fatal(err)
	}
	if len(h3.Interactions) != 0 {
		t.Fatalf("wrong number of interactions %v != %v", len(h2.Interactions), 2)
	}
	if len(h3.Announcements) != 1 {
		t.Fatalf("wrong number of announcements %v != %v", len(h2.Announcements), 1)
	}

	// Apply a consensus change to make sure the ccid is updated.
	ccid2 := modules.ConsensusChangeID{1, 2, 3}
	hdb.ProcessConsensusChange(modules.ConsensusChange{ID: ccid2})

	// Connect to the same DB again.
	conn2 := NewEphemeralSQLiteConnection(dbName)
	hdb2, ccid, err := NewSQLStore(conn2, false)
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

// TestSQLHosts tests the Hosts method of the SQLHostDB type.
func TestSQLHosts(t *testing.T) {
	hdb, _, _, err := newTestSQLStore()
	if err != nil {
		t.Fatal(err)
	}

	// Prepare interactions for 3 hosts. One interaction will be in the past
	// and one of them uses the current time and one is in the future.
	hostKey1 := consensus.GeneratePrivateKey().PublicKey()
	hostKey2 := consensus.GeneratePrivateKey().PublicKey()
	hostKey3 := consensus.GeneratePrivateKey().PublicKey()

	currentTime := time.Now()
	pastHi := hostdb.Interaction{
		Timestamp: currentTime.Add(-time.Hour),
	}
	currentHi := hostdb.Interaction{
		Timestamp: currentTime,
	}
	futureHi := hostdb.Interaction{
		Timestamp: currentTime.Add(time.Hour),
	}

	// Host1 - All interactions.
	if err := hdb.RecordInteraction(hostKey1, pastHi); err != nil {
		t.Fatal(err)
	}
	if err := hdb.RecordInteraction(hostKey1, currentHi); err != nil {
		t.Fatal(err)
	}
	if err := hdb.RecordInteraction(hostKey1, futureHi); err != nil {
		t.Fatal(err)
	}

	// Host2 - Current and past.
	if err := hdb.RecordInteraction(hostKey2, currentHi); err != nil {
		t.Fatal(err)
	}
	if err := hdb.RecordInteraction(hostKey2, pastHi); err != nil {
		t.Fatal(err)
	}

	// Host3 - past.
	if err := hdb.RecordInteraction(hostKey3, pastHi); err != nil {
		t.Fatal(err)
	}

	// Helper for testing.
	var hosts []hostdb.Host
	checkHosts := func(hostKeys ...consensus.PublicKey) error {
		if len(hostKeys) != len(hosts) {
			return fmt.Errorf("expected %v hosts but got %v", len(hostKeys), len(hosts))
		}
		hostMap := make(map[consensus.PublicKey]struct{})
		for _, h := range hosts {
			hostMap[h.PublicKey] = struct{}{}
		}
		for _, hk := range hostKeys {
			_, exists := hostMap[hk]
			if !exists {
				return fmt.Errorf("host %v missing from map", hk)
			}
		}
		return nil
	}

	// Case1 - Timestamp 1 second after futureHi. 3 hosts expected.
	hosts, err = hdb.Hosts(futureHi.Timestamp.Add(time.Second), 3)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkHosts(hostKey1, hostKey2, hostKey3); err != nil {
		t.Fatal(err)
	}

	// Case2 - Timestamp 1 second after currentHi. 2 hosts expected.
	hosts, err = hdb.Hosts(currentHi.Timestamp.Add(time.Second), 3)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkHosts(hostKey2, hostKey3); err != nil {
		t.Fatal(err)
	}

	// Case3 - Timestamp 1 second after pastHi. 1 host expected.
	hosts, err = hdb.Hosts(pastHi.Timestamp.Add(time.Second), 3)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkHosts(hostKey3); err != nil {
		t.Fatal(err)
	}

	// Case4 - Timestamp 1 second before pastHi. 0 hosts expected.
	hosts, err = hdb.Hosts(pastHi.Timestamp.Add(-time.Second), 3)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkHosts(); err != nil {
		t.Fatal(err)
	}

	// Case5 - Same as Case 1 but with a limit of 1. So we expect any of the
	// 3 hosts to be returned.
	hosts, err = hdb.Hosts(futureHi.Timestamp.Add(time.Second), 1)
	if err != nil {
		t.Fatal(err)
	}
	err1 := checkHosts(hostKey1)
	err2 := checkHosts(hostKey2)
	err3 := checkHosts(hostKey3)
	if err1 != nil && err2 != nil && err3 != nil {
		t.Fatal(err1, err2, err3)
	}
}
