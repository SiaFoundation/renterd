package stores

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/siad/modules"
)

func (s *SQLStore) insertTestAnnouncement(hk consensus.PublicKey, a hostdb.Announcement) error {
	return insertAnnouncements(s.db, []announcement{
		{
			hostKey:      hk,
			announcement: a,
		},
	})
}

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

	// Try to fetch a random host. Should fail.
	hk := consensus.GeneratePrivateKey().PublicKey()
	_, err = hdb.Host(hk)
	if !errors.Is(err, ErrHostNotFound) {
		t.Fatal(err)
	}

	// Add the host
	err = hdb.addTestHost(hk)
	if err != nil {
		t.Fatal(err)
	}

	// Assert it's returned
	allHosts, err := hdb.Hosts(time.Now(), -1)
	if err != nil {
		t.Fatal(err)
	}
	if len(allHosts) != 1 || allHosts[0].PublicKey != hk {
		t.Fatal("unexpected result", len(allHosts))
	}

	// Add an interaction one hour in the past
	currentTime := time.Now().UTC().Round(time.Second)
	hi1 := hostdb.Interaction{
		Timestamp: currentTime.Add(-time.Hour),
		Type:      "foo1",

		Result: []byte{1},
	}
	if err := hdb.RecordInteraction(hk, hi1); err != nil {
		t.Fatal(err)
	}

	// Assert we can include/exclude the host if we play around with the notSince param
	allHosts, err = hdb.Hosts(time.Now().Add(-2*time.Hour), -1)
	if err != nil || len(allHosts) != 0 {
		t.Fatal("unexpected result", err, len(allHosts))
	}
	allHosts, err = hdb.Hosts(time.Now().Add(-30*time.Minute), -1)
	if err != nil || len(allHosts) != 1 || allHosts[0].PublicKey != hk {
		t.Fatal("unexpected result", err, len(allHosts))
	}

	// Add another interaction.
	hi2 := hostdb.Interaction{
		Timestamp: currentTime,
		Type:      "foo2",
		Result:    []byte{2},
	}
	if err := hdb.RecordInteraction(hk, hi2); err != nil {
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
	err = hdb.insertTestAnnouncement(hk, a)
	if err != nil {
		t.Fatal(err)
	}

	// Read the host and verify that the announcement related fields were
	// set.
	var h dbHost
	tx = hdb.db.Where("last_announcement = ? AND net_address = ?", a.Timestamp, a.NetAddress).Find(&h)
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	if h.PublicKey != hk {
		t.Fatal("wrong host returned")
	}

	// Same thing again but with hosts.
	hosts, err := hdb.hosts()
	if err != nil {
		t.Fatal(err)
	}
	if len(hosts) != 1 {
		t.Fatal("wrong number of hosts", len(hosts))
	}
	h1 := hosts[0]
	h1.Interactions = h.Interactions // ignore for comparison
	if !reflect.DeepEqual(h1, h) {
		fmt.Println(h1)
		fmt.Println(h)
		t.Fatal("mismatch")
	}

	// Same thing again but with Host.
	h2, err := hdb.Host(hk)
	if err != nil {
		t.Fatal(err)
	}
	if h2.NetAddress != h.NetAddress {
		t.Fatal("wrong net address")
	}
	if h2.KnownSince.IsZero() {
		t.Fatal("known since not set")
	}

	// Insert another announcement for an unknown host.
	unknownKey := consensus.PublicKey{1, 4, 7}
	err = hdb.insertTestAnnouncement(unknownKey, a)
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
	if h3.NetAddress != a.NetAddress {
		t.Fatal("wrong net address")
	}
	if h3.KnownSince.IsZero() {
		t.Fatal("known since not set")
	}

	// Wait for the persist interval to pass to make sure an empty consensus
	// change triggers a persist.
	time.Sleep(testPersistInterval)

	// Apply a consensus change.
	ccid2 := modules.ConsensusChangeID{1, 2, 3}
	hdb.ProcessConsensusChange(modules.ConsensusChange{ID: ccid2})

	// Connect to the same DB again.
	conn2 := NewEphemeralSQLiteConnection(dbName)
	hdb2, ccid, err := NewSQLStore(conn2, false, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if ccid != ccid2 {
		t.Fatal("ccid wasn't updated", ccid, ccid2)
	}
	_, err = hdb2.Host(hk)
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

// TestInsertAnnouncements is a test for insertAnnouncements.
func TestInsertAnnouncements(t *testing.T) {
	hdb, _, _, err := newTestSQLStore()
	if err != nil {
		t.Fatal(err)
	}

	// Create announcements for 2 hosts.
	ann1 := announcement{
		hostKey: consensus.GeneratePrivateKey().PublicKey(),
		announcement: hostdb.Announcement{
			Index:      consensus.ChainIndex{Height: 1, ID: consensus.BlockID{1}},
			Timestamp:  time.Now(),
			NetAddress: "ann1",
		},
	}
	ann2 := announcement{
		hostKey:      consensus.GeneratePrivateKey().PublicKey(),
		announcement: hostdb.Announcement{},
	}
	ann3 := announcement{
		hostKey:      consensus.GeneratePrivateKey().PublicKey(),
		announcement: hostdb.Announcement{},
	}

	// Insert the first one and check that all fields are set.
	if err := insertAnnouncements(hdb.db, []announcement{ann1}); err != nil {
		t.Fatal(err)
	}
	var ann dbAnnouncement
	if err := hdb.db.Find(&ann).Error; err != nil {
		t.Fatal(err)
	}
	ann.Model = Model{} // ignore
	expectedAnn := dbAnnouncement{
		HostKey:     ann1.hostKey,
		BlockHeight: 1,
		BlockID:     consensus.BlockID{1}.String(),
		NetAddress:  "ann1",
	}
	if ann != expectedAnn {
		t.Fatal("mismatch")
	}
	// Insert the first and second one.
	if err := insertAnnouncements(hdb.db, []announcement{ann1, ann2}); err != nil {
		t.Fatal(err)
	}

	// Insert the first one twice. The second one again and the third one.
	if err := insertAnnouncements(hdb.db, []announcement{ann1, ann2, ann1, ann3}); err != nil {
		t.Fatal(err)
	}

	// There should be 3 hosts in the db.
	hosts, err := hdb.hosts()
	if err != nil {
		t.Fatal(err)
	}
	if len(hosts) != 3 {
		t.Fatal("invalid number of hosts")
	}

	// There should be 7 announcements total.
	var announcements []dbAnnouncement
	if err := hdb.db.Find(&announcements).Error; err != nil {
		t.Fatal(err)
	}
	if len(announcements) != 7 {
		t.Fatal("invalid number of announcements")
	}
}

// addTestHost ensures a host with given hostkey exists in the db.
func (s *SQLStore) addTestHost(hk consensus.PublicKey) error {
	return s.db.FirstOrCreate(&dbHost{}, &dbHost{PublicKey: hk}).Error
}

// hosts returns all hosts in the db. Only used in testing since preloading all
// interactions for all hosts is expensive in production.
func (db *SQLStore) hosts() ([]dbHost, error) {
	var hosts []dbHost
	tx := db.db.Preload("Interactions").
		Find(&hosts)
	if tx.Error != nil {
		return nil, tx.Error
	}
	return hosts, nil
}
