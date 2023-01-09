package stores

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/renterd/rhp/v2"
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
	tx := hdb.db.Where("last_announcement = ? AND net_address = ?", a.Timestamp, a.NetAddress).Find(&h)
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

// TestRecordInteractions is a test for RecordHostInteractions.
//func TestRecordInteractions(t *testing.T) {
//	hdb, _, _, err := newTestSQLStore()
//	if err != nil {
//		t.Fatal(err)
//	}
//	defer hdb.Close()
//
//	// Add a host.
//	hk := consensus.GeneratePrivateKey().PublicKey()
//	err = hdb.addTestHost(hk)
//	if err != nil {
//		t.Fatal(err)
//	}
//
//	// It shouldn't have any interactions.
//	host, err := hdb.Host(hk)
//	if err != nil {
//		t.Fatal(err)
//	}
//	if host.Interactions != (hostdb.Interactions{}) {
//		t.Fatal("mismatch")
//	}
//
//	// Add one successful and two failed interactions.
//	if err := hdb.RecordHostInteractions(hk, 1, 2); err != nil {
//		t.Fatal(err)
//	}
//	host, err = hdb.Host(hk)
//	if err != nil {
//		t.Fatal(err)
//	}
//	if host.Interactions != (hostdb.Interactions{
//		SuccessfulInteractions: 1,
//		FailedInteractions:     2,
//	}) {
//		t.Fatal("mismatch")
//	}
//
//	// Add some more
//	if err := hdb.RecordHostInteractions(hk, 3, 10); err != nil {
//		t.Fatal(err)
//	}
//	host, err = hdb.Host(hk)
//	if err != nil {
//		t.Fatal(err)
//	}
//	if host.Interactions != (hostdb.Interactions{
//		SuccessfulInteractions: 4,
//		FailedInteractions:     12,
//	}) {
//		t.Fatal("mismatch")
//	}
//}

// TestRecordScan is a test for RecordHostScan.
func TestRecordScan(t *testing.T) {
	hdb, _, _, err := newTestSQLStore()
	if err != nil {
		t.Fatal(err)
	}
	defer hdb.Close()

	// Add a host.
	hk := consensus.GeneratePrivateKey().PublicKey()
	err = hdb.addTestHost(hk)
	if err != nil {
		t.Fatal(err)
	}

	// The host shouldn't have any interaction related fields set.
	host, err := hdb.Host(hk)
	if err != nil {
		t.Fatal(err)
	}
	if host.Interactions != (hostdb.Interactions{}) {
		t.Fatal("mismatch")
	}
	if host.Settings != nil {
		t.Fatal("mismatch")
	}

	// Fetch the host directly to get the creation time.
	h, err := hostByPubKey(hdb.db, hk)
	if err != nil {
		t.Fatal(err)
	}
	if h.CreatedAt.IsZero() {
		t.Fatal("creation time not set")
	}

	// Record a scan.
	firstScanTime := time.Now().UTC()
	settings := rhp.HostSettings{NetAddress: "host.com"}
	if err := hdb.RecordHostScan(hk, firstScanTime, true, settings); err != nil {
		t.Fatal(err)
	}
	host, err = hdb.Host(hk)
	if err != nil {
		t.Fatal(err)
	}

	// We expect no uptime or downtime from only a single scan.
	uptime := time.Duration(0)
	downtime := time.Duration(0)
	if host.Interactions != (hostdb.Interactions{
		TotalScans:             1,
		LastScan:               firstScanTime,
		LastScanSuccess:        true,
		PreviousScanSuccess:    false,
		Uptime:                 uptime,
		Downtime:               downtime,
		SuccessfulInteractions: 1,
		FailedInteractions:     0,
	}) {
		t.Fatal("mismatch")
	}
	if !reflect.DeepEqual(*host.Settings, settings) {
		t.Fatal("mismatch")
	}

	// Record another scan 1 hour after the previous one.
	secondScanTime := firstScanTime.Add(time.Hour)
	if err := hdb.RecordHostScan(hk, secondScanTime, true, settings); err != nil {
		t.Fatal(err)
	}
	host, err = hdb.Host(hk)
	if err != nil {
		t.Fatal(err)
	}
	uptime += secondScanTime.Sub(firstScanTime)
	if host.Interactions != (hostdb.Interactions{
		TotalScans:             2,
		LastScan:               secondScanTime,
		LastScanSuccess:        true,
		PreviousScanSuccess:    true,
		Uptime:                 uptime,
		Downtime:               downtime,
		SuccessfulInteractions: 2,
		FailedInteractions:     0,
	}) {
		t.Fatal("mismatch")
	}

	// Record another scan 2 hours after the second one. This time it fails.
	thirdScanTime := secondScanTime.Add(2 * time.Hour)
	if err := hdb.RecordHostScan(hk, thirdScanTime, false, settings); err != nil {
		t.Fatal(err)
	}
	host, err = hdb.Host(hk)
	if err != nil {
		t.Fatal(err)
	}
	downtime += thirdScanTime.Sub(secondScanTime)
	if host.Interactions != (hostdb.Interactions{
		TotalScans:             3,
		LastScan:               thirdScanTime,
		LastScanSuccess:        false,
		PreviousScanSuccess:    true,
		Uptime:                 uptime,
		Downtime:               downtime,
		SuccessfulInteractions: 2,
		FailedInteractions:     1,
	}) {
		t.Fatal("mismatch")
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
	tx := db.db.Find(&hosts)
	if tx.Error != nil {
		return nil, tx.Error
	}
	return hosts, nil
}
