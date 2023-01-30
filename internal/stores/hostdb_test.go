package stores

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/rhp/v2"
	"go.sia.tech/siad/modules"
)

func (s *SQLStore) insertTestAnnouncement(hk types.PublicKey, a hostdb.Announcement) error {
	return insertAnnouncements(s.db, []announcement{
		{
			hostKey:      publicKey(hk),
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
	hk := types.GeneratePrivateKey().PublicKey()
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
	allHosts, err := hdb.Hosts(0, -1)
	if err != nil {
		t.Fatal(err)
	}
	if len(allHosts) != 1 || allHosts[0].PublicKey != hk {
		t.Fatal("unexpected result", len(allHosts))
	}

	// Insert an announcement for the host and another one for an unknown
	// host.
	a := hostdb.Announcement{
		Index: types.ChainIndex{
			Height: 42,
			ID:     types.BlockID{1, 2, 3},
		},
		Timestamp:  time.Now().UTC().Round(time.Second),
		NetAddress: "address",
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
	if types.PublicKey(h.PublicKey) != hk {
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
	unknownKey := types.PublicKey{1, 4, 7}
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

// TestRecordInteractions is a test for RecordInteractions.
func TestRecordInteractions(t *testing.T) {
	hdb, _, _, err := newTestSQLStore()
	if err != nil {
		t.Fatal(err)
	}
	defer hdb.Close()

	// Add a host.
	hk := types.GeneratePrivateKey().PublicKey()
	err = hdb.addTestHost(hk)
	if err != nil {
		t.Fatal(err)
	}

	// It shouldn't have any interactions.
	host, err := hdb.Host(hk)
	if err != nil {
		t.Fatal(err)
	}
	if host.Interactions != (hostdb.Interactions{}) {
		t.Fatal("mismatch")
	}

	createInteractions := func(hk types.PublicKey, successful, failed int) (interactions []hostdb.Interaction) {
		for i := 0; i < successful+failed; i++ {
			interactions = append(interactions, hostdb.Interaction{
				Host:      hk,
				Result:    []byte{1, 2, 3},
				Success:   i < successful,
				Timestamp: time.Now(),
				Type:      "test",
			})
		}
		return
	}

	// Add one successful and two failed interactions.
	if err := hdb.RecordInteractions(createInteractions(hk, 1, 2)); err != nil {
		t.Fatal(err)
	}
	host, err = hdb.Host(hk)
	if err != nil {
		t.Fatal(err)
	}
	if host.Interactions != (hostdb.Interactions{
		SuccessfulInteractions: 1,
		FailedInteractions:     2,
	}) {
		t.Fatal("mismatch")
	}

	// Add some more
	if err := hdb.RecordInteractions(createInteractions(hk, 3, 10)); err != nil {
		t.Fatal(err)
	}
	host, err = hdb.Host(hk)
	if err != nil {
		t.Fatal(err)
	}
	if host.Interactions != (hostdb.Interactions{
		SuccessfulInteractions: 4,
		FailedInteractions:     12,
	}) {
		t.Fatal("mismatch")
	}

	// Check that interactions were created.
	var interactions []dbInteraction
	if err := hdb.db.Find(&interactions).Error; err != nil {
		t.Fatal(err)
	}
	if len(interactions) != 16 {
		t.Fatal("wrong number of interactions")
	}
	for _, interaction := range interactions {
		if !bytes.Equal(interaction.Result, []byte{1, 2, 3}) {
			t.Fatal("result mismatch")
		}
		if interaction.Timestamp.IsZero() {
			t.Fatal("timestamp not set")
		}
		if interaction.Type != "test" {
			t.Fatal("type not set")
		}
		if types.PublicKey(interaction.Host) != hk {
			t.Fatal("wrong host")
		}
	}
}

func (s *SQLStore) addTestScan(hk types.PublicKey, t time.Time, err error, settings rhp.HostSettings) error {
	var sr hostdb.ScanResult
	if err == nil {
		sr.Settings = settings
	} else {
		sr.Error = err.Error()
	}
	result, _ := json.Marshal(sr)
	return s.RecordInteractions([]hostdb.Interaction{
		{
			Host:      hk,
			Result:    result,
			Success:   err == nil,
			Timestamp: t,
			Type:      hostdb.InteractionTypeScan,
		},
	})
}

// TestSQLHosts tests the Hosts method of the SQLHostDB type.
func TestSQLHosts(t *testing.T) {
	db, _, _, err := newTestSQLStore()
	if err != nil {
		t.Fatal(err)
	}

	// add 3 hosts
	hks, err := db.addTestHosts(3)
	if err != nil {
		t.Fatal(err)
	}
	hk1, hk2, hk3 := hks[0], hks[1], hks[2]

	// assert the hosts method returns the expected hosts
	if hosts, err := db.Hosts(0, -1); err != nil || len(hosts) != 3 {
		t.Fatal("unexpected", len(hosts), err)
	}
	if hosts, err := db.Hosts(0, 1); err != nil || len(hosts) != 1 {
		t.Fatal("unexpected", len(hosts), err)
	} else if host := hosts[0]; host.PublicKey != hk1 {
		t.Fatal("unexpected host", hk1, hk2, hk3, host.PublicKey)
	}
	if hosts, err := db.Hosts(1, 1); err != nil || len(hosts) != 1 {
		t.Fatal("unexpected", len(hosts), err)
	} else if host := hosts[0]; host.PublicKey != hk2 {
		t.Fatal("unexpected host", hk1, hk2, hk3, host.PublicKey)
	}
	if hosts, err := db.Hosts(3, 1); err != nil || len(hosts) != 0 {
		t.Fatal("unexpected", len(hosts), err)
	}
	if _, err := db.Hosts(-1, -1); err != ErrNegativeOffset {
		t.Fatal("unexpected error", err)
	}

	// Add a scan for each host.
	n := time.Now()
	if err := db.addTestScan(hk1, n.Add(-time.Minute), nil, rhp.HostSettings{}); err != nil {
		t.Fatal(err)
	}
	if err := db.addTestScan(hk2, n.Add(-2*time.Minute), nil, rhp.HostSettings{}); err != nil {
		t.Fatal(err)
	}
	if err := db.addTestScan(hk3, n.Add(-3*time.Minute), nil, rhp.HostSettings{}); err != nil {
		t.Fatal(err)
	}

	// Fetch all hosts using the HostsForScanning method.
	hostAddresses, err := db.HostsForScanning(n, 0, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(hostAddresses) != 3 {
		t.Fatal("wrong number of addresses")
	}
	if hostAddresses[0].PublicKey != hk3 {
		t.Fatal("wrong key")
	}
	if hostAddresses[1].PublicKey != hk2 {
		t.Fatal("wrong key")
	}
	if hostAddresses[2].PublicKey != hk1 {
		t.Fatal("wrong key")
	}

	// Fetch one host by setting the cutoff exactly to hk2.
	hostAddresses, err = db.HostsForScanning(n.Add(-2*time.Minute), 0, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(hostAddresses) != 1 {
		t.Fatal("wrong number of addresses")
	}

	// Fetch no hosts.
	hostAddresses, err = db.HostsForScanning(time.Time{}, 0, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(hostAddresses) != 0 {
		t.Fatal("wrong number of addresses")
	}
}

// TestRecordScan is a test for recording scans.
func TestRecordScan(t *testing.T) {
	hdb, _, _, err := newTestSQLStore()
	if err != nil {
		t.Fatal(err)
	}
	defer hdb.Close()

	// Add a host.
	hk := types.GeneratePrivateKey().PublicKey()
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

	scanInteraction := func(hk types.PublicKey, scanTime time.Time, settings rhp.HostSettings, success bool) []hostdb.Interaction {
		var err string
		if !success {
			err = "failure"
		}
		result, _ := json.Marshal(struct {
			Settings rhp.HostSettings `json:"settings"`
			Error    string           `json:"error"`
		}{
			Settings: settings,
			Error:    err,
		})
		return []hostdb.Interaction{
			{
				Host:      hk,
				Result:    result,
				Success:   success,
				Timestamp: scanTime,
				Type:      hostdb.InteractionTypeScan,
			}}
	}

	// Record a scan.
	firstScanTime := time.Now().UTC()
	settings := rhp.HostSettings{NetAddress: "host.com"}
	if err := hdb.RecordInteractions(scanInteraction(hk, firstScanTime, settings, true)); err != nil {
		t.Fatal(err)
	}
	host, err = hdb.Host(hk)
	if err != nil {
		t.Fatal(err)
	}

	// We expect no uptime or downtime from only a single scan.
	uptime := time.Duration(0)
	downtime := time.Duration(0)
	if host.Interactions.LastScan.UnixNano() != firstScanTime.UnixNano() {
		t.Fatal("wrong time")
	}
	host.Interactions.LastScan = time.Time{}
	if host.Interactions != (hostdb.Interactions{
		TotalScans:              1,
		LastScan:                time.Time{},
		LastScanSuccess:         true,
		SecondToLastScanSuccess: false,
		Uptime:                  uptime,
		Downtime:                downtime,
		SuccessfulInteractions:  1,
		FailedInteractions:      0,
	}) {
		t.Fatal("mismatch")
	}
	if !reflect.DeepEqual(*host.Settings, settings) {
		t.Fatal("mismatch")
	}

	// Record another scan 1 hour after the previous one.
	secondScanTime := firstScanTime.Add(time.Hour)
	if err := hdb.RecordInteractions(scanInteraction(hk, secondScanTime, settings, true)); err != nil {
		t.Fatal(err)
	}
	host, err = hdb.Host(hk)
	if err != nil {
		t.Fatal(err)
	}
	if host.Interactions.LastScan.UnixNano() != secondScanTime.UnixNano() {
		t.Fatal("wrong time")
	}
	host.Interactions.LastScan = time.Time{}
	uptime += secondScanTime.Sub(firstScanTime)
	if host.Interactions != (hostdb.Interactions{
		TotalScans:              2,
		LastScan:                time.Time{},
		LastScanSuccess:         true,
		SecondToLastScanSuccess: true,
		Uptime:                  uptime,
		Downtime:                downtime,
		SuccessfulInteractions:  2,
		FailedInteractions:      0,
	}) {
		t.Fatal("mismatch")
	}

	// Record another scan 2 hours after the second one. This time it fails.
	thirdScanTime := secondScanTime.Add(2 * time.Hour)
	if err := hdb.RecordInteractions(scanInteraction(hk, thirdScanTime, settings, false)); err != nil {
		t.Fatal(err)
	}
	host, err = hdb.Host(hk)
	if err != nil {
		t.Fatal(err)
	}
	if host.Interactions.LastScan.UnixNano() != thirdScanTime.UnixNano() {
		t.Fatal("wrong time")
	}
	host.Interactions.LastScan = time.Time{}
	downtime += thirdScanTime.Sub(secondScanTime)
	if host.Interactions != (hostdb.Interactions{
		TotalScans:              3,
		LastScan:                time.Time{},
		LastScanSuccess:         false,
		SecondToLastScanSuccess: true,
		Uptime:                  uptime,
		Downtime:                downtime,
		SuccessfulInteractions:  2,
		FailedInteractions:      1,
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
		hostKey: publicKey(types.GeneratePrivateKey().PublicKey()),
		announcement: hostdb.Announcement{
			Index:      types.ChainIndex{Height: 1, ID: types.BlockID{1}},
			Timestamp:  time.Now(),
			NetAddress: "foo.bar:1000",
		},
	}
	ann2 := announcement{
		hostKey:      publicKey(types.GeneratePrivateKey().PublicKey()),
		announcement: hostdb.Announcement{},
	}
	ann3 := announcement{
		hostKey:      publicKey(types.GeneratePrivateKey().PublicKey()),
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
		BlockID:     types.BlockID{1}.String(),
		NetAddress:  "foo.bar:1000",
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

	// Add an entry to the blocklist to block host 1
	entry1 := "foo.bar"
	err = hdb.AddHostBlocklistEntry(entry1)
	if err != nil {
		t.Fatal(err)
	}

	// Insert multiple announcements for host 1 - this asserts that the UNIQUE
	// constraint on the blocklist table isn't triggered when inserting multiple
	// announcements for a host that's on the blocklist
	if err := insertAnnouncements(hdb.db, []announcement{ann1, ann1}); err != nil {
		t.Fatal(err)
	}
}

func TestSQLHostBlocklist(t *testing.T) {
	hdb, _, _, err := newTestSQLStore()
	if err != nil {
		t.Fatal(err)
	}

	numEntries := func() int {
		t.Helper()
		bl, err := hdb.HostBlocklist()
		if err != nil {
			t.Fatal(err)
		}
		return len(bl)
	}

	numHosts := func() int {
		t.Helper()
		hosts, err := hdb.Hosts(0, -1)
		if err != nil {
			t.Fatal(err)
		}
		return len(hosts)
	}

	numRelations := func() (cnt int64) {
		t.Helper()
		err = hdb.db.Table("host_blocklist_entry_hosts").Count(&cnt).Error
		if err != nil {
			t.Fatal(err)
		}
		return
	}

	isBlocked := func(hk types.PublicKey) bool {
		t.Helper()
		hosts, err := hdb.Hosts(0, -1)
		if err != nil {
			t.Fatal(err)
		}
		for _, host := range hosts {
			if host.PublicKey == hk {
				return false
			}
		}
		return true
	}

	// add three hosts
	hk1 := types.GeneratePrivateKey().PublicKey()
	if err := hdb.addCustomTestHost(hk1, "foo.bar.com:1000"); err != nil {
		t.Fatal(err)
	}
	hk2 := types.GeneratePrivateKey().PublicKey()
	if err := hdb.addCustomTestHost(hk2, "bar.baz.com:2000"); err != nil {
		t.Fatal(err)
	}
	hk3 := types.GeneratePrivateKey().PublicKey()
	if err := hdb.addCustomTestHost(hk3, "foobar.com:3000"); err != nil {
		t.Fatal(err)
	}

	// assert we can find them
	if numHosts() != 3 {
		t.Fatalf("unexpected number of hosts, %v != 3", numHosts())
	}

	// assert blocklist is empty
	if numEntries() != 0 {
		t.Fatalf("unexpected number of entries in blocklist, %v != 0", numEntries())
	}

	// assert we can add entries to the blocklist
	entry1 := "foo.bar.com"
	err = hdb.AddHostBlocklistEntry(entry1)
	if err != nil {
		t.Fatal(err)
	}
	entry2 := "bar.com"
	err = hdb.AddHostBlocklistEntry(entry2)
	if err != nil {
		t.Fatal(err)
	}
	if numEntries() != 2 {
		t.Fatalf("unexpected number of entries in blocklist, %v != 2", numEntries())
	}
	if numRelations() != 2 {
		t.Fatalf("unexpected number of entries in join table, %v != 2", numRelations())
	}

	// assert only host 1 is blocked now
	if !isBlocked(hk1) || isBlocked(hk2) || isBlocked(hk3) {
		t.Fatal("unexpected host is blocked", isBlocked(hk1), isBlocked(hk2), isBlocked(hk3))
	}
	if _, err = hdb.Host(hk1); err != ErrHostNotFound {
		t.Fatal("unexpected err", err)
	}

	// assert adding the same entry is a no-op
	err = hdb.AddHostBlocklistEntry(entry1)
	if err != nil {
		t.Fatal(err)
	}
	if numEntries() != 2 {
		t.Fatalf("unexpected number of entries in blocklist, %v != 2", numEntries())
	}

	// assert we can remove an entry
	err = hdb.RemoveHostBlocklistEntry(entry1)
	if err != nil {
		t.Fatal(err)
	}
	if numEntries() != 1 {
		t.Fatalf("unexpected number of entries in blocklist, %v != 1", numEntries())
	}
	if numRelations() != 1 {
		t.Fatalf("unexpected number of entries in join table, %v != 1", numRelations())
	}

	// assert the host is still blocked
	if !isBlocked(hk1) || isBlocked(hk2) {
		t.Fatal("unexpected host is blocked", isBlocked(hk1), isBlocked(hk2))
	}

	// assert removing a non-existing entry is a no-op
	err = hdb.RemoveHostBlocklistEntry(entry1)
	if err != nil {
		t.Fatal(err)
	}
	// remove the other entry and assert the delete cascaded properly
	err = hdb.RemoveHostBlocklistEntry(entry2)
	if err != nil {
		t.Fatal(err)
	}
	if numEntries() != 0 {
		t.Fatalf("unexpected number of entries in blocklist, %v != 0", numEntries())
	}
	if isBlocked(hk1) || isBlocked(hk2) {
		t.Fatal("unexpected host is blocked", isBlocked(hk1), isBlocked(hk2))
	}
	if numRelations() != 0 {
		t.Fatalf("unexpected number of entries in join table, %v != 0", numRelations())
	}

	// block the second host
	entry3 := "baz.com"
	err = hdb.AddHostBlocklistEntry(entry3)
	if err != nil {
		t.Fatal(err)
	}
	if isBlocked(hk1) || !isBlocked(hk2) {
		t.Fatal("unexpected host is blocked", isBlocked(hk1), isBlocked(hk2))
	}
	if numRelations() != 1 {
		t.Fatalf("unexpected number of entries in join table, %v != 1", numRelations())
	}

	// delete the host and assert the delete cascaded properly
	if err = hdb.db.Model(&dbHost{}).Where(&dbHost{PublicKey: publicKey(hk2)}).Delete(&dbHost{}).Error; err != nil {
		t.Fatal(err)
	}
	if numHosts() != 2 {
		t.Fatalf("unexpected number of hosts, %v != 2", numHosts())
	}
	if numRelations() != 0 {
		t.Fatalf("unexpected number of entries in join table, %v != 0", numRelations())
	}
	if numEntries() != 1 {
		t.Fatalf("unexpected number of entries in blocklist, %v != 1", numEntries())
	}

	// add two hosts, one that should be blocked by 'baz.com' and one that should not
	hk4 := types.GeneratePrivateKey().PublicKey()
	if err := hdb.addCustomTestHost(hk4, "foo.baz.com:3000"); err != nil {
		t.Fatal(err)
	}
	hk5 := types.GeneratePrivateKey().PublicKey()
	if err := hdb.addCustomTestHost(hk5, "foo.baz.commmmm:3000"); err != nil {
		t.Fatal(err)
	}

	if _, err = hdb.Host(hk4); err != ErrHostNotFound {
		t.Fatal("expected host to be blocked")
	}
	if _, err = hdb.Host(hk5); err != nil {
		t.Fatal("expected host to be found")
	}

	// now update host 4's address so it's no longer blocked
	if err := hdb.addCustomTestHost(hk4, "foo.baz.commmmm:3000"); err != nil {
		t.Fatal(err)
	}
	if _, err = hdb.Host(hk4); err != nil {
		t.Fatal("expected host to be found")
	}
	if numRelations() != 0 {
		t.Fatalf("unexpected number of entries in join table, %v != 0", numRelations())
	}

	// add another entry that blocks multiple hosts
	entry4 := "com"
	err = hdb.AddHostBlocklistEntry(entry4)
	if err != nil {
		t.Fatal(err)
	}

	// assert 3 out of 5 hosts are blocked
	if !isBlocked(hk1) || !isBlocked(hk2) || !isBlocked(hk3) || isBlocked(hk4) || isBlocked(hk5) {
		t.Fatal("unexpected host is blocked", isBlocked(hk1), isBlocked(hk2), isBlocked(hk3), isBlocked(hk4), isBlocked(hk5))
	}
}

// addTestHosts adds 'n' hosts to the db and returns their keys.
func (s *SQLStore) addTestHosts(n int) (keys []types.PublicKey, err error) {
	cnt, err := s.contractsCount()
	if err != nil {
		return nil, err
	}

	for i := 0; i < n; i++ {
		keys = append(keys, types.PublicKey{byte(int(cnt) + i + 1)})
		if err := s.addTestHost(keys[len(keys)-1]); err != nil {
			return nil, err
		}
	}
	return
}

// addTestHost ensures a host with given hostkey exists.
func (s *SQLStore) addTestHost(hk types.PublicKey) error {
	return s.addCustomTestHost(hk, "")
}

// addCustomTestHost ensures a host with given hostkey and net address exists.
func (s *SQLStore) addCustomTestHost(hk types.PublicKey, na string) error {
	return insertAnnouncements(s.db, []announcement{{
		hostKey:      publicKey(hk),
		announcement: hostdb.Announcement{NetAddress: na},
	}})
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

func (s *SQLStore) hostsCount() (cnt int64, err error) {
	err = s.db.
		Model(&dbHost{}).
		Count(&cnt).
		Error
	return
}
