package stores

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/hostdb"
	stypes "go.sia.tech/siad/types"
	"gorm.io/gorm"
)

func (s *SQLStore) insertTestAnnouncement(a announcement) error {
	return insertAnnouncements(s.db, []announcement{a})
}

// TestSQLHostDB tests the basic functionality of SQLHostDB using an in-memory
// SQLite DB.
func TestSQLHostDB(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)

	// Try to fetch a random host. Should fail.
	ctx := context.Background()
	hk := types.GeneratePrivateKey().PublicKey()
	_, err := ss.Host(ctx, hk)
	if !errors.Is(err, api.ErrHostNotFound) {
		t.Fatal(err)
	}

	// Add the host
	err = ss.addTestHost(hk)
	if err != nil {
		t.Fatal(err)
	}

	// Assert it's returned
	allHosts, err := ss.Hosts(ctx, 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	if len(allHosts) != 1 || allHosts[0].PublicKey != hk {
		t.Fatal("unexpected result", len(allHosts))
	}

	// Insert an announcement for the host and another one for an unknown
	// host.
	a := announcement{
		blockHeight: 42,
		blockID:     types.BlockID{1, 2, 3},
		hk:          hk,
		timestamp:   time.Now().UTC().Round(time.Second),
		HostAnnouncement: chain.HostAnnouncement{
			NetAddress: "address",
		},
	}
	err = ss.insertTestAnnouncement(a)
	if err != nil {
		t.Fatal(err)
	}

	// Read the host and verify that the announcement related fields were
	// set.
	var h dbHost
	tx := ss.db.Where("last_announcement = ? AND net_address = ?", a.timestamp, a.NetAddress).Find(&h)
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	if types.PublicKey(h.PublicKey) != hk {
		t.Fatal("wrong host returned")
	}

	// Same thing again but with hosts.
	hosts, err := ss.hosts()
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
	h2, err := ss.Host(ctx, hk)
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
	unknownKeyAnn := a
	unknownKeyAnn.hk = types.PublicKey{1, 4, 7}
	err = ss.insertTestAnnouncement(unknownKeyAnn)
	if err != nil {
		t.Fatal(err)
	}
	h3, err := ss.Host(ctx, unknownKeyAnn.hk)
	if err != nil {
		t.Fatal(err)
	}
	if h3.NetAddress != a.NetAddress {
		t.Fatal("wrong net address")
	}
	if h3.KnownSince.IsZero() {
		t.Fatal("known since not set")
	}
}

func (s *SQLStore) addTestScan(hk types.PublicKey, t time.Time, err error, settings rhpv2.HostSettings) error {
	return s.RecordHostScans(context.Background(), []hostdb.HostScan{
		{
			HostKey:   hk,
			Settings:  settings,
			Success:   err == nil,
			Timestamp: t,
		},
	})
}

// TestSQLHosts tests the Hosts method of the SQLHostDB type.
func TestSQLHosts(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()
	ctx := context.Background()

	hks, err := ss.addTestHosts(3)
	if err != nil {
		t.Fatal(err)
	}
	hk1, hk2, hk3 := hks[0], hks[1], hks[2]

	// assert the hosts method returns the expected hosts
	if hosts, err := ss.Hosts(ctx, 0, -1); err != nil || len(hosts) != 3 {
		t.Fatal("unexpected", len(hosts), err)
	}
	if hosts, err := ss.Hosts(ctx, 0, 1); err != nil || len(hosts) != 1 {
		t.Fatal("unexpected", len(hosts), err)
	} else if host := hosts[0]; host.PublicKey != hk1 {
		t.Fatal("unexpected host", hk1, hk2, hk3, host.PublicKey)
	}
	if hosts, err := ss.Hosts(ctx, 1, 1); err != nil || len(hosts) != 1 {
		t.Fatal("unexpected", len(hosts), err)
	} else if host := hosts[0]; host.PublicKey != hk2 {
		t.Fatal("unexpected host", hk1, hk2, hk3, host.PublicKey)
	}
	if hosts, err := ss.Hosts(ctx, 3, 1); err != nil || len(hosts) != 0 {
		t.Fatal("unexpected", len(hosts), err)
	}
	if _, err := ss.Hosts(ctx, -1, -1); err != ErrNegativeOffset {
		t.Fatal("unexpected error", err)
	}

	// Add a scan for each host.
	n := time.Now()
	if err := ss.addTestScan(hk1, n.Add(-time.Minute), nil, rhpv2.HostSettings{}); err != nil {
		t.Fatal(err)
	}
	if err := ss.addTestScan(hk2, n.Add(-2*time.Minute), nil, rhpv2.HostSettings{}); err != nil {
		t.Fatal(err)
	}
	if err := ss.addTestScan(hk3, n.Add(-3*time.Minute), nil, rhpv2.HostSettings{}); err != nil {
		t.Fatal(err)
	}

	// Fetch all hosts using the HostsForScanning method.
	hostAddresses, err := ss.HostsForScanning(ctx, n, 0, 3)
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
	hostAddresses, err = ss.HostsForScanning(ctx, n.Add(-2*time.Minute), 0, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(hostAddresses) != 1 {
		t.Fatal("wrong number of addresses")
	}

	// Fetch no hosts.
	hostAddresses, err = ss.HostsForScanning(ctx, time.Time{}, 0, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(hostAddresses) != 0 {
		t.Fatal("wrong number of addresses")
	}
}

// TestSearchHosts is a unit test for SearchHosts.
func TestSearchHosts(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()
	ctx := context.Background()

	// add 3 hosts
	var hks []types.PublicKey
	for i := 0; i < 3; i++ {
		if err := ss.addCustomTestHost(types.PublicKey{byte(i)}, fmt.Sprintf("-%v-", i+1)); err != nil {
			t.Fatal(err)
		}
		hks = append(hks, types.PublicKey{byte(i)})
	}
	hk1, hk2, hk3 := hks[0], hks[1], hks[2]

	// Search by address.
	if hosts, err := ss.SearchHosts(ctx, api.HostFilterModeAll, "1", nil, 0, -1); err != nil || len(hosts) != 1 {
		t.Fatal("unexpected", len(hosts), err)
	}
	// Filter by key.
	if hosts, err := ss.SearchHosts(ctx, api.HostFilterModeAll, "", []types.PublicKey{hk1, hk2}, 0, -1); err != nil || len(hosts) != 2 {
		t.Fatal("unexpected", len(hosts), err)
	}
	// Filter by address and key.
	if hosts, err := ss.SearchHosts(ctx, api.HostFilterModeAll, "1", []types.PublicKey{hk1, hk2}, 0, -1); err != nil || len(hosts) != 1 {
		t.Fatal("unexpected", len(hosts), err)
	}
	// Filter by key and limit results
	if hosts, err := ss.SearchHosts(ctx, api.HostFilterModeAll, "3", []types.PublicKey{hk3}, 0, -1); err != nil || len(hosts) != 1 {
		t.Fatal("unexpected", len(hosts), err)
	}
}

// TestRecordScan is a test for recording scans.
func TestRecordScan(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	// Add a host.
	hk := types.GeneratePrivateKey().PublicKey()
	err := ss.addCustomTestHost(hk, "host.com")
	if err != nil {
		t.Fatal(err)
	}

	// The host shouldn't have any interaction related fields set.
	ctx := context.Background()
	host, err := ss.Host(ctx, hk)
	if err != nil {
		t.Fatal(err)
	}
	if host.Interactions != (hostdb.Interactions{}) {
		t.Fatal("mismatch")
	}
	if host.Settings != (rhpv2.HostSettings{}) {
		t.Fatal("mismatch")
	}

	// Fetch the host directly to get the creation time.
	h, err := hostByPubKey(ss.db, hk)
	if err != nil {
		t.Fatal(err)
	}
	if h.CreatedAt.IsZero() {
		t.Fatal("creation time not set")
	}

	// Record a scan.
	firstScanTime := time.Now().UTC()
	settings := rhpv2.HostSettings{NetAddress: "host.com"}
	if err := ss.RecordHostScans(ctx, []hostdb.HostScan{newTestScan(hk, firstScanTime, settings, true)}); err != nil {
		t.Fatal(err)
	}
	host, err = ss.Host(ctx, hk)
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
	if expected := (hostdb.Interactions{
		TotalScans:              1,
		LastScan:                time.Time{},
		LastScanSuccess:         true,
		SecondToLastScanSuccess: false,
		Uptime:                  uptime,
		Downtime:                downtime,
		SuccessfulInteractions:  1,
		FailedInteractions:      0,
	}); host.Interactions != expected {
		t.Fatal("mismatch", cmp.Diff(host.Interactions, expected))
	}
	if !reflect.DeepEqual(host.Settings, settings) {
		t.Fatal("mismatch")
	}

	// Record another scan 1 hour after the previous one.
	secondScanTime := firstScanTime.Add(time.Hour)
	if err := ss.RecordHostScans(ctx, []hostdb.HostScan{newTestScan(hk, secondScanTime, settings, true)}); err != nil {
		t.Fatal(err)
	}
	host, err = ss.Host(ctx, hk)
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
	if err := ss.RecordHostScans(ctx, []hostdb.HostScan{newTestScan(hk, thirdScanTime, settings, false)}); err != nil {
		t.Fatal(err)
	}
	host, err = ss.Host(ctx, hk)
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

func TestRemoveHosts(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	// add a host
	hk := types.GeneratePrivateKey().PublicKey()
	err := ss.addTestHost(hk)
	if err != nil {
		t.Fatal(err)
	}

	// fetch the host and assert the recent downtime is zero
	h, err := hostByPubKey(ss.db, hk)
	if err != nil {
		t.Fatal(err)
	}
	if h.RecentDowntime != 0 {
		t.Fatal("downtime is not zero")
	}

	// assert no hosts are removed
	removed, err := ss.RemoveOfflineHosts(context.Background(), 0, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 0 {
		t.Fatal("expected no hosts to be removed")
	}

	now := time.Now().UTC()
	t1 := now.Add(-time.Minute * 120) // 2 hours ago
	t2 := now.Add(-time.Minute * 90)  // 1.5 hours ago (30min downtime)
	hi1 := newTestScan(hk, t1, rhpv2.HostSettings{NetAddress: "host.com"}, false)
	hi2 := newTestScan(hk, t2, rhpv2.HostSettings{NetAddress: "host.com"}, false)

	// record interactions
	if err := ss.RecordHostScans(context.Background(), []hostdb.HostScan{hi1, hi2}); err != nil {
		t.Fatal(err)
	}

	// fetch the host and assert the recent downtime is 30 minutes and he has 2 recent scan failures
	h, err = hostByPubKey(ss.db, hk)
	if err != nil {
		t.Fatal(err)
	}
	if h.RecentDowntime.Minutes() != 30 {
		t.Fatal("downtime is not 30 minutes", h.RecentDowntime.Minutes())
	}
	if h.RecentScanFailures != 2 {
		t.Fatal("recent scan failures is not 2", h.RecentScanFailures)
	}

	// assert no hosts are removed
	removed, err = ss.RemoveOfflineHosts(context.Background(), 0, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 0 {
		t.Fatal("expected no hosts to be removed")
	}

	// record interactions
	t3 := now.Add(-time.Minute * 60) // 1 hour ago (60min downtime)
	hi3 := newTestScan(hk, t3, rhpv2.HostSettings{NetAddress: "host.com"}, false)
	if err := ss.RecordHostScans(context.Background(), []hostdb.HostScan{hi3}); err != nil {
		t.Fatal(err)
	}

	// assert no hosts are removed at 61 minutes
	removed, err = ss.RemoveOfflineHosts(context.Background(), 0, time.Minute*61)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 0 {
		t.Fatal("expected no hosts to be removed")
	}

	// assert no hosts are removed at 60 minutes if we require at least 4 failed scans
	removed, err = ss.RemoveOfflineHosts(context.Background(), 4, time.Minute*60)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 0 {
		t.Fatal("expected no hosts to be removed")
	}

	// assert hosts gets removed at 60 minutes if we require at least 3 failed scans
	removed, err = ss.RemoveOfflineHosts(context.Background(), 3, time.Minute*60)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 1 {
		t.Fatal("expected 1 host to be removed")
	}

	// assert host is removed from the database
	if _, err = hostByPubKey(ss.db, hk); err != gorm.ErrRecordNotFound {
		t.Fatal("expected record not found error")
	}
}

// TestInsertAnnouncements is a test for insertAnnouncements.
func TestInsertAnnouncements(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	// Create announcements for 2 hosts.
	ann1 := announcement{
		timestamp:   time.Now(),
		blockHeight: 1,
		blockID:     types.BlockID{1},
		hk:          types.GeneratePrivateKey().PublicKey(),
		HostAnnouncement: chain.HostAnnouncement{
			NetAddress: "foo.bar:1000",
		},
	}
	ann2 := announcement{
		hk:               types.GeneratePrivateKey().PublicKey(),
		HostAnnouncement: chain.HostAnnouncement{},
	}
	ann3 := announcement{
		hk:               types.GeneratePrivateKey().PublicKey(),
		HostAnnouncement: chain.HostAnnouncement{},
	}

	// Insert the first one and check that all fields are set.
	if err := insertAnnouncements(ss.db, []announcement{ann1}); err != nil {
		t.Fatal(err)
	}
	var ann dbAnnouncement
	if err := ss.db.Find(&ann).Error; err != nil {
		t.Fatal(err)
	}
	ann.Model = Model{} // ignore
	expectedAnn := dbAnnouncement{
		HostKey:     publicKey(ann1.hk),
		BlockHeight: 1,
		BlockID:     types.BlockID{1}.String(),
		NetAddress:  "foo.bar:1000",
	}
	if ann != expectedAnn {
		t.Fatal("mismatch")
	}
	// Insert the first and second one.
	if err := insertAnnouncements(ss.db, []announcement{ann1, ann2}); err != nil {
		t.Fatal(err)
	}

	// Insert the first one twice. The second one again and the third one.
	if err := insertAnnouncements(ss.db, []announcement{ann1, ann2, ann1, ann3}); err != nil {
		t.Fatal(err)
	}

	// There should be 3 hosts in the db.
	hosts, err := ss.hosts()
	if err != nil {
		t.Fatal(err)
	}
	if len(hosts) != 3 {
		t.Fatal("invalid number of hosts")
	}

	// There should be 7 announcements total.
	var announcements []dbAnnouncement
	if err := ss.db.Find(&announcements).Error; err != nil {
		t.Fatal(err)
	}
	if len(announcements) != 7 {
		t.Fatal("invalid number of announcements")
	}

	// Add an entry to the blocklist to block host 1
	entry1 := "foo.bar"
	err = ss.UpdateHostBlocklistEntries(context.Background(), []string{entry1}, nil, false)
	if err != nil {
		t.Fatal(err)
	}

	// Insert multiple announcements for host 1 - this asserts that the UNIQUE
	// constraint on the blocklist table isn't triggered when inserting multiple
	// announcements for a host that's on the blocklist
	if err := insertAnnouncements(ss.db, []announcement{ann1, ann1}); err != nil {
		t.Fatal(err)
	}
}

func TestSQLHostAllowlist(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	ctx := context.Background()

	numEntries := func() int {
		t.Helper()
		bl, err := ss.HostAllowlist(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return len(bl)
	}

	numHosts := func() int {
		t.Helper()
		hosts, err := ss.Hosts(ctx, 0, -1)
		if err != nil {
			t.Fatal(err)
		}
		return len(hosts)
	}

	numRelations := func() (cnt int64) {
		t.Helper()
		err := ss.db.Table("host_allowlist_entry_hosts").Count(&cnt).Error
		if err != nil {
			t.Fatal(err)
		}
		return
	}

	isAllowed := func(hk types.PublicKey) bool {
		t.Helper()
		host, err := ss.Host(ctx, hk)
		if err != nil {
			t.Fatal(err)
		}
		return !host.Blocked
	}

	// add three hosts
	hks, err := ss.addTestHosts(3)
	if err != nil {
		t.Fatal(err)
	}
	hk1, hk2, hk3 := hks[0], hks[1], hks[2]

	// assert we can find them
	if numHosts() != 3 {
		t.Fatalf("unexpected number of hosts, %v != 3", numHosts())
	}

	// assert allowlist is empty
	if numEntries() != 0 {
		t.Fatalf("unexpected number of entries in allowlist, %v != 0", numEntries())
	}

	// assert we can add entries to the allowlist
	err = ss.UpdateHostAllowlistEntries(ctx, []types.PublicKey{hk1, hk2}, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if numEntries() != 2 {
		t.Fatalf("unexpected number of entries in allowlist, %v != 2", numEntries())
	}
	if numRelations() != 2 {
		t.Fatalf("unexpected number of entries in join table, %v != 2", numRelations())
	}

	// assert both h1 and h2 are allowed now
	if !isAllowed(hk1) || !isAllowed(hk2) || isAllowed(hk3) {
		t.Fatal("unexpected hosts are allowed", isAllowed(hk1), isAllowed(hk2), isAllowed(hk3))
	}

	// assert adding the same entry is a no-op and we can remove an entry at the same time
	err = ss.UpdateHostAllowlistEntries(ctx, []types.PublicKey{hk1}, []types.PublicKey{hk2}, false)
	if err != nil {
		t.Fatal(err)
	}
	if numEntries() != 1 {
		t.Fatalf("unexpected number of entries in allowlist, %v != 1", numEntries())
	}
	if numRelations() != 1 {
		t.Fatalf("unexpected number of entries in join table, %v != 1", numRelations())
	}

	// assert only h1 is allowed now
	if !isAllowed(hk1) || isAllowed(hk2) || isAllowed(hk3) {
		t.Fatal("unexpected hosts are allowed", isAllowed(hk1), isAllowed(hk2), isAllowed(hk3))
	}

	// assert removing a non-existing entry is a no-op
	err = ss.UpdateHostAllowlistEntries(ctx, nil, []types.PublicKey{hk2}, false)
	if err != nil {
		t.Fatal(err)
	}

	assertSearch := func(total, allowed, blocked int) error {
		t.Helper()
		hosts, err := ss.SearchHosts(context.Background(), api.HostFilterModeAll, "", nil, 0, -1)
		if err != nil {
			return err
		}
		if len(hosts) != total {
			return fmt.Errorf("invalid number of hosts: %v", len(hosts))
		}
		hosts, err = ss.SearchHosts(context.Background(), api.HostFilterModeAllowed, "", nil, 0, -1)
		if err != nil {
			return err
		}
		if len(hosts) != allowed {
			return fmt.Errorf("invalid number of hosts: %v", len(hosts))
		}
		hosts, err = ss.SearchHosts(context.Background(), api.HostFilterModeBlocked, "", nil, 0, -1)
		if err != nil {
			return err
		}
		if len(hosts) != blocked {
			return fmt.Errorf("invalid number of hosts: %v", len(hosts))
		}
		return nil
	}

	// Search for hosts using different modes. Should have 3 hosts in total, 2
	// allowed ones and 2 blocked ones.
	if err := assertSearch(3, 1, 2); err != nil {
		t.Fatal(err)
	}

	// remove host 1
	if err = ss.db.Model(&dbHost{}).Where(&dbHost{PublicKey: publicKey(hk1)}).Delete(&dbHost{}).Error; err != nil {
		t.Fatal(err)
	}
	if numHosts() != 0 {
		t.Fatalf("unexpected number of hosts, %v != 0", numHosts())
	}
	if numRelations() != 0 {
		t.Fatalf("unexpected number of entries in join table, %v != 0", numRelations())
	}
	if numEntries() != 1 {
		t.Fatalf("unexpected number of entries in blocklist, %v != 1", numEntries())
	}

	// Search for hosts using different modes. Should have 2 hosts in total, 0
	// allowed ones and 2 blocked ones.
	if err := assertSearch(2, 0, 2); err != nil {
		t.Fatal(err)
	}

	// remove the allowlist entry for h1
	err = ss.UpdateHostAllowlistEntries(ctx, nil, []types.PublicKey{hk1}, false)
	if err != nil {
		t.Fatal(err)
	}

	// Search for hosts using different modes. Should have 2 hosts in total, 2
	// allowed ones and 0 blocked ones.
	if err := assertSearch(2, 2, 0); err != nil {
		t.Fatal(err)
	}

	// assert hosts reappear
	if numHosts() != 2 {
		t.Fatalf("unexpected number of hosts, %v != 2", numHosts())
	}
	if numEntries() != 0 {
		t.Fatalf("unexpected number of entries in blocklist, %v != 0", numEntries())
	}
}

func TestSQLHostBlocklist(t *testing.T) {
	cfg := defaultTestSQLStoreConfig
	cfg.persistent = true
	cfg.dir = "/Users/peterjan/testing"
	os.RemoveAll(cfg.dir)
	ss := newTestSQLStore(t, cfg)
	defer ss.Close()

	ctx := context.Background()

	numEntries := func() int {
		t.Helper()
		bl, err := ss.HostBlocklist(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return len(bl)
	}

	numHosts := func() int {
		t.Helper()
		hosts, err := ss.Hosts(ctx, 0, -1)
		if err != nil {
			t.Fatal(err)
		}
		return len(hosts)
	}

	numAllowlistRelations := func() (cnt int64) {
		t.Helper()
		err := ss.db.Table("host_allowlist_entry_hosts").Count(&cnt).Error
		if err != nil {
			t.Fatal(err)
		}
		return
	}

	numBlocklistRelations := func() (cnt int64) {
		t.Helper()
		err := ss.db.Table("host_blocklist_entry_hosts").Count(&cnt).Error
		if err != nil {
			t.Fatal(err)
		}
		return
	}

	isBlocked := func(hk types.PublicKey) bool {
		t.Helper()
		host, _ := ss.Host(ctx, hk)
		return host.Blocked
	}

	// add three hosts
	hk1 := types.GeneratePrivateKey().PublicKey()
	if err := ss.addCustomTestHost(hk1, "foo.bar.com:1000"); err != nil {
		t.Fatal(err)
	}
	hk2 := types.GeneratePrivateKey().PublicKey()
	if err := ss.addCustomTestHost(hk2, "bar.baz.com:2000"); err != nil {
		t.Fatal(err)
	}
	hk3 := types.GeneratePrivateKey().PublicKey()
	if err := ss.addCustomTestHost(hk3, "foobar.com:3000"); err != nil {
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
	err := ss.UpdateHostBlocklistEntries(ctx, []string{"foo.bar.com", "bar.com"}, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if numEntries() != 2 {
		t.Fatalf("unexpected number of entries in blocklist, %v != 2", numEntries())
	}
	if numBlocklistRelations() != 2 {
		t.Fatalf("unexpected number of entries in join table, %v != 2", numBlocklistRelations())
	}

	// assert only host 1 is blocked now
	if !isBlocked(hk1) || isBlocked(hk2) || isBlocked(hk3) {
		t.Fatal("unexpected host is blocked", isBlocked(hk1), isBlocked(hk2), isBlocked(hk3))
	}
	if host, err := ss.Host(ctx, hk1); err != nil {
		t.Fatal("unexpected err", err)
	} else if !host.Blocked {
		t.Fatal("expected host to be blocked")
	}

	// assert adding the same entry is a no-op, and we can remove entries at the same time
	err = ss.UpdateHostBlocklistEntries(ctx, []string{"foo.bar.com", "bar.com"}, []string{"foo.bar.com"}, false)
	if err != nil {
		t.Fatal(err)
	}
	if numEntries() != 1 {
		t.Fatalf("unexpected number of entries in blocklist, %v != 1", numEntries())
	}
	if numBlocklistRelations() != 1 {
		t.Fatalf("unexpected number of entries in join table, %v != 1", numBlocklistRelations())
	}

	// assert the host is still blocked
	if !isBlocked(hk1) || isBlocked(hk2) {
		t.Fatal("unexpected host is blocked", isBlocked(hk1), isBlocked(hk2))
	}

	// assert removing a non-existing entry is a no-op
	err = ss.UpdateHostBlocklistEntries(ctx, nil, []string{"foo.bar.com"}, false)
	if err != nil {
		t.Fatal(err)
	}
	// remove the other entry and assert the delete cascaded properly
	err = ss.UpdateHostBlocklistEntries(ctx, nil, []string{"bar.com"}, false)
	if err != nil {
		t.Fatal(err)
	}
	if numEntries() != 0 {
		t.Fatalf("unexpected number of entries in blocklist, %v != 0", numEntries())
	}
	if isBlocked(hk1) || isBlocked(hk2) {
		t.Fatal("unexpected host is blocked", isBlocked(hk1), isBlocked(hk2))
	}
	if numBlocklistRelations() != 0 {
		t.Fatalf("unexpected number of entries in join table, %v != 0", numBlocklistRelations())
	}

	// block the second host
	err = ss.UpdateHostBlocklistEntries(ctx, []string{"baz.com"}, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if isBlocked(hk1) || !isBlocked(hk2) {
		t.Fatal("unexpected host is blocked", isBlocked(hk1), isBlocked(hk2))
	}
	if numBlocklistRelations() != 1 {
		t.Fatalf("unexpected number of entries in join table, %v != 1", numBlocklistRelations())
	}

	// delete host 2 and assert the delete cascaded properly
	if err = ss.db.Model(&dbHost{}).Where(&dbHost{PublicKey: publicKey(hk2)}).Delete(&dbHost{}).Error; err != nil {
		t.Fatal(err)
	}
	if numHosts() != 2 {
		t.Fatalf("unexpected number of hosts, %v != 2", numHosts())
	}
	if numBlocklistRelations() != 0 {
		t.Fatalf("unexpected number of entries in join table, %v != 0", numBlocklistRelations())
	}
	if numEntries() != 1 {
		t.Fatalf("unexpected number of entries in blocklist, %v != 1", numEntries())
	}

	// add two hosts, one that should be blocked by 'baz.com' and one that should not
	hk4 := types.GeneratePrivateKey().PublicKey()
	if err := ss.addCustomTestHost(hk4, "foo.baz.com:3000"); err != nil {
		t.Fatal(err)
	}
	hk5 := types.GeneratePrivateKey().PublicKey()
	if err := ss.addCustomTestHost(hk5, "foo.baz.commmmm:3000"); err != nil {
		t.Fatal(err)
	}

	if host, err := ss.Host(ctx, hk4); err != nil {
		t.Fatal(err)
	} else if !host.Blocked {
		t.Fatal("expected host to be blocked")
	}
	if _, err = ss.Host(ctx, hk5); err != nil {
		t.Fatal("expected host to be found")
	}

	// now update host 4's address so it's no longer blocked
	if err := ss.addCustomTestHost(hk4, "foo.baz.commmmm:3000"); err != nil {
		t.Fatal(err)
	}
	if _, err = ss.Host(ctx, hk4); err != nil {
		t.Fatal("expected host to be found")
	}
	if numBlocklistRelations() != 0 {
		t.Fatalf("unexpected number of entries in join table, %v != 0", numBlocklistRelations())
	}

	// add another entry that blocks multiple hosts
	err = ss.UpdateHostBlocklistEntries(ctx, []string{"com"}, nil, false)
	if err != nil {
		t.Fatal(err)
	}

	// assert 2 out of 5 hosts are blocked
	if !isBlocked(hk1) || !isBlocked(hk3) || isBlocked(hk4) || isBlocked(hk5) {
		t.Fatal("unexpected host is blocked", isBlocked(hk1), isBlocked(hk3), isBlocked(hk4), isBlocked(hk5))
	}

	// add host 5 to the allowlist
	err = ss.UpdateHostAllowlistEntries(ctx, []types.PublicKey{hk5}, nil, false)
	if err != nil {
		t.Fatal(err)
	}

	// assert all hosts except host 5 are blocked
	if !isBlocked(hk1) || !isBlocked(hk3) || !isBlocked(hk4) || isBlocked(hk5) {
		t.Fatal("unexpected host is blocked", isBlocked(hk1), isBlocked(hk3), isBlocked(hk4), isBlocked(hk5))
	}

	// add a rule to block host 5
	err = ss.UpdateHostBlocklistEntries(ctx, []string{"foo.baz.commmmm"}, nil, false)
	if err != nil {
		t.Fatal(err)
	}

	// assert all hosts are blocked
	if !isBlocked(hk1) || !isBlocked(hk3) || !isBlocked(hk4) || !isBlocked(hk5) {
		t.Fatal("unexpected host is blocked", isBlocked(hk1), isBlocked(hk3), isBlocked(hk4), isBlocked(hk5))
	}

	// clear the blocklist
	if numBlocklistRelations() == 0 {
		t.Fatalf("expected more than zero entries in join table, instead there were %v", numBlocklistRelations())
	}
	err = ss.UpdateHostBlocklistEntries(ctx, nil, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	if numBlocklistRelations() != 0 {
		t.Fatalf("expected zero entries in join table, instead there were %v", numBlocklistRelations())
	}

	// clear the allowlist
	if numAllowlistRelations() == 0 {
		t.Fatalf("expected more than zero entries in join table, instead there were %v", numBlocklistRelations())
	}
	err = ss.UpdateHostAllowlistEntries(ctx, nil, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	if numAllowlistRelations() != 0 {
		t.Fatalf("expected zero entries in join table, instead there were %v", numBlocklistRelations())
	}
}

func TestSQLHostBlocklistBasic(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	ctx := context.Background()

	// add a host
	hk1 := types.GeneratePrivateKey().PublicKey()
	if err := ss.addCustomTestHost(hk1, "foo.bar.com:1000"); err != nil {
		t.Fatal(err)
	}

	// block that host
	err := ss.UpdateHostBlocklistEntries(ctx, []string{"foo.bar.com"}, nil, false)
	if err != nil {
		t.Fatal(err)
	}

	// assert it's blocked
	host, _ := ss.Host(ctx, hk1)
	if !host.Blocked {
		t.Fatal("unexpected")
	}

	// reannounce to ensure it's no longer blocked
	if err := ss.addCustomTestHost(hk1, "bar.baz.com:1000"); err != nil {
		t.Fatal(err)
	}

	// assert it's no longer blocked
	host, _ = ss.Host(ctx, hk1)
	if host.Blocked {
		t.Fatal("unexpected")
	}
}

// TestAnnouncementMaxAge verifies old announcements are ignored.
func TestAnnouncementMaxAge(t *testing.T) {
	t.Skip("TODO: rewrite")
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
	// NOTE: insert through subscriber to ensure allowlist/blocklist get updated
	s.cs.announcements = append(s.cs.announcements, announcement{
		blockHeight:      s.cs.tip.Height,
		blockID:          s.cs.tip.ID,
		hk:               hk,
		timestamp:        time.Now(),
		HostAnnouncement: chain.HostAnnouncement{NetAddress: na},
	})
	return s.cs.commit()
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

func hostByPubKey(tx *gorm.DB, hostKey types.PublicKey) (dbHost, error) {
	var h dbHost
	err := tx.Where("public_key", publicKey(hostKey)).
		Take(&h).Error
	return h, err
}

// newTestScan returns a host interaction with given parameters.
func newTestScan(hk types.PublicKey, scanTime time.Time, settings rhpv2.HostSettings, success bool) hostdb.HostScan {
	return hostdb.HostScan{
		HostKey:   hk,
		Success:   success,
		Timestamp: scanTime,
		Settings:  settings,
	}
}

func newTestPK() (types.PublicKey, types.PrivateKey) {
	sk := types.GeneratePrivateKey()
	pk := sk.PublicKey()
	return pk, sk
}

func newTestHostAnnouncement(na string) (chain.HostAnnouncement, types.PrivateKey) {
	_, sk := newTestPK()
	a := chain.HostAnnouncement{
		NetAddress: na,
	}
	return a, sk
}

func newTestTransaction(ha chain.HostAnnouncement, sk types.PrivateKey) stypes.Transaction {
	return stypes.Transaction{ArbitraryData: [][]byte{ha.ToArbitraryData(sk)}}
}
