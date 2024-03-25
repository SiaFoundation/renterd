package stores

import "testing"

// TestInsertAnnouncements is a test for insertAnnouncements.
func TestInsertAnnouncements(t *testing.T) {
	t.Skip("TODO: fix test")
	// 	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	// 	defer ss.Close()

	// 	// Create announcements for 3 hosts.
	// 	ann1 := newTestAnnouncement(types.GeneratePrivateKey().PublicKey(), "foo.bar:1000")
	// 	ann2 := newTestAnnouncement(types.GeneratePrivateKey().PublicKey(), "")
	// 	ann3 := newTestAnnouncement(types.GeneratePrivateKey().PublicKey(), "")

	// 	// Insert the first one and check that all fields are set.
	// 	if err := insertAnnouncements(ss.db, []announcement{ann1}); err != nil {
	// 		t.Fatal(err)
	// 	}
	// 	var ann dbAnnouncement
	// 	if err := ss.db.Find(&ann).Error; err != nil {
	// 		t.Fatal(err)
	// 	}
	// 	ann.Model = Model{} // ignore
	// 	expectedAnn := dbAnnouncement{
	// 		HostKey:     publicKey(ann1.hk),
	// 		BlockHeight: ann1.blockHeight,
	// 		BlockID:     ann1.blockID.String(),
	// 		NetAddress:  "foo.bar:1000",
	// 	}
	// 	if ann != expectedAnn {
	// 		t.Fatal("mismatch", cmp.Diff(ann, expectedAnn))
	// 	}
	// 	// Insert the first and second one.
	// 	if err := insertAnnouncements(ss.db, []announcement{ann1, ann2}); err != nil {
	// 		t.Fatal(err)
	// 	}

	// 	// Insert the first one twice. The second one again and the third one.
	// 	if err := insertAnnouncements(ss.db, []announcement{ann1, ann2, ann1, ann3}); err != nil {
	// 		t.Fatal(err)
	// 	}

	// 	// There should be 3 hosts in the db.
	// 	hosts, err := ss.hosts()
	// 	if err != nil {
	// 		t.Fatal(err)
	// 	}
	// 	if len(hosts) != 3 {
	// 		t.Fatal("invalid number of hosts")
	// 	}

	// 	// There should be 7 announcements total.
	// 	var announcements []dbAnnouncement
	// 	if err := ss.db.Find(&announcements).Error; err != nil {
	// 		t.Fatal(err)
	// 	}
	// 	if len(announcements) != 7 {
	// 		t.Fatal("invalid number of announcements")
	// }

	// 	// Add an entry to the blocklist to block host 1
	// 	entry1 := "foo.bar"
	// 	err = ss.UpdateHostBlocklistEntries(context.Background(), []string{entry1}, nil, false)
	// 	if err != nil {
	// 		t.Fatal(err)
	// 	}

	// // Insert multiple announcements for host 1 - this asserts that the UNIQUE
	// // constraint on the blocklist table isn't triggered when inserting multiple
	// // announcements for a host that's on the blocklist
	//
	//	if err := insertAnnouncements(ss.db, []announcement{ann1, ann1}); err != nil {
	//		t.Fatal(err)
	//	}
}

// TestAnnouncementMaxAge verifies old announcements are ignored.
func TestAnnouncementMaxAge(t *testing.T) {
	t.Skip("TODO: fix test")
	// 	db := newTestSQLStore(t, defaultTestSQLStoreConfig)
	// 	defer db.Close()

	// 	// assert we don't have any announcements
	// 	if len(db.cs.announcements) != 0 {
	// 		t.Fatal("expected 0 announcements")
	// 	}

	// 	// fabricate two blocks with announcements, one before the cutoff and one after
	// 	b1 := types.Block{
	// 		Transactions: []types.Transaction{newTestTransaction(newTestHostAnnouncement("foo.com:1000"))},
	// 		Timestamp:    time.Now().Add(-db.cs.announcementMaxAge).Add(-time.Second),
	// 	}
	// 	b2 := types.Block{
	// 		Transactions: []types.Transaction{newTestTransaction(newTestHostAnnouncement("foo.com:1001"))},
	// 		Timestamp:    time.Now().Add(-db.cs.announcementMaxAge).Add(time.Second),
	// 	}

	// 	// process b1, expect no announcements
	// 	db.cs.processChainApplyUpdateHostDB(chain.ApplyUpdate{Block: b1})
	// 	if len(db.cs.announcements) != 0 {
	// 		t.Fatal("expected 0 announcements")
	// 	}

	// // process b2, expect 1 announcement
	// db.cs.processChainApplyUpdateHostDB(chain.ApplyUpdate{Block: b2})
	//
	//	if len(db.cs.announcements) != 1 {
	//		t.Fatal("expected 1 announcement")
	//	} else if db.cs.announcements[0].HostAnnouncement.NetAddress != "foo.com:1001" {
	//
	//		t.Fatal("unexpected announcement")
	//	}
}

// func (s *SQLStore) insertTestAnnouncement(a announcement) error {
// 	return insertAnnouncements(s.db, []announcement{a})
// }

// func newTestPK() (types.PublicKey, types.PrivateKey) {
// 	sk := types.GeneratePrivateKey()
// 	pk := sk.PublicKey()
// 	return pk, sk
// }

// func newTestHostAnnouncement(na string) (chain.HostAnnouncement, types.PrivateKey) {
// 	_, sk := newTestPK()
// 	a := chain.HostAnnouncement{
// 		NetAddress: na,
// 	}
// 	return a, sk
// }

// func newTestTransaction(ha chain.HostAnnouncement, sk types.PrivateKey) types.Transaction {
// 	return types.Transaction{ArbitraryData: [][]byte{ha.ToArbitraryData(sk)}}
// }
