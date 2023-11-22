package stores

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/object"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
	"lukechampine.com/frand"
)

func generateMultisigUC(m, n uint64, salt string) types.UnlockConditions {
	uc := types.UnlockConditions{
		PublicKeys:         make([]types.UnlockKey, n),
		SignaturesRequired: uint64(m),
	}
	for i := range uc.PublicKeys {
		uc.PublicKeys[i].Algorithm = types.SpecifierEd25519
		uc.PublicKeys[i].Key = frand.Bytes(32)
	}
	return uc
}

// TestObjectBasic tests the hydration of raw objects works when we fetch
// objects from the metadata store.
func TestObjectBasic(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	// create 2 hosts
	hks, err := ss.addTestHosts(2)
	if err != nil {
		t.Fatal(err)
	}
	hk1, hk2 := hks[0], hks[1]

	// create 2 contracts
	fcids, _, err := ss.addTestContracts(hks)
	if err != nil {
		t.Fatal(err)
	}
	fcid1, fcid2 := fcids[0], fcids[1]

	// create an object
	want := object.Object{
		Key: object.GenerateEncryptionKey(),
		Slabs: []object.SlabSlice{
			{
				Slab: object.Slab{
					Health:    1.0,
					Key:       object.GenerateEncryptionKey(),
					MinShards: 1,
					Shards:    newTestShards(hk1, fcid1, types.Hash256{1}),
				},
				Offset: 10,
				Length: 100,
			},
			{
				Slab: object.Slab{
					Health:    1.0,
					Key:       object.GenerateEncryptionKey(),
					MinShards: 2,
					Shards:    newTestShards(hk2, fcid2, types.Hash256{2}),
				},
				Offset: 20,
				Length: 200,
			},
		},
	}

	// add the object
	if err := ss.UpdateObject(context.Background(), api.DefaultBucketName, t.Name(), testContractSet, testETag, testMimeType, want); err != nil {
		t.Fatal(err)
	}

	// fetch the object
	got, err := ss.Object(context.Background(), api.DefaultBucketName, t.Name())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.Object, want) {
		t.Fatal("object mismatch", cmp.Diff(got.Object, want))
	}

	// delete a sector
	var sectors []dbSector
	if err := ss.db.Find(&sectors).Error; err != nil {
		t.Fatal(err)
	} else if len(sectors) != 2 {
		t.Fatal("unexpected number of sectors")
	} else if tx := ss.db.Delete(sectors[0]); tx.Error != nil || tx.RowsAffected != 1 {
		t.Fatal("unexpected number of sectors deleted", tx.Error, tx.RowsAffected)
	}

	// fetch the object again and assert we receive an indication it was corrupted
	_, err = ss.Object(context.Background(), api.DefaultBucketName, t.Name())
	if !errors.Is(err, api.ErrObjectCorrupted) {
		t.Fatal("unexpected err", err)
	}

	// create an object without slabs
	want2 := object.Object{
		Key:   object.GenerateEncryptionKey(),
		Slabs: []object.SlabSlice{},
	}

	// add the object
	if err := ss.UpdateObject(context.Background(), api.DefaultBucketName, t.Name(), testContractSet, testETag, testMimeType, want2); err != nil {
		t.Fatal(err)
	}

	// fetch the object
	got2, err := ss.Object(context.Background(), api.DefaultBucketName, t.Name())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got2.Object, want2) {
		t.Fatal("object mismatch", cmp.Diff(got2.Object, want2))
	}
}

// TestSQLContractStore tests SQLContractStore functionality.
func TestSQLContractStore(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	// Create a host for the contract.
	hk := types.GeneratePrivateKey().PublicKey()
	err := ss.addTestHost(hk)
	if err != nil {
		t.Fatal(err)
	}

	// Add an announcement.
	err = ss.insertTestAnnouncement(hk, hostdb.Announcement{NetAddress: "address"})
	if err != nil {
		t.Fatal(err)
	}

	// Create random unlock conditions for the host.
	uc := generateMultisigUC(1, 2, "salt")
	uc.PublicKeys[1].Key = hk[:]
	uc.Timelock = 192837

	// Create a contract and set all fields.
	fcid := types.FileContractID{1, 1, 1, 1, 1}
	c := rhpv2.ContractRevision{
		Revision: types.FileContractRevision{
			ParentID:         fcid,
			UnlockConditions: uc,
			FileContract: types.FileContract{
				RevisionNumber: 200,
				Filesize:       4096,
				FileMerkleRoot: types.Hash256{222},
				WindowStart:    400,
				WindowEnd:      500,
				ValidProofOutputs: []types.SiacoinOutput{
					{
						Value:   types.NewCurrency64(121),
						Address: types.Address{2, 1, 2},
					},
				},
				MissedProofOutputs: []types.SiacoinOutput{
					{
						Value:   types.NewCurrency64(323),
						Address: types.Address{2, 3, 2},
					},
				},
				UnlockHash: types.Hash256{6, 6, 6},
			},
		},
		Signatures: [2]types.TransactionSignature{
			{
				ParentID:       types.Hash256(fcid),
				PublicKeyIndex: 0,
				Timelock:       100000,
				CoveredFields:  types.CoveredFields{WholeTransaction: true},
				Signature:      []byte("signature1"),
			},
			{
				ParentID:       types.Hash256(fcid),
				PublicKeyIndex: 1,
				Timelock:       200000,
				CoveredFields:  types.CoveredFields{WholeTransaction: true},
				Signature:      []byte("signature2"),
			},
		},
	}

	// Look it up. Should fail.
	ctx := context.Background()
	_, err = ss.Contract(ctx, c.ID())
	if !errors.Is(err, api.ErrContractNotFound) {
		t.Fatal(err)
	}
	contracts, err := ss.Contracts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(contracts) != 0 {
		t.Fatalf("should have 0 contracts but got %v", len(contracts))
	}

	// Insert it.
	contractPrice := types.NewCurrency64(1)
	totalCost := types.NewCurrency64(456)
	startHeight := uint64(100)
	returned, err := ss.AddContract(ctx, c, contractPrice, totalCost, startHeight, api.ContractStatePending)
	if err != nil {
		t.Fatal(err)
	}
	expected := api.ContractMetadata{
		ID:          fcid,
		HostIP:      "address",
		HostKey:     hk,
		StartHeight: 100,
		State:       api.ContractStatePending,
		WindowStart: 400,
		WindowEnd:   500,
		RenewedFrom: types.FileContractID{},
		Spending: api.ContractSpending{
			Uploads:     types.ZeroCurrency,
			Downloads:   types.ZeroCurrency,
			FundAccount: types.ZeroCurrency,
		},
		ContractPrice: types.NewCurrency64(1),
		TotalCost:     totalCost,
		Size:          c.Revision.Filesize,
	}
	if !reflect.DeepEqual(returned, expected) {
		t.Fatal("contract mismatch")
	}

	// Look it up again.
	fetched, err := ss.Contract(ctx, c.ID())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(fetched, expected) {
		t.Fatal("contract mismatch")
	}
	contracts, err = ss.Contracts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(contracts) != 1 {
		t.Fatalf("should have 1 contracts but got %v", len(contracts))
	}
	if !reflect.DeepEqual(contracts[0], expected) {
		t.Fatal("contract mismatch")
	}

	// Add a contract set with our contract and assert we can fetch it using the set name
	if err := ss.SetContractSet(ctx, "foo", []types.FileContractID{contracts[0].ID}); err != nil {
		t.Fatal(err)
	}
	if contracts, err := ss.ContractSetContracts(ctx, "foo"); err != nil {
		t.Fatal(err)
	} else if len(contracts) != 1 {
		t.Fatalf("should have 1 contracts but got %v", len(contracts))
	}
	if _, err := ss.ContractSetContracts(ctx, "bar"); !errors.Is(err, api.ErrContractSetNotFound) {
		t.Fatal(err)
	}

	// Add another contract set.
	if err := ss.SetContractSet(ctx, "foo2", []types.FileContractID{contracts[0].ID}); err != nil {
		t.Fatal(err)
	}

	// Fetch contract sets.
	sets, err := ss.ContractSets(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(sets) != 3 { // 2 sets + default set
		t.Fatal("wrong number of sets")
	}
	if sets[0] != "foo" || sets[1] != "foo2" || sets[2] != testContractSet {
		t.Fatal("wrong sets returned", sets)
	}

	// Delete the contract.
	if err := ss.ArchiveContract(ctx, c.ID(), api.ContractArchivalReasonRemoved); err != nil {
		t.Fatal(err)
	}

	// Look it up. Should fail.
	_, err = ss.Contract(ctx, c.ID())
	if !errors.Is(err, api.ErrContractNotFound) {
		t.Fatal(err)
	}
	contracts, err = ss.Contracts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(contracts) != 0 {
		t.Fatalf("should have 0 contracts but got %v", len(contracts))
	}

	// Make sure the db was cleaned up properly through the CASCADE delete.
	tableCountCheck := func(table interface{}, tblCount int64) error {
		var count int64
		if err := ss.db.Model(table).Count(&count).Error; err != nil {
			return err
		}
		if count != tblCount {
			return fmt.Errorf("expected %v objects in table %v but got %v", tblCount, table.(schema.Tabler).TableName(), count)
		}
		return nil
	}
	if err := tableCountCheck(&dbContract{}, 0); err != nil {
		t.Fatal(err)
	}

	// Check join table count as well.
	var count int64
	if err := ss.db.Table("contract_sectors").Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("expected %v objects in contract_sectors but got %v", 0, count)
	}
}

func TestContractsForHost(t *testing.T) {
	// create a SQL store
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	// add 2 hosts
	hks, err := ss.addTestHosts(2)
	if err != nil {
		t.Fatal(err)
	}

	// add 2 contracts
	_, _, err = ss.addTestContracts(hks)
	if err != nil {
		t.Fatal(err)
	}

	// fetch raw hosts
	var hosts []dbHost
	if err := ss.db.
		Model(&dbHost{}).
		Find(&hosts).
		Error; err != nil {
		t.Fatal(err)
	}
	if len(hosts) != 2 {
		t.Fatal("unexpected number of hosts")
	}

	contracts, _ := contractsForHost(ss.db, hosts[0])
	if len(contracts) != 1 || contracts[0].Host.convert().PublicKey.String() != hosts[0].convert().PublicKey.String() {
		t.Fatal("unexpected", len(contracts), contracts)
	}

	contracts, _ = contractsForHost(ss.db, hosts[1])
	if len(contracts) != 1 || contracts[0].Host.convert().PublicKey.String() != hosts[1].convert().PublicKey.String() {
		t.Fatalf("unexpected contracts, %+v", contracts)
	}
}

// TestContractRoots tests the ContractRoots function on the store.
func TestContractRoots(t *testing.T) {
	// create a SQL store
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	// add a contract
	hks, err := ss.addTestHosts(1)
	if err != nil {
		t.Fatal(err)
	}
	fcids, _, err := ss.addTestContracts(hks)
	if err != nil {
		t.Fatal(err)
	}

	// add an object
	root := types.Hash256{1}
	obj := object.Object{
		Key: object.GenerateEncryptionKey(),
		Slabs: []object.SlabSlice{
			{
				Slab: object.Slab{
					Key:       object.GenerateEncryptionKey(),
					MinShards: 1,
					Shards:    newTestShards(hks[0], fcids[0], types.Hash256{1}),
				},
			},
		},
	}

	// add the object.
	if err := ss.UpdateObject(context.Background(), api.DefaultBucketName, t.Name(), testContractSet, testETag, testMimeType, obj); err != nil {
		t.Fatal(err)
	}

	roots, err := ss.ContractRoots(context.Background(), fcids[0])
	if err != nil {
		t.Fatal(err)
	}
	if len(roots) != 1 || roots[0] != root {
		t.Fatal("unexpected", roots)
	}
}

// TestRenewContract is a test for AddRenewedContract.
func TestRenewedContract(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	// Create a host for the contract and another one for redundancy.
	hks, err := ss.addTestHosts(2)
	if err != nil {
		t.Fatal(err)
	}
	hk, hk2 := hks[0], hks[1]

	// Add announcements.
	err = ss.insertTestAnnouncement(hk, hostdb.Announcement{NetAddress: "address"})
	if err != nil {
		t.Fatal(err)
	}
	err = ss.insertTestAnnouncement(hk2, hostdb.Announcement{NetAddress: "address2"})
	if err != nil {
		t.Fatal(err)
	}

	// Create random unlock conditions for the hosts.
	uc := generateMultisigUC(1, 2, "salt")
	uc.PublicKeys[1].Key = hk[:]
	uc.Timelock = 192837

	uc2 := generateMultisigUC(1, 2, "salt")
	uc2.PublicKeys[1].Key = hk2[:]
	uc2.Timelock = 192837

	// Insert the contracts.
	fcid1 := types.FileContractID{1, 1, 1, 1, 1}
	c := rhpv2.ContractRevision{
		Revision: types.FileContractRevision{
			ParentID:         fcid1,
			UnlockConditions: uc,
			FileContract: types.FileContract{
				Filesize:       1,
				WindowStart:    2,
				WindowEnd:      3,
				RevisionNumber: 4,
			},
		},
	}
	oldContractPrice := types.NewCurrency64(1)
	oldContractTotal := types.NewCurrency64(111)
	oldContractStartHeight := uint64(100)
	ctx := context.Background()
	added, err := ss.AddContract(ctx, c, oldContractPrice, oldContractTotal, oldContractStartHeight, api.ContractStatePending)
	if err != nil {
		t.Fatal(err)
	}

	// Assert the contract is returned.
	if added.RenewedFrom != (types.FileContractID{}) {
		t.Fatal("unexpected")
	}

	fcid2 := types.FileContractID{9, 9, 9, 9, 9}
	c2 := c
	c2.Revision.ParentID = fcid2
	c2.Revision.UnlockConditions = uc2
	_, err = ss.AddContract(ctx, c2, oldContractPrice, oldContractTotal, oldContractStartHeight, api.ContractStatePending)
	if err != nil {
		t.Fatal(err)
	}

	// add an object for that contract.
	obj := object.Object{
		Key: object.GenerateEncryptionKey(),
		Slabs: []object.SlabSlice{
			// good slab
			{
				Slab: object.Slab{
					Key:       object.GenerateEncryptionKey(),
					MinShards: 1,
					Shards:    append(newTestShards(hk, fcid1, types.Hash256{1}), newTestShards(hk2, fcid2, types.Hash256{2})...),
				},
			},
		},
	}

	// create a contract set with both contracts.
	if err := ss.SetContractSet(context.Background(), "test", []types.FileContractID{fcid1, fcid2}); err != nil {
		t.Fatal(err)
	}

	// add the object.
	if err := ss.UpdateObject(context.Background(), api.DefaultBucketName, "foo", testContractSet, testETag, testMimeType, obj); err != nil {
		t.Fatal(err)
	}

	// mock recording of spending records to ensure the cached fields get updated
	spending := api.ContractSpending{
		Uploads:     types.Siacoins(1),
		Downloads:   types.Siacoins(2),
		FundAccount: types.Siacoins(3),
		Deletions:   types.Siacoins(4),
		SectorRoots: types.Siacoins(5),
	}
	if err := ss.RecordContractSpending(context.Background(), []api.ContractSpendingRecord{
		{ContractID: fcid1, RevisionNumber: 1, Size: rhpv2.SectorSize, ContractSpending: spending},
		{ContractID: fcid2, RevisionNumber: 1, Size: rhpv2.SectorSize, ContractSpending: spending},
	}); err != nil {
		t.Fatal(err)
	}

	// no slabs should be unhealthy.
	if err := ss.RefreshHealth(context.Background()); err != nil {
		t.Fatal(err)
	}
	slabs, err := ss.UnhealthySlabs(context.Background(), 0.99, "test", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(slabs) > 0 {
		t.Fatal("shouldn't return any slabs", len(slabs))
	}

	// Assert we can't fetch the renewed contract.
	_, err = ss.RenewedContract(context.Background(), fcid1)
	if !errors.Is(err, api.ErrContractNotFound) {
		t.Fatal("unexpected")
	}

	// Renew it.
	fcid1Renewed := types.FileContractID{2, 2, 2, 2, 2}
	rev := rhpv2.ContractRevision{
		Revision: types.FileContractRevision{
			ParentID:         fcid1Renewed,
			UnlockConditions: uc,
			FileContract: types.FileContract{
				MissedProofOutputs: []types.SiacoinOutput{},
				ValidProofOutputs:  []types.SiacoinOutput{},
			},
		},
	}
	newContractPrice := types.NewCurrency64(2)
	newContractTotal := types.NewCurrency64(222)
	newContractStartHeight := uint64(200)
	if _, err := ss.AddRenewedContract(ctx, rev, newContractPrice, newContractTotal, newContractStartHeight, fcid1, api.ContractStatePending); err != nil {
		t.Fatal(err)
	}

	// Assert we can fetch the renewed contract.
	renewed, err := ss.RenewedContract(context.Background(), fcid1)
	if err != nil {
		t.Fatal("unexpected", err)
	}
	if renewed.ID != fcid1Renewed {
		t.Fatal("unexpected")
	}

	// make sure the contract set was updated.
	setContracts, err := ss.ContractSetContracts(context.Background(), "test")
	if err != nil {
		t.Fatal(err)
	}
	if len(setContracts) != 2 || (setContracts[0].ID != fcid1Renewed && setContracts[1].ID != fcid1Renewed) {
		t.Fatal("contract set wasn't updated", setContracts)
	}

	// slab should still be in good shape.
	if err := ss.RefreshHealth(context.Background()); err != nil {
		t.Fatal(err)
	}
	slabs, err = ss.UnhealthySlabs(context.Background(), 0.99, "test", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(slabs) > 0 {
		t.Fatal("shouldn't return any slabs", len(slabs))
	}

	// Contract should be gone from active contracts.
	_, err = ss.Contract(ctx, fcid1)
	if !errors.Is(err, api.ErrContractNotFound) {
		t.Fatal(err)
	}

	// New contract should exist.
	newContract, err := ss.Contract(ctx, fcid1Renewed)
	if err != nil {
		t.Fatal(err)
	}
	expected := api.ContractMetadata{
		ID:          fcid1Renewed,
		HostIP:      "address",
		HostKey:     hk,
		StartHeight: newContractStartHeight,
		RenewedFrom: fcid1,
		Size:        rhpv2.SectorSize,
		State:       api.ContractStatePending,
		Spending: api.ContractSpending{
			Uploads:     types.ZeroCurrency,
			Downloads:   types.ZeroCurrency,
			FundAccount: types.ZeroCurrency,
		},
		ContractPrice: types.NewCurrency64(2),
		TotalCost:     newContractTotal,
	}
	if !reflect.DeepEqual(newContract, expected) {
		t.Fatal("mismatch")
	}

	// Archived contract should exist.
	var ac dbArchivedContract
	err = ss.db.Model(&dbArchivedContract{}).
		Where("fcid", fileContractID(fcid1)).
		Take(&ac).
		Error
	if err != nil {
		t.Fatal(err)
	}

	ac.Model = Model{}
	expectedContract := dbArchivedContract{
		Host:      publicKey(c.HostKey()),
		RenewedTo: fileContractID(fcid1Renewed),
		Reason:    api.ContractArchivalReasonRenewed,

		ContractCommon: ContractCommon{
			FCID: fileContractID(fcid1),

			ContractPrice:  currency(oldContractPrice),
			TotalCost:      currency(oldContractTotal),
			ProofHeight:    0,
			RevisionHeight: 0,
			RevisionNumber: "1",
			StartHeight:    100,
			WindowStart:    2,
			WindowEnd:      3,
			Size:           rhpv2.SectorSize,
			State:          contractStatePending,

			UploadSpending:      currency(types.Siacoins(1)),
			DownloadSpending:    currency(types.Siacoins(2)),
			FundAccountSpending: currency(types.Siacoins(3)),
			DeleteSpending:      currency(types.Siacoins(4)),
			ListSpending:        currency(types.Siacoins(5)),
		},
	}
	if !reflect.DeepEqual(ac, expectedContract) {
		t.Fatal("mismatch", cmp.Diff(ac, expectedContract))
	}

	// Renew it once more.
	fcid3 := types.FileContractID{3, 3, 3, 3, 3}
	rev = rhpv2.ContractRevision{
		Revision: types.FileContractRevision{
			ParentID:         fcid3,
			UnlockConditions: uc,
			FileContract: types.FileContract{
				MissedProofOutputs: []types.SiacoinOutput{},
				ValidProofOutputs:  []types.SiacoinOutput{},
			},
		},
	}
	newContractPrice = types.NewCurrency64(3)
	newContractTotal = types.NewCurrency64(333)
	newContractStartHeight = uint64(300)

	// Assert the renewed contract is returned
	renewedContract, err := ss.AddRenewedContract(ctx, rev, newContractPrice, newContractTotal, newContractStartHeight, fcid1Renewed, api.ContractStatePending)
	if err != nil {
		t.Fatal(err)
	}
	if renewedContract.RenewedFrom != fcid1Renewed {
		t.Fatal("unexpected")
	}
}

// TestAncestorsContracts verifies that AncestorContracts returns the right
// ancestors in the correct order.
func TestAncestorsContracts(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	hk := types.PublicKey{1, 2, 3}
	if err := ss.addTestHost(hk); err != nil {
		t.Fatal(err)
	}

	// Create a chain of 4 contracts.
	// Their start heights are 0, 1, 2, 3.
	fcids := []types.FileContractID{{1}, {2}, {3}, {4}}
	if _, err := ss.addTestContract(fcids[0], hk); err != nil {
		t.Fatal(err)
	}
	for i := 1; i < len(fcids); i++ {
		if _, err := ss.addTestRenewedContract(fcids[i], fcids[i-1], hk, uint64(i)); err != nil {
			t.Fatal(err)
		}
	}

	// Fetch the ancestors but only the ones with a startHeight >= 1. That
	// should return 2 contracts. The active one with height 3 isn't
	// returned and the one with height 0 is also not returned.
	contracts, err := ss.AncestorContracts(context.Background(), fcids[len(fcids)-1], 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(contracts) != len(fcids)-2 {
		t.Fatal("wrong number of contracts returned", len(contracts))
	}
	for i := 0; i < len(contracts)-1; i++ {
		if !reflect.DeepEqual(contracts[i], api.ArchivedContract{
			ID:          fcids[len(fcids)-2-i],
			HostKey:     hk,
			RenewedTo:   fcids[len(fcids)-1-i],
			StartHeight: 2,
			Size:        4096,
			State:       api.ContractStatePending,
			WindowStart: 400,
			WindowEnd:   500,
		}) {
			t.Fatal("wrong contract", i, contracts[i])
		}
	}
}

func TestArchiveContracts(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	// add 3 hosts
	hks, err := ss.addTestHosts(3)
	if err != nil {
		t.Fatal(err)
	}

	// add 3 contracts
	fcids, _, err := ss.addTestContracts(hks)
	if err != nil {
		t.Fatal(err)
	}

	// archive 2 of them
	toArchive := map[types.FileContractID]string{
		fcids[1]: "foo",
		fcids[2]: "bar",
	}
	if err := ss.ArchiveContracts(context.Background(), toArchive); err != nil {
		t.Fatal(err)
	}

	// assert the first one is still active
	active, err := ss.Contracts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(active) != 1 || active[0].ID != fcids[0] {
		t.Fatal("wrong contracts", active)
	}

	// assert the two others were archived
	ffcids := make([]fileContractID, 2)
	ffcids[0] = fileContractID(fcids[1])
	ffcids[1] = fileContractID(fcids[2])
	var acs []dbArchivedContract
	err = ss.db.Model(&dbArchivedContract{}).
		Where("fcid IN (?)", ffcids).
		Find(&acs).
		Error
	if err != nil {
		t.Fatal(err)
	}
	if len(acs) != 2 {
		t.Fatal("wrong number of archived contracts", len(acs))
	}
	if acs[0].Reason != "foo" || acs[1].Reason != "bar" {
		t.Fatal("unexpected reason", acs[0].Reason, acs[1].Reason)
	}
}

func (s *SQLStore) addTestContracts(keys []types.PublicKey) (fcids []types.FileContractID, contracts []api.ContractMetadata, err error) {
	cnt, err := s.contractsCount()
	if err != nil {
		return nil, nil, err
	}
	for i, key := range keys {
		fcids = append(fcids, types.FileContractID{byte(int(cnt) + i + 1)})
		contract, err := s.addTestContract(fcids[len(fcids)-1], key)
		if err != nil {
			return nil, nil, err
		}
		contracts = append(contracts, contract)
	}
	return
}

func (s *SQLStore) addTestContract(fcid types.FileContractID, hk types.PublicKey) (api.ContractMetadata, error) {
	rev := testContractRevision(fcid, hk)
	return s.AddContract(context.Background(), rev, types.ZeroCurrency, types.ZeroCurrency, 0, api.ContractStatePending)
}

func (s *SQLStore) addTestRenewedContract(fcid, renewedFrom types.FileContractID, hk types.PublicKey, startHeight uint64) (api.ContractMetadata, error) {
	rev := testContractRevision(fcid, hk)
	return s.AddRenewedContract(context.Background(), rev, types.ZeroCurrency, types.ZeroCurrency, startHeight, renewedFrom, api.ContractStatePending)
}

func (s *SQLStore) contractsCount() (cnt int64, err error) {
	err = s.db.
		Model(&dbContract{}).
		Count(&cnt).
		Error
	return
}

func testContractRevision(fcid types.FileContractID, hk types.PublicKey) rhpv2.ContractRevision {
	uc := generateMultisigUC(1, 2, "salt")
	uc.PublicKeys[1].Key = hk[:]
	uc.Timelock = 192837
	return rhpv2.ContractRevision{
		Revision: types.FileContractRevision{
			ParentID:         fcid,
			UnlockConditions: uc,
			FileContract: types.FileContract{
				RevisionNumber: 200,
				Filesize:       4096,
				FileMerkleRoot: types.Hash256{222},
				WindowStart:    400,
				WindowEnd:      500,
				ValidProofOutputs: []types.SiacoinOutput{
					{
						Value:   types.NewCurrency64(121),
						Address: types.Address{2, 1, 2},
					},
				},
				MissedProofOutputs: []types.SiacoinOutput{
					{
						Value:   types.NewCurrency64(323),
						Address: types.Address{2, 3, 2},
					},
				},
				UnlockHash: types.Hash256{6, 6, 6},
			},
		},
		Signatures: [2]types.TransactionSignature{
			{
				ParentID:       types.Hash256(fcid),
				PublicKeyIndex: 0,
				Timelock:       100000,
				CoveredFields:  types.CoveredFields{WholeTransaction: true},
				Signature:      []byte("signature1"),
			},
			{
				ParentID:       types.Hash256(fcid),
				PublicKeyIndex: 1,
				Timelock:       200000,
				CoveredFields:  types.CoveredFields{WholeTransaction: true},
				Signature:      []byte("signature2"),
			},
		},
	}
}

// TestSQLMetadataStore tests basic MetadataStore functionality.
func TestSQLMetadataStore(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	// Create 2 hosts
	hks, err := ss.addTestHosts(2)
	if err != nil {
		t.Fatal(err)
	}
	hk1, hk2 := hks[0], hks[1]

	// Create 2 contracts
	fcids, contracts, err := ss.addTestContracts(hks)
	if err != nil {
		t.Fatal(err)
	}
	fcid1, fcid2 := fcids[0], fcids[1]

	// Extract start height and total cost
	startHeight1, totalCost1 := contracts[0].StartHeight, contracts[0].TotalCost
	startHeight2, totalCost2 := contracts[1].StartHeight, contracts[1].TotalCost

	// Create an object with 2 slabs pointing to 2 different sectors.
	obj1 := object.Object{
		Key: object.GenerateEncryptionKey(),
		Slabs: []object.SlabSlice{
			{
				Slab: object.Slab{
					Health:    1,
					Key:       object.GenerateEncryptionKey(),
					MinShards: 1,
					Shards:    newTestShards(hk1, fcid1, types.Hash256{1}),
				},
				Offset: 10,
				Length: 100,
			},
			{
				Slab: object.Slab{
					Health:    1,
					Key:       object.GenerateEncryptionKey(),
					MinShards: 2,
					Shards:    newTestShards(hk2, fcid2, types.Hash256{2}),
				},
				Offset: 20,
				Length: 200,
			},
		},
	}

	// Store it.
	ctx := context.Background()
	objID := "key1"
	if err := ss.UpdateObject(ctx, api.DefaultBucketName, objID, testContractSet, testETag, testMimeType, obj1); err != nil {
		t.Fatal(err)
	}

	// Try to store it again. Should work.
	if err := ss.UpdateObject(ctx, api.DefaultBucketName, objID, testContractSet, testETag, testMimeType, obj1); err != nil {
		t.Fatal(err)
	}

	// Fetch it using get and verify every field.
	obj, err := ss.dbObject(objID)
	if err != nil {
		t.Fatal(err)
	}

	obj1Key, err := obj1.Key.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	obj1Slab0Key, err := obj1.Slabs[0].Key.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	obj1Slab1Key, err := obj1.Slabs[1].Key.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	// Set the Model fields to zero before comparing. These are set by gorm
	// itself and contain a few timestamps which would make the following
	// code a lot more verbose.
	obj.Model = Model{}
	for i := range obj.Slabs {
		obj.Slabs[i].Model = Model{}
	}

	one := uint(1)
	expectedObj := dbObject{
		DBBucketID: 1,
		ObjectID:   objID,
		Key:        obj1Key,
		Size:       obj1.TotalSize(),
		Slabs: []dbSlice{
			{
				DBObjectID:  &one,
				DBSlabID:    1,
				ObjectIndex: 1,
				Offset:      10,
				Length:      100,
			},
			{
				DBObjectID:  &one,
				DBSlabID:    2,
				ObjectIndex: 2,
				Offset:      20,
				Length:      200,
			},
		},
		MimeType: testMimeType,
		Etag:     testETag,
	}
	if !reflect.DeepEqual(obj, expectedObj) {
		t.Fatal("object mismatch", cmp.Diff(obj, expectedObj))
	}

	// Fetch it and verify again.
	fullObj, err := ss.Object(ctx, api.DefaultBucketName, objID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(fullObj.Object, obj1) {
		t.Fatal("object mismatch", cmp.Diff(fullObj, obj1))
	}

	expectedObjSlab1 := dbSlab{
		DBContractSetID: 1,
		Health:          1,
		Key:             obj1Slab0Key,
		MinShards:       1,
		TotalShards:     1,
		Shards: []dbSector{
			{
				DBSlabID:   1,
				SlabIndex:  1,
				Root:       obj1.Slabs[0].Shards[0].Root[:],
				LatestHost: publicKey(obj1.Slabs[0].Shards[0].LatestHost),
				Contracts: []dbContract{
					{
						HostID: 1,
						Host: dbHost{
							PublicKey: publicKey(hk1),
						},

						ContractCommon: ContractCommon{
							FCID: fileContractID(fcid1),

							TotalCost:      currency(totalCost1),
							RevisionNumber: "0",
							StartHeight:    startHeight1,
							WindowStart:    400,
							WindowEnd:      500,
							Size:           4096,
							State:          contractStatePending,

							UploadSpending:      zeroCurrency,
							DownloadSpending:    zeroCurrency,
							FundAccountSpending: zeroCurrency,
						},
					},
				},
			},
		},
	}

	expectedObjSlab2 := dbSlab{
		DBContractSetID: 1,
		Health:          1,
		Key:             obj1Slab1Key,
		MinShards:       2,
		TotalShards:     1,
		Shards: []dbSector{
			{
				DBSlabID:   2,
				SlabIndex:  1,
				Root:       obj1.Slabs[1].Shards[0].Root[:],
				LatestHost: publicKey(obj1.Slabs[1].Shards[0].LatestHost),
				Contracts: []dbContract{
					{
						HostID: 2,
						Host: dbHost{
							PublicKey: publicKey(hk2),
						},
						ContractCommon: ContractCommon{
							FCID: fileContractID(fcid2),

							TotalCost:      currency(totalCost2),
							RevisionNumber: "0",
							StartHeight:    startHeight2,
							WindowStart:    400,
							WindowEnd:      500,
							Size:           4096,
							State:          contractStatePending,

							UploadSpending:      zeroCurrency,
							DownloadSpending:    zeroCurrency,
							FundAccountSpending: zeroCurrency,
						},
					},
				},
			},
		},
	}

	// Compare slabs.
	slab1, err := ss.dbSlab(obj1Slab0Key)
	if err != nil {
		t.Fatal(err)
	}
	slab2, err := ss.dbSlab(obj1Slab1Key)
	if err != nil {
		t.Fatal(err)
	}
	slabs := []*dbSlab{&slab1, &slab2}
	for i := range slabs {
		slabs[i].Model = Model{}
		slabs[i].Shards[0].Model = Model{}
		slabs[i].Shards[0].Contracts[0].Model = Model{}
		slabs[i].Shards[0].Contracts[0].Host.Model = Model{}
		slabs[i].HealthValidUntil = 0
	}
	if !reflect.DeepEqual(slab1, expectedObjSlab1) {
		t.Fatal("mismatch", cmp.Diff(slab1, expectedObjSlab1))
	}
	if !reflect.DeepEqual(slab2, expectedObjSlab2) {
		t.Fatal("mismatch", cmp.Diff(slab2, expectedObjSlab2))
	}

	// Remove the first slab of the object.
	obj1.Slabs = obj1.Slabs[1:]
	if err := ss.UpdateObject(ctx, api.DefaultBucketName, objID, testContractSet, testETag, testMimeType, obj1); err != nil {
		t.Fatal(err)
	}
	fullObj, err = ss.Object(ctx, api.DefaultBucketName, objID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(fullObj.Object, obj1) {
		t.Fatal("object mismatch")
	}

	// Sanity check the db at the end of the test. We expect:
	// - 1 element in the object table since we only stored and overwrote a single object
	// - 1 element in the slabs table since we updated the object to only have 1 slab
	// - 1 element in the slices table for the same reason
	// - 1 element in the sectors table for the same reason
	countCheck := func(objCount, sliceCount, slabCount, sectorCount int64) error {
		tableCountCheck := func(table interface{}, tblCount int64) error {
			var count int64
			if err := ss.db.Model(table).Count(&count).Error; err != nil {
				return err
			}
			if count != tblCount {
				return fmt.Errorf("expected %v objects in table %v but got %v", tblCount, table.(schema.Tabler).TableName(), count)
			}
			return nil
		}
		// Check all tables.
		if err := tableCountCheck(&dbObject{}, objCount); err != nil {
			return err
		}
		if err := tableCountCheck(&dbSlice{}, sliceCount); err != nil {
			return err
		}
		if err := tableCountCheck(&dbSlab{}, slabCount); err != nil {
			return err
		}
		if err := tableCountCheck(&dbSector{}, sectorCount); err != nil {
			return err
		}
		return nil
	}
	if err := countCheck(1, 1, 1, 1); err != nil {
		t.Fatal(err)
	}

	// Delete the object. Due to the cascade this should delete everything
	// but the sectors.
	if err := ss.RemoveObject(ctx, api.DefaultBucketName, objID); err != nil {
		t.Fatal(err)
	}
	if err := countCheck(0, 0, 0, 0); err != nil {
		t.Fatal(err)
	}
}

// TestObjectHealth verifies the object's health is returned correctly by all
// methods that return the object's metadata.
func TestObjectHealth(t *testing.T) {
	// create db
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	// add hosts and contracts
	hks, err := ss.addTestHosts(5)
	if err != nil {
		t.Fatal(err)
	}
	fcids, _, err := ss.addTestContracts(hks)
	if err != nil {
		t.Fatal(err)
	}

	// all contracts are good
	if err := ss.SetContractSet(context.Background(), testContractSet, fcids); err != nil {
		t.Fatal(err)
	}

	// add an object with 2 slabs
	add := object.Object{
		Key: object.GenerateEncryptionKey(),
		Slabs: []object.SlabSlice{
			{
				Slab: object.Slab{
					Key:       object.GenerateEncryptionKey(),
					MinShards: 1,
					Shards: []object.Sector{
						newTestShard(hks[0], fcids[0], types.Hash256{1}),
						newTestShard(hks[1], fcids[1], types.Hash256{2}),
						newTestShard(hks[2], fcids[2], types.Hash256{3}),
						newTestShard(hks[3], fcids[3], types.Hash256{4}),
					},
				},
			},
			{
				Slab: object.Slab{
					Key:       object.GenerateEncryptionKey(),
					MinShards: 1,
					Shards: []object.Sector{
						newTestShard(hks[1], fcids[1], types.Hash256{5}),
						newTestShard(hks[2], fcids[2], types.Hash256{6}),
						newTestShard(hks[3], fcids[3], types.Hash256{7}),
						newTestShard(hks[4], fcids[4], types.Hash256{8}),
					},
				},
			},
		},
	}

	if err := ss.UpdateObject(context.Background(), api.DefaultBucketName, "/foo", testContractSet, testETag, testMimeType, add); err != nil {
		t.Fatal(err)
	}

	// refresh health
	if err := ss.RefreshHealth(context.Background()); err != nil {
		t.Fatal(err)
	}

	// assert health
	obj, err := ss.Object(context.Background(), api.DefaultBucketName, "/foo")
	if err != nil {
		t.Fatal(err)
	} else if obj.Health != 1 {
		t.Fatal("wrong health", obj.Health)
	}

	// update contract to impact the object's health
	if err := ss.SetContractSet(context.Background(), testContractSet, []types.FileContractID{fcids[0], fcids[2], fcids[3], fcids[4]}); err != nil {
		t.Fatal(err)
	}
	if err := ss.RefreshHealth(context.Background()); err != nil {
		t.Fatal(err)
	}
	expectedHealth := float64(2) / float64(3)

	// assert health
	obj, err = ss.Object(context.Background(), api.DefaultBucketName, "/foo")
	if err != nil {
		t.Fatal(err)
	} else if obj.Health != expectedHealth {
		t.Fatal("wrong health", obj.Health)
	}

	// assert (raw) object and object health methods
	raw, err := ss.object(context.Background(), ss.db, api.DefaultBucketName, "/foo")
	if err != nil {
		t.Fatal(err)
	} else if len(raw) == 0 {
		t.Fatal("object not found")
	}
	health, err := ss.objectHealth(context.Background(), ss.db, raw[0].ObjectID)
	if err != nil {
		t.Fatal(err)
	} else if health != expectedHealth {
		t.Fatal("wrong health", health)
	}

	// assert health is returned correctly by ObjectEntries
	entries, _, err := ss.ObjectEntries(context.Background(), api.DefaultBucketName, "/", "", "", "", "", 0, -1)
	if err != nil {
		t.Fatal(err)
	} else if len(entries) != 1 {
		t.Fatal("wrong number of entries", len(entries))
	} else if entries[0].Health != expectedHealth {
		t.Fatal("wrong health", entries[0].Health)
	}

	// assert health is returned correctly by SearchObject
	entries, err = ss.SearchObjects(context.Background(), api.DefaultBucketName, "foo", 0, -1)
	if err != nil {
		t.Fatal(err)
	} else if len(entries) != 1 {
		t.Fatal("wrong number of entries", len(entries))
	} else if entries[0].Health != expectedHealth {
		t.Fatal("wrong health", entries[0].Health)
	}

	// update contract set again to make sure the 2nd slab has even worse health
	if err := ss.SetContractSet(context.Background(), testContractSet, []types.FileContractID{fcids[0], fcids[2], fcids[3]}); err != nil {
		t.Fatal(err)
	}
	if err := ss.RefreshHealth(context.Background()); err != nil {
		t.Fatal(err)
	}
	expectedHealth = float64(1) / float64(3)

	// assert health is the min. health of the slabs
	obj, err = ss.Object(context.Background(), api.DefaultBucketName, "/foo")
	if err != nil {
		t.Fatal(err)
	} else if obj.Health != expectedHealth {
		t.Fatal("wrong health", obj.Health)
	} else if obj.Slabs[0].Health <= expectedHealth {
		t.Fatal("wrong health", obj.Slabs[0].Health)
	} else if obj.Slabs[1].Health != expectedHealth {
		t.Fatal("wrong health", obj.Slabs[1].Health)
	}

	// add an empty object
	add = object.Object{
		Key:   object.GenerateEncryptionKey(),
		Slabs: nil,
	}
	if err := ss.UpdateObject(context.Background(), api.DefaultBucketName, "/bar", testContractSet, testETag, testMimeType, add); err != nil {
		t.Fatal(err)
	}

	// assert the health is 1
	obj, err = ss.Object(context.Background(), api.DefaultBucketName, "/bar")
	if err != nil {
		t.Fatal(err)
	} else if obj.Health != 1 {
		t.Fatal("wrong health", obj.Health)
	}
}

// TestObjectEntries is a test for the ObjectEntries method.
func TestObjectEntries(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	objects := []struct {
		path string
		size int64
	}{
		{"/foo/bar", 1},
		{"/foo/bat", 2},
		{"/foo/baz/quux", 3},
		{"/foo/baz/quuz", 4},
		{"/gab/guub", 5},
		{"/fileś/śpecial", 6}, // utf8
		{"/FOO/bar", 7},
	}

	// shuffle to ensure order does not influence the outcome of the test
	frand.Shuffle(len(objects), func(i, j int) { objects[i], objects[j] = objects[j], objects[i] })

	ctx := context.Background()
	for _, o := range objects {
		obj := newTestObject(frand.Intn(9) + 1)
		obj.Slabs = obj.Slabs[:1]
		obj.Slabs[0].Length = uint32(o.size)
		err := ss.UpdateObject(ctx, api.DefaultBucketName, o.path, testContractSet, testETag, testMimeType, obj)
		if err != nil {
			t.Fatal(err)
		}
	}

	// assertMetadata asserts both ModTime, MimeType and ETag and clears them so the
	// entries are ready for comparison
	assertMetadata := func(entries []api.ObjectMetadata) {
		for i := range entries {
			// assert mod time
			if !strings.HasSuffix(entries[i].Name, "/") && entries[i].ModTime.IsZero() {
				t.Fatal("mod time should be set")
			}
			entries[i].ModTime = time.Time{}

			// assert mime type
			if entries[i].MimeType != testMimeType {
				t.Fatal("unexpected mime type", entries[i].MimeType)
			}
			entries[i].MimeType = ""

			// assert etag
			if entries[i].ETag == "" {
				t.Fatal("etag should be set")
			}
			entries[i].ETag = ""
		}
	}

	// override health of some slabs
	if err := ss.overrideSlabHealth("/foo/baz/quuz", 0.5); err != nil {
		t.Fatal(err)
	}
	if err := ss.overrideSlabHealth("/foo/baz/quux", 0.75); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		path    string
		prefix  string
		sortBy  string
		sortDir string
		want    []api.ObjectMetadata
	}{
		{"/", "", "", "", []api.ObjectMetadata{{Name: "/FOO/", Size: 7, Health: 1}, {Name: "/fileś/", Size: 6, Health: 1}, {Name: "/foo/", Size: 10, Health: .5}, {Name: "/gab/", Size: 5, Health: 1}}},
		{"/foo/", "", "", "", []api.ObjectMetadata{{Name: "/foo/bar", Size: 1, Health: 1}, {Name: "/foo/bat", Size: 2, Health: 1}, {Name: "/foo/baz/", Size: 7, Health: .5}}},
		{"/foo/baz/", "", "", "", []api.ObjectMetadata{{Name: "/foo/baz/quux", Size: 3, Health: .75}, {Name: "/foo/baz/quuz", Size: 4, Health: .5}}},
		{"/gab/", "", "", "", []api.ObjectMetadata{{Name: "/gab/guub", Size: 5, Health: 1}}},
		{"/fileś/", "", "", "", []api.ObjectMetadata{{Name: "/fileś/śpecial", Size: 6, Health: 1}}},

		{"/", "f", "", "", []api.ObjectMetadata{{Name: "/fileś/", Size: 6, Health: 1}, {Name: "/foo/", Size: 10, Health: .5}}},
		{"/", "F", "", "", []api.ObjectMetadata{{Name: "/FOO/", Size: 7, Health: 1}}},
		{"/foo/", "fo", "", "", []api.ObjectMetadata{}},
		{"/foo/baz/", "quux", "", "", []api.ObjectMetadata{{Name: "/foo/baz/quux", Size: 3, Health: .75}}},
		{"/gab/", "/guub", "", "", []api.ObjectMetadata{}},

		{"/", "", "name", "ASC", []api.ObjectMetadata{{Name: "/FOO/", Size: 7, Health: 1}, {Name: "/fileś/", Size: 6, Health: 1}, {Name: "/foo/", Size: 10, Health: .5}, {Name: "/gab/", Size: 5, Health: 1}}},
		{"/", "", "name", "DESC", []api.ObjectMetadata{{Name: "/gab/", Size: 5, Health: 1}, {Name: "/foo/", Size: 10, Health: .5}, {Name: "/fileś/", Size: 6, Health: 1}, {Name: "/FOO/", Size: 7, Health: 1}}},

		{"/", "", "health", "ASC", []api.ObjectMetadata{{Name: "/foo/", Size: 10, Health: .5}, {Name: "/FOO/", Size: 7, Health: 1}, {Name: "/fileś/", Size: 6, Health: 1}, {Name: "/gab/", Size: 5, Health: 1}}},
		{"/", "", "health", "DESC", []api.ObjectMetadata{{Name: "/FOO/", Size: 7, Health: 1}, {Name: "/fileś/", Size: 6, Health: 1}, {Name: "/gab/", Size: 5, Health: 1}, {Name: "/foo/", Size: 10, Health: .5}}},
	}
	for _, test := range tests {
		got, _, err := ss.ObjectEntries(ctx, api.DefaultBucketName, test.path, test.prefix, test.sortBy, test.sortDir, "", 0, -1)
		if err != nil {
			t.Fatal(err)
		}
		assertMetadata(got)

		if !(len(got) == 0 && len(test.want) == 0) && !reflect.DeepEqual(got, test.want) {
			t.Fatalf("\nlist: %v\nprefix: %v\ngot: %v\nwant: %v", test.path, test.prefix, got, test.want)
		}

		for offset := 0; offset < len(test.want); offset++ {
			got, hasMore, err := ss.ObjectEntries(ctx, api.DefaultBucketName, test.path, test.prefix, test.sortBy, test.sortDir, "", offset, 1)
			if err != nil {
				t.Fatal(err)
			}
			assertMetadata(got)

			if len(got) != 1 || got[0] != test.want[offset] {
				t.Fatalf("\noffset: %v\nlist: %v\nprefix: %v\ngot: %v\nwant: %v", offset, test.path, test.prefix, got, test.want[offset])
			}

			moreRemaining := len(test.want)-offset-1 > 0
			if hasMore != moreRemaining {
				t.Fatalf("invalid value for hasMore (%t) at offset (%d) test (%+v)", hasMore, offset, test)
			}

			// make sure we stay within bounds
			if offset+1 >= len(test.want) {
				continue
			}

			got, hasMore, err = ss.ObjectEntries(ctx, api.DefaultBucketName, test.path, test.prefix, test.sortBy, test.sortDir, test.want[offset].Name, 0, 1)
			if err != nil {
				t.Fatal(err)
			}
			assertMetadata(got)

			if len(got) != 1 || got[0] != test.want[offset+1] {
				t.Fatalf("\noffset: %v\nlist: %v\nprefix: %v\nmarker: %v\ngot: %v\nwant: %v", offset+1, test.path, test.prefix, test.want[offset].Name, got, test.want[offset+1])
			}

			moreRemaining = len(test.want)-offset-2 > 0
			if hasMore != moreRemaining {
				t.Fatalf("invalid value for hasMore (%t) at marker (%s) test (%+v)", hasMore, test.want[offset].Name, test)
			}
		}
	}
}

// TestSearchObjects is a test for the SearchObjects method.
func TestSearchObjects(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()
	objects := []struct {
		path string
		size int64
	}{
		{"/foo/bar", 1},
		{"/foo/bat", 2},
		{"/foo/baz/quux", 3},
		{"/foo/baz/quuz", 4},
		{"/gab/guub", 5},
		{"/FOO/bar", 6}, // test case sensitivity
	}
	ctx := context.Background()
	for _, o := range objects {
		obj := newTestObject(frand.Intn(9) + 1)
		obj.Slabs = obj.Slabs[:1]
		obj.Slabs[0].Length = uint32(o.size)
		if err := ss.UpdateObject(ctx, api.DefaultBucketName, o.path, testContractSet, testETag, testMimeType, obj); err != nil {
			t.Fatal(err)
		}
	}
	tests := []struct {
		path string
		want []api.ObjectMetadata
	}{
		{"/", []api.ObjectMetadata{{Name: "/FOO/bar", Size: 6, Health: 1}, {Name: "/foo/bar", Size: 1, Health: 1}, {Name: "/foo/bat", Size: 2, Health: 1}, {Name: "/foo/baz/quux", Size: 3, Health: 1}, {Name: "/foo/baz/quuz", Size: 4, Health: 1}, {Name: "/gab/guub", Size: 5, Health: 1}}},
		{"/foo/b", []api.ObjectMetadata{{Name: "/foo/bar", Size: 1, Health: 1}, {Name: "/foo/bat", Size: 2, Health: 1}, {Name: "/foo/baz/quux", Size: 3, Health: 1}, {Name: "/foo/baz/quuz", Size: 4, Health: 1}}},
		{"o/baz/quu", []api.ObjectMetadata{{Name: "/foo/baz/quux", Size: 3, Health: 1}, {Name: "/foo/baz/quuz", Size: 4, Health: 1}}},
		{"uu", []api.ObjectMetadata{{Name: "/foo/baz/quux", Size: 3, Health: 1}, {Name: "/foo/baz/quuz", Size: 4, Health: 1}, {Name: "/gab/guub", Size: 5, Health: 1}}},
	}
	for _, test := range tests {
		got, err := ss.SearchObjects(ctx, api.DefaultBucketName, test.path, 0, -1)
		if err != nil {
			t.Fatal(err)
		}
		if !(len(got) == 0 && len(test.want) == 0) && !reflect.DeepEqual(got, test.want) {
			t.Errorf("\nkey: %v\ngot: %v\nwant: %v", test.path, got, test.want)
		}
		for offset := 0; offset < len(test.want); offset++ {
			got, err := ss.SearchObjects(ctx, api.DefaultBucketName, test.path, offset, 1)
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != 1 || got[0] != test.want[offset] {
				t.Errorf("\nkey: %v\ngot: %v\nwant: %v", test.path, got, test.want[offset])
			}
		}
	}
}

// TestUnhealthySlabs tests the functionality of UnhealthySlabs.
func TestUnhealthySlabs(t *testing.T) {
	// create db
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	// add 4 hosts
	hks, err := ss.addTestHosts(4)
	if err != nil {
		t.Fatal(err)
	}
	hk1, hk2, hk3, hk4 := hks[0], hks[1], hks[2], hks[3]

	// add 4 contracts
	fcids, _, err := ss.addTestContracts(hks)
	if err != nil {
		t.Fatal(err)
	}
	fcid1, fcid2, fcid3, fcid4 := fcids[0], fcids[1], fcids[2], fcids[3]

	// update the contract set
	goodContracts := []types.FileContractID{fcid1, fcid2, fcid3}
	if err := ss.SetContractSet(context.Background(), testContractSet, goodContracts); err != nil {
		t.Fatal(err)
	}

	// add an object
	obj := object.Object{
		Key: object.GenerateEncryptionKey(),
		Slabs: []object.SlabSlice{
			// good slab
			{
				Slab: object.Slab{
					Key:       object.GenerateEncryptionKey(),
					MinShards: 1,
					Shards: []object.Sector{
						newTestShard(hk1, fcid1, types.Hash256{1}),
						newTestShard(hk2, fcid2, types.Hash256{2}),
						newTestShard(hk3, fcid3, types.Hash256{3}),
					},
				},
			},
			// unhealthy slab - hk4 is bad (1/3)
			{
				Slab: object.Slab{
					Key:       object.GenerateEncryptionKey(),
					MinShards: 1,
					Shards: []object.Sector{
						newTestShard(hk1, fcid1, types.Hash256{4}),
						newTestShard(hk2, fcid2, types.Hash256{5}),
						newTestShard(hk4, fcid4, types.Hash256{6}),
					},
				},
			},
			// unhealthy slab - hk4 is bad (2/3)
			{
				Slab: object.Slab{
					Key:       object.GenerateEncryptionKey(),
					MinShards: 1,
					Shards: []object.Sector{
						newTestShard(hk1, fcid1, types.Hash256{7}),
						newTestShard(hk4, fcid4, types.Hash256{8}),
						newTestShard(hk4, fcid4, types.Hash256{9}),
					},
				},
			},
			// unhealthy slab - hk5 is deleted (1/3)
			{
				Slab: object.Slab{
					Key:       object.GenerateEncryptionKey(),
					MinShards: 1,
					Shards: []object.Sector{
						newTestShard(hk1, fcid1, types.Hash256{10}),
						newTestShard(hk2, fcid2, types.Hash256{11}),
						newTestShard(types.PublicKey{5}, types.FileContractID{5}, types.Hash256{12}),
					},
				},
			},
			// unhealthy slab - h1 is reused
			{
				Slab: object.Slab{
					Key:       object.GenerateEncryptionKey(),
					MinShards: 1,
					Shards: []object.Sector{
						newTestShard(hk1, fcid1, types.Hash256{13}),
						newTestShard(hk1, fcid4, types.Hash256{14}),
						newTestShard(hk1, fcid4, types.Hash256{15}),
					},
				},
			},
			// lost slab - no good pieces (0/3)
			{
				Slab: object.Slab{
					Key:       object.GenerateEncryptionKey(),
					MinShards: 1,
					Shards: []object.Sector{
						newTestShard(hk1, fcid1, types.Hash256{16}),
						newTestShard(hk2, fcid2, types.Hash256{17}),
						newTestShard(hk3, fcid3, types.Hash256{18}),
					},
				},
			},
		},
	}

	ctx := context.Background()
	if err := ss.UpdateObject(ctx, api.DefaultBucketName, "foo", testContractSet, testETag, testMimeType, obj); err != nil {
		t.Fatal(err)
	}

	if err := ss.RefreshHealth(context.Background()); err != nil {
		t.Fatal(err)
	}
	slabs, err := ss.UnhealthySlabs(ctx, 0.99, testContractSet, -1)
	if err != nil {
		t.Fatal(err)
	}
	if len(slabs) != 4 {
		t.Fatalf("unexpected amount of slabs to migrate, %v!=4", len(slabs))
	}

	expected := []api.UnhealthySlab{
		{Key: obj.Slabs[2].Key, Health: 0},
		{Key: obj.Slabs[4].Key, Health: 0},
		{Key: obj.Slabs[1].Key, Health: 0.5},
		{Key: obj.Slabs[3].Key, Health: 0.5},
	}
	if !reflect.DeepEqual(slabs, expected) {
		t.Fatal("slabs are not returned in the correct order")
	}

	if err := ss.RefreshHealth(context.Background()); err != nil {
		t.Fatal(err)
	}
	slabs, err = ss.UnhealthySlabs(ctx, 0.49, testContractSet, -1)
	if err != nil {
		t.Fatal(err)
	}
	if len(slabs) != 2 {
		t.Fatalf("unexpected amount of slabs to migrate, %v!=2", len(slabs))
	}

	expected = []api.UnhealthySlab{
		{Key: obj.Slabs[2].Key, Health: 0},
		{Key: obj.Slabs[4].Key, Health: 0},
	}
	if !reflect.DeepEqual(slabs, expected) {
		t.Fatal("slabs are not returned in the correct order", slabs, expected)
	}

	// Fetch unhealthy slabs again but for different contract set.
	if err := ss.RefreshHealth(context.Background()); err != nil {
		t.Fatal(err)
	}
	slabs, err = ss.UnhealthySlabs(ctx, 0.49, "foo", -1)
	if err != nil {
		t.Fatal(err)
	}
	if len(slabs) != 0 {
		t.Fatal("expected no slabs to migrate", len(slabs))
	}
}

func TestUnhealthySlabsNegHealth(t *testing.T) {
	// create db
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	// add a host
	hks, err := ss.addTestHosts(1)
	if err != nil {
		t.Fatal(err)
	}
	hk1 := hks[0]

	// add a contract
	fcids, _, err := ss.addTestContracts(hks)
	if err != nil {
		t.Fatal(err)
	}
	fcid1 := fcids[0]

	// add it to the contract set
	if err := ss.SetContractSet(context.Background(), testContractSet, fcids); err != nil {
		t.Fatal(err)
	}

	// create an object
	obj := object.Object{
		Key: object.GenerateEncryptionKey(),
		Slabs: []object.SlabSlice{
			{
				Slab: object.Slab{
					Key:       object.GenerateEncryptionKey(),
					MinShards: 2,
					Shards: []object.Sector{
						newTestShard(hk1, fcid1, types.Hash256{1}),
						newTestShard(hk1, fcid1, types.Hash256{2}),
					},
				},
			},
		},
	}

	// add the object
	ctx := context.Background()
	if err := ss.UpdateObject(ctx, api.DefaultBucketName, "foo", testContractSet, testETag, testMimeType, obj); err != nil {
		t.Fatal(err)
	}

	// assert it's unhealthy
	if err := ss.RefreshHealth(context.Background()); err != nil {
		t.Fatal(err)
	}
	slabs, err := ss.UnhealthySlabs(ctx, 0.99, testContractSet, -1)
	if err != nil {
		t.Fatal(err)
	}
	if len(slabs) != 1 {
		t.Fatalf("unexpected amount of slabs to migrate, %v!=1", len(slabs))
	}
}

func TestUnhealthySlabsNoContracts(t *testing.T) {
	// create db
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	// add a host
	hks, err := ss.addTestHosts(1)
	if err != nil {
		t.Fatal(err)
	}
	hk1 := hks[0]

	// add a contract
	fcids, _, err := ss.addTestContracts(hks)
	if err != nil {
		t.Fatal(err)
	}
	fcid1 := fcids[0]

	// add it to the contract set
	if err := ss.SetContractSet(context.Background(), testContractSet, fcids); err != nil {
		t.Fatal(err)
	}

	// create an object
	obj := object.Object{
		Key: object.GenerateEncryptionKey(),
		Slabs: []object.SlabSlice{
			{
				Slab: object.Slab{
					Key:       object.GenerateEncryptionKey(),
					MinShards: 1,
					Shards:    newTestShards(hk1, fcid1, types.Hash256{1}),
				},
			},
		},
	}

	// add the object
	ctx := context.Background()
	if err := ss.UpdateObject(ctx, api.DefaultBucketName, "foo", testContractSet, testETag, testMimeType, obj); err != nil {
		t.Fatal(err)
	}

	// assert it's healthy
	if err := ss.RefreshHealth(context.Background()); err != nil {
		t.Fatal(err)
	}
	slabs, err := ss.UnhealthySlabs(ctx, 0.99, testContractSet, -1)
	if err != nil {
		t.Fatal(err)
	}
	if len(slabs) != 0 {
		t.Fatalf("unexpected amount of slabs to migrate, %v!=0", len(slabs))
	}

	// delete the sector - we manually invalidate the slabs for the contract
	// before deletion.
	err = invalidateSlabHealthByFCID(context.Background(), ss.db, []fileContractID{fileContractID(fcid1)})
	if err != nil {
		t.Fatal(err)
	}
	if err := ss.db.Table("contract_sectors").Where("TRUE").Delete(&dbContractSector{}).Error; err != nil {
		t.Fatal(err)
	}

	// assert it's unhealthy
	if err := ss.RefreshHealth(context.Background()); err != nil {
		t.Fatal(err)
	}
	slabs, err = ss.UnhealthySlabs(ctx, 0.99, testContractSet, -1)
	if err != nil {
		t.Fatal(err)
	}
	if len(slabs) != 1 {
		t.Fatalf("unexpected amount of slabs to migrate, %v!=1", len(slabs))
	}
}

// TestUnhealthySlabs tests the functionality of UnhealthySlabs on slabs that
// don't have any redundancy.
func TestUnhealthySlabsNoRedundancy(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	// add 3 hosts
	hks, err := ss.addTestHosts(4)
	if err != nil {
		t.Fatal(err)
	}
	hk1, hk2, hk3 := hks[0], hks[1], hks[2]

	// add 4 contracts
	fcids, _, err := ss.addTestContracts(hks)
	if err != nil {
		t.Fatal(err)
	}
	fcid1, fcid2, fcid3 := fcids[0], fcids[1], fcids[2]

	// select the first two contracts as good contracts
	goodContracts := []types.FileContractID{fcid1, fcid2}
	if err := ss.SetContractSet(context.Background(), testContractSet, goodContracts); err != nil {
		t.Fatal(err)
	}

	// add an object
	obj := object.Object{
		Key: object.GenerateEncryptionKey(),
		Slabs: []object.SlabSlice{
			// hk1 is good so this slab should have full health.
			{
				Slab: object.Slab{
					Key:       object.GenerateEncryptionKey(),
					MinShards: 1,
					Shards:    newTestShards(hk1, fcid1, types.Hash256{1}),
				},
			},
			// hk4 is bad so this slab should have no health.
			{
				Slab: object.Slab{
					Key:       object.GenerateEncryptionKey(),
					MinShards: 2,
					Shards: []object.Sector{
						newTestShard(hk2, fcid2, types.Hash256{2}),
						newTestShard(hk3, fcid3, types.Hash256{4}),
					},
				},
			},
		},
	}

	ctx := context.Background()
	if err := ss.UpdateObject(ctx, api.DefaultBucketName, "foo", testContractSet, testETag, testMimeType, obj); err != nil {
		t.Fatal(err)
	}

	if err := ss.RefreshHealth(context.Background()); err != nil {
		t.Fatal(err)
	}
	slabs, err := ss.UnhealthySlabs(ctx, 0.99, testContractSet, -1)
	if err != nil {
		t.Fatal(err)
	}
	if len(slabs) != 1 {
		t.Fatalf("unexpected amount of slabs to migrate, %v!=1", len(slabs))
	}

	expected := []api.UnhealthySlab{
		{Key: obj.Slabs[1].Slab.Key, Health: -1},
	}
	if !reflect.DeepEqual(slabs, expected) {
		t.Fatal("slabs are not returned in the correct order")
	}
}

// TestContractSectors is a test for the contract_sectors join table. It
// verifies that deleting contracts or sectors also cleans up the join table.
func TestContractSectors(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	// Create a host, contract and sector to upload to that host into the
	// given contract.
	hk1 := types.PublicKey{1}
	fcid1 := types.FileContractID{1}
	err := ss.addTestHost(hk1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = ss.addTestContract(fcid1, hk1)
	if err != nil {
		t.Fatal(err)
	}
	sectorGood := newTestShard(hk1, fcid1, types.Hash256{1})

	// Create object.
	obj := object.Object{
		Key: object.GenerateEncryptionKey(),
		Slabs: []object.SlabSlice{
			{
				Slab: object.Slab{
					Key:       object.GenerateEncryptionKey(),
					MinShards: 1,
					Shards: []object.Sector{
						sectorGood,
					},
				},
			},
		},
	}
	ctx := context.Background()
	if err := ss.UpdateObject(ctx, api.DefaultBucketName, "foo", testContractSet, testETag, testMimeType, obj); err != nil {
		t.Fatal(err)
	}

	// Delete the contract.
	err = ss.ArchiveContract(ctx, fcid1, api.ContractArchivalReasonRemoved)
	if err != nil {
		t.Fatal(err)
	}

	// Check the join table. Should be empty.
	var css []dbContractSector
	if err := ss.db.Find(&css).Error; err != nil {
		t.Fatal(err)
	}
	if len(css) != 0 {
		t.Fatal("table should be empty", len(css))
	}

	// Add the contract back.
	_, err = ss.addTestContract(fcid1, hk1)
	if err != nil {
		t.Fatal(err)
	}

	// Add the object again.
	if err := ss.UpdateObject(ctx, api.DefaultBucketName, "foo", testContractSet, testETag, testMimeType, obj); err != nil {
		t.Fatal(err)
	}

	// Delete the object.
	if err := ss.RemoveObject(ctx, api.DefaultBucketName, "foo"); err != nil {
		t.Fatal(err)
	}

	// Delete the sector.
	if err := ss.db.Delete(&dbSector{Model: Model{ID: 1}}).Error; err != nil {
		t.Fatal(err)
	}
	if err := ss.db.Find(&css).Error; err != nil {
		t.Fatal(err)
	}
	if len(css) != 0 {
		t.Fatal("table should be empty")
	}
}

// TestUpdateSlab verifies the functionality of UpdateSlab.
func TestUpdateSlab(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	// add 3 hosts
	hks, err := ss.addTestHosts(3)
	if err != nil {
		t.Fatal(err)
	}
	hk1, hk2, hk3 := hks[0], hks[1], hks[2]

	// add 3 contracts
	fcids, _, err := ss.addTestContracts(hks)
	if err != nil {
		t.Fatal(err)
	}
	fcid1, fcid2, fcid3 := fcids[0], fcids[1], fcids[2]

	// add an object
	obj := object.Object{
		Key: object.GenerateEncryptionKey(),
		Slabs: []object.SlabSlice{
			{
				Slab: object.Slab{
					Key:       object.GenerateEncryptionKey(),
					MinShards: 1,
					Shards: []object.Sector{
						newTestShard(hk1, fcid1, types.Hash256{1}),
						newTestShard(hk2, fcid2, types.Hash256{2}),
					},
				},
			},
		},
	}
	ctx := context.Background()
	if err := ss.UpdateObject(ctx, api.DefaultBucketName, "foo", testContractSet, testETag, testMimeType, obj); err != nil {
		t.Fatal(err)
	}

	// extract the slab key
	key, err := obj.Slabs[0].Key.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	// helper to fetch a slab from the database
	fetchSlab := func() (slab dbSlab) {
		t.Helper()
		if err = ss.db.
			Where(&dbSlab{Key: key}).
			Preload("Shards.Contracts").
			Take(&slab).
			Error; err != nil {
			t.Fatal(err)
		}
		return
	}

	// helper to extract the FCID from a list of contracts
	contractIds := func(contracts []dbContract) (ids []fileContractID) {
		for _, c := range contracts {
			ids = append(ids, fileContractID(c.FCID))
		}
		return
	}

	// fetch inserted slab
	inserted := fetchSlab()

	// assert both sectors were upload to one contract/host
	for i := 0; i < 2; i++ {
		if cids := contractIds(inserted.Shards[i].Contracts); len(cids) != 1 {
			t.Fatalf("sector %d was uploaded to unexpected amount of contracts, %v!=1", i+1, len(cids))
		} else if inserted.Shards[i].LatestHost != publicKey(hks[i]) {
			t.Fatalf("sector %d was uploaded to unexpected amount of hosts, %v!=1", i+1, len(hks))
		}
	}

	// select contracts h1 and h3 as good contracts (h2 is bad)
	goodContracts := []types.FileContractID{fcid1, fcid3}
	if err := ss.SetContractSet(ctx, testContractSet, goodContracts); err != nil {
		t.Fatal(err)
	}

	// fetch slabs for migration and assert there is only one
	if err := ss.RefreshHealth(context.Background()); err != nil {
		t.Fatal(err)
	}
	toMigrate, err := ss.UnhealthySlabs(ctx, 0.99, testContractSet, -1)
	if err != nil {
		t.Fatal(err)
	}
	if len(toMigrate) != 1 {
		t.Fatal("unexpected number of slabs to migrate", len(toMigrate))
	}

	// migrate the sector from h2 to h3
	slab := obj.Slabs[0].Slab
	slab.Shards[1] = newTestShard(hk3, fcid3, types.Hash256{2})

	// update the slab to reflect the migration
	err = ss.UpdateSlab(ctx, slab, testContractSet)
	if err != nil {
		t.Fatal(err)
	}

	// fetch updated slab
	updated := fetchSlab()

	// assert the first sector is still only on one host, also assert it's h1
	if cids := contractIds(updated.Shards[0].Contracts); len(cids) != 1 {
		t.Fatalf("sector 1 was uploaded to unexpected amount of contracts, %v!=1", len(cids))
	} else if types.FileContractID(cids[0]) != fcid1 {
		t.Fatal("sector 1 was uploaded to unexpected contract", cids[0])
	} else if updated.Shards[0].LatestHost != publicKey(hks[0]) {
		t.Fatal("host key was invalid", updated.Shards[0].LatestHost, publicKey(hks[0]))
	} else if hks[0] != hk1 {
		t.Fatal("sector 1 was uploaded to unexpected host", hks[0])
	}

	// assert the second sector however is uploaded to two hosts, assert it's h2 and h3
	if cids := contractIds(updated.Shards[1].Contracts); len(cids) != 2 {
		t.Fatalf("sector 1 was uploaded to unexpected amount of contracts, %v!=2", len(cids))
	} else if types.FileContractID(cids[0]) != fcid2 || types.FileContractID(cids[1]) != fcid3 {
		t.Fatal("sector 1 was uploaded to unexpected contracts", cids[0], cids[1])
	} else if updated.Shards[0].LatestHost != publicKey(hks[0]) {
		t.Fatal("host key was invalid", updated.Shards[0].LatestHost, publicKey(hks[0]))
	}

	// assert there's still only one entry in the dbslab table
	var cnt int64
	if err := ss.db.Model(&dbSlab{}).Count(&cnt).Error; err != nil {
		t.Fatal(err)
	} else if cnt != 1 {
		t.Fatalf("unexpected number of entries in dbslab, %v != 1", cnt)
	}

	// fetch slabs for migration and assert there are none left
	if err := ss.RefreshHealth(context.Background()); err != nil {
		t.Fatal(err)
	}
	toMigrate, err = ss.UnhealthySlabs(ctx, 0.99, testContractSet, -1)
	if err != nil {
		t.Fatal(err)
	}
	if len(toMigrate) != 0 {
		t.Fatal("unexpected number of slabs to migrate", len(toMigrate))
	}

	if obj, err := ss.dbObject("foo"); err != nil {
		t.Fatal(err)
	} else if len(obj.Slabs) != 1 {
		t.Fatalf("unexpected number of slabs, %v != 1", len(obj.Slabs))
	} else if obj.Slabs[0].ID != updated.ID {
		t.Fatalf("unexpected slab, %v != %v", obj.Slabs[0].ID, updated.ID)
	}

	// update the slab to change its contract set.
	if err := ss.SetContractSet(ctx, "other", nil); err != nil {
		t.Fatal(err)
	}
	err = ss.UpdateSlab(ctx, slab, "other")
	if err != nil {
		t.Fatal(err)
	}
	var s dbSlab
	if err := ss.db.Model(&dbSlab{}).
		Joins("DBContractSet").
		Preload("Shards").
		Where("key = ?", key).
		Take(&s).
		Error; err != nil {
		t.Fatal(err)
	} else if s.DBContractSet.Name != "other" {
		t.Fatal("contract set was not updated")
	}
}

func newTestObject(slabs int) object.Object {
	obj := object.Object{}

	obj.Slabs = make([]object.SlabSlice, slabs)
	obj.Key = object.GenerateEncryptionKey()
	for i := range obj.Slabs {
		n := uint8(frand.Uint64n(10) + 1)
		offset := uint32(frand.Uint64n(1 << 22))
		length := offset + uint32(frand.Uint64n(1<<22))
		obj.Slabs[i] = object.SlabSlice{
			Slab: object.Slab{
				Key:       object.GenerateEncryptionKey(),
				MinShards: n,
				Shards:    make([]object.Sector, n*2),
			},
			Offset: offset,
			Length: length,
		}
		for j := range obj.Slabs[i].Shards {
			var fcid types.FileContractID
			frand.Read(fcid[:])
			obj.Slabs[i].Shards[j] = newTestShard(frand.Entropy256(), fcid, frand.Entropy256())
		}
	}
	return obj
}

// TestRecordContractSpending tests RecordContractSpending.
func TestRecordContractSpending(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	// Create a host for the contract.
	hk := types.GeneratePrivateKey().PublicKey()
	err := ss.addTestHost(hk)
	if err != nil {
		t.Fatal(err)
	}

	// Add an announcement.
	err = ss.insertTestAnnouncement(hk, hostdb.Announcement{NetAddress: "address"})
	if err != nil {
		t.Fatal(err)
	}

	fcid := types.FileContractID{1, 1, 1, 1, 1}
	cm, err := ss.addTestContract(fcid, hk)
	if err != nil {
		t.Fatal(err)
	}
	if cm.Spending != (api.ContractSpending{}) {
		t.Fatal("spending should be all 0")
	}

	// Record some spending.
	expectedSpending := api.ContractSpending{
		Uploads:     types.Siacoins(1),
		Downloads:   types.Siacoins(2),
		FundAccount: types.Siacoins(3),
		Deletions:   types.Siacoins(4),
		SectorRoots: types.Siacoins(5),
	}
	err = ss.RecordContractSpending(context.Background(), []api.ContractSpendingRecord{
		// non-existent contract
		{
			ContractID: types.FileContractID{1, 2, 3},
		},
		// valid spending
		{
			ContractID:       fcid,
			ContractSpending: expectedSpending,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	cm2, err := ss.Contract(context.Background(), fcid)
	if err != nil {
		t.Fatal(err)
	}
	if cm2.Spending != expectedSpending {
		t.Fatal("invalid spending", cm2.Spending, expectedSpending)
	}

	// Record the same spending again.
	err = ss.RecordContractSpending(context.Background(), []api.ContractSpendingRecord{
		{
			ContractID:       fcid,
			ContractSpending: expectedSpending,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	expectedSpending = expectedSpending.Add(expectedSpending)
	cm3, err := ss.Contract(context.Background(), fcid)
	if err != nil {
		t.Fatal(err)
	}
	if cm3.Spending != expectedSpending {
		t.Fatal("invalid spending")
	}
}

// TestRenameObjects is a unit test for RenameObject and RenameObjects.
func TestRenameObjects(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	// Create a few objects.
	objects := []string{
		"/fileś/1a",
		"/fileś/2a",
		"/fileś/3a",
		"/fileś/CASE",
		"/fileś/case",
		"/fileś/dir/1b",
		"/fileś/dir/2b",
		"/fileś/dir/3b",
		"/foo",
		"/bar",
		"/baz",
		"/baz2",
		"/baz3",
	}
	ctx := context.Background()
	for _, path := range objects {
		obj := newTestObject(1)
		if err := ss.UpdateObject(ctx, api.DefaultBucketName, path, testContractSet, testETag, testMimeType, obj); err != nil {
			t.Fatal(err)
		}
	}

	// Try renaming objects that don't exist.
	if err := ss.RenameObject(ctx, api.DefaultBucketName, "/fileś", "/fileś2", false); !errors.Is(err, api.ErrObjectNotFound) {
		t.Fatal(err)
	}
	if err := ss.RenameObjects(ctx, api.DefaultBucketName, "/fileś1", "/fileś2", false); !errors.Is(err, api.ErrObjectNotFound) {
		t.Fatal(err)
	}

	// Perform some renames.
	if err := ss.RenameObjects(ctx, api.DefaultBucketName, "/fileś/dir/", "/fileś/", false); err != nil {
		t.Fatal(err)
	}
	if err := ss.RenameObject(ctx, api.DefaultBucketName, "/foo", "/fileś/foo", false); err != nil {
		t.Fatal(err)
	}
	if err := ss.RenameObject(ctx, api.DefaultBucketName, "/bar", "/fileś/bar", false); err != nil {
		t.Fatal(err)
	}
	if err := ss.RenameObject(ctx, api.DefaultBucketName, "/baz", "/fileś/baz", false); err != nil {
		t.Fatal(err)
	}
	if err := ss.RenameObjects(ctx, api.DefaultBucketName, "/fileś/case", "/fileś/case1", false); err != nil {
		t.Fatal(err)
	}
	if err := ss.RenameObjects(ctx, api.DefaultBucketName, "/fileś/CASE", "/fileś/case2", false); err != nil {
		t.Fatal(err)
	}
	if err := ss.RenameObjects(ctx, api.DefaultBucketName, "/baz2", "/fileś/baz", false); !errors.Is(err, api.ErrObjectExists) {
		t.Fatal(err)
	} else if err := ss.RenameObjects(ctx, api.DefaultBucketName, "/baz2", "/fileś/baz", true); err != nil {
		t.Fatal(err)
	}
	if err := ss.RenameObject(ctx, api.DefaultBucketName, "/baz3", "/fileś/baz", false); !errors.Is(err, api.ErrObjectExists) {
		t.Fatal(err)
	} else if err := ss.RenameObject(ctx, api.DefaultBucketName, "/baz3", "/fileś/baz", true); err != nil {
		t.Fatal(err)
	}

	// Paths after.
	objectsAfter := []string{
		"/fileś/1a",
		"/fileś/2a",
		"/fileś/3a",
		"/fileś/1b",
		"/fileś/2b",
		"/fileś/3b",
		"/fileś/foo",
		"/fileś/bar",
		"/fileś/baz",
		"/fileś/case1",
		"/fileś/case2",
	}
	objectsAfterMap := make(map[string]struct{})
	for _, path := range objectsAfter {
		objectsAfterMap[path] = struct{}{}
	}

	// Assert that number of objects matches.
	objs, err := ss.SearchObjects(ctx, api.DefaultBucketName, "/", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(objs) != len(objectsAfter) {
		t.Fatal("unexpected number of objects", len(objs), len(objectsAfter))
	}

	// Assert paths are correct.
	for _, obj := range objs {
		if _, exists := objectsAfterMap[obj.Name]; !exists {
			t.Fatal("unexpected path", obj.Name)
		}
	}
}

// TestObjectsStats is a unit test for ObjectsStats.
func TestObjectsStats(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	// Fetch stats on clean database.
	info, err := ss.ObjectsStats(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(info, api.ObjectsStatsResponse{}) {
		t.Fatal("unexpected stats", info)
	}

	// Create a few objects of different size.
	var objectsSize uint64
	var sectorsSize uint64
	for i := 0; i < 2; i++ {
		obj := newTestObject(1)
		objectsSize += uint64(obj.TotalSize())
		for _, slab := range obj.Slabs {
			sectorsSize += uint64(len(slab.Shards) * rhpv2.SectorSize)

			for _, s := range slab.Shards {
				for hpk, fcids := range s.Contracts {
					if err := ss.addTestHost(hpk); err != nil {
						t.Fatal(err)
					}
					for _, fcid := range fcids {
						_, err := ss.addTestContract(fcid, hpk)
						if err != nil {
							t.Fatal(err)
						}
					}
				}
			}
		}

		key := hex.EncodeToString(frand.Bytes(32))
		err := ss.UpdateObject(context.Background(), api.DefaultBucketName, key, testContractSet, testETag, testMimeType, obj)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Get all entries in contract_sectors and store them again with a different
	// contract id. This should cause the uploaded size to double.
	var contractSectors []dbContractSector
	err = ss.db.Find(&contractSectors).Error
	if err != nil {
		t.Fatal(err)
	}
	var newContractID types.FileContractID
	frand.Read(newContractID[:])
	_, err = ss.addTestContract(newContractID, types.PublicKey{})
	if err != nil {
		t.Fatal(err)
	}
	newContract, err := ss.contract(context.Background(), fileContractID(newContractID))
	if err != nil {
		t.Fatal(err)
	}
	for _, contractSector := range contractSectors {
		contractSector.DBContractID = newContract.ID
		err = ss.db.Create(&contractSector).Error
		if err != nil {
			t.Fatal(err)
		}
	}

	// Check sizes.
	info, err = ss.ObjectsStats(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if info.TotalObjectsSize != objectsSize {
		t.Fatal("wrong size", info.TotalObjectsSize, objectsSize)
	}
	if info.TotalSectorsSize != sectorsSize {
		t.Fatal("wrong size", info.TotalSectorsSize, sectorsSize)
	}
	if info.TotalUploadedSize != sectorsSize*2 {
		t.Fatal("wrong size", info.TotalUploadedSize, sectorsSize*2)
	}
	if info.NumObjects != 2 {
		t.Fatal("wrong number of objects", info.NumObjects, 2)
	}
}

func TestPartialSlab(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	// create 2 hosts
	hks, err := ss.addTestHosts(2)
	if err != nil {
		t.Fatal(err)
	}
	hk1, hk2 := hks[0], hks[1]

	// create 2 contracts
	fcids, _, err := ss.addTestContracts(hks)
	if err != nil {
		t.Fatal(err)
	}
	fcid1, fcid2 := fcids[0], fcids[1]

	// helper function to assert buffer stats returned by SlabBuffers.
	assertBuffer := func(name string, size int64, complete, locked bool) {
		t.Helper()
		buffers, err := ss.SlabBuffers(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if len(buffers) == 0 {
			t.Fatal("no buffers")
		}
		var buf api.SlabBuffer
		for _, b := range buffers {
			if b.Filename == name {
				buf = b
				break
			}
		}
		if buf == (api.SlabBuffer{}) {
			t.Fatal("buffer not found for name", name)
		}
		if buf.ContractSet != testContractSet {
			t.Fatal("wrong contract set", buf.ContractSet, testContractSet)
		}
		if buf.Filename != name {
			t.Fatal("wrong filename", buf.Filename, name)
		}
		if buf.MaxSize != int64(bufferedSlabSize(1)) {
			t.Fatal("wrong max size", buf.MaxSize, bufferedSlabSize(1))
		}
		if buf.Size != size {
			t.Fatal("wrong size", buf.Size, size)
		}
		if buf.Complete != complete {
			t.Fatal("wrong complete", buf.Complete, complete)
		}
		if buf.Locked != locked {
			t.Fatal("wrong locked", buf.Locked, locked)
		}
	}

	// prepare the data for 3 partial slabs. The first one is very small. The
	// second one almost fills a buffer except for 1 byte. The third one spans 2
	// buffers.
	fullSlabSize := bufferedSlabSize(1)
	slab1Data := []byte{1, 2, 3, 4}
	slab2Data := frand.Bytes(int(fullSlabSize) - len(slab1Data) - 1) // leave 1 byte
	slab3Data := []byte{5, 6}                                        // 1 byte more than fits in the slab

	// Add the first slab.
	ctx := context.Background()
	slabs, bufferSize, err := ss.AddPartialSlab(ctx, slab1Data, 1, 2, testContractSet)
	if err != nil {
		t.Fatal(err)
	}
	if len(slabs) != 1 {
		t.Fatal("expected 1 slab to be created", len(slabs))
	}
	if slabs[0].Length != uint32(len(slab1Data)) || slabs[0].Offset != 0 {
		t.Fatal("wrong offset/length", slabs[0].Offset, slabs[0].Length)
	} else if bufferSize != rhpv2.SectorSize {
		t.Fatal("unexpected buffer size", bufferSize)
	}
	data, err := ss.FetchPartialSlab(ctx, slabs[0].Key, slabs[0].Offset, slabs[0].Length)
	if err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(data, slab1Data) {
		t.Fatal("wrong data")
	}

	var buffer dbBufferedSlab
	sk, _ := slabs[0].Key.MarshalBinary()
	if err := ss.db.Joins("DBSlab").Take(&buffer, "DBSlab.key = ?", secretKey(sk)).Error; err != nil {
		t.Fatal(err)
	}
	if buffer.Filename == "" {
		t.Fatal("empty filename")
	}
	buffer1Name := buffer.Filename
	assertBuffer(buffer1Name, 4, false, false)

	// Use the added partial slab to create an object.
	testObject := func(partialSlabs []object.PartialSlab) object.Object {
		return object.Object{
			Key: object.GenerateEncryptionKey(),
			Slabs: []object.SlabSlice{
				{
					Slab: object.Slab{
						Health:    1.0,
						Key:       object.GenerateEncryptionKey(),
						MinShards: 1,
						Shards: []object.Sector{
							newTestShard(hk1, fcid1, types.Hash256{1}),
							newTestShard(hk2, fcid2, types.Hash256{2}),
						},
					},
					Offset: 0,
					Length: rhpv2.SectorSize,
				},
			},
			PartialSlabs: slabs,
		}
	}
	obj := testObject(slabs)
	err = ss.UpdateObject(context.Background(), api.DefaultBucketName, "key", testContractSet, testETag, testMimeType, obj)
	if err != nil {
		t.Fatal(err)
	}
	fetched, err := ss.Object(context.Background(), api.DefaultBucketName, "key")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(obj, fetched.Object) {
		t.Fatal("mismatch", cmp.Diff(obj, fetched.Object))
	}

	// Add the second slab.
	slabs, bufferSize, err = ss.AddPartialSlab(ctx, slab2Data, 1, 2, testContractSet)
	if err != nil {
		t.Fatal(err)
	}
	if len(slabs) != 1 {
		t.Fatal("expected 1 slab to be created", len(slabs))
	}
	if slabs[0].Length != uint32(len(slab2Data)) || slabs[0].Offset != uint32(len(slab1Data)) {
		t.Fatal("wrong offset/length", slabs[0].Offset, slabs[0].Length)
	} else if bufferSize != rhpv2.SectorSize {
		t.Fatal("unexpected buffer size", bufferSize)
	}
	data, err = ss.FetchPartialSlab(ctx, slabs[0].Key, slabs[0].Offset, slabs[0].Length)
	if err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(data, slab2Data) {
		t.Fatal("wrong data")
	}
	buffer = dbBufferedSlab{}
	sk, _ = slabs[0].Key.MarshalBinary()
	if err := ss.db.Joins("DBSlab").Take(&buffer, "DBSlab.key = ?", secretKey(sk)).Error; err != nil {
		t.Fatal(err)
	}
	assertBuffer(buffer1Name, 4194303, false, false)

	// Create an object again.
	obj2 := testObject(slabs)
	err = ss.UpdateObject(context.Background(), api.DefaultBucketName, "key2", testContractSet, testETag, testMimeType, obj2)
	if err != nil {
		t.Fatal(err)
	}
	fetched, err = ss.Object(context.Background(), api.DefaultBucketName, "key2")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(obj2, fetched.Object) {
		t.Fatal("mismatch", cmp.Diff(obj2, fetched.Object))
	}

	// Add third slab.
	slabs, bufferSize, err = ss.AddPartialSlab(ctx, slab3Data, 1, 2, testContractSet)
	if err != nil {
		t.Fatal(err)
	}
	if len(slabs) != 2 {
		t.Fatal("expected 2 slabs to be created", len(slabs))
	}
	if slabs[0].Length != 1 || slabs[0].Offset != uint32(len(slab1Data)+len(slab2Data)) {
		t.Fatal("wrong offset/length", slabs[0].Offset, slabs[0].Length)
	}
	if slabs[1].Length != uint32(len(slab3Data)-1) || slabs[1].Offset != 0 {
		t.Fatal("wrong offset/length", slabs[0].Offset, slabs[0].Length)
	}
	if bufferSize != 2*rhpv2.SectorSize {
		t.Fatal("unexpected buffer size", bufferSize)
	}
	if data1, err := ss.FetchPartialSlab(ctx, slabs[0].Key, slabs[0].Offset, slabs[0].Length); err != nil {
		t.Fatal(err)
	} else if data2, err := ss.FetchPartialSlab(ctx, slabs[1].Key, slabs[1].Offset, slabs[1].Length); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(slab3Data, append(data1, data2...)) {
		t.Fatal("wrong data")
	}
	buffer = dbBufferedSlab{}
	sk, _ = slabs[0].Key.MarshalBinary()
	if err := ss.db.Joins("DBSlab").Take(&buffer, "DBSlab.key = ?", secretKey(sk)).Error; err != nil {
		t.Fatal(err)
	}
	assertBuffer(buffer1Name, rhpv2.SectorSize, true, false)
	buffer = dbBufferedSlab{}
	sk, _ = slabs[1].Key.MarshalBinary()
	if err := ss.db.Joins("DBSlab").Take(&buffer, "DBSlab.key = ?", secretKey(sk)).Error; err != nil {
		t.Fatal(err)
	}
	buffer2Name := buffer.Filename
	assertBuffer(buffer2Name, 1, false, false)

	// Create an object again.
	obj3 := testObject(slabs)
	err = ss.UpdateObject(context.Background(), api.DefaultBucketName, "key3", testContractSet, testETag, testMimeType, obj3)
	if err != nil {
		t.Fatal(err)
	}
	fetched, err = ss.Object(context.Background(), api.DefaultBucketName, "key3")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(obj3, fetched.Object) {
		t.Fatal("mismatch", cmp.Diff(obj3, fetched.Object))
	}

	// Fetch the buffer for uploading
	packedSlabs, err := ss.PackedSlabsForUpload(ctx, time.Hour, 1, 2, testContractSet, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(packedSlabs) != 1 {
		t.Fatal("expected 1 slab to be returned", len(packedSlabs))
	}
	assertBuffer(buffer1Name, rhpv2.SectorSize, true, true)
	assertBuffer(buffer2Name, 1, false, false)

	var foo []dbBufferedSlab
	if err := ss.db.Find(&foo).Error; err != nil {
		t.Fatal(err)
	}
	buffer = dbBufferedSlab{}
	if err := ss.db.Take(&buffer, "id = ?", packedSlabs[0].BufferID).Error; err != nil {
		t.Fatal(err)
	}

	// Mark slab as uploaded.
	err = ss.MarkPackedSlabsUploaded(context.Background(), []api.UploadedPackedSlab{
		{
			BufferID: buffer.ID,
			Shards: []object.Sector{
				newTestShard(hk1, fcid1, types.Hash256{3}),
				newTestShard(hk2, fcid2, types.Hash256{4}),
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	buffer = dbBufferedSlab{}
	if err := ss.db.Take(&buffer, "id = ?", packedSlabs[0].BufferID).Error; !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatal("shouldn't be able to find buffer", err)
	}
	assertBuffer(buffer2Name, 1, false, false)

	_, err = ss.FetchPartialSlab(ctx, slabs[0].Key, slabs[0].Offset, slabs[0].Length)
	if !errors.Is(err, api.ErrObjectNotFound) {
		t.Fatal("expected ErrObjectNotFound", err)
	}

	files, err := os.ReadDir(ss.slabBufferMgr.dir)
	if err != nil {
		t.Fatal(err)
	}
	filesFound := make(map[string]struct{})
	for _, file := range files {
		filesFound[file.Name()] = struct{}{}
	}
	if _, exists := filesFound[buffer1Name]; exists {
		t.Fatal("buffer file should have been deleted", buffer1Name)
	} else if _, exists := filesFound[buffer2Name]; !exists {
		t.Fatal("buffer file should not have been deleted", buffer2Name)
	}

	// Add 2 more partial slabs.
	_, _, err = ss.AddPartialSlab(ctx, frand.Bytes(rhpv2.SectorSize/2), 1, 2, testContractSet)
	if err != nil {
		t.Fatal(err)
	}
	_, bufferSize, err = ss.AddPartialSlab(ctx, frand.Bytes(rhpv2.SectorSize/2), 1, 2, testContractSet)
	if err != nil {
		t.Fatal(err)
	}

	// Fetch the buffers we have. Should be 1 completed and 1 incomplete.
	buffersBefore, err := ss.SlabBuffers(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(buffersBefore) != 2 {
		t.Fatal("expected 2 buffers", len(buffersBefore))
	}
	if !buffersBefore[0].Complete {
		t.Fatal("expected buffer to be complete")
	} else if buffersBefore[1].Complete {
		t.Fatal("expected buffer to be incomplete")
	}
	if bufferSize != 2*rhpv2.SectorSize {
		t.Fatal("unexpected buffer size", bufferSize)
	}

	// Close manager to make sure we can restart the database without
	// issues due to open files.
	// NOTE: Close on the database doesn't work because that will wipe the
	// in-memory ss.
	if err := ss.slabBufferMgr.Close(); err != nil {
		t.Fatal(err)
	}

	// Restart it. The buffer should still be there.
	ss2 := ss.Reopen()
	defer ss2.Close()
	buffersAfter, err := ss2.SlabBuffers(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !cmp.Equal(buffersBefore, buffersAfter) {
		t.Fatal("buffers don't match", cmp.Diff(buffersBefore, buffersAfter))
	}
}

func TestContractSizes(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	// define a helper function that calculates the amount of data that can be
	// pruned by inspecting the contract sizes
	prunableData := func(fcid *types.FileContractID) (n uint64) {
		t.Helper()

		sizes, err := ss.ContractSizes(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		for id, size := range sizes {
			if fcid != nil && id != *fcid {
				continue
			}
			n += size.Prunable
		}
		return
	}

	// create hosts
	hks, err := ss.addTestHosts(2)
	if err != nil {
		t.Fatal(err)
	}

	// create contracts
	fcids, _, err := ss.addTestContracts(hks)
	if err != nil {
		t.Fatal(err)
	}

	// add an object to both contracts
	for i := 0; i < 2; i++ {
		if err := ss.UpdateObject(context.Background(), api.DefaultBucketName, fmt.Sprintf("obj_%d", i+1), testContractSet, testETag, testMimeType, object.Object{
			Key: object.GenerateEncryptionKey(),
			Slabs: []object.SlabSlice{
				{
					Slab: object.Slab{
						Key:       object.GenerateEncryptionKey(),
						MinShards: 1,
						Shards:    newTestShards(hks[i], fcids[i], types.Hash256{byte(i)}),
					},
				},
			},
		}); err != nil {
			t.Fatal(err)
		}
		if err := ss.RecordContractSpending(context.Background(), []api.ContractSpendingRecord{
			{
				ContractID:     fcids[i],
				RevisionNumber: 1,
				Size:           rhpv2.SectorSize,
			},
		}); err != nil {
			t.Fatal(err)
		}
	}

	// assert there's two objects
	s, err := ss.ObjectsStats(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if s.NumObjects != 2 {
		t.Fatal("expected 2 objects", s.NumObjects)
	}

	// assert there's no data to be pruned
	if n := prunableData(nil); n != 0 {
		t.Fatal("expected no prunable data", n)
	}

	// remove the first object
	if err := ss.RemoveObject(context.Background(), api.DefaultBucketName, "obj_1"); err != nil {
		t.Fatal(err)
	}

	// assert there's one sector that can be pruned and assert it's from fcid 1
	if n := prunableData(nil); n != rhpv2.SectorSize {
		t.Fatal("unexpected amount of prunable data", n)
	}
	if n := prunableData(&fcids[1]); n != 0 {
		t.Fatal("expected no prunable data", n)
	}

	// remove the second object
	if err := ss.RemoveObject(context.Background(), api.DefaultBucketName, "obj_2"); err != nil {
		t.Fatal(err)
	}

	// assert there's now two sectors that can be pruned
	if n := prunableData(nil); n != rhpv2.SectorSize*2 {
		t.Fatal("unexpected amount of prunable data", n)
	} else if n := prunableData(&fcids[0]); n != rhpv2.SectorSize {
		t.Fatal("unexpected amount of prunable data", n)
	} else if n := prunableData(&fcids[1]); n != rhpv2.SectorSize {
		t.Fatal("unexpected amount of prunable data", n)
	}

	if size, err := ss.ContractSize(context.Background(), fcids[0]); err != nil {
		t.Fatal("unexpected err", err)
	} else if size.Prunable != rhpv2.SectorSize {
		t.Fatal("unexpected prunable data", size.Prunable)
	}

	if size, err := ss.ContractSize(context.Background(), fcids[1]); err != nil {
		t.Fatal("unexpected err", err)
	} else if size.Prunable != rhpv2.SectorSize {
		t.Fatal("unexpected prunable data", size.Prunable)
	}

	// archive all contracts
	if err := ss.ArchiveAllContracts(context.Background(), t.Name()); err != nil {
		t.Fatal(err)
	}

	// assert there's no data to be pruned
	if n := prunableData(nil); n != 0 {
		t.Fatal("expected no prunable data", n)
	}

	// assert passing a non-existent fcid returns an error
	_, err = ss.ContractSize(context.Background(), types.FileContractID{9})
	if err != api.ErrContractNotFound {
		t.Fatal(err)
	}
}

// dbObject retrieves a dbObject from the store.
func (s *SQLStore) dbObject(key string) (dbObject, error) {
	var obj dbObject
	tx := s.db.Where(&dbObject{ObjectID: key}).
		Preload("Slabs").
		Take(&obj)
	if errors.Is(tx.Error, gorm.ErrRecordNotFound) {
		return dbObject{}, api.ErrObjectNotFound
	}
	return obj, nil
}

// dbSlab retrieves a dbSlab from the store.
func (s *SQLStore) dbSlab(key []byte) (dbSlab, error) {
	var slab dbSlab
	tx := s.db.Where(&dbSlab{Key: key}).
		Preload("Shards.Contracts.Host").
		Take(&slab)
	if errors.Is(tx.Error, gorm.ErrRecordNotFound) {
		return dbSlab{}, api.ErrObjectNotFound
	}
	return slab, nil
}

func TestObjectsBySlabKey(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	// create a host
	hks, err := ss.addTestHosts(1)
	if err != nil {
		t.Fatal(err)
	}
	hk1 := hks[0]

	// create a contract
	fcids, _, err := ss.addTestContracts(hks)
	if err != nil {
		t.Fatal(err)
	}
	fcid1 := fcids[0]

	// create a slab.
	slab := object.Slab{
		Health:    1.0,
		Key:       object.GenerateEncryptionKey(),
		MinShards: 1,
		Shards:    newTestShards(hk1, fcid1, types.Hash256{1}),
	}

	// Add 3 objects that all reference the slab.
	obj := object.Object{
		Key: object.GenerateEncryptionKey(),
		Slabs: []object.SlabSlice{
			{
				Slab:   slab,
				Offset: 1,
				Length: 0, // incremented later
			},
		},
	}
	for _, name := range []string{"obj1", "obj2", "obj3"} {
		obj.Slabs[0].Length++
		err = ss.UpdateObject(context.Background(), api.DefaultBucketName, name, testContractSet, testETag, testMimeType, obj)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Fetch the objects by slab.
	objs, err := ss.ObjectsBySlabKey(context.Background(), api.DefaultBucketName, slab.Key)
	if err != nil {
		t.Fatal(err)
	}
	for i, name := range []string{"obj1", "obj2", "obj3"} {
		if objs[i].Name != name {
			t.Fatal("unexpected object name", objs[i].Name, name)
		}
		if objs[i].Size != int64(i)+1 {
			t.Fatal("unexpected object size", objs[i].Size, i+1)
		}
		if objs[i].Health != 1.0 {
			t.Fatal("unexpected object health", objs[i].Health)
		}
	}
}

func TestBuckets(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	// List the buckets. Should be the default one.
	buckets, err := ss.ListBuckets(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if len(buckets) != 1 {
		t.Fatal("expected 1 bucket", len(buckets))
	} else if buckets[0].Name != api.DefaultBucketName {
		t.Fatal("expected default bucket")
	}

	// Create 2 more buckets and delete the default one. This should result in
	// 2 buckets.
	b1, b2 := "bucket1", "bucket2"
	if err := ss.CreateBucket(context.Background(), b1, api.BucketPolicy{}); err != nil {
		t.Fatal(err)
	} else if err := ss.CreateBucket(context.Background(), b2, api.BucketPolicy{}); err != nil {
		t.Fatal(err)
	} else if err := ss.DeleteBucket(context.Background(), api.DefaultBucketName); err != nil {
		t.Fatal(err)
	} else if buckets, err := ss.ListBuckets(context.Background()); err != nil {
		t.Fatal(err)
	} else if len(buckets) != 2 {
		t.Fatal("expected 2 buckets", len(buckets))
	} else if buckets[0].Name != b1 {
		t.Fatal("unexpected bucket", buckets[0])
	} else if buckets[1].Name != b2 {
		t.Fatal("unexpected bucket", buckets[1])
	}

	// Creating an existing buckets shouldn't work and neither should deleting
	// one that doesn't exist.
	if err := ss.CreateBucket(context.Background(), b1, api.BucketPolicy{}); !errors.Is(err, api.ErrBucketExists) {
		t.Fatal("expected ErrBucketExists", err)
	} else if err := ss.DeleteBucket(context.Background(), "foo"); !errors.Is(err, api.ErrBucketNotFound) {
		t.Fatal("expected ErrBucketNotFound", err)
	}
}

func TestBucketObjects(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	// Adding an object to a bucket that doesn't exist shouldn't work.
	obj := newTestObject(1)
	err := ss.UpdateObject(context.Background(), "unknown-bucket", "foo", testContractSet, testETag, testMimeType, obj)
	if !errors.Is(err, api.ErrBucketNotFound) {
		t.Fatal("expected ErrBucketNotFound", err)
	}

	// Create buckest for the test.
	b1, b2 := "bucket1", "bucket2"
	if err := ss.CreateBucket(context.Background(), b1, api.BucketPolicy{}); err != nil {
		t.Fatal(err)
	} else if err := ss.CreateBucket(context.Background(), b2, api.BucketPolicy{}); err != nil {
		t.Fatal(err)
	} else if err := ss.CreateBucket(context.Background(), b2, api.BucketPolicy{}); !errors.Is(err, api.ErrBucketExists) {
		t.Fatal(err)
	}

	// Create some objects for the test spread over 2 buckets.
	objects := []struct {
		path   string
		size   int64
		bucket string
	}{
		{"/foo/bar", 1, b1},
		{"/foo/bar", 2, b2},
		{"/bar", 3, b1},
		{"/bar", 4, b2},
	}
	ctx := context.Background()
	for _, o := range objects {
		obj := newTestObject(frand.Intn(9) + 1)
		obj.Slabs = obj.Slabs[:1]
		obj.Slabs[0].Length = uint32(o.size)
		err := ss.UpdateObject(ctx, o.bucket, o.path, testContractSet, testETag, testMimeType, obj)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Deleting a bucket with objects shouldn't work.
	if err := ss.DeleteBucket(ctx, b1); !errors.Is(err, api.ErrBucketNotEmpty) {
		t.Fatal(err)
	}

	// List the objects in the buckets.
	if entries, _, err := ss.ObjectEntries(context.Background(), b1, "/foo/", "", "", "", "", 0, -1); err != nil {
		t.Fatal(err)
	} else if len(entries) != 1 {
		t.Fatal("expected 1 entry", len(entries))
	} else if entries[0].Size != 1 {
		t.Fatal("unexpected size", entries[0].Size)
	} else if entries, _, err := ss.ObjectEntries(context.Background(), b2, "/foo/", "", "", "", "", 0, -1); err != nil {
		t.Fatal(err)
	} else if len(entries) != 1 {
		t.Fatal("expected 1 entry", len(entries))
	} else if entries[0].Size != 2 {
		t.Fatal("unexpected size", entries[0].Size)
	}

	// Search the objects in the buckets.
	if objects, err := ss.SearchObjects(context.Background(), b1, "", 0, -1); err != nil {
		t.Fatal(err)
	} else if len(objects) != 2 {
		t.Fatal("expected 2 objects", len(objects))
	} else if objects[0].Size != 3 || objects[1].Size != 1 {
		t.Fatal("unexpected size", objects[0].Size, objects[1].Size)
	} else if objects, err := ss.SearchObjects(context.Background(), b2, "", 0, -1); err != nil {
		t.Fatal(err)
	} else if len(objects) != 2 {
		t.Fatal("expected 2 objects", len(objects))
	} else if objects[0].Size != 4 || objects[1].Size != 2 {
		t.Fatal("unexpected size", objects[0].Size, objects[1].Size)
	}

	// Rename object foo/bar in bucket 1 to foo/baz but not in bucket 2.
	if err := ss.RenameObject(context.Background(), b1, "/foo/bar", "/foo/baz", false); err != nil {
		t.Fatal(err)
	} else if entries, _, err := ss.ObjectEntries(context.Background(), b1, "/foo/", "", "", "", "", 0, -1); err != nil {
		t.Fatal(err)
	} else if len(entries) != 1 {
		t.Fatal("expected 2 entries", len(entries))
	} else if entries[0].Name != "/foo/baz" {
		t.Fatal("unexpected name", entries[0].Name)
	} else if entries, _, err := ss.ObjectEntries(context.Background(), b2, "/foo/", "", "", "", "", 0, -1); err != nil {
		t.Fatal(err)
	} else if len(entries) != 1 {
		t.Fatal("expected 2 entries", len(entries))
	} else if entries[0].Name != "/foo/bar" {
		t.Fatal("unexpected name", entries[0].Name)
	}

	// Rename foo/bar in bucket 2 using the batch rename.
	if err := ss.RenameObjects(context.Background(), b2, "/foo/bar", "/foo/bam", false); err != nil {
		t.Fatal(err)
	} else if entries, _, err := ss.ObjectEntries(context.Background(), b1, "/foo/", "", "", "", "", 0, -1); err != nil {
		t.Fatal(err)
	} else if len(entries) != 1 {
		t.Fatal("expected 2 entries", len(entries))
	} else if entries[0].Name != "/foo/baz" {
		t.Fatal("unexpected name", entries[0].Name)
	} else if entries, _, err := ss.ObjectEntries(context.Background(), b2, "/foo/", "", "", "", "", 0, -1); err != nil {
		t.Fatal(err)
	} else if len(entries) != 1 {
		t.Fatal("expected 2 entries", len(entries))
	} else if entries[0].Name != "/foo/bam" {
		t.Fatal("unexpected name", entries[0].Name)
	}

	// Delete foo/baz in bucket 1 but first try bucket 2 since that should fail.
	if err := ss.RemoveObject(context.Background(), b2, "/foo/baz"); !errors.Is(err, api.ErrObjectNotFound) {
		t.Fatal(err)
	} else if err := ss.RemoveObject(context.Background(), b1, "/foo/baz"); err != nil {
		t.Fatal(err)
	} else if entries, _, err := ss.ObjectEntries(context.Background(), b1, "/foo/", "", "", "", "", 0, -1); err != nil {
		t.Fatal(err)
	} else if len(entries) > 0 {
		t.Fatal("expected 0 entries", len(entries))
	} else if entries, _, err := ss.ObjectEntries(context.Background(), b2, "/foo/", "", "", "", "", 0, -1); err != nil {
		t.Fatal(err)
	} else if len(entries) != 1 {
		t.Fatal("expected 1 entry", len(entries))
	}

	// Delete all files in bucket 2.
	if entries, _, err := ss.ObjectEntries(context.Background(), b2, "/", "", "", "", "", 0, -1); err != nil {
		t.Fatal(err)
	} else if len(entries) != 2 {
		t.Fatal("expected 2 entries", len(entries))
	} else if err := ss.RemoveObjects(context.Background(), b2, "/"); err != nil {
		t.Fatal(err)
	} else if entries, _, err := ss.ObjectEntries(context.Background(), b2, "/", "", "", "", "", 0, -1); err != nil {
		t.Fatal(err)
	} else if len(entries) != 0 {
		t.Fatal("expected 0 entries", len(entries))
	} else if entries, _, err := ss.ObjectEntries(context.Background(), b1, "/", "", "", "", "", 0, -1); err != nil {
		t.Fatal(err)
	} else if len(entries) != 1 {
		t.Fatal("expected 1 entry", len(entries))
	}

	// Fetch /bar from bucket 1.
	if obj, err := ss.Object(context.Background(), b1, "/bar"); err != nil {
		t.Fatal(err)
	} else if obj.Size != 3 {
		t.Fatal("unexpected size", obj.Size)
	} else if _, err := ss.Object(context.Background(), b2, "/bar"); !errors.Is(err, api.ErrObjectNotFound) {
		t.Fatal(err)
	}

	// See if we can fetch the object by slab.
	var ec object.EncryptionKey
	if obj, err := ss.object(context.Background(), ss.db, b1, "/bar"); err != nil {
		t.Fatal(err)
	} else if err := ec.UnmarshalBinary(obj[0].SlabKey); err != nil {
		t.Fatal(err)
	} else if objects, err := ss.ObjectsBySlabKey(context.Background(), b1, ec); err != nil {
		t.Fatal(err)
	} else if len(objects) != 1 {
		t.Fatal("expected 1 object", len(objects))
	} else if objects, err := ss.ObjectsBySlabKey(context.Background(), b2, ec); err != nil {
		t.Fatal(err)
	} else if len(objects) != 0 {
		t.Fatal("expected 0 objects", len(objects))
	}
}

func TestCopyObject(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	// Create the buckets.
	ctx := context.Background()
	if err := ss.CreateBucket(ctx, "src", api.BucketPolicy{}); err != nil {
		t.Fatal(err)
	} else if err := ss.CreateBucket(ctx, "dst", api.BucketPolicy{}); err != nil {
		t.Fatal(err)
	}

	// Create one object.
	obj := newTestObject(1)
	err := ss.UpdateObject(ctx, "src", "/foo", testContractSet, testETag, testMimeType, obj)
	if err != nil {
		t.Fatal(err)
	}

	// Copy it within the same bucket.
	if om, err := ss.CopyObject(ctx, "src", "src", "/foo", "/bar", ""); err != nil {
		t.Fatal(err)
	} else if entries, _, err := ss.ObjectEntries(ctx, "src", "/", "", "", "", "", 0, -1); err != nil {
		t.Fatal(err)
	} else if len(entries) != 2 {
		t.Fatal("expected 2 entries", len(entries))
	} else if entries[0].Name != "/bar" || entries[1].Name != "/foo" {
		t.Fatal("unexpected names", entries[0].Name, entries[1].Name)
	} else if om.ModTime.IsZero() {
		t.Fatal("expected mod time to be set")
	}

	// Copy it cross buckets.
	if om, err := ss.CopyObject(ctx, "src", "dst", "/foo", "/bar", ""); err != nil {
		t.Fatal(err)
	} else if entries, _, err := ss.ObjectEntries(ctx, "dst", "/", "", "", "", "", 0, -1); err != nil {
		t.Fatal(err)
	} else if len(entries) != 1 {
		t.Fatal("expected 1 entry", len(entries))
	} else if entries[0].Name != "/bar" {
		t.Fatal("unexpected names", entries[0].Name, entries[1].Name)
	} else if om.ModTime.IsZero() {
		t.Fatal("expected mod time to be set")
	}
}

func TestMarkSlabUploadedAfterRenew(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)

	// create host.
	hks, err := ss.addTestHosts(1)
	if err != nil {
		t.Fatal(err)
	}
	hk := hks[0]

	// create contracts
	fcids, _, err := ss.addTestContracts(hks)
	if err != nil {
		t.Fatal(err)
	}
	fcid := fcids[0]

	// create a full buffered slab.
	completeSize := bufferedSlabSize(1)
	_, _, err = ss.AddPartialSlab(context.Background(), frand.Bytes(completeSize), 1, 1, testContractSet)
	if err != nil {
		t.Fatal(err)
	}

	// fetch it for upload.
	packedSlabs, err := ss.PackedSlabsForUpload(context.Background(), time.Hour, 1, 1, testContractSet, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(packedSlabs) != 1 {
		t.Fatal("expected 1 slab to be returned", len(packedSlabs))
	}

	// renew the contract.
	fcidRenewed := types.FileContractID{2, 2, 2, 2, 2}
	uc := generateMultisigUC(1, 2, "salt")
	rev := rhpv2.ContractRevision{
		Revision: types.FileContractRevision{
			ParentID:         fcidRenewed,
			UnlockConditions: uc,
			FileContract: types.FileContract{
				MissedProofOutputs: []types.SiacoinOutput{},
				ValidProofOutputs:  []types.SiacoinOutput{},
			},
		},
	}
	_, err = ss.AddRenewedContract(context.Background(), rev, types.NewCurrency64(1), types.NewCurrency64(1), 100, fcid, api.ContractStatePending)
	if err != nil {
		t.Fatal(err)
	}

	// mark it as uploaded.
	err = ss.MarkPackedSlabsUploaded(context.Background(), []api.UploadedPackedSlab{
		{
			BufferID: packedSlabs[0].BufferID,
			Shards:   newTestShards(hk, fcid, types.Hash256{1}),
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	var count int64
	if err := ss.db.Model(&dbContractSector{}).Count(&count).Error; err != nil {
		t.Fatal(err)
	} else if count != 1 {
		t.Fatal("expected 1 sector", count)
	}
}

func TestListObjects(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()
	objects := []struct {
		path string
		size int64
	}{
		{"/foo/bar", 1},
		{"/foo/bat", 2},
		{"/foo/baz/quux", 3},
		{"/foo/baz/quuz", 4},
		{"/gab/guub", 5},
		{"/FOO/bar", 6}, // test case sensitivity
	}

	// assert mod time & clear it afterwards so we can compare
	assertModTime := func(entries []api.ObjectMetadata) {
		for i := range entries {
			if !strings.HasSuffix(entries[i].Name, "/") && entries[i].ModTime.IsZero() {
				t.Fatal("mod time should be set")
			}
			entries[i].ModTime = time.Time{}
		}
	}

	ctx := context.Background()
	for _, o := range objects {
		obj := newTestObject(frand.Intn(9) + 1)
		obj.Slabs = obj.Slabs[:1]
		obj.Slabs[0].Length = uint32(o.size)
		if err := ss.UpdateObject(ctx, api.DefaultBucketName, o.path, testContractSet, testETag, testMimeType, obj); err != nil {
			t.Fatal(err)
		}
	}

	// override health of some slabs
	if err := ss.overrideSlabHealth("/foo/baz/quuz", 0.5); err != nil {
		t.Fatal(err)
	}
	if err := ss.overrideSlabHealth("/foo/baz/quux", 0.75); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		prefix  string
		sortBy  string
		sortDir string
		marker  string
		want    []api.ObjectMetadata
	}{
		{"/", "", "", "", []api.ObjectMetadata{{Name: "/FOO/bar", Size: 6, Health: 1}, {Name: "/foo/bar", Size: 1, Health: 1}, {Name: "/foo/bat", Size: 2, Health: 1}, {Name: "/foo/baz/quux", Size: 3, Health: .75}, {Name: "/foo/baz/quuz", Size: 4, Health: .5}, {Name: "/gab/guub", Size: 5, Health: 1}}},
		{"/", "", "ASC", "", []api.ObjectMetadata{{Name: "/FOO/bar", Size: 6, Health: 1}, {Name: "/foo/bar", Size: 1, Health: 1}, {Name: "/foo/bat", Size: 2, Health: 1}, {Name: "/foo/baz/quux", Size: 3, Health: .75}, {Name: "/foo/baz/quuz", Size: 4, Health: .5}, {Name: "/gab/guub", Size: 5, Health: 1}}},
		{"/", "", "DESC", "", []api.ObjectMetadata{{Name: "/gab/guub", Size: 5, Health: 1}, {Name: "/foo/baz/quuz", Size: 4, Health: .5}, {Name: "/foo/baz/quux", Size: 3, Health: .75}, {Name: "/foo/bat", Size: 2, Health: 1}, {Name: "/foo/bar", Size: 1, Health: 1}, {Name: "/FOO/bar", Size: 6, Health: 1}}},
		{"/", "health", "ASC", "", []api.ObjectMetadata{{Name: "/foo/baz/quuz", Size: 4, Health: .5}, {Name: "/foo/baz/quux", Size: 3, Health: .75}, {Name: "/FOO/bar", Size: 6, Health: 1}, {Name: "/foo/bar", Size: 1, Health: 1}, {Name: "/foo/bat", Size: 2, Health: 1}, {Name: "/gab/guub", Size: 5, Health: 1}}},
		{"/", "health", "DESC", "", []api.ObjectMetadata{{Name: "/FOO/bar", Size: 6, Health: 1}, {Name: "/foo/bar", Size: 1, Health: 1}, {Name: "/foo/bat", Size: 2, Health: 1}, {Name: "/gab/guub", Size: 5, Health: 1}, {Name: "/foo/baz/quux", Size: 3, Health: .75}, {Name: "/foo/baz/quuz", Size: 4, Health: .5}}},
		{"/foo/b", "", "", "", []api.ObjectMetadata{{Name: "/foo/bar", Size: 1, Health: 1}, {Name: "/foo/bat", Size: 2, Health: 1}, {Name: "/foo/baz/quux", Size: 3, Health: .75}, {Name: "/foo/baz/quuz", Size: 4, Health: .5}}},
		{"o/baz/quu", "", "", "", []api.ObjectMetadata{}},
		{"/foo", "", "", "", []api.ObjectMetadata{{Name: "/foo/bar", Size: 1, Health: 1}, {Name: "/foo/bat", Size: 2, Health: 1}, {Name: "/foo/baz/quux", Size: 3, Health: .75}, {Name: "/foo/baz/quuz", Size: 4, Health: .5}}},
	}
	for _, test := range tests {
		res, err := ss.ListObjects(ctx, api.DefaultBucketName, test.prefix, test.sortBy, test.sortDir, "", -1)
		if err != nil {
			t.Fatal(err)
		}

		// assert mod time & clear it afterwards so we can compare
		assertModTime(res.Objects)

		got := res.Objects
		if !(len(got) == 0 && len(test.want) == 0) && !reflect.DeepEqual(got, test.want) {
			t.Fatalf("\nkey: %v\ngot: %v\nwant: %v", test.prefix, got, test.want)
		}
		if len(res.Objects) > 0 {
			marker := ""
			for offset := 0; offset < len(test.want); offset++ {
				res, err := ss.ListObjects(ctx, api.DefaultBucketName, test.prefix, test.sortBy, test.sortDir, marker, 1)
				if err != nil {
					t.Fatal(err)
				}

				// assert mod time & clear it afterwards so we can compare
				assertModTime(res.Objects)

				got := res.Objects
				if len(got) != 1 {
					t.Fatalf("expected 1 object, got %v", len(got))
				} else if got[0].Name != test.want[offset].Name {
					t.Fatalf("expected %v, got %v, offset %v, marker %v", test.want[offset].Name, got[0].Name, offset, marker)
				}
				marker = res.NextMarker
			}
		}
	}
}

func TestDeleteHostSector(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	// create 2 hosts.
	hks, err := ss.addTestHosts(2)
	if err != nil {
		t.Fatal(err)
	}
	hk1, hk2 := hks[0], hks[1]

	// create 2 contracts with each
	_, _, err = ss.addTestContracts([]types.PublicKey{hk1, hk1, hk2, hk2})
	if err != nil {
		t.Fatal(err)
	}

	// get all contracts
	var dbContracts []dbContract
	if err := ss.db.Model(&dbContract{}).Preload("Host").Find(&dbContracts).Error; err != nil {
		t.Fatal(err)
	}

	// create a healthy slab with one sector that is uploaded to all contracts.
	root := types.Hash256{1, 2, 3}
	slab := dbSlab{
		DBContractSetID:  1,
		Key:              []byte(object.GenerateEncryptionKey().String()),
		Health:           1.0,
		HealthValidUntil: time.Now().Add(time.Hour).Unix(),
		TotalShards:      1,
		Shards: []dbSector{
			{
				Contracts:  dbContracts,
				Root:       root[:],
				LatestHost: publicKey(hk1), // hk1 is latest host
			},
		},
	}
	if err := ss.db.Create(&slab).Error; err != nil {
		t.Fatal(err)
	}

	// Make sure 4 contractSector entries exist.
	var n int64
	if err := ss.db.Model(&dbContractSector{}).
		Count(&n).
		Error; err != nil {
		t.Fatal(err)
	} else if n != 4 {
		t.Fatal("expected 4 contract-sector links", n)
	}

	// Prune the sector from hk1.
	if err := ss.DeleteHostSector(context.Background(), hk1, root); err != nil {
		t.Fatal(err)
	}

	// Make sure 2 contractSector entries exist.
	if err := ss.db.Model(&dbContractSector{}).
		Count(&n).
		Error; err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal("expected 2 contract-sector links", n)
	}

	// Find the slab. It should have an invalid health.
	var s dbSlab
	if err := ss.db.Preload("Shards").Take(&s).Error; err != nil {
		t.Fatal(err)
	} else if s.HealthValid() {
		t.Fatal("expected health to be invalid")
	} else if s.Shards[0].LatestHost != publicKey(hk2) {
		t.Fatal("expected hk2 to be latest host", types.PublicKey(s.Shards[0].LatestHost))
	}

	// Fetch the sector and assert the contracts association.
	var sectors []dbSector
	if err := ss.db.Model(&dbSector{}).Preload("Contracts").Find(&sectors).Preload("Contracts").Error; err != nil {
		t.Fatal(err)
	} else if len(sectors) != 1 {
		t.Fatal("expected 1 sector", len(sectors))
	} else if sector := sectors[0]; len(sector.Contracts) != 2 {
		t.Fatal("expected 2 contracts", len(sector.Contracts))
	}

	hi, err := ss.Host(context.Background(), hk1)
	if err != nil {
		t.Fatal(err)
	} else if hi.Interactions.LostSectors != 2 {
		t.Fatalf("expected 2 lost sector, got %v", hi.Interactions.LostSectors)
	}

	// Reset lost sectors again.
	if err := ss.ResetLostSectors(context.Background(), hk1); err != nil {
		t.Fatal(err)
	}

	hi, err = ss.Host(context.Background(), hk1)
	if err != nil {
		t.Fatal(err)
	} else if hi.Interactions.LostSectors != 0 {
		t.Fatalf("expected 0 lost sector, got %v", hi.Interactions.LostSectors)
	}
}
func newTestShards(hk types.PublicKey, fcid types.FileContractID, root types.Hash256) []object.Sector {
	return []object.Sector{
		newTestShard(hk, fcid, root),
	}
}

func newTestShard(hk types.PublicKey, fcid types.FileContractID, root types.Hash256) object.Sector {
	return object.Sector{
		LatestHost: hk,
		Contracts: map[types.PublicKey][]types.FileContractID{
			hk: {fcid},
		},
		Root: root,
	}
}

func TestUpdateSlabSanityChecks(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)

	// create hosts and contracts.
	hks, err := ss.addTestHosts(5)
	if err != nil {
		t.Fatal(err)
	}
	_, contracts, err := ss.addTestContracts(hks)
	if err != nil {
		t.Fatal(err)
	}

	// prepare a slab.
	var shards []object.Sector
	for i := 0; i < 5; i++ {
		shards = append(shards, newTestShard(hks[i], contracts[i].ID, types.Hash256{byte(i + 1)}))
	}
	slab := object.Slab{
		Key:    object.GenerateEncryptionKey(),
		Shards: shards,
		Health: 1,
	}

	// set slab.
	err = ss.UpdateObject(context.Background(), api.DefaultBucketName, "foo", testContractSet, testETag, testMimeType, object.Object{
		Key:   object.GenerateEncryptionKey(),
		Slabs: []object.SlabSlice{{Slab: slab}},
	})
	if err != nil {
		t.Fatal(err)
	}

	// verify slab.
	rSlab, err := ss.Slab(context.Background(), slab.Key)
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(slab, rSlab) {
		t.Fatal("unexpected slab", cmp.Diff(slab, rSlab, cmp.AllowUnexported(object.EncryptionKey{})))
	}

	// change the length to fail the update.
	if err := ss.UpdateSlab(context.Background(), object.Slab{
		Key:    slab.Key,
		Shards: shards[:len(shards)-1],
	}, testContractSet); !errors.Is(err, errInvalidNumberOfShards) {
		t.Fatal(err)
	}

	// reverse the order of the shards to fail the update.
	reversedShards := append([]object.Sector{}, shards...)
	for i := 0; i < len(reversedShards)/2; i++ {
		j := len(reversedShards) - i - 1
		reversedShards[i], reversedShards[j] = reversedShards[j], reversedShards[i]
	}
	reversedSlab := object.Slab{
		Key:    slab.Key,
		Shards: reversedShards,
	}
	if err := ss.UpdateSlab(context.Background(), reversedSlab, testContractSet); !errors.Is(err, errShardRootChanged) {
		t.Fatal(err)
	}
}

func TestSlabHealthInvalidation(t *testing.T) {
	// create db
	cfg := defaultTestSQLStoreConfig
	ss := newTestSQLStore(t, cfg)
	defer ss.Close()

	// define a helper to assert the health validity of a given slab
	assertHealthValid := func(slabKey object.EncryptionKey, expected bool) {
		t.Helper()

		var slab dbSlab
		if key, err := slabKey.MarshalBinary(); err != nil {
			t.Fatal(err)
		} else if err := ss.db.Model(&dbSlab{}).Where(&dbSlab{Key: key}).Take(&slab).Error; err != nil {
			t.Fatal(err)
		} else if slab.HealthValid() != expected {
			t.Fatal("unexpected health valid", slab.HealthValid(), slab.HealthValidUntil, time.Now(), time.Unix(slab.HealthValidUntil, 0))
		}
	}

	// define a helper to refresh the health
	refreshHealth := func(slabKeys ...object.EncryptionKey) {
		t.Helper()

		// refresh health
		if err := ss.RefreshHealth(context.Background()); err != nil {
			t.Fatal(err)
		}

		// assert all slabs
		for _, slabKey := range slabKeys {
			assertHealthValid(slabKey, true)
		}
	}

	// add hosts and contracts
	hks, err := ss.addTestHosts(4)
	if err != nil {
		t.Fatal(err)
	}
	fcids, _, err := ss.addTestContracts(hks)
	if err != nil {
		t.Fatal(err)
	}

	// prepare a slab with pieces on h1 and h2
	s1 := object.GenerateEncryptionKey()
	err = ss.UpdateObject(context.Background(), api.DefaultBucketName, "o1", testContractSet, testETag, testMimeType, object.Object{
		Key: object.GenerateEncryptionKey(),
		Slabs: []object.SlabSlice{{Slab: object.Slab{
			Key: s1,
			Shards: []object.Sector{
				newTestShard(hks[0], fcids[0], types.Hash256{0}),
				newTestShard(hks[1], fcids[1], types.Hash256{1}),
			},
		}}},
	})
	if err != nil {
		t.Fatal(err)
	}

	// prepare a slab with pieces on h3 and h4
	s2 := object.GenerateEncryptionKey()
	err = ss.UpdateObject(context.Background(), api.DefaultBucketName, "o2", testContractSet, testETag, testMimeType, object.Object{
		Key: object.GenerateEncryptionKey(),
		Slabs: []object.SlabSlice{{Slab: object.Slab{
			Key: s2,
			Shards: []object.Sector{
				newTestShard(hks[2], fcids[2], types.Hash256{2}),
				newTestShard(hks[3], fcids[3], types.Hash256{3}),
			},
		}}},
	})
	if err != nil {
		t.Fatal(err)
	}

	// assert there are 0 contracts in the contract set
	cscs, err := ss.ContractSetContracts(context.Background(), testContractSet)
	if err != nil {
		t.Fatal(err)
	} else if len(cscs) != 0 {
		t.Fatal("expected 0 contracts", len(cscs))
	}

	// refresh health
	refreshHealth(s1, s2)

	// add 2 contracts to the contract set
	if err := ss.SetContractSet(context.Background(), testContractSet, fcids[:2]); err != nil {
		t.Fatal(err)
	}
	assertHealthValid(s1, false)
	assertHealthValid(s2, true)

	// refresh health
	refreshHealth(s1, s2)

	// switch out the contract set with two new contracts
	if err := ss.SetContractSet(context.Background(), testContractSet, fcids[2:]); err != nil {
		t.Fatal(err)
	}
	assertHealthValid(s1, false)
	assertHealthValid(s2, false)

	// assert there are 2 contracts in the contract set
	cscs, err = ss.ContractSetContracts(context.Background(), testContractSet)
	if err != nil {
		t.Fatal(err)
	} else if len(cscs) != 2 {
		t.Fatal("expected 2 contracts", len(cscs))
	} else if cscs[0].ID != (types.FileContractID{3}) || cscs[1].ID != (types.FileContractID{4}) {
		t.Fatal("unexpected contracts", cscs)
	}

	// refresh health
	refreshHealth(s1, s2)

	// archive the contract for h3 and assert s2 was invalidated
	if err := ss.ArchiveContract(context.Background(), types.FileContractID{3}, "test"); err != nil {
		t.Fatal(err)
	}
	assertHealthValid(s1, true)
	assertHealthValid(s2, false)

	// archive the contract for h1 and assert s1 was invalidated
	if err := ss.ArchiveContract(context.Background(), types.FileContractID{1}, "test"); err != nil {
		t.Fatal(err)
	}
	assertHealthValid(s1, false)
	assertHealthValid(s2, false)

	// assert the health validity is always updated to a random time in the future that matches the boundaries
	for i := 0; i < 1e3; i++ {
		// reset health validity
		if tx := ss.db.Exec("UPDATE slabs SET health_valid_until = 0;"); tx.Error != nil {
			t.Fatal(err)
		}

		// refresh health
		now := time.Now().Round(time.Second)
		if err := ss.RefreshHealth(context.Background()); err != nil {
			t.Fatal(err)
		}

		// fetch slab
		var slab dbSlab
		if key, err := s1.MarshalBinary(); err != nil {
			t.Fatal(err)
		} else if err := ss.db.Model(&dbSlab{}).Where(&dbSlab{Key: key}).Take(&slab).Error; err != nil {
			t.Fatal(err)
		}

		// assert it's validity is within expected bounds
		min := now.Add(refreshHealthMinHealthValidity)
		max := now.Add(refreshHealthMaxHealthValidity)
		validUntil := time.Unix(slab.HealthValidUntil, 0)
		if !(min.Before(validUntil) && max.After(validUntil)) {
			t.Fatal("valid until not in boundaries", min, max, validUntil, now)
		}
	}
}
func (s *SQLStore) overrideSlabHealth(objectID string, health float64) (err error) {
	err = s.db.Exec(fmt.Sprintf(`
	UPDATE slabs SET health = %v WHERE id IN (
		SELECT sla.id
		FROM objects o
		INNER JOIN slices sli ON o.id = sli.db_object_id
		INNER JOIN slabs sla ON sli.db_slab_id = sla.id
		WHERE o.object_id = "%s"
	)`, health, objectID)).Error
	return
}
