package stores

import (
	"bytes"
	"context"
	dsql "database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/config"
	isql "go.sia.tech/renterd/internal/sql"
	"go.sia.tech/renterd/internal/test"
	"go.sia.tech/renterd/object"
	sql "go.sia.tech/renterd/stores/sql"
	"lukechampine.com/frand"
)

func (s *testSQLStore) InsertSlab(slab object.Slab) {
	s.t.Helper()
	obj := object.Object{
		Key: object.GenerateEncryptionKey(),
		Slabs: object.SlabSlices{
			object.SlabSlice{
				Slab: slab,
			},
		},
	}
	err := s.UpdateObject(context.Background(), api.DefaultBucketName, "/"+hex.EncodeToString(frand.Bytes(16)), testContractSet, "", "", api.ObjectUserMetadata{}, obj)
	if err != nil {
		s.t.Fatal(err)
	}
}

func (s *SQLStore) RemoveObjectBlocking(ctx context.Context, bucket, path string) error {
	ts := time.Now()
	time.Sleep(time.Millisecond)
	if err := s.RemoveObject(ctx, bucket, path); err != nil {
		return err
	}
	return s.waitForPruneLoop(ts)
}

func (s *SQLStore) RemoveObjectsBlocking(ctx context.Context, bucket, prefix string) error {
	ts := time.Now()
	time.Sleep(time.Millisecond)
	if err := s.RemoveObjects(ctx, bucket, prefix); err != nil {
		return err
	}
	return s.waitForPruneLoop(ts)
}

func (s *SQLStore) RenameObjectBlocking(ctx context.Context, bucket, keyOld, keyNew string, force bool) error {
	ts := time.Now()
	time.Sleep(time.Millisecond)
	if err := s.RenameObject(ctx, bucket, keyOld, keyNew, force); err != nil {
		return err
	}
	return s.waitForPruneLoop(ts)
}

func (s *SQLStore) RenameObjectsBlocking(ctx context.Context, bucket, prefixOld, prefixNew string, force bool) error {
	ts := time.Now()
	time.Sleep(time.Millisecond)
	if err := s.RenameObjects(ctx, bucket, prefixOld, prefixNew, force); err != nil {
		return err
	}
	return s.waitForPruneLoop(ts)
}

func (s *SQLStore) UpdateObjectBlocking(ctx context.Context, bucket, path, contractSet, eTag, mimeType string, metadata api.ObjectUserMetadata, o object.Object) error {
	var ts time.Time
	_, err := s.Object(ctx, bucket, path)
	if err == nil {
		ts = time.Now()
		time.Sleep(time.Millisecond)
	}
	if err := s.UpdateObject(ctx, bucket, path, contractSet, eTag, mimeType, metadata, o); err != nil {
		return err
	}
	return s.waitForPruneLoop(ts)
}

func (s *SQLStore) waitForPruneLoop(ts time.Time) error {
	return test.Retry(100, 100*time.Millisecond, func() error {
		s.mu.Lock()
		defer s.mu.Unlock()
		if !s.lastPrunedAt.After(ts) {
			return errors.New("slabs have not been pruned yet")
		}
		return nil
	})
}

func randomMultisigUC() types.UnlockConditions {
	uc := types.UnlockConditions{
		PublicKeys:         make([]types.UnlockKey, 2),
		SignaturesRequired: 1,
	}
	for i := range uc.PublicKeys {
		uc.PublicKeys[i].Algorithm = types.SpecifierEd25519
		uc.PublicKeys[i].Key = frand.Bytes(32)
	}
	return uc
}

func updateAllObjectsHealth(db *isql.DB) error {
	_, err := db.Exec(context.Background(), `
UPDATE objects
SET health = (
	SELECT COALESCE(MIN(slabs.health), 1)
	FROM slabs
	INNER JOIN slices sli ON sli.db_slab_id = slabs.id
	WHERE sli.db_object_id = objects.id)
`)
	return err
}

func TestPrunableContractRoots(t *testing.T) {
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

	// add 4 objects
	for i := 1; i <= 4; i++ {
		if _, err := ss.addTestObject(fmt.Sprintf("/%s_%d", t.Name(), i), object.Object{
			Key: object.GenerateEncryptionKey(),
			Slabs: []object.SlabSlice{
				{
					Slab: object.Slab{
						Key:       object.GenerateEncryptionKey(),
						MinShards: 1,
						Shards:    newTestShards(hks[0], fcids[0], types.Hash256{byte(i)}),
					},
				},
			},
		}); err != nil {
			t.Fatal(err)
		}
	}

	// assert there's 4 roots in the database
	roots, err := ss.ContractRoots(context.Background(), fcids[0])
	if err != nil {
		t.Fatal(err)
	} else if len(roots) != 4 {
		t.Fatal("unexpected number of roots", len(roots))
	}

	// diff the roots - should be empty
	indices, err := ss.PrunableContractRoots(context.Background(), fcids[0], roots)
	if err != nil {
		t.Fatal(err)
	} else if len(indices) != 0 {
		t.Fatal("unexpected number of indices", len(indices))
	}

	// delete every other object
	if err := ss.RemoveObjectBlocking(context.Background(), api.DefaultBucketName, fmt.Sprintf("/%s_1", t.Name())); err != nil {
		t.Fatal(err)
	}
	if err := ss.RemoveObjectBlocking(context.Background(), api.DefaultBucketName, fmt.Sprintf("/%s_3", t.Name())); err != nil {
		t.Fatal(err)
	}

	// assert there's 2 roots left
	updated, err := ss.ContractRoots(context.Background(), fcids[0])
	if err != nil {
		t.Fatal(err)
	} else if len(updated) != 2 {
		t.Fatal("unexpected number of roots", len(updated))
	}

	// diff the roots again, should return indices 0 and 2
	indices, err = ss.PrunableContractRoots(context.Background(), fcids[0], roots)
	if err != nil {
		t.Fatal(err)
	} else if len(indices) != 2 {
		t.Fatal("unexpected number of indices", len(indices))
	} else if indices[0] != 0 || indices[1] != 2 {
		t.Fatal("unexpected indices", indices)
	}
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
	got, err := ss.addTestObject("/"+t.Name(), want)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(*got.Object, want) {
		t.Fatal("object mismatch", got.Object, want)
	}

	// update the sector to have a non-consecutive slab index
	_, err = ss.DB().Exec(context.Background(), "UPDATE sectors SET slab_index = 100 WHERE slab_index = 1")
	if err != nil {
		t.Fatalf("failed to update sector: %v", err)
	}

	// fetch the object again and assert we receive an indication it was corrupted
	_, err = ss.Object(context.Background(), api.DefaultBucketName, "/"+t.Name())
	if !errors.Is(err, api.ErrObjectCorrupted) {
		t.Fatal("unexpected err", err)
	}

	// create an object without slabs
	want2 := object.Object{
		Key:   object.GenerateEncryptionKey(),
		Slabs: []object.SlabSlice{},
	}

	// add the object
	got2, err := ss.addTestObject("/"+t.Name(), want2)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(*got2.Object, want2) {
		t.Fatal("object mismatch", cmp.Diff(got2.Object, want2))
	}
}

func TestObjectMetadata(t *testing.T) {
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
	got, err := ss.addTestObject("/"+t.Name(), want)
	if err != nil {
		t.Fatal(err)
	}

	// assert it matches
	if !reflect.DeepEqual(*got.Object, want) {
		t.Log(got.Object)
		t.Log(want)
		t.Fatal("object mismatch", cmp.Diff(got.Object, want, cmp.AllowUnexported(object.EncryptionKey{})))
	}
	if !reflect.DeepEqual(got.Metadata, testMetadata) {
		t.Fatal("meta mismatch", cmp.Diff(got.Metadata, testMetadata))
	}

	// assert metadata CASCADE on object delete
	if cnt := ss.Count("object_user_metadata"); cnt != 2 {
		t.Fatal("unexpected number of metadata entries", cnt)
	}

	// remove the object
	if err := ss.RemoveObjectBlocking(context.Background(), api.DefaultBucketName, "/"+t.Name()); err != nil {
		t.Fatal(err)
	}

	// assert records are gone
	if cnt := ss.Count("object_user_metadata"); cnt != 0 {
		t.Fatal("unexpected number of metadata entries", cnt)
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
	if err := ss.announceHost(hk, "address"); err != nil {
		t.Fatal(err)
	}

	// Create random unlock conditions for the host.
	uc := randomMultisigUC()
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
	contracts, err := ss.Contracts(ctx, api.ContractsOpts{})
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
		t.Fatal("contract mismatch", cmp.Diff(returned, expected))
	}

	// Look it up again.
	fetched, err := ss.Contract(ctx, c.ID())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(fetched, expected) {
		t.Fatal("contract mismatch")
	}
	contracts, err = ss.Contracts(ctx, api.ContractsOpts{})
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
	if err := ss.UpdateContractSet(ctx, "foo", []types.FileContractID{contracts[0].ID}, nil); err != nil {
		t.Fatal(err)
	}
	if contracts, err := ss.Contracts(ctx, api.ContractsOpts{ContractSet: "foo"}); err != nil {
		t.Fatal(err)
	} else if len(contracts) != 1 {
		t.Fatalf("should have 1 contracts but got %v", len(contracts))
	}
	if _, err := ss.Contracts(ctx, api.ContractsOpts{ContractSet: "bar"}); !errors.Is(err, api.ErrContractSetNotFound) {
		t.Fatal(err)
	}

	// Add another contract set.
	if err := ss.UpdateContractSet(ctx, "foo2", []types.FileContractID{contracts[0].ID}, nil); err != nil {
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
	contracts, err = ss.Contracts(ctx, api.ContractsOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(contracts) != 0 {
		t.Fatalf("should have 0 contracts but got %v", len(contracts))
	}

	// Make sure the db was cleaned up properly through the CASCADE delete.
	if count := ss.Count("contracts"); count != 0 {
		t.Fatalf("expected %v rows in contracts but got %v", 0, count)
	}

	// Check join table count as well.
	if count := ss.Count("contract_sectors"); count != 0 {
		t.Fatalf("expected %v objects in contract_sectors but got %v", 0, count)
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
	_, err = ss.addTestObject("/"+t.Name(), obj)
	if err != nil {
		t.Fatal(err)
	}

	// fetch roots
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
	if err := ss.announceHost(hk, "address"); err != nil {
		t.Fatal(err)
	}
	if err := ss.announceHost(hk2, "address2"); err != nil {
		t.Fatal(err)
	}

	// Create random unlock conditions for the hosts.
	uc := randomMultisigUC()
	uc.PublicKeys[1].Key = hk[:]
	uc.Timelock = 192837

	uc2 := randomMultisigUC()
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
	if err := ss.UpdateContractSet(context.Background(), "test", []types.FileContractID{fcid1, fcid2}, nil); err != nil {
		t.Fatal(err)
	}

	// add the object.
	if _, err := ss.addTestObject("/"+t.Name(), obj); err != nil {
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
		t.Fatal("unexpected", err)
	}

	// Renew it.
	fcid1Renewed := types.FileContractID{2, 2, 2, 2, 2}
	rev := rhpv2.ContractRevision{
		Revision: types.FileContractRevision{
			ParentID:         fcid1Renewed,
			UnlockConditions: uc,
			FileContract: types.FileContract{
				Filesize:           2 * rhpv2.SectorSize,
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
	setContracts, err := ss.Contracts(ctx, api.ContractsOpts{ContractSet: "test"})
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
		Size:        2 * rhpv2.SectorSize,
		State:       api.ContractStatePending,
		Spending: api.ContractSpending{
			Uploads:     types.ZeroCurrency,
			Downloads:   types.ZeroCurrency,
			FundAccount: types.ZeroCurrency,
		},
		ContractPrice: types.NewCurrency64(2),
		ContractSets:  []string{"test"},
		TotalCost:     newContractTotal,
	}
	if !reflect.DeepEqual(newContract, expected) {
		t.Fatal("mismatch")
	}

	// Archived contract should exist.
	ancestors, err := ss.AncestorContracts(context.Background(), fcid1Renewed, 0)
	if err != nil {
		t.Fatal(err)
	} else if len(ancestors) != 1 {
		t.Fatalf("expected 1 ancestor but got %v", len(ancestors))
	}
	ac := ancestors[0]

	expectedContract := api.ArchivedContract{
		ID:        fcid1,
		HostIP:    "address",
		HostKey:   c.HostKey(),
		RenewedTo: fcid1Renewed,
		Spending: api.ContractSpending{
			Uploads:     types.Siacoins(1),
			Downloads:   types.Siacoins(2),
			FundAccount: types.Siacoins(3),
			Deletions:   types.Siacoins(4),
			SectorRoots: types.ZeroCurrency, // currently not persisted
		},

		ArchivalReason: api.ContractArchivalReasonRenewed,
		ContractPrice:  oldContractPrice,
		ProofHeight:    0,
		RenewedFrom:    types.FileContractID{},
		RevisionHeight: 0,
		RevisionNumber: 1,
		Size:           rhpv2.SectorSize,
		StartHeight:    100,
		State:          api.ContractStatePending,
		TotalCost:      oldContractTotal,
		WindowStart:    2,
		WindowEnd:      3,
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

	// Create a chain of 6 contracts.
	// Their start heights are 0, 1, 2, 3, 4, 5, 6.
	fcids := []types.FileContractID{{1}, {2}, {3}, {4}, {5}, {6}}
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
	for i := 0; i < len(contracts); i++ {
		var renewedFrom, renewedTo types.FileContractID
		if j := len(fcids) - 3 - i; j >= 0 {
			renewedFrom = fcids[j]
		}
		if j := len(fcids) - 1 - i; j >= 0 {
			renewedTo = fcids[j]
		}
		expected := api.ArchivedContract{
			ArchivalReason: api.ContractArchivalReasonRenewed,
			ID:             fcids[len(fcids)-2-i],
			HostKey:        hk,
			RenewedFrom:    renewedFrom,
			RenewedTo:      renewedTo,
			RevisionNumber: 200,
			StartHeight:    uint64(len(fcids) - 2 - i),
			Size:           4096,
			State:          api.ContractStatePending,
			WindowStart:    400,
			WindowEnd:      500,
		}
		if !reflect.DeepEqual(contracts[i], expected) {
			t.Log(cmp.Diff(contracts[i], expected))
			t.Fatal("wrong contract", i, contracts[i])
		}
	}

	// Fetch the ancestors with startHeight >= 5. That should return 0 contracts.
	contracts, err = ss.AncestorContracts(context.Background(), fcids[len(fcids)-1], 5)
	if err != nil {
		t.Fatal(err)
	} else if len(contracts) != 0 {
		t.Fatalf("should have 0 contracts but got %v", len(contracts))
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
	active, err := ss.Contracts(context.Background(), api.ContractsOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(active) != 1 || active[0].ID != fcids[0] {
		t.Fatal("wrong contracts", active)
	}

	// assert the two others were archived
	ffcids := make([]sql.FileContractID, 2)
	ffcids[0] = sql.FileContractID(fcids[1])
	ffcids[1] = sql.FileContractID(fcids[2])
	rows, err := ss.DB().Query(context.Background(), "SELECT reason FROM archived_contracts WHERE fcid IN (?, ?)",
		sql.FileContractID(ffcids[0]), sql.FileContractID(ffcids[1]))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var cnt int
	for rows.Next() {
		var reason string
		if err := rows.Scan(&reason); err != nil {
			t.Fatal(err)
		} else if cnt == 0 && reason != "foo" {
			t.Fatal("unexpected reason", reason)
		} else if cnt == 1 && reason != "bar" {
			t.Fatal("unexpected reason", reason)
		}
		cnt++
	}
	if cnt != 2 {
		t.Fatal("wrong number of archived contracts", cnt)
	}
}

func testContractRevision(fcid types.FileContractID, hk types.PublicKey) rhpv2.ContractRevision {
	uc := randomMultisigUC()
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
	objID := "/key1"
	if _, err := ss.addTestObject(objID, obj1); err != nil {
		t.Fatal(err)
	}

	// Fetch it using get and verify every field.
	obj, err := ss.Object(context.Background(), api.DefaultBucketName, objID)
	if err != nil {
		t.Fatal(err)
	}

	// compare timestamp separately
	if obj.ModTime.IsZero() {
		t.Fatal("unexpected", obj.ModTime)
	}
	obj.ModTime = api.TimeRFC3339{}

	obj1Slab0Key := obj1.Slabs[0].Key
	obj1Slab1Key := obj1.Slabs[1].Key

	expectedObj := api.Object{
		ObjectMetadata: api.ObjectMetadata{
			ETag:     testETag,
			Health:   1,
			ModTime:  api.TimeRFC3339{},
			Name:     objID,
			Size:     obj1.TotalSize(),
			MimeType: testMimeType,
		},
		Metadata: testMetadata,
		Object: &object.Object{
			Key: obj1.Key,
			Slabs: []object.SlabSlice{
				{
					Offset: 10,
					Length: 100,
					Slab: object.Slab{
						Health:    1,
						Key:       obj1Slab0Key,
						MinShards: 1,
						Shards: []object.Sector{
							{
								LatestHost: hk1,
								Root:       types.Hash256{1},
								Contracts: map[types.PublicKey][]types.FileContractID{
									hk1: {fcid1},
								},
							},
						},
					},
				},
				{
					Offset: 20,
					Length: 200,
					Slab: object.Slab{
						Health:    1,
						Key:       obj1Slab1Key,
						MinShards: 2,
						Shards: []object.Sector{
							{
								LatestHost: hk2,
								Root:       types.Hash256{2},
								Contracts: map[types.PublicKey][]types.FileContractID{
									hk2: {fcid2},
								},
							},
						},
					},
				},
			},
		},
	}
	if !reflect.DeepEqual(obj, expectedObj) {
		t.Fatal("object mismatch", cmp.Diff(obj, expectedObj, cmp.AllowUnexported(object.EncryptionKey{}), cmp.Comparer(api.CompareTimeRFC3339)))
	}

	// Try to store it again. Should work.
	if _, err := ss.addTestObject(objID, obj1); err != nil {
		t.Fatal(err)
	}

	// Fetch it again and verify.
	obj, err = ss.Object(context.Background(), api.DefaultBucketName, objID)
	if err != nil {
		t.Fatal(err)
	}

	// compare timestamp separately
	if obj.ModTime.IsZero() {
		t.Fatal("unexpected", obj.ModTime)
	}
	obj.ModTime = api.TimeRFC3339{}

	// The expected object is the same.
	if !reflect.DeepEqual(obj, expectedObj) {
		t.Fatal("object mismatch", cmp.Diff(obj, expectedObj, cmp.AllowUnexported(object.EncryptionKey{}), cmp.Comparer(api.CompareTimeRFC3339)))
	}

	// Fetch it and verify again.
	fullObj, err := ss.Object(ctx, api.DefaultBucketName, objID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(*fullObj.Object, obj1) {
		t.Fatal("object mismatch", cmp.Diff(fullObj, obj1))
	}

	expectedObjSlab1 := object.Slab{
		Health:    1,
		Key:       obj1Slab0Key,
		MinShards: 1,
		Shards: []object.Sector{
			{
				Contracts: map[types.PublicKey][]types.FileContractID{
					hk1: {fcid1},
				},
				LatestHost: hk1,
				Root:       types.Hash256{1},
			},
		},
	}

	expectedContract1 := api.ContractMetadata{
		ID:             fcid1,
		HostIP:         "",
		HostKey:        hk1,
		SiamuxAddr:     "",
		ProofHeight:    0,
		RevisionHeight: 0,
		RevisionNumber: 0,
		Size:           4096,
		StartHeight:    startHeight1,
		State:          api.ContractStatePending,
		WindowStart:    400,
		WindowEnd:      500,
		ContractPrice:  types.ZeroCurrency,
		RenewedFrom:    types.FileContractID{},
		Spending: api.ContractSpending{
			Uploads:     types.ZeroCurrency,
			Downloads:   types.ZeroCurrency,
			FundAccount: types.ZeroCurrency,
		},
		TotalCost:    totalCost1,
		ContractSets: nil,
	}

	expectedObjSlab2 := object.Slab{
		Health:    1,
		Key:       obj1Slab1Key,
		MinShards: 2,
		Shards: []object.Sector{
			{
				Contracts: map[types.PublicKey][]types.FileContractID{
					hk2: {fcid2},
				},
				LatestHost: hk2,
				Root:       types.Hash256{2},
			},
		},
	}

	expectedContract2 := api.ContractMetadata{
		ID:             fcid2,
		HostIP:         "",
		HostKey:        hk2,
		SiamuxAddr:     "",
		ProofHeight:    0,
		RevisionHeight: 0,
		RevisionNumber: 0,
		Size:           4096,
		StartHeight:    startHeight2,
		State:          api.ContractStatePending,
		WindowStart:    400,
		WindowEnd:      500,
		ContractPrice:  types.ZeroCurrency,
		RenewedFrom:    types.FileContractID{},
		Spending: api.ContractSpending{
			Uploads:     types.ZeroCurrency,
			Downloads:   types.ZeroCurrency,
			FundAccount: types.ZeroCurrency,
		},
		TotalCost:    totalCost2,
		ContractSets: nil,
	}

	// Compare slabs.
	slab1, err := ss.Slab(context.Background(), obj1Slab0Key)
	if err != nil {
		t.Fatal(err)
	}
	contract1, err := ss.Contract(context.Background(), fcid1)
	if err != nil {
		t.Fatal(err)
	}
	slab2, err := ss.Slab(context.Background(), obj1Slab1Key)
	if err != nil {
		t.Fatal(err)
	}
	contract2, err := ss.Contract(context.Background(), fcid2)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(slab1, expectedObjSlab1) {
		t.Fatal("mismatch", cmp.Diff(slab1, expectedObjSlab1))
	}
	if !reflect.DeepEqual(slab2, expectedObjSlab2) {
		t.Fatal("mismatch", cmp.Diff(slab2, expectedObjSlab2))
	}
	if !reflect.DeepEqual(contract1, expectedContract1) {
		t.Fatal("mismatch", cmp.Diff(contract1, expectedContract1))
	}
	if !reflect.DeepEqual(contract2, expectedContract2) {
		t.Fatal("mismatch", cmp.Diff(contract2, expectedContract2))
	}

	// Remove the first slab of the object.
	obj1.Slabs = obj1.Slabs[1:]
	fullObj, err = ss.addTestObject(objID, obj1)
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(*fullObj.Object, obj1) {
		t.Fatal("object mismatch")
	}

	// Sanity check the db at the end of the test. We expect:
	// - 1 element in the object table since we only stored and overwrote a single object
	// - 1 element in the slabs table since we updated the object to only have 1 slab
	// - 1 element in the slices table for the same reason
	// - 1 element in the sectors table for the same reason
	countCheck := func(objCount, sliceCount, slabCount, sectorCount int64) error {
		tableCountCheck := func(table string, tblCount int64) error {
			if count := ss.Count(table); count != tblCount {
				return fmt.Errorf("expected %v objects in table %v but got %v", tblCount, table, count)
			}
			return nil
		}
		// Check all tables.
		if err := tableCountCheck("objects", objCount); err != nil {
			return err
		}
		if err := tableCountCheck("slices", sliceCount); err != nil {
			return err
		}
		if err := tableCountCheck("slabs", slabCount); err != nil {
			return err
		}
		if err := tableCountCheck("sectors", sectorCount); err != nil {
			return err
		}
		return nil
	}
	if err := countCheck(1, 1, 1, 1); err != nil {
		t.Fatal(err)
	}

	// Delete the object. Due to the cascade this should delete everything
	// but the sectors.
	if err := ss.RemoveObjectBlocking(ctx, api.DefaultBucketName, objID); err != nil {
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
	if err := ss.UpdateContractSet(context.Background(), testContractSet, fcids, nil); err != nil {
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

	if _, err := ss.addTestObject("/foo", add); err != nil {
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
	if err := ss.UpdateContractSet(context.Background(), testContractSet, []types.FileContractID{fcids[0], fcids[2], fcids[3], fcids[4]}, []types.FileContractID{fcids[1]}); err != nil {
		t.Fatal(err)
	}
	if err := ss.RefreshHealth(context.Background()); err != nil {
		t.Fatal(err)
	}
	expectedHealth := float64(2) / float64(3)

	// assert object method
	obj, err = ss.Object(context.Background(), api.DefaultBucketName, "/foo")
	if err != nil {
		t.Fatal(err)
	} else if obj.Health != expectedHealth {
		t.Fatal("wrong health", obj.Health, expectedHealth)
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
	if err := ss.UpdateContractSet(context.Background(), testContractSet, []types.FileContractID{fcids[0], fcids[2], fcids[3]}, []types.FileContractID{fcids[4]}); err != nil {
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

	// add an empty object and assert health is 1
	if obj, err := ss.addTestObject("/bar", object.Object{
		Key:   object.GenerateEncryptionKey(),
		Slabs: nil,
	}); err != nil {
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
		_, err := ss.addTestObject(o.path, obj)
		if err != nil {
			t.Fatal(err)
		}
	}

	// assertMetadata asserts both ModTime, MimeType and ETag and clears them so the
	// entries are ready for comparison
	assertMetadata := func(entries []api.ObjectMetadata) {
		t.Helper()
		for i := range entries {
			// assert mod time
			if !strings.HasSuffix(entries[i].Name, "/") && entries[i].ModTime.IsZero() {
				t.Fatal("mod time should be set")
			}
			entries[i].ModTime = api.TimeRFC3339{}

			// assert mime type
			isDir := strings.HasSuffix(entries[i].Name, "/")
			if (isDir && entries[i].MimeType != "") || (!isDir && entries[i].MimeType != testMimeType) {
				t.Fatal("unexpected mime type", entries[i].MimeType)
			}
			entries[i].MimeType = ""

			// assert etag
			if isDir != (entries[i].ETag == "") {
				t.Fatal("etag should be set for files and empty for dirs")
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

	// update health of objects to match the overridden health of the slabs
	if err := updateAllObjectsHealth(ss.DB()); err != nil {
		t.Fatal()
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

		{"/", "", "size", "DESC", []api.ObjectMetadata{{Name: "/foo/", Size: 10, Health: .5}, {Name: "/FOO/", Size: 7, Health: 1}, {Name: "/fileś/", Size: 6, Health: 1}, {Name: "/gab/", Size: 5, Health: 1}}},
		{"/", "", "size", "ASC", []api.ObjectMetadata{{Name: "/gab/", Size: 5, Health: 1}, {Name: "/fileś/", Size: 6, Health: 1}, {Name: "/FOO/", Size: 7, Health: 1}, {Name: "/foo/", Size: 10, Health: .5}}},
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

func TestObjectEntriesExplicitDir(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	objects := []struct {
		path string
		size int64
	}{
		{"/dir/", 0},     // empty dir - created first
		{"/dir/file", 1}, // file uploaded to dir
		{"/dir2/", 2},    // empty dir - remains empty
	}

	ctx := context.Background()
	for _, o := range objects {
		obj := newTestObject(frand.Intn(9) + 1)
		obj.Slabs = obj.Slabs[:1]
		obj.Slabs[0].Length = uint32(o.size)
		_, err := ss.addTestObject(o.path, obj)
		if err != nil {
			t.Fatal(err)
		}
	}

	// set file health to 0.5
	if err := ss.overrideSlabHealth("/dir/file", 0.5); err != nil {
		t.Fatal(err)
	}

	// update health of objects to match the overridden health of the slabs
	if err := updateAllObjectsHealth(ss.DB()); err != nil {
		t.Fatal()
	}

	tests := []struct {
		path    string
		prefix  string
		sortBy  string
		sortDir string
		want    []api.ObjectMetadata
	}{
		{"/", "", "", "", []api.ObjectMetadata{
			{Name: "/dir/", Size: 1, Health: 0.5},
			{ETag: "d34db33f", Name: "/dir2/", Size: 2, Health: 1, MimeType: testMimeType}, // has MimeType and ETag since it's a file
		}},
		{"/dir/", "", "", "", []api.ObjectMetadata{{ETag: "d34db33f", Name: "/dir/file", Size: 1, Health: 0.5, MimeType: testMimeType}}},
	}
	for _, test := range tests {
		got, _, err := ss.ObjectEntries(ctx, api.DefaultBucketName, test.path, test.prefix, test.sortBy, test.sortDir, "", 0, -1)
		if err != nil {
			t.Fatal(err)
		}
		for i := range got {
			got[i].ModTime = api.TimeRFC3339{} // ignore time for comparison
		}
		if !reflect.DeepEqual(got, test.want) {
			t.Fatalf("\nlist: %v\nprefix: %v\ngot: %v\nwant: %v", test.path, test.prefix, got, test.want)
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
		if _, err := ss.addTestObject(o.path, obj); err != nil {
			t.Fatal(err)
		}
	}

	metadataEquals := func(got api.ObjectMetadata, want api.ObjectMetadata) bool {
		t.Helper()
		return got.Name == want.Name &&
			got.Size == want.Size &&
			got.Health == want.Health
	}

	assertEqual := func(got []api.ObjectMetadata, want []api.ObjectMetadata) {
		t.Helper()
		if len(got) != len(want) {
			t.Fatalf("unexpected result, we want %d items and we got %d items \ndiff: %v", len(want), len(got), cmp.Diff(got, want, cmp.Comparer(api.CompareTimeRFC3339)))
		}
		for i := range got {
			if !metadataEquals(got[i], want[i]) {
				t.Fatalf("unexpected result, got %v, want %v", got, want)
			}
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
		assertEqual(got, test.want)
		for offset := 0; offset < len(test.want); offset++ {
			if got, err := ss.SearchObjects(ctx, api.DefaultBucketName, test.path, offset, 1); err != nil {
				t.Fatal(err)
			} else if len(got) != 1 {
				t.Errorf("\nkey: %v unexpected number of objects, %d != 1", test.path, len(got))
			} else if !metadataEquals(got[0], test.want[offset]) {
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
	if err := ss.UpdateContractSet(context.Background(), testContractSet, goodContracts, nil); err != nil {
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

	if _, err := ss.addTestObject("/"+t.Name(), obj); err != nil {
		t.Fatal(err)
	}

	// add a partial slab
	_, _, err = ss.AddPartialSlab(context.Background(), []byte{1, 2, 3}, 1, 3, testContractSet)
	if err != nil {
		t.Fatal(err)
	}

	if err := ss.RefreshHealth(context.Background()); err != nil {
		t.Fatal(err)
	}
	slabs, err := ss.UnhealthySlabs(context.Background(), 0.99, testContractSet, -1)
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
	slabs, err = ss.UnhealthySlabs(context.Background(), 0.49, testContractSet, -1)
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
	slabs, err = ss.UnhealthySlabs(context.Background(), 0.49, t.Name(), -1)
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
	if err := ss.UpdateContractSet(context.Background(), testContractSet, fcids, nil); err != nil {
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
	if _, err := ss.addTestObject("/"+t.Name(), obj); err != nil {
		t.Fatal(err)
	}

	// assert it's unhealthy
	if err := ss.RefreshHealth(context.Background()); err != nil {
		t.Fatal(err)
	}
	slabs, err := ss.UnhealthySlabs(context.Background(), 0.99, testContractSet, -1)
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
	if err := ss.UpdateContractSet(context.Background(), testContractSet, fcids, nil); err != nil {
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
	if _, err := ss.addTestObject("/"+t.Name(), obj); err != nil {
		t.Fatal(err)
	}

	// assert it's healthy
	if err := ss.RefreshHealth(context.Background()); err != nil {
		t.Fatal(err)
	}
	slabs, err := ss.UnhealthySlabs(context.Background(), 0.99, testContractSet, -1)
	if err != nil {
		t.Fatal(err)
	}
	if len(slabs) != 0 {
		t.Fatalf("unexpected amount of slabs to migrate, %v!=0", len(slabs))
	}

	// delete the sector - we manually invalidate the slabs for the contract
	// before deletion.
	err = ss.invalidateSlabHealthByFCID(context.Background(), []types.FileContractID{(fcid1)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ss.DB().Exec(context.Background(), "DELETE FROM contract_sectors"); err != nil {
		t.Fatal(err)
	}

	// assert it's unhealthy
	if err := ss.RefreshHealth(context.Background()); err != nil {
		t.Fatal(err)
	}
	slabs, err = ss.UnhealthySlabs(context.Background(), 0.99, testContractSet, -1)
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
	if err := ss.UpdateContractSet(context.Background(), testContractSet, goodContracts, nil); err != nil {
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

	if _, err := ss.addTestObject("/"+t.Name(), obj); err != nil {
		t.Fatal(err)
	}

	if err := ss.RefreshHealth(context.Background()); err != nil {
		t.Fatal(err)
	}
	slabs, err := ss.UnhealthySlabs(context.Background(), 0.99, testContractSet, -1)
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
	if _, err := ss.addTestObject("/"+t.Name(), obj); err != nil {
		t.Fatal(err)
	}

	// Delete the contract.
	err = ss.ArchiveContract(context.Background(), fcid1, api.ContractArchivalReasonRemoved)
	if err != nil {
		t.Fatal(err)
	}

	// Check the join table. Should be empty.
	if n := ss.Count("contract_sectors"); n != 0 {
		t.Fatal("table should be empty", n)
	}

	// Add the contract back.
	_, err = ss.addTestContract(fcid1, hk1)
	if err != nil {
		t.Fatal(err)
	}

	// Add the object again.
	if _, err := ss.addTestObject("/"+t.Name(), obj); err != nil {
		t.Fatal(err)
	}

	// Delete the object.
	if err := ss.RemoveObjectBlocking(context.Background(), api.DefaultBucketName, "/"+t.Name()); err != nil {
		t.Fatal(err)
	}

	// Delete the sector.
	if _, err := ss.DB().Exec(context.Background(), "DELETE FROM sectors WHERE id = ?", 1); err != nil {
		t.Fatal(err)
	} else if n := ss.Count("contract_sectors"); n != 0 {
		t.Fatal("table should be empty", n)
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
	if _, err := ss.addTestObject("/"+t.Name(), obj); err != nil {
		t.Fatal(err)
	}

	// extract the slab key
	key, err := obj.Slabs[0].Key.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	// helper to fetch a slab from the database
	fetchSlab := func() (slab object.Slab) {
		t.Helper()
		if slab, err = ss.Slab(ctx, obj.Slabs[0].Key); err != nil {
			t.Fatal(err)
		}
		return
	}

	// helper to fetch contract ids for a sector
	contractIds := func(root types.Hash256) (fcids []types.FileContractID) {
		t.Helper()
		rows, err := ss.DB().Query(context.Background(), `
			SELECT fcid
			FROM contracts c
			INNER JOIN contract_sectors cs ON c.id = cs.db_contract_id
			INNER JOIN sectors s ON s.id = cs.db_sector_id
			WHERE s.root = ?
		`, sql.Hash256(root))
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		for rows.Next() {
			var fcid types.FileContractID
			if err := rows.Scan((*sql.FileContractID)(&fcid)); err != nil {
				t.Fatal(err)
			}
			fcids = append(fcids, fcid)
		}
		return
	}

	// fetch inserted slab
	inserted := fetchSlab()

	// assert both sectors were upload to one contract/host
	for i := 0; i < 2; i++ {
		if cids := contractIds(types.Hash256(inserted.Shards[i].Root)); len(cids) != 1 {
			t.Fatalf("sector %d was uploaded to unexpected amount of contracts, %v!=1", i+1, len(cids))
		} else if inserted.Shards[i].LatestHost != hks[i] {
			t.Fatalf("sector %d was uploaded to unexpected amount of hosts, %v!=1", i+1, len(hks))
		}
	}

	// select contracts h1 and h3 as good contracts (h2 is bad)
	goodContracts := []types.FileContractID{fcid1, fcid3}
	if err := ss.UpdateContractSet(ctx, testContractSet, goodContracts, nil); err != nil {
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
	if cids := contractIds(types.Hash256(updated.Shards[0].Root)); len(cids) != 1 {
		t.Fatalf("sector 1 was uploaded to unexpected amount of contracts, %v!=1", len(cids))
	} else if types.FileContractID(cids[0]) != fcid1 {
		t.Fatal("sector 1 was uploaded to unexpected contract", cids[0])
	} else if updated.Shards[0].LatestHost != hks[0] {
		t.Fatal("host key was invalid", updated.Shards[0].LatestHost, sql.PublicKey(hks[0]))
	} else if hks[0] != hk1 {
		t.Fatal("sector 1 was uploaded to unexpected host", hks[0])
	}

	// assert the second sector however is uploaded to two hosts, assert it's h2 and h3
	if cids := contractIds(types.Hash256(updated.Shards[1].Root)); len(cids) != 2 {
		t.Fatalf("sector 1 was uploaded to unexpected amount of contracts, %v!=2", len(cids))
	} else if types.FileContractID(cids[0]) != fcid2 || types.FileContractID(cids[1]) != fcid3 {
		t.Fatal("sector 1 was uploaded to unexpected contracts", cids[0], cids[1])
	} else if updated.Shards[0].LatestHost != hks[0] {
		t.Fatal("host key was invalid", updated.Shards[0].LatestHost, sql.PublicKey(hks[0]))
	}

	// assert there's still only one entry in the dbslab table
	if cnt := ss.Count("slabs"); cnt != 1 {
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

	if obj, err := ss.Object(context.Background(), api.DefaultBucketName, "/"+t.Name()); err != nil {
		t.Fatal(err)
	} else if len(obj.Slabs) != 1 {
		t.Fatalf("unexpected number of slabs, %v != 1", len(obj.Slabs))
	} else if obj.Slabs[0].Key.String() != updated.Key.String() {
		t.Fatalf("unexpected slab, %v != %v", obj.Slabs[0].Key, updated.Key)
	}

	// update the slab to change its contract set.
	if err := ss.UpdateContractSet(ctx, "other", nil, nil); err != nil {
		t.Fatal(err)
	}
	err = ss.UpdateSlab(ctx, slab, "other")
	if err != nil {
		t.Fatal(err)
	}
	var csID int64
	if err := ss.DB().QueryRow(context.Background(), "SELECT db_contract_set_id FROM slabs WHERE `key` = ?", key).
		Scan(&csID); err != nil {
		t.Fatal(err)
	} else if csID != ss.ContractSetID("other") {
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
	if err := ss.announceHost(hk, "address"); err != nil {
		t.Fatal(err)
	}

	fcid := types.FileContractID{1, 1, 1, 1, 1}
	cm, err := ss.addTestContract(fcid, hk)
	if err != nil {
		t.Fatal(err)
	} else if cm.Spending != (api.ContractSpending{}) {
		t.Fatal("spending should be all 0")
	} else if cm.Size != 0 && cm.RevisionNumber != 0 {
		t.Fatalf("unexpected size or revision number, %v %v", cm.Size, cm.RevisionNumber)
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
			RevisionNumber:   100,
			Size:             200,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	cm2, err := ss.Contract(context.Background(), fcid)
	if err != nil {
		t.Fatal(err)
	} else if cm2.Spending != expectedSpending {
		t.Fatal("invalid spending", cm2.Spending, expectedSpending)
	} else if cm2.Size != 200 && cm2.RevisionNumber != 100 {
		t.Fatalf("unexpected size or revision number, %v %v", cm2.Size, cm2.RevisionNumber)
	}

	// Record the same spending again but with a lower revision number. This
	// shouldn't update the size.
	err = ss.RecordContractSpending(context.Background(), []api.ContractSpendingRecord{
		{
			ContractID:       fcid,
			ContractSpending: expectedSpending,
			RevisionNumber:   100,
			Size:             200,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	expectedSpending = expectedSpending.Add(expectedSpending)
	cm3, err := ss.Contract(context.Background(), fcid)
	if err != nil {
		t.Fatal(err)
	} else if cm3.Spending != expectedSpending {
		t.Fatal("invalid spending")
	} else if cm2.Size != 200 && cm2.RevisionNumber != 100 {
		t.Fatalf("unexpected size or revision number, %v %v", cm2.Size, cm2.RevisionNumber)
	}
}

// TestRenameObjects is a unit test for RenameObject and RenameObjects.
func TestRenameObjectsA(t *testing.T) {
	cfg := defaultTestSQLStoreConfig
	cfg.persistent = true
	cfg.dir = "/users/peterjan/testing2"
	os.RemoveAll(cfg.dir)
	ss := newTestSQLStore(t, cfg)
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
		"/folder/file1",
		"/folder/foo/file2",
		"/foo",
		"/bar",
		"/baz",
		"/baz2",
		"/baz3",
	}
	ctx := context.Background()
	for _, path := range objects {
		if _, err := ss.addTestObject(path, newTestObject(1)); err != nil {
			t.Fatal(err)
		}
	}

	// Try renaming objects that don't exist.
	if err := ss.RenameObjectBlocking(ctx, api.DefaultBucketName, "/fileś", "/fileś2", false); !errors.Is(err, api.ErrObjectNotFound) {
		t.Fatal(err)
	}
	if err := ss.RenameObjectsBlocking(ctx, api.DefaultBucketName, "/fileś1", "/fileś2", false); !errors.Is(err, api.ErrObjectNotFound) {
		t.Fatal(err)
	}

	// Perform some renames.
	if err := ss.RenameObjectsBlocking(ctx, api.DefaultBucketName, "/folder/", "/fileś/", false); err != nil {
		t.Fatal(err)
	}
	if err := ss.RenameObjectsBlocking(ctx, api.DefaultBucketName, "/fileś/dir/", "/fileś/", false); err != nil {
		t.Fatal(err)
	}
	if err := ss.RenameObjectBlocking(ctx, api.DefaultBucketName, "/foo", "/fileś/foo", false); err != nil {
		t.Fatal(err)
	}
	if err := ss.RenameObjectBlocking(ctx, api.DefaultBucketName, "/bar", "/fileś/bar", false); err != nil {
		t.Fatal(err)
	}
	if err := ss.RenameObjectBlocking(ctx, api.DefaultBucketName, "/baz", "/fileś/baz", false); err != nil {
		t.Fatal(err)
	}
	if err := ss.RenameObjectsBlocking(ctx, api.DefaultBucketName, "/fileś/case", "/fileś/case1", false); err != nil {
		t.Fatal(err)
	}
	if err := ss.RenameObjectsBlocking(ctx, api.DefaultBucketName, "/fileś/CASE", "/fileś/case2", false); err != nil {
		t.Fatal(err)
	}
	if err := ss.RenameObjectsBlocking(ctx, api.DefaultBucketName, "/baz2", "/fileś/baz", false); !errors.Is(err, api.ErrObjectExists) {
		t.Fatal(err)
	} else if err := ss.RenameObjectsBlocking(ctx, api.DefaultBucketName, "/baz2", "/fileś/baz", true); err != nil {
		t.Fatal(err)
	} else if err := ss.RenameObjectBlocking(ctx, api.DefaultBucketName, "/baz3", "/fileś/baz", true); err != nil {
		t.Fatal(err)
	}

	// Paths after.
	objectsAfter := []string{
		"/fileś/file1",
		"/fileś/foo/file2",
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

	// Assert directories are correct
	expectedDirs := []struct {
		id       int64
		parentID int64
		name     string
	}{
		{
			id:       1,
			parentID: 0,
			name:     "/",
		},
		{
			id:       2,
			parentID: 1,
			name:     "/fileś/",
		},
		{
			id:       5,
			parentID: 2,
			name:     "/fileś/foo/",
		},
	}

	var n int64
	if err := ss.DB().QueryRow(ctx, "SELECT COUNT(*) FROM directories").Scan(&n); err != nil {
		t.Fatal(err)
	} else if n != int64(len(expectedDirs)) {
		t.Fatalf("unexpected number of directories, %v != %v", n, len(expectedDirs))
	}

	type row struct {
		ID       int64
		ParentID int64
		Name     string
	}
	rows, err := ss.DB().Query(context.Background(), "SELECT id, COALESCE(db_parent_id, 0), name FROM directories ORDER BY id ASC")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var i int
	for rows.Next() {
		var dir row
		if err := rows.Scan(&dir.ID, &dir.ParentID, &dir.Name); err != nil {
			t.Fatal(err)
		} else if dir.ID != expectedDirs[i].id {
			t.Fatalf("unexpected directory id, %v != %v", dir.ID, expectedDirs[i].id)
		} else if dir.ParentID != expectedDirs[i].parentID {
			t.Fatalf("unexpected directory parent id, %v != %v", dir.ParentID, expectedDirs[i].parentID)
		} else if dir.Name != expectedDirs[i].name {
			t.Fatalf("unexpected directory name, %v != %v", dir.Name, expectedDirs[i].name)
		}
		i++
	}
	if len(expectedDirs) != i {
		t.Fatalf("expected %v dirs, got %v", len(expectedDirs), i)
	}
}

func TestRenameObjectsRegression(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	// define directory structure
	objects := []string{
		"/firefly/s1/",
		"/firefly/s2/",
		"/suits/s1/",
		"/lost/",
		"/movie",

		"/firefly/trailer",
		"/firefly/s1/ep1",
		"/firefly/s1/ep2",
		"/firefly/s2/ep1",
	}

	testBucket := api.DefaultBucketName

	// define a helper to assert the number of objects with given prefix
	ctx := context.Background()
	assertNumObjects := func(path string, n int) {
		t.Helper()

		var err error
		var objects []api.ObjectMetadata
		if strings.HasSuffix(path, "/") {
			objects, _, err = ss.ObjectEntries(ctx, testBucket, path, "", "", "", "", 0, -1)
		} else {
			objects, err = ss.SearchObjects(ctx, testBucket, path, 0, -1)
		}

		if err != nil {
			t.Fatal(err)
		} else if len(objects) != n {
			t.Fatalf("unexpected number of objects %d != %d, objects:\n%+v", len(objects), n, objects)
		}
	}

	// persist the structure
	for _, path := range objects {
		var s int
		if !strings.HasSuffix(path, "/") {
			s = 1
		}
		if _, err := ss.addTestObject(path, newTestObject(s)); err != nil {
			t.Fatal(err)
		}
	}

	// assert the structure
	assertNumObjects("/", 4)
	assertNumObjects("/firefly", 6)
	assertNumObjects("/firefly/", 3)
	assertNumObjects("/firefly/s1/", 2)
	assertNumObjects("/firefly/s2/", 1)
	assertNumObjects("/suits/", 1)
	assertNumObjects("/lost/", 0)

	// assert we can't rename to an already existing directory without force
	if err := ss.RenameObjects(ctx, testBucket, "/firefly/s1/", "/firefly/s2/", false); !errors.Is(err, api.ErrObjectExists) {
		t.Fatal("unexpected error", err)
	}

	// assert we can forcefully rename it
	if err := ss.RenameObjects(ctx, testBucket, "/firefly/s1/", "/firefly/s2/", true); err != nil {
		t.Fatal(err)
	}
	assertNumObjects("/firefly/s2/", 2)

	// assert we can rename it and its children still point to the right directory
	if err := ss.RenameObjects(ctx, testBucket, "/firefly/s2/", "/firefly/s02/", false); err != nil {
		t.Fatal(err)
	}
	assertNumObjects("/firefly/", 2)
	assertNumObjects("/firefly/s2/", 0)
	assertNumObjects("/firefly/s02/", 2)

	// assert we rename a grand parent and all children remain intact
	if err := ss.RenameObjects(ctx, testBucket, "/firefly/", "/gotham/", true); err != nil {
		t.Fatal(err)
	}

	assertNumObjects("/gotham/", 2)
	assertNumObjects("/gotham/s02/", 2)
	assertNumObjects("/", 4)
}

// TestObjectsStats is a unit test for ObjectsStats.
func TestObjectsStats(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	// Fetch stats on clean database.
	info, err := ss.ObjectsStats(context.Background(), api.ObjectsStatsOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(info, api.ObjectsStatsResponse{MinHealth: 1}) {
		t.Fatal("unexpected stats", info)
	}

	// Create a few objects of different size.
	var objectsSize uint64
	var sectorsSize uint64
	var totalUploadedSize uint64
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
						c, err := ss.addTestContract(fcid, hpk)
						if err != nil {
							t.Fatal(err)
						}
						totalUploadedSize += c.Size
					}
				}
			}
		}

		key := "/" + hex.EncodeToString(frand.Bytes(32))
		if _, err := ss.addTestObject(key, obj); err != nil {
			t.Fatal(err)
		}
	}

	// Get all entries in contract_sectors and store them again with a different
	// contract id. This should cause the uploaded size to double.
	var newContractID types.FileContractID
	frand.Read(newContractID[:])
	hks, err := ss.addTestHosts(1)
	if err != nil {
		t.Fatal(err)
	}
	hk := hks[0]
	c, err := ss.addTestContract(newContractID, hk)
	if err != nil {
		t.Fatal(err)
	}
	totalUploadedSize += c.Size
	if _, err := ss.DB().Exec(context.Background(), `
		INSERT INTO contract_sectors (db_contract_id, db_sector_id)
		SELECT (
			SELECT id FROM contracts WHERE fcid = ?
		), db_sector_id
		FROM contract_sectors
	`, sql.FileContractID(newContractID)); err != nil {
		t.Fatal(err)
	}

	// Check sizes.
	for _, opts := range []api.ObjectsStatsOpts{
		{},                              // any bucket
		{Bucket: api.DefaultBucketName}, // specific bucket
	} {
		info, err = ss.ObjectsStats(context.Background(), opts)
		if err != nil {
			t.Fatal(err)
		} else if info.TotalObjectsSize != objectsSize {
			t.Fatal("wrong size", info.TotalObjectsSize, objectsSize)
		} else if info.TotalSectorsSize != sectorsSize {
			t.Fatal("wrong size", info.TotalSectorsSize, sectorsSize)
		} else if info.TotalUploadedSize != totalUploadedSize {
			t.Fatal("wrong size", info.TotalUploadedSize, totalUploadedSize)
		} else if info.NumObjects != 2 {
			t.Fatal("wrong number of objects", info.NumObjects, 2)
		}
	}

	// Check other bucket.
	if err := ss.CreateBucket(context.Background(), "other", api.BucketPolicy{}); err != nil {
		t.Fatal(err)
	} else if info, err := ss.ObjectsStats(context.Background(), api.ObjectsStatsOpts{Bucket: "other"}); err != nil {
		t.Fatal(err)
	} else if info.TotalObjectsSize != 0 {
		t.Fatal("wrong size", info.TotalObjectsSize)
	} else if info.TotalSectorsSize != 0 {
		t.Fatal("wrong size", info.TotalSectorsSize, 0)
	} else if info.TotalUploadedSize != totalUploadedSize {
		t.Fatal("wrong size", info.TotalUploadedSize, totalUploadedSize)
	} else if info.NumObjects != 0 {
		t.Fatal("wrong number of objects", info.NumObjects)
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

	type bufferedSlab struct {
		ID       uint
		Filename string
	}
	fetchBuffer := func(ec object.EncryptionKey) (b bufferedSlab) {
		t.Helper()
		if err := ss.DB().QueryRow(context.Background(), `
			SELECT bs.id, bs.filename
			FROM buffered_slabs bs
			INNER JOIN slabs sla ON sla.db_buffered_slab_id = bs.id
			WHERE sla.key = ?
		`, sql.EncryptionKey(ec)).
			Scan(&b.ID, &b.Filename); err != nil && !errors.Is(err, dsql.ErrNoRows) {
			t.Fatal(err)
		}
		return
	}

	buffer := fetchBuffer(slabs[0].Key)
	if buffer.Filename == "" {
		t.Fatal("empty filename")
	}
	buffer1Name := buffer.Filename
	assertBuffer(buffer1Name, 4, false, false)

	// Use the added partial slab to create an object.
	testObject := func(partialSlabs []object.SlabSlice) object.Object {
		obj := object.Object{
			Key: object.GenerateEncryptionKey(),
			Slabs: []object.SlabSlice{
				{
					Slab: object.Slab{
						Health:    1.0,
						Key:       object.GenerateEncryptionKey(),
						MinShards: 1,
						Shards: []object.Sector{
							newTestShard(hk1, fcid1, frand.Entropy256()),
							newTestShard(hk2, fcid2, frand.Entropy256()),
						},
					},
					Offset: 0,
					Length: rhpv2.SectorSize,
				},
			},
		}
		obj.Slabs = append(obj.Slabs, partialSlabs...)
		return obj
	}
	obj := testObject(slabs)
	fetched, err := ss.addTestObject("/key", obj)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(obj, *fetched.Object) {
		t.Fatal("mismatch", cmp.Diff(obj, fetched.Object, cmp.AllowUnexported(object.EncryptionKey{})))
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
	assertBuffer(buffer1Name, 4194303, false, false)

	// Create an object again.
	obj2 := testObject(slabs)
	fetched, err = ss.addTestObject("/key2", obj2)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(obj2, *fetched.Object) {
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
	assertBuffer(buffer1Name, rhpv2.SectorSize, true, false)

	buffer = fetchBuffer(slabs[1].Key)
	buffer2Name := buffer.Filename
	assertBuffer(buffer2Name, 1, false, false)

	// Create an object again.
	obj3 := testObject(slabs)
	fetched, err = ss.addTestObject("/key3", obj3)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(obj3, *fetched.Object) {
		t.Fatal("mismatch", cmp.Diff(obj3, fetched.Object, cmp.AllowUnexported(object.EncryptionKey{})))
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

	buffer = fetchBuffer(packedSlabs[0].Key)
	if buffer.ID != packedSlabs[0].BufferID {
		t.Fatalf("wrong buffer id, %v != %v", buffer.ID, packedSlabs[0].BufferID)
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

	buffer = fetchBuffer(packedSlabs[0].Key)
	if buffer != (bufferedSlab{}) {
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
	slices1, _, err := ss.AddPartialSlab(ctx, frand.Bytes(rhpv2.SectorSize/2), 1, 2, testContractSet)
	if err != nil {
		t.Fatal(err)
	}
	slices2, bufferSize, err := ss.AddPartialSlab(ctx, frand.Bytes(rhpv2.SectorSize/2), 1, 2, testContractSet)
	if err != nil {
		t.Fatal(err)
	}

	// Associate them with an object.
	if _, err := ss.addTestObject("/"+t.Name(), object.Object{
		Key:   object.GenerateEncryptionKey(),
		Slabs: append(slices1, slices2...),
	}); err != nil {
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
		if _, err := ss.addTestObject(fmt.Sprintf("/obj_%d", i+1), object.Object{
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
	s, err := ss.ObjectsStats(context.Background(), api.ObjectsStatsOpts{})
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
	if err := ss.RemoveObjectBlocking(context.Background(), api.DefaultBucketName, "/obj_1"); err != nil {
		t.Fatal(err)
	}

	// assert there's one sector that can be pruned and assert it's from fcid 1
	if n := prunableData(nil); n != rhpv2.SectorSize {
		t.Fatalf("unexpected amount of prunable data %v", n)
	} else if n := prunableData(&fcids[1]); n != 0 {
		t.Fatalf("expected no prunable data %v", n)
	}

	// remove the second object
	if err := ss.RemoveObjectBlocking(context.Background(), api.DefaultBucketName, "/obj_2"); err != nil {
		t.Fatal(err)
	}

	// assert there's now two sectors that can be pruned
	if n := prunableData(nil); n != rhpv2.SectorSize*2 {
		t.Fatalf("unexpected amount of prunable data %v", n)
	} else if n := prunableData(&fcids[0]); n != rhpv2.SectorSize {
		t.Fatalf("unexpected amount of prunable data %v", n)
	} else if n := prunableData(&fcids[1]); n != rhpv2.SectorSize {
		t.Fatalf("unexpected amount of prunable data %v", n)
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
	for _, name := range []string{"/obj1", "/obj2", "/obj3"} {
		obj.Slabs[0].Length++
		if _, err := ss.addTestObject(name, obj); err != nil {
			t.Fatal(err)
		}
	}

	// Fetch the objects by slab.
	objs, err := ss.ObjectsBySlabKey(context.Background(), api.DefaultBucketName, slab.Key)
	if err != nil {
		t.Fatal(err)
	}
	for i, name := range []string{"/obj1", "/obj2", "/obj3"} {
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
	err := ss.UpdateObject(context.Background(), "unknown-bucket", "/foo", testContractSet, testETag, testMimeType, testMetadata, obj)
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
		err := ss.UpdateObject(ctx, o.bucket, o.path, testContractSet, testETag, testMimeType, testMetadata, obj)
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
	if err := ss.RenameObjectBlocking(context.Background(), b1, "/foo/bar", "/foo/baz", false); err != nil {
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
	if err := ss.RenameObjectsBlocking(context.Background(), b2, "/foo/bar", "/foo/bam", false); err != nil {
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
	if err := ss.RemoveObjectBlocking(context.Background(), b2, "/foo/baz"); !errors.Is(err, api.ErrObjectNotFound) {
		t.Fatal(err)
	} else if err := ss.RemoveObjectBlocking(context.Background(), b1, "/foo/baz"); err != nil {
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
	} else if err := ss.RemoveObjectsBlocking(context.Background(), b2, "/"); err != nil {
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
	if obj, err := ss.Object(context.Background(), b1, "/bar"); err != nil {
		t.Fatal(err)
	} else if objects, err := ss.ObjectsBySlabKey(context.Background(), b1, obj.Slabs[0].Key); err != nil {
		t.Fatal(err)
	} else if len(objects) != 1 {
		t.Fatal("expected 1 object", len(objects))
	} else if objects, err := ss.ObjectsBySlabKey(context.Background(), b2, obj.Slabs[0].Key); err != nil {
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
	err := ss.UpdateObject(ctx, "src", "/foo", testContractSet, testETag, testMimeType, testMetadata, obj)
	if err != nil {
		t.Fatal(err)
	}

	// Copy it within the same bucket.
	if om, err := ss.CopyObject(ctx, "src", "src", "/foo", "/bar", "", nil); err != nil {
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
	if om, err := ss.CopyObject(ctx, "src", "dst", "/foo", "/bar", "", nil); err != nil {
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
	defer ss.Close()

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
	slabs, _, err := ss.AddPartialSlab(context.Background(), frand.Bytes(completeSize), 1, 1, testContractSet)
	if err != nil {
		t.Fatal(err)
	}

	// add it to an object to prevent it from getting pruned.
	_, err = ss.addTestObject("/"+t.Name(), object.Object{
		Key:   object.GenerateEncryptionKey(),
		Slabs: slabs,
	})
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
	uc := randomMultisigUC()
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
	} else if count := ss.Count("contract_sectors"); count != 1 {
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
			entries[i].ModTime = api.TimeRFC3339{}
		}
	}

	ctx := context.Background()
	for _, o := range objects {
		obj := newTestObject(frand.Intn(9) + 1)
		obj.Slabs = obj.Slabs[:1]
		obj.Slabs[0].Length = uint32(o.size)
		if _, err := ss.addTestObject(o.path, obj); err != nil {
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

	// update health of objects to match the overridden health of the slabs
	if err := updateAllObjectsHealth(ss.DB()); err != nil {
		t.Fatal()
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
		{"/foo", "size", "ASC", "", []api.ObjectMetadata{{Name: "/foo/bar", Size: 1, Health: 1}, {Name: "/foo/bat", Size: 2, Health: 1}, {Name: "/foo/baz/quux", Size: 3, Health: .75}, {Name: "/foo/baz/quuz", Size: 4, Health: .5}}},
		{"/foo", "size", "DESC", "", []api.ObjectMetadata{{Name: "/foo/baz/quuz", Size: 4, Health: .5}, {Name: "/foo/baz/quux", Size: 3, Health: .75}, {Name: "/foo/bat", Size: 2, Health: 1}, {Name: "/foo/bar", Size: 1, Health: 1}}},
	}
	// set common fields
	for i := range tests {
		for j := range tests[i].want {
			tests[i].want[j].ETag = testETag
			tests[i].want[j].MimeType = testMimeType
		}
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
	fcids, _, err := ss.addTestContracts([]types.PublicKey{hk1, hk1, hk2, hk2})
	if err != nil {
		t.Fatal(err)
	}

	// create a healthy slab with one sector that is uploaded to all contracts.
	root := types.Hash256{1, 2, 3}
	ss.InsertSlab(object.Slab{
		Key:       object.GenerateEncryptionKey(),
		MinShards: 1,
		Shards: []object.Sector{
			{
				Contracts: map[types.PublicKey][]types.FileContractID{
					hk1: fcids,
				},
				Root:       root,
				LatestHost: hk1,
			},
		},
	})

	// Make sure 4 contractSector entries exist.
	if n := ss.Count("contract_sectors"); n != 4 {
		t.Fatal("expected 4 contract-sector links", n)
	}

	// Prune the sector from hk1.
	if n, err := ss.DeleteHostSector(context.Background(), hk1, root); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal("no sectors were pruned", n)
	}

	// Make sure 2 contractSector entries exist.
	if n := ss.Count("contract_sectors"); n != 2 {
		t.Fatal("expected 2 contract-sector links", n)
	}

	// Find the slab. It should have an invalid health.
	var slabID int64
	var validUntil int64
	if err := ss.DB().QueryRow(context.Background(), "SELECT id, health_valid_until FROM slabs").Scan(&slabID, &validUntil); err != nil {
		t.Fatal(err)
	} else if time.Now().Before(time.Unix(validUntil, 0)) {
		t.Fatal("expected health to be invalid")
	}

	sectorContractCnt := func(root types.Hash256) (n int) {
		t.Helper()
		err := ss.DB().QueryRow(context.Background(), `
			SELECT COUNT(*)
			FROM contract_sectors cs
			INNER JOIN sectors s ON s.id = cs.db_sector_id
			WHERE s.root = ?
		`, (*sql.Hash256)(&root)).Scan(&n)
		if err != nil {
			t.Fatal(err)
		}
		return
	}

	// helper to fetch sectors
	type sector struct {
		LatestHost types.PublicKey
		Root       types.Hash256
		SlabID     int64
	}
	fetchSectors := func() (sectors []sector) {
		t.Helper()
		rows, err := ss.DB().Query(context.Background(), "SELECT root, latest_host, db_slab_id FROM sectors")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		for rows.Next() {
			var s sector
			if err := rows.Scan((*sql.PublicKey)(&s.Root), (*sql.Hash256)(&s.LatestHost), &s.SlabID); err != nil {
				t.Fatal(err)
			}
			sectors = append(sectors, s)
		}
		return
	}

	// Fetch the sector and assert the contracts association.
	if sectors := fetchSectors(); len(sectors) != 1 {
		t.Fatal("expected 1 sector", len(sectors))
	} else if cnt := sectorContractCnt(types.Hash256(sectors[0].Root)); cnt != 2 {
		t.Fatal("expected 2 contracts", cnt)
	} else if sectors[0].LatestHost != hk2 {
		t.Fatalf("expected latest host to be hk2, got %v", sectors[0].LatestHost)
	} else if sectors[0].SlabID != slabID {
		t.Fatalf("expected slab id to be %v, got %v", slabID, sectors[0].SlabID)
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

	// Prune the sector from hk2.
	if n, err := ss.DeleteHostSector(context.Background(), hk2, root); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal("no sectors were pruned", n)
	}

	hi, err = ss.Host(context.Background(), hk2)
	if err != nil {
		t.Fatal(err)
	} else if hi.Interactions.LostSectors != 2 {
		t.Fatalf("expected 0 lost sector, got %v", hi.Interactions.LostSectors)
	}

	// Fetch the sector and check the public key has the default value
	if sectors := fetchSectors(); len(sectors) != 1 {
		t.Fatal("expected 1 sector", len(sectors))
	} else if cnt := sectorContractCnt(types.Hash256(sectors[0].Root)); cnt != 0 {
		t.Fatal("expected 0 contracts", cnt)
	} else if sector := sectors[0]; sector.LatestHost != [32]byte{} {
		t.Fatal("expected latest host to be empty", sector.LatestHost)
	} else if sectors[0].SlabID != slabID {
		t.Fatalf("expected slab id to be %v, got %v", slabID, sectors[0].SlabID)
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
	defer ss.Close()

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
	_, err = ss.addTestObject("/"+t.Name(), object.Object{
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
	}, testContractSet); !errors.Is(err, isql.ErrInvalidNumberOfShards) {
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
	if err := ss.UpdateSlab(context.Background(), reversedSlab, testContractSet); !errors.Is(err, isql.ErrShardRootChanged) {
		t.Fatal(err)
	}
}

func TestSlabHealthInvalidation(t *testing.T) {
	// create db
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	// define a helper to assert the health validity of a given slab
	assertHealthValid := func(slabKey object.EncryptionKey, expected bool) {
		t.Helper()

		var validUntil int64
		if err := ss.DB().QueryRow(context.Background(), "SELECT health_valid_until FROM slabs WHERE `key` = ?", sql.EncryptionKey(slabKey)).Scan(&validUntil); err != nil {
			t.Fatal(err)
		} else if valid := time.Now().Before(time.Unix(validUntil, 0)); valid != expected {
			t.Fatal("unexpected health valid", valid)
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
	_, err = ss.addTestObject("/o1", object.Object{
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
	err = ss.UpdateObject(context.Background(), api.DefaultBucketName, "/o2", testContractSet, testETag, testMimeType, testMetadata, object.Object{
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
	cscs, err := ss.Contracts(context.Background(), api.ContractsOpts{ContractSet: testContractSet})
	if err != nil {
		t.Fatal(err)
	} else if len(cscs) != 0 {
		t.Fatal("expected 0 contracts", len(cscs))
	}

	// refresh health
	refreshHealth(s1, s2)

	// add 2 contracts to the contract set
	if err := ss.UpdateContractSet(context.Background(), testContractSet, fcids[:2], nil); err != nil {
		t.Fatal(err)
	}
	assertHealthValid(s1, false)
	assertHealthValid(s2, true)

	// refresh health
	refreshHealth(s1, s2)

	// switch out the contract set with two new contracts
	if err := ss.UpdateContractSet(context.Background(), testContractSet, fcids[2:], fcids[:2]); err != nil {
		t.Fatal(err)
	}
	assertHealthValid(s1, false)
	assertHealthValid(s2, false)

	// assert there are 2 contracts in the contract set
	cscs, err = ss.Contracts(context.Background(), api.ContractsOpts{ContractSet: testContractSet})
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
		if _, err := ss.DB().Exec(context.Background(), "UPDATE slabs SET health_valid_until = 0;"); err != nil {
			t.Fatal(err)
		}

		// refresh health
		now := time.Now()
		if err := ss.RefreshHealth(context.Background()); err != nil {
			t.Fatal(err)
		}

		// fetch health_valid_until
		var validUntil int64
		if err := ss.DB().QueryRow(context.Background(), "SELECT health_valid_until FROM slabs").Scan(&validUntil); err != nil {
			t.Fatal(err)
		}

		// assert it's validity is within expected bounds
		minValidity := now.Add(refreshHealthMinHealthValidity).Add(-time.Second) // avoid NDF
		maxValidity := now.Add(refreshHealthMaxHealthValidity).Add(time.Second)  // avoid NDF
		validUntilUnix := time.Unix(validUntil, 0)
		if !(minValidity.Before(validUntilUnix) && maxValidity.After(validUntilUnix)) {
			t.Fatal("valid until not in boundaries", minValidity, maxValidity, validUntil, now)
		}
	}
}

func TestRefreshHealth(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	// define a helper function to return an object's health
	health := func(name string) float64 {
		t.Helper()
		o, err := ss.Object(context.Background(), api.DefaultBucketName, name)
		if err != nil {
			t.Fatal(err)
		}
		return o.Health
	}

	// add test hosts
	hks, err := ss.addTestHosts(8)
	if err != nil {
		t.Fatal(err)
	}

	// add test contract & set it as contract set
	fcids, _, err := ss.addTestContracts(hks)
	if err != nil {
		t.Fatal(err)
	}
	err = ss.UpdateContractSet(context.Background(), testContractSet, fcids, nil)
	if err != nil {
		t.Fatal(err)
	}

	// add two test objects
	o1 := "/" + t.Name() + "1"
	if added, err := ss.addTestObject(o1, object.Object{
		Key: object.GenerateEncryptionKey(),
		Slabs: []object.SlabSlice{{Slab: object.Slab{
			MinShards: 2,
			Key:       object.GenerateEncryptionKey(),
			Shards: []object.Sector{
				newTestShard(hks[0], fcids[0], types.Hash256{0}),
				newTestShard(hks[1], fcids[1], types.Hash256{1}),
				newTestShard(hks[2], fcids[2], types.Hash256{2}),
				newTestShard(hks[3], fcids[3], types.Hash256{3}),
			},
		}}},
	}); err != nil {
		t.Fatal(err)
	} else if added.Health != 1 {
		t.Fatal("expected health to be 1, got", added.Health)
	}

	o2 := "/" + t.Name() + "2"
	if added, err := ss.addTestObject(o2, object.Object{
		Key: object.GenerateEncryptionKey(),
		Slabs: []object.SlabSlice{{Slab: object.Slab{
			MinShards: 2,
			Key:       object.GenerateEncryptionKey(),
			Shards: []object.Sector{
				newTestShard(hks[4], fcids[4], types.Hash256{4}),
				newTestShard(hks[5], fcids[5], types.Hash256{5}),
				newTestShard(hks[6], fcids[6], types.Hash256{6}),
				newTestShard(hks[7], fcids[7], types.Hash256{7}),
			},
		}}},
	}); err != nil {
		t.Fatal(err)
	} else if added.Health != 1 {
		t.Fatal("expected health to be 1, got", added.Health)
	}

	// update contract set to not contain the first contract
	err = ss.UpdateContractSet(context.Background(), testContractSet, fcids[1:], fcids[:1])
	if err != nil {
		t.Fatal(err)
	}
	err = ss.RefreshHealth(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if health(o1) != .5 {
		t.Fatal("expected health to be .5, got", health(o1))
	} else if health(o2) != 1 {
		t.Fatal("expected health to be 1, got", health(o2))
	}

	// update contract set again to increase health of o1 again and lower health
	// of o2
	err = ss.UpdateContractSet(context.Background(), testContractSet, fcids[:6], fcids[6:])
	if err != nil {
		t.Fatal(err)
	}
	err = ss.RefreshHealth(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if health(o1) != 1 {
		t.Fatal("expected health to be .4, got", health(o1))
	} else if health(o2) != 0 {
		t.Fatal("expected health to be 0, got", health(o2))
	}

	// add another object that is empty
	o3 := "/" + t.Name() + "3"
	if added, err := ss.addTestObject(o3, object.Object{
		Key: object.GenerateEncryptionKey(),
	}); err != nil {
		t.Fatal(err)
	} else if added.Health != 1 {
		t.Fatal("expected health to be 1, got", added.Health)
	}

	// a refresh should keep the health at 1
	if err := ss.RefreshHealth(context.Background()); err != nil {
		t.Fatal(err)
	} else if health(o3) != 1 {
		t.Fatalf("expected health to be 1, got %v", health(o3))
	}
}

func TestSlabCleanup(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	// create contract set
	err := ss.db.Transaction(context.Background(), func(tx sql.DatabaseTx) error {
		return tx.UpdateContractSet(context.Background(), testContractSet, nil, nil)
	})
	if err != nil {
		t.Fatal(err)
	}
	csID := ss.ContractSetID(testContractSet)

	// create buffered slab
	bsID := uint(1)
	if _, err := ss.DB().Exec(context.Background(), "INSERT INTO buffered_slabs (filename) VALUES ('foo');"); err != nil {
		t.Fatal(err)
	}

	var dirID int64
	err = ss.db.Transaction(context.Background(), func(tx sql.DatabaseTx) error {
		var err error
		dirID, err = tx.InsertDirectories(context.Background(), api.DefaultBucketName, "/")
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	// create objects
	insertObjStmt, err := ss.DB().Prepare(context.Background(), "INSERT INTO objects (db_directory_id, object_id, db_bucket_id, health) VALUES (?, ?, ?, ?);")
	if err != nil {
		t.Fatal(err)
	}
	defer insertObjStmt.Close()

	var obj1ID, obj2ID int64
	if res, err := insertObjStmt.Exec(context.Background(), dirID, "/1", ss.DefaultBucketID(), 1); err != nil {
		t.Fatal(err)
	} else if obj1ID, err = res.LastInsertId(); err != nil {
		t.Fatal(err)
	} else if res, err := insertObjStmt.Exec(context.Background(), dirID, "/2", ss.DefaultBucketID(), 1); err != nil {
		t.Fatal(err)
	} else if obj2ID, err = res.LastInsertId(); err != nil {
		t.Fatal(err)
	}

	// create a slab
	var slabID int64
	if res, err := ss.DB().Exec(context.Background(), "INSERT INTO slabs (db_contract_set_id, `key`, health_valid_until) VALUES (?, ?, ?);", csID, sql.EncryptionKey(object.GenerateEncryptionKey()), 100); err != nil {
		t.Fatal(err)
	} else if slabID, err = res.LastInsertId(); err != nil {
		t.Fatal(err)
	}

	// statement to reference slabs by inserting a slice for an object
	insertSlabRefStmt, err := ss.DB().Prepare(context.Background(), "INSERT INTO slices (db_object_id, db_slab_id) VALUES (?, ?);")
	if err != nil {
		t.Fatal(err)
	}
	defer insertSlabRefStmt.Close()

	// reference the slab
	if _, err := insertSlabRefStmt.Exec(context.Background(), obj1ID, slabID); err != nil {
		t.Fatal(err)
	} else if _, err := insertSlabRefStmt.Exec(context.Background(), obj2ID, slabID); err != nil {
		t.Fatal(err)
	}

	// delete the object
	err = ss.RemoveObjectBlocking(context.Background(), api.DefaultBucketName, "/1")
	if err != nil {
		t.Fatal(err)
	}

	// check slab count
	if slabCntr := ss.Count("slabs"); slabCntr != 1 {
		t.Fatalf("expected 1 slabs, got %v", slabCntr)
	}

	// delete second object
	err = ss.RemoveObjectBlocking(context.Background(), api.DefaultBucketName, "/2")
	if err != nil {
		t.Fatal(err)
	} else if slabCntr := ss.Count("slabs"); slabCntr != 0 {
		t.Fatalf("expected 0 slabs, got %v", slabCntr)
	}

	// create another slab referencing the buffered slab
	var bufferedSlabID int64
	if res, err := ss.DB().Exec(context.Background(), "INSERT INTO slabs (db_buffered_slab_id, db_contract_set_id, `key`, health_valid_until) VALUES (?, ?, ?, ?);", bsID, csID, sql.EncryptionKey(object.GenerateEncryptionKey()), 100); err != nil {
		t.Fatal(err)
	} else if bufferedSlabID, err = res.LastInsertId(); err != nil {
		t.Fatal(err)
	}

	var obj3ID int64
	if res, err := insertObjStmt.Exec(context.Background(), dirID, "3", ss.DefaultBucketID(), 1); err != nil {
		t.Fatal(err)
	} else if obj3ID, err = res.LastInsertId(); err != nil {
		t.Fatal(err)
	} else if _, err := insertSlabRefStmt.Exec(context.Background(), obj3ID, bufferedSlabID); err != nil {
		t.Fatal(err)
	}
	if slabCntr := ss.Count("slabs"); slabCntr != 1 {
		t.Fatalf("expected 1 slabs, got %v", slabCntr)
	}

	// delete third object
	err = ss.RemoveObjectBlocking(context.Background(), api.DefaultBucketName, "3")
	if err != nil {
		t.Fatal(err)
	} else if slabCntr := ss.Count("slabs"); slabCntr != 1 {
		t.Fatalf("expected 1 slabs, got %v", slabCntr)
	}
}

func TestUpdateObjectReuseSlab(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	minShards, totalShards := 10, 30

	// create 90 hosts, enough for 3 slabs with 30 each
	hks, err := ss.addTestHosts(3 * totalShards)
	if err != nil {
		t.Fatal(err)
	}

	// create one contract each
	fcids, _, err := ss.addTestContracts(hks)
	if err != nil {
		t.Fatal(err)
	}

	// create an object
	obj := object.Object{
		Key: object.GenerateEncryptionKey(),
	}
	// add 2 slabs
	for i := 0; i < 2; i++ {
		obj.Slabs = append(obj.Slabs, object.SlabSlice{
			Offset: 0,
			Length: uint32(minShards) * rhpv2.SectorSize,
			Slab: object.Slab{
				Key:       object.GenerateEncryptionKey(),
				MinShards: uint8(minShards),
			},
		})
	}
	// 30 shards each
	for i := 0; i < len(obj.Slabs); i++ {
		for j := 0; j < totalShards; j++ {
			obj.Slabs[i].Shards = append(obj.Slabs[i].Shards, object.Sector{
				Contracts: map[types.PublicKey][]types.FileContractID{
					hks[i*totalShards+j]: {
						fcids[i*totalShards+j],
					},
				},
				LatestHost: hks[i*totalShards+j],
				Root:       frand.Entropy256(),
			})
		}
	}

	// add the object
	_, err = ss.addTestObject("/1", obj)
	if err != nil {
		t.Fatal(err)
	}

	// helper to fetch relevant fields from an object
	fetchObj := func(bid int64, oid string) (id, bucketID int64, objectID string, health float64, size int64) {
		t.Helper()
		err := ss.DB().QueryRow(context.Background(), `
			SELECT id, db_bucket_id, object_id, health, size
			FROM objects
			WHERE db_bucket_id = ? AND object_id = ?
		`, bid, oid).Scan(&id, &bucketID, &objectID, &health, &size)
		if err != nil {
			t.Fatal(err)
		}
		return
	}

	// fetch the object
	id, bid, oid, health, size := fetchObj(ss.DefaultBucketID(), "/1")
	if id != 1 {
		t.Fatal("unexpected id", id)
	} else if bid != ss.DefaultBucketID() {
		t.Fatal("bucket id mismatch", bid)
	} else if oid != "/1" {
		t.Fatal("object id mismatch", oid)
	} else if health != 1 {
		t.Fatal("health mismatch", health)
	} else if size != obj.TotalSize() {
		t.Fatal("size mismatch", size)
	}

	// helper to fetch object's slices
	type slice struct {
		ID          int64
		ObjectIndex int64
		Offset      int64
		Length      int64
		SlabID      int64
	}
	fetchSlicesByObjectID := func(oid int64) (slices []slice) {
		t.Helper()
		rows, err := ss.DB().Query(context.Background(), "SELECT id, object_index, offset, length, db_slab_id FROM slices WHERE db_object_id = ?", oid)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		for rows.Next() {
			var s slice
			if err := rows.Scan(&s.ID, &s.ObjectIndex, &s.Offset, &s.Length, &s.SlabID); err != nil {
				t.Fatal(err)
			}
			slices = append(slices, s)
		}
		return
	}

	// fetch its slices
	slices := fetchSlicesByObjectID(id)
	if len(slices) != 2 {
		t.Fatal("invalid number of slices", len(slices))
	}

	// helper to fetch sectors
	type sector struct {
		ID         int64
		SlabID     int64
		LatestHost types.PublicKey
		Root       types.Hash256
	}
	fetchSectorsBySlabID := func(slabID int64) (sectors []sector) {
		t.Helper()
		rows, err := ss.DB().Query(context.Background(), "SELECT id, db_slab_id, root, latest_host FROM sectors WHERE db_slab_id = ?", slabID)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		for rows.Next() {
			var s sector
			if err := rows.Scan(&s.ID, &s.SlabID, (*sql.PublicKey)(&s.Root), (*sql.Hash256)(&s.LatestHost)); err != nil {
				t.Fatal(err)
			}
			sectors = append(sectors, s)
		}
		return
	}

	// helper type to fetch a slab
	type slab struct {
		ID               int64
		ContractSetID    int64
		Health           float64
		HealthValidUntil int64
		MinShards        uint8
		TotalShards      uint8
		Key              object.EncryptionKey
	}
	fetchSlabStmt, err := ss.DB().Prepare(context.Background(), "SELECT id, db_contract_set_id, health, health_valid_until, min_shards, total_shards, `key` FROM slabs WHERE id = ?")
	if err != nil {
		t.Fatal(err)
	}
	defer fetchSlabStmt.Close()

	for i, slice := range slices {
		if slice.ID != int64(i+1) {
			t.Fatal("unexpected id", slice.ID)
		} else if slice.ObjectIndex != int64(i+1) {
			t.Fatal("unexpected object index", slice.ObjectIndex)
		} else if slice.Offset != 0 || slice.Length != int64(minShards)*rhpv2.SectorSize {
			t.Fatal("invalid offset/length", slice.Offset, slice.Length)
		}

		// fetch the slab
		var slab slab
		err = fetchSlabStmt.QueryRow(context.Background(), slice.SlabID).
			Scan(&slab.ID, &slab.ContractSetID, &slab.Health, &slab.HealthValidUntil, &slab.MinShards, &slab.TotalShards, (*sql.EncryptionKey)(&slab.Key))
		if err != nil {
			t.Fatal(err)
		} else if slab.ID != int64(i+1) {
			t.Fatal("unexpected id", slab.ID)
		} else if slab.ContractSetID != 1 {
			t.Fatal("invalid contract set id", slab.ContractSetID)
		} else if slab.Health != 1 {
			t.Fatal("invalid health", slab.Health)
		} else if slab.HealthValidUntil != 0 {
			t.Fatal("invalid health validity", slab.HealthValidUntil)
		} else if slab.MinShards != uint8(minShards) {
			t.Fatal("invalid minShards", slab.MinShards)
		} else if slab.TotalShards != uint8(totalShards) {
			t.Fatal("invalid totalShards", slab.TotalShards)
		} else if slab.Key.String() != obj.Slabs[i].Key.String() {
			t.Fatal("wrong key")
		}

		// fetch the sectors
		sectors := fetchSectorsBySlabID(int64(slab.ID))
		if len(sectors) != totalShards {
			t.Fatal("invalid number of sectors", len(sectors))
		}
		for j, sector := range sectors {
			if sector.ID != int64(i*totalShards+j+1) {
				t.Fatal("invalid id", sector.ID)
			} else if sector.SlabID != int64(slab.ID) {
				t.Fatal("invalid slab id", sector.SlabID)
			} else if sector.LatestHost != hks[i*totalShards+j] {
				t.Fatal("invalid host")
			} else if sector.Root != obj.Slabs[i].Shards[j].Root {
				t.Fatal("invalid root")
			}
		}
	}

	obj2 := object.Object{
		Key: object.GenerateEncryptionKey(),
	}
	// add 1 slab with 30 shards
	obj2.Slabs = append(obj2.Slabs, object.SlabSlice{
		Offset: 0,
		Length: uint32(minShards) * rhpv2.SectorSize,
		Slab: object.Slab{
			Key:       object.GenerateEncryptionKey(),
			MinShards: uint8(minShards),
		},
	})
	// 30 shards each
	for i := 0; i < totalShards; i++ {
		obj2.Slabs[0].Shards = append(obj2.Slabs[0].Shards, object.Sector{
			Contracts: map[types.PublicKey][]types.FileContractID{
				hks[len(obj.Slabs)*totalShards+i]: {
					fcids[len(obj.Slabs)*totalShards+i],
				},
			},
			LatestHost: hks[len(obj.Slabs)*totalShards+i],
			Root:       frand.Entropy256(),
		})
	}
	// add the second slab of the first object too
	obj2.Slabs = append(obj2.Slabs, obj.Slabs[1])

	// add the object
	_, err = ss.addTestObject("/2", obj2)
	if err != nil {
		t.Fatal(err)
	}

	// fetch the object
	id2, bid2, oid2, health2, size2 := fetchObj(ss.DefaultBucketID(), "/2")
	if id2 != 2 {
		t.Fatal("unexpected id", id)
	} else if bid2 != ss.DefaultBucketID() {
		t.Fatal("bucket id mismatch", bid)
	} else if oid2 != "/2" {
		t.Fatal("object id mismatch", oid)
	} else if health2 != 1 {
		t.Fatal("health mismatch", health)
	} else if size2 != obj.TotalSize() {
		t.Fatal("size mismatch", size)
	}

	// fetch its slices
	slices2 := fetchSlicesByObjectID(id2)
	if len(slices2) != 2 {
		t.Fatal("invalid number of slices", len(slices2))
	}

	// check the first one
	slice2 := slices2[0]
	if slice2.ID != int64(len(slices)+1) {
		t.Fatal("unexpected id", slice2.ID)
	} else if slice2.ObjectIndex != 1 {
		t.Fatal("unexpected object index", slice2.ObjectIndex)
	} else if slice2.Offset != 0 || slice2.Length != int64(minShards)*rhpv2.SectorSize {
		t.Fatal("invalid offset/length", slice2.Offset, slice2.Length)
	}

	// fetch the slab
	var slab2 slab
	err = fetchSlabStmt.QueryRow(context.Background(), slice2.SlabID).
		Scan(&slab2.ID, &slab2.ContractSetID, &slab2.Health, &slab2.HealthValidUntil, &slab2.MinShards, &slab2.TotalShards, (*sql.EncryptionKey)(&slab2.Key))
	if err != nil {
		t.Fatal(err)
	} else if slab2.ID != int64(len(slices)+1) {
		t.Fatal("unexpected id", slab2.ID)
	} else if slab2.ContractSetID != 1 {
		t.Fatal("invalid contract set id", slab2.ContractSetID)
	} else if slab2.Health != 1 {
		t.Fatal("invalid health", slab2.Health)
	} else if slab2.HealthValidUntil != 0 {
		t.Fatal("invalid health validity", slab2.HealthValidUntil)
	} else if slab2.MinShards != uint8(minShards) {
		t.Fatal("invalid minShards", slab2.MinShards)
	} else if slab2.TotalShards != uint8(totalShards) {
		t.Fatal("invalid totalShards", slab2.TotalShards)
	} else if slab2.Key.String() != obj2.Slabs[0].Key.String() {
		t.Fatal("wrong key")
	}

	// fetch the sectors
	sectors2 := fetchSectorsBySlabID(int64(slab2.ID))
	if len(sectors2) != totalShards {
		t.Fatal("invalid number of sectors", len(sectors2))
	}
	for j, sector := range sectors2 {
		if sector.ID != int64((len(obj.Slabs))*totalShards+j+1) {
			t.Fatal("invalid id", sector.ID)
		} else if sector.SlabID != int64(slab2.ID) {
			t.Fatal("invalid slab id", sector.SlabID)
		} else if sector.LatestHost != hks[(len(obj.Slabs))*totalShards+j] {
			t.Fatal("invalid host")
		} else if sector.Root != obj2.Slabs[0].Shards[j].Root {
			t.Fatal("invalid root")
		}
	}

	// the second slab of obj2 should be the same as the first in obj
	if slices2[1].SlabID != 2 {
		t.Fatal("wrong slab")
	}

	type contractSector struct {
		ContractID int64
		SectorID   int64
	}
	var contractSectors []contractSector
	rows, err := ss.DB().Query(context.Background(), "SELECT db_contract_id, db_sector_id FROM contract_sectors")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var cs contractSector
		if err := rows.Scan(&cs.ContractID, &cs.SectorID); err != nil {
			t.Fatal(err)
		}
		contractSectors = append(contractSectors, cs)
	}
	if len(contractSectors) != 3*totalShards {
		t.Fatal("invalid number of contract sectors", len(contractSectors))
	}
	for i, cs := range contractSectors {
		if cs.ContractID != int64(i+1) {
			t.Fatal("invalid contract id")
		} else if cs.SectorID != int64(i+1) {
			t.Fatal("invalid sector id")
		}
	}
}

// TestUpdateObjectParallel calls UpdateObject from multiple threads in parallel
// while retries are disabled to make sure calling the same method from multiple
// threads won't cause deadlocks.
//
// NOTE: This test only covers the optimistic case of inserting objects without
// overwriting them. As soon as combining deletions and insertions within the
// same transaction, deadlocks become more likely due to the gap locks MySQL
// uses.
func TestUpdateObjectParallel(t *testing.T) {
	if config.MySQLConfigFromEnv().URI == "" {
		// it's pretty much impossile to optimise for both sqlite and mysql at
		// the same time so we skip this test for SQLite for now
		// TODO: once we moved away from gorm and implement separate interfaces
		// for SQLite and MySQL, we have more control over the used queries and
		// can revisit this
		t.SkipNow()
	}
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	ss.retryTransactionIntervals = []time.Duration{0} // don't retry
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

	c := make(chan string)
	ctx, cancel := context.WithCancel(context.Background())
	work := func() {
		t.Helper()
		defer cancel()
		for name := range c {
			// create an object
			obj := object.Object{
				Key: object.GenerateEncryptionKey(),
				Slabs: []object.SlabSlice{
					{
						Slab: object.Slab{
							Health:    1.0,
							Key:       object.GenerateEncryptionKey(),
							MinShards: 1,
							Shards:    newTestShards(hk1, fcid1, frand.Entropy256()),
						},
						Offset: 10,
						Length: 100,
					},
					{
						Slab: object.Slab{
							Health:    1.0,
							Key:       object.GenerateEncryptionKey(),
							MinShards: 2,
							Shards:    newTestShards(hk2, fcid2, frand.Entropy256()),
						},
						Offset: 20,
						Length: 200,
					},
				},
			}

			// update the object
			if err := ss.UpdateObject(context.Background(), api.DefaultBucketName, name, testContractSet, testETag, testMimeType, testMetadata, obj); err != nil {
				t.Error(err)
				return
			}
		}
	}

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			work()
			wg.Done()
		}()
	}

	// create 1000 objects and then overwrite them
	for i := 0; i < 1000; i++ {
		select {
		case c <- fmt.Sprintf("/object-%d", i):
		case <-ctx.Done():
			return
		}
	}

	close(c)
	wg.Wait()
}

func TestDirectories(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	paths := []string{
		"/foo",
		"/bar/baz",
		"///somefile",
		"/dir/fakedir/",
		"/",
		"/bar/fileinsamedirasbefore",
	}

	for _, p := range paths {
		var dirID int64
		err := ss.db.Transaction(context.Background(), func(tx sql.DatabaseTx) error {
			var err error
			dirID, err = tx.InsertDirectories(context.Background(), api.DefaultBucketName, p)
			return err
		})
		if err != nil {
			t.Fatal(err)
		} else if dirID == 0 {
			t.Fatalf("unexpected dir id %v", dirID)
		}
	}

	expectedDirs := []struct {
		name     string
		id       int64
		parentID int64
	}{
		{
			name:     "/",
			id:       1,
			parentID: 0,
		},
		{
			name:     "/bar/",
			id:       2,
			parentID: 1,
		},
		{
			name:     "//",
			id:       3,
			parentID: 1,
		},
		{
			name:     "///",
			id:       4,
			parentID: 3,
		},
		{
			name:     "/dir/",
			id:       5,
			parentID: 1,
		},
	}

	type row struct {
		ID       int64
		ParentID int64
		Name     string
	}
	rows, err := ss.DB().Query(context.Background(), "SELECT id, COALESCE(db_parent_id, 0), name FROM directories ORDER BY id ASC")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var nDirs int
	for i := 0; rows.Next(); i++ {
		var dir row
		if err := rows.Scan(&dir.ID, &dir.ParentID, &dir.Name); err != nil {
			t.Fatal(err)
		} else if dir.ID != expectedDirs[i].id {
			t.Fatalf("unexpected id %v", dir.ID)
		} else if dir.ParentID != expectedDirs[i].parentID {
			t.Fatalf("unexpected parent id %v", dir.ParentID)
		} else if dir.Name != expectedDirs[i].name {
			t.Fatalf("unexpected name '%v' != '%v'", dir.Name, expectedDirs[i].name)
		}
		nDirs++
	}
	if len(expectedDirs) != nDirs {
		t.Fatalf("expected %v dirs, got %v", len(expectedDirs), nDirs)
	}

	now := time.Now()
	ss.Retry(100, 100*time.Millisecond, func() error {
		ss.triggerSlabPruning()
		return ss.waitForPruneLoop(now)
	})

	if n := ss.Count("directories"); n != 1 {
		t.Fatal("expected 1 dir, got", n)
	}
}
