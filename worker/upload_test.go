package worker

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/object"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

const (
	testBucket      = "testbucket"
	testContractSet = "testcontractset"
)

var (
	testRedundancySettings = api.RedundancySettings{MinShards: 2, TotalShards: 6}
)

func TestUpload(t *testing.T) {
	// create upload params
	params := testParameters(t.Name())

	// create test hosts and contracts
	hosts := newMockHosts(params.rs.TotalShards * 2)
	contracts := newMockContracts(hosts)

	// mock dependencies
	cs := newMockContractStore(contracts)
	hm := newMockHostManager(hosts)
	os := newMockObjectStore()
	mm := &mockMemoryManager{}

	// create managers
	dl := newDownloadManager(context.Background(), hm, mm, os, 0, 0, zap.NewNop().Sugar())
	ul := newUploadManager(context.Background(), hm, mm, os, cs, 0, 0, time.Minute, zap.NewNop().Sugar())

	// create test data
	data := make([]byte, 128)
	if _, err := frand.Read(data); err != nil {
		t.Fatal(err)
	}

	// create upload contracts
	metadatas := make([]api.ContractMetadata, len(contracts))
	for i, h := range hosts {
		metadatas[i] = api.ContractMetadata{
			ID:      h.c.rev.ParentID,
			HostKey: h.hk,
		}
	}

	// upload data
	_, _, err := ul.Upload(context.Background(), bytes.NewReader(data), metadatas, params, lockingPriorityUpload)
	if err != nil {
		t.Fatal(err)
	}

	// grab the object
	o, err := os.Object(context.Background(), testBucket, t.Name(), api.GetObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// build used hosts
	used := make(map[types.PublicKey]struct{})
	for _, shard := range o.Object.Object.Slabs[0].Shards {
		used[shard.LatestHost] = struct{}{}
	}

	// download the data and assert it matches
	var buf bytes.Buffer
	err = dl.DownloadObject(context.Background(), &buf, o.Object.Object, 0, uint64(o.Object.Size), metadatas)
	if err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(data, buf.Bytes()) {
		t.Fatal("data mismatch")
	}

	// filter contracts to have (at most) min shards used contracts
	var n int
	var filtered []api.ContractMetadata
	for _, md := range metadatas {
		// add unused contracts
		if _, used := used[md.HostKey]; !used {
			filtered = append(filtered, md)
			continue
		}

		// add min shards used contracts
		if n < int(params.rs.MinShards) {
			filtered = append(filtered, md)
			n++
		}
	}

	// download the data again and assert it matches
	buf.Reset()
	err = dl.DownloadObject(context.Background(), &buf, o.Object.Object, 0, uint64(o.Object.Size), filtered)
	if err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(data, buf.Bytes()) {
		t.Fatal("data mismatch")
	}

	// filter out one contract - expect download to fail
	for i, md := range filtered {
		if _, used := used[md.HostKey]; used {
			filtered = append(filtered[:i], filtered[i+1:]...)
			break
		}
	}

	// download the data again and assert it fails
	buf.Reset()
	err = dl.DownloadObject(context.Background(), &buf, o.Object.Object, 0, uint64(o.Object.Size), filtered)
	if !errors.Is(err, errDownloadNotEnoughHosts) {
		t.Fatal("expected not enough hosts error", err)
	}

	// try and upload into a bucket that does not exist
	params.bucket = "doesnotexist"
	_, _, err = ul.Upload(context.Background(), bytes.NewReader(data), metadatas, params, lockingPriorityUpload)
	if !errors.Is(err, errBucketNotFound) {
		t.Fatal("expected bucket not found error", err)
	}

	// upload data using a cancelled context - assert we don't hang
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err = ul.Upload(ctx, bytes.NewReader(data), metadatas, params, lockingPriorityUpload)
	if err == nil || !errors.Is(err, errUploadInterrupted) {
		t.Fatal(err)
	}
}

func TestUploadPackedSlab(t *testing.T) {
	// mock worker
	w := newMockWorker(testRedundancySettings.TotalShards * 2)

	// convenience variables
	os := w.os
	mm := w.mm
	dl := w.dl
	ul := w.ul

	// create test data
	data := make([]byte, 128)
	if _, err := frand.Read(data); err != nil {
		t.Fatal(err)
	}

	// create upload params
	params := testParameters(t.Name())
	params.packing = true

	// upload data
	_, _, err := ul.Upload(context.Background(), bytes.NewReader(data), w.contracts.values(), params, lockingPriorityUpload)
	if err != nil {
		t.Fatal(err)
	}

	// assert our object store contains a partial slab
	if len(os.partials) != 1 {
		t.Fatal("expected 1 partial slab")
	}

	// grab the object
	o, err := os.Object(context.Background(), testBucket, t.Name(), api.GetObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// download the data and assert it matches
	var buf bytes.Buffer
	err = dl.DownloadObject(context.Background(), &buf, o.Object.Object, 0, uint64(o.Object.Size), w.contracts.values())
	if err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(data, buf.Bytes()) {
		t.Fatal("data mismatch")
	}

	// fetch packed slabs for upload
	pss, err := os.PackedSlabsForUpload(context.Background(), time.Minute, uint8(params.rs.MinShards), uint8(params.rs.TotalShards), testContractSet, 1)
	if err != nil {
		t.Fatal(err)
	} else if len(pss) != 1 {
		t.Fatal("expected 1 packed slab")
	}
	ps := pss[0]
	mem := mm.AcquireMemory(context.Background(), uint64(params.rs.TotalShards*rhpv2.SectorSize))

	// upload the packed slab
	err = ul.UploadPackedSlab(context.Background(), params.rs, ps, mem, w.contracts.values(), 0, lockingPriorityUpload)
	if err != nil {
		t.Fatal(err)
	}

	// assert our object store contains zero partial slabs
	if len(os.partials) != 0 {
		t.Fatal("expected no partial slabs")
	}

	// re-grab the object
	o, err = os.Object(context.Background(), testBucket, t.Name(), api.GetObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// download the data again and assert it matches
	buf.Reset()
	err = dl.DownloadObject(context.Background(), &buf, o.Object.Object, 0, uint64(o.Object.Size), w.contracts.values())
	if err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(data, buf.Bytes()) {
		t.Fatal("data mismatch")
	}
}

func TestMigrateShards(t *testing.T) {
	// mock worker
	w := newMockWorker(testRedundancySettings.TotalShards * 2)

	// convenience variables
	os := w.os
	mm := w.mm
	dl := w.dl
	ul := w.ul

	// create test data
	data := make([]byte, 128)
	if _, err := frand.Read(data); err != nil {
		t.Fatal(err)
	}

	// create upload params
	params := testParameters(t.Name())

	// upload data
	_, _, err := ul.Upload(context.Background(), bytes.NewReader(data), w.contracts.values(), params, lockingPriorityUpload)
	if err != nil {
		t.Fatal(err)
	}

	// grab the slab
	o, err := os.Object(context.Background(), testBucket, t.Name(), api.GetObjectOptions{})
	if err != nil {
		t.Fatal(err)
	} else if len(o.Object.Object.Slabs) != 1 {
		t.Fatal("expected 1 slab")
	}
	slab := o.Object.Object.Slabs[0]

	// build usedHosts hosts
	usedHosts := make(map[types.PublicKey]struct{})
	for _, shard := range slab.Shards {
		usedHosts[shard.LatestHost] = struct{}{}
	}

	// mark odd shards as bad
	var badIndices []int
	badHosts := make(map[types.PublicKey]struct{})
	for i, shard := range slab.Shards {
		if i%2 != 0 {
			badIndices = append(badIndices, i)
			badHosts[shard.LatestHost] = struct{}{}
		}
	}

	// download the slab
	shards, _, err := dl.DownloadSlab(context.Background(), slab.Slab, w.contracts.values())
	if err != nil {
		t.Fatal(err)
	}

	// encrypt the shards
	o.Object.Object.Slabs[0].Slab.Encrypt(shards)

	// filter it down to the shards we need to migrate
	for i, si := range badIndices {
		shards[i] = shards[si]
	}
	shards = shards[:len(badIndices)]

	// recreate upload contracts
	contracts := make([]api.ContractMetadata, 0)
	for hk := range w.hm.hosts {
		_, used := usedHosts[hk]
		_, bad := badHosts[hk]
		if !used && !bad {
			contracts = append(contracts, w.contracts[hk])
		}
	}

	// migrate those shards away from bad hosts
	mem := mm.AcquireMemory(context.Background(), uint64(len(badIndices))*rhpv2.SectorSize)
	err = ul.MigrateShards(context.Background(), &o.Object.Object.Slabs[0].Slab, badIndices, shards, testContractSet, contracts, 0, lockingPriorityUpload, mem)
	if err != nil {
		t.Fatal(err)
	}

	// re-grab the slab
	o, err = os.Object(context.Background(), testBucket, t.Name(), api.GetObjectOptions{})
	if err != nil {
		t.Fatal(err)
	} else if len(o.Object.Object.Slabs) != 1 {
		t.Fatal("expected 1 slab")
	}
	slab = o.Object.Object.Slabs[0]

	// assert none of the shards are on bad hosts
	for _, shard := range slab.Shards {
		if _, bad := badHosts[shard.LatestHost]; bad {
			t.Fatal("shard is on bad host", shard.LatestHost)
		}
	}

	// create download contracts
	contracts = contracts[:0]
	for hk := range w.hm.hosts {
		if _, bad := badHosts[hk]; !bad {
			contracts = append(contracts, w.contracts[hk])
		}
	}

	// download the data and assert it matches
	var buf bytes.Buffer
	err = dl.DownloadObject(context.Background(), &buf, o.Object.Object, 0, uint64(o.Object.Size), contracts)
	if err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(data, buf.Bytes()) {
		t.Fatal("data mismatch")
	}
}

func testParameters(path string) uploadParameters {
	return uploadParameters{
		bucket: testBucket,
		path:   path,

		ec:               object.GenerateEncryptionKey(), // random key
		encryptionOffset: 0,                              // from the beginning

		contractSet: testContractSet,
		rs:          testRedundancySettings,
	}
}
