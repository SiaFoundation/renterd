package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/test"
	"go.sia.tech/renterd/object"
	"lukechampine.com/frand"
)

var (
	testBucket             = "testbucket"
	testContractSet        = "testcontractset"
	testRedundancySettings = api.RedundancySettings{MinShards: 2, TotalShards: 6}
)

func TestUpload(t *testing.T) {
	// create test worker
	w := newTestWorker(t)

	// add hosts to worker
	w.AddHosts(testRedundancySettings.TotalShards * 2)

	// convenience variables
	os := w.os
	dl := w.downloadManager
	ul := w.uploadManager

	// create test data
	data := frand.Bytes(128)

	// create upload params
	params := testParameters(t.Name())

	// upload data
	_, _, err := ul.Upload(context.Background(), bytes.NewReader(data), w.Contracts(), params, lockingPriorityUpload)
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
	err = dl.DownloadObject(context.Background(), &buf, *o.Object.Object, 0, uint64(o.Object.Size), w.Contracts())
	if err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(data, buf.Bytes()) {
		t.Fatal("data mismatch")
	}

	// filter contracts to have (at most) min shards used contracts
	var n int
	var filtered []api.ContractMetadata
	for _, md := range w.Contracts() {
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
	err = dl.DownloadObject(context.Background(), &buf, *o.Object.Object, 0, uint64(o.Object.Size), filtered)
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
	err = dl.DownloadObject(context.Background(), &buf, *o.Object.Object, 0, uint64(o.Object.Size), filtered)
	if !errors.Is(err, errDownloadNotEnoughHosts) {
		t.Fatal("expected not enough hosts error", err)
	}

	// try and upload into a bucket that does not exist
	params.bucket = "doesnotexist"
	_, _, err = ul.Upload(context.Background(), bytes.NewReader(data), w.Contracts(), params, lockingPriorityUpload)
	if !errors.Is(err, api.ErrBucketNotFound) {
		t.Fatal("expected bucket not found error", err)
	}

	// upload data using a cancelled context - assert we don't hang
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err = ul.Upload(ctx, bytes.NewReader(data), w.Contracts(), params, lockingPriorityUpload)
	if err == nil || !errors.Is(err, errUploadInterrupted) {
		t.Fatal(err)
	}
}

func TestUploadPackedSlab(t *testing.T) {
	// create test worker
	w := newTestWorker(t)

	// add hosts to worker
	w.AddHosts(testRedundancySettings.TotalShards)

	// convenience variables
	os := w.os
	mm := w.ulmm
	dl := w.downloadManager
	ul := w.uploadManager

	// create upload params
	params := testParameters(t.Name())
	params.packing = true
	opts := testOpts()
	opts = append(opts, WithPacking(true))

	// create test data
	data := frand.Bytes(128)

	// upload data
	_, _, err := ul.Upload(context.Background(), bytes.NewReader(data), w.Contracts(), params, lockingPriorityUpload)
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
	err = dl.DownloadObject(context.Background(), &buf, *o.Object.Object, 0, uint64(o.Object.Size), w.Contracts())
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

	// upload the packed slab
	mem := mm.AcquireMemory(context.Background(), params.rs.SlabSize())
	err = ul.UploadPackedSlab(context.Background(), params.rs, ps, mem, w.Contracts(), 0, lockingPriorityUpload)
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
	err = dl.DownloadObject(context.Background(), &buf, *o.Object.Object, 0, uint64(o.Object.Size), w.Contracts())
	if err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(data, buf.Bytes()) {
		t.Fatal("data mismatch")
	}

	// define a helper that counts packed slabs
	packedSlabsCount := func() int {
		t.Helper()
		os.mu.Lock()
		cnt := len(os.partials)
		os.mu.Unlock()
		return cnt
	}

	// define a helper that uploads data using the worker
	var c int
	uploadBytes := func(n int) {
		t.Helper()
		params.path = fmt.Sprintf("%s_%d", t.Name(), c)
		_, err := w.upload(context.Background(), params.bucket, params.path, bytes.NewReader(frand.Bytes(n)), w.Contracts(), opts...)
		if err != nil {
			t.Fatal(err)
		}
		c++
	}

	// block aysnc packed slab uploads
	w.BlockAsyncPackedSlabUploads(params)

	// configure max buffer size
	os.setSlabBufferMaxSizeSoft(128)

	// upload 2x64 bytes using the worker and assert we still have two packed
	// slabs (buffer limit not reached)
	uploadBytes(64)
	uploadBytes(64)
	if packedSlabsCount() != 2 {
		t.Fatal("expected 2 packed slabs")
	}

	// upload one more byte and assert we still have two packed slabs (one got
	// uploaded synchronously because buffer limit was reached)
	uploadBytes(1)
	if packedSlabsCount() != 2 {
		t.Fatal("expected 2 packed slabs")
	}

	// unblock asynchronous uploads
	w.UnblockAsyncPackedSlabUploads(params)
	uploadBytes(129) // ensure background thread is running

	// assert packed slabs get uploaded asynchronously
	if err := test.Retry(100, 100*time.Millisecond, func() error {
		if packedSlabsCount() != 0 {
			return errors.New("expected 0 packed slabs")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestMigrateLostSector(t *testing.T) {
	// create test worker
	w := newTestWorker(t)

	// add hosts to worker
	w.AddHosts(testRedundancySettings.TotalShards * 2)

	// convenience variables
	os := w.os
	mm := w.ulmm
	dl := w.downloadManager
	ul := w.uploadManager

	// create test data
	data := frand.Bytes(128)

	// create upload params
	params := testParameters(t.Name())

	// upload data
	_, _, err := ul.Upload(context.Background(), bytes.NewReader(data), w.Contracts(), params, lockingPriorityUpload)
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

	// assume the host of the first shard lost its sector
	badHost := slab.Shards[0].LatestHost
	badContract := slab.Shards[0].Contracts[badHost][0]
	err = os.DeleteHostSector(context.Background(), badHost, slab.Shards[0].Root)
	if err != nil {
		t.Fatal(err)
	}

	// download the slab
	shards, _, err := dl.DownloadSlab(context.Background(), slab.Slab, w.Contracts())
	if err != nil {
		t.Fatal(err)
	}

	// encrypt the shards
	o.Object.Object.Slabs[0].Slab.Encrypt(shards)

	// filter it down to the shards we need to migrate
	shards = shards[:1]

	// recreate upload contracts
	contracts := make([]api.ContractMetadata, 0)
	for _, c := range w.Contracts() {
		_, used := usedHosts[c.HostKey]
		if !used && c.HostKey != badHost {
			contracts = append(contracts, c)
		}
	}

	// migrate the shard away from the bad host
	mem := mm.AcquireMemory(context.Background(), rhpv2.SectorSize)
	err = ul.UploadShards(context.Background(), o.Object.Object.Slabs[0].Slab, []int{0}, shards, testContractSet, contracts, 0, lockingPriorityUpload, mem)
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

	// assert the bad shard is on a good host now
	shard := slab.Shards[0]
	if shard.LatestHost == badHost {
		t.Fatal("latest host is bad")
	} else if len(shard.Contracts) != 1 {
		t.Fatal("expected 1 contract")
	}
	for _, fcids := range shard.Contracts {
		for _, fcid := range fcids {
			if fcid == badContract {
				t.Fatal("contract belongs to bad host")
			}
		}
	}
}

func TestUploadShards(t *testing.T) {
	// create test worker
	w := newTestWorker(t)

	// add hosts to worker
	w.AddHosts(testRedundancySettings.TotalShards * 2)

	// convenience variables
	os := w.os
	mm := w.ulmm
	dl := w.downloadManager
	ul := w.uploadManager

	// create test data
	data := frand.Bytes(128)

	// create upload params
	params := testParameters(t.Name())

	// upload data
	_, _, err := ul.Upload(context.Background(), bytes.NewReader(data), w.Contracts(), params, lockingPriorityUpload)
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
	shards, _, err := dl.DownloadSlab(context.Background(), slab.Slab, w.Contracts())
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
	for _, c := range w.Contracts() {
		_, used := usedHosts[c.HostKey]
		_, bad := badHosts[c.HostKey]
		if !used && !bad {
			contracts = append(contracts, c)
		}
	}

	// migrate those shards away from bad hosts
	mem := mm.AcquireMemory(context.Background(), uint64(len(badIndices))*rhpv2.SectorSize)
	err = ul.UploadShards(context.Background(), o.Object.Object.Slabs[0].Slab, badIndices, shards, testContractSet, contracts, 0, lockingPriorityUpload, mem)
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
	for i, shard := range slab.Shards {
		if i%2 == 0 && len(shard.Contracts) != 1 {
			t.Fatalf("expected 1 contract, got %v", len(shard.Contracts))
		} else if i%2 != 0 && len(shard.Contracts) != 2 {
			t.Fatalf("expected 2 contracts, got %v", len(shard.Contracts))
		}
		if _, bad := badHosts[shard.LatestHost]; bad {
			t.Fatal("shard is on bad host", shard.LatestHost)
		}
	}

	// create download contracts
	contracts = contracts[:0]
	for _, c := range w.Contracts() {
		if _, bad := badHosts[c.HostKey]; !bad {
			contracts = append(contracts, c)
		}
	}

	// download the data and assert it matches
	var buf bytes.Buffer
	err = dl.DownloadObject(context.Background(), &buf, *o.Object.Object, 0, uint64(o.Object.Size), contracts)
	if err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(data, buf.Bytes()) {
		t.Fatal("data mismatch")
	}
}

func TestRefreshUploaders(t *testing.T) {
	// create test worker
	w := newTestWorker(t)

	// add hosts to worker
	w.AddHosts(testRedundancySettings.TotalShards)

	// convenience variables
	ul := w.uploadManager
	cs := w.cs
	hm := w.hm

	// create test data
	data := frand.Bytes(128)

	// create upload params
	params := testParameters(t.Name())
	opts := testOpts()

	// upload data
	contracts := w.Contracts()
	_, err := w.upload(context.Background(), params.bucket, t.Name(), bytes.NewReader(data), contracts, opts...)
	if err != nil {
		t.Fatal(err)
	}

	// assert we have the expected number of uploaders
	if len(ul.uploaders) != len(contracts) {
		t.Fatalf("unexpected number of uploaders, %v != %v", len(ul.uploaders), len(contracts))
	}

	// renew the first contract
	c1 := contracts[0]
	c1Renewed := w.RenewContract(c1.HostKey)

	// remove the host from the second contract
	c2 := contracts[1]
	delete(hm.hosts, c2.HostKey)
	delete(cs.contracts, c2.ID)

	// add a new host/contract
	hNew := w.AddHost()

	// upload data
	contracts = w.Contracts()
	_, _, err = ul.Upload(context.Background(), bytes.NewReader(data), contracts, params, lockingPriorityUpload)
	if err != nil {
		t.Fatal(err)
	}

	// assert we added and renewed exactly one uploader
	var added, renewed int
	for _, ul := range ul.uploaders {
		switch ul.ContractID() {
		case hNew.metadata.ID:
			added++
		case c1Renewed.metadata.ID:
			renewed++
		default:
		}
	}
	if added != 1 {
		t.Fatalf("expected 1 added uploader, got %v", added)
	} else if renewed != 1 {
		t.Fatalf("expected 1 renewed uploader, got %v", renewed)
	}

	// assert we have one more uploader than we used to
	if len(ul.uploaders) != len(contracts)+1 {
		t.Fatalf("unexpected number of uploaders, %v != %v", len(ul.uploaders), len(contracts)+1)
	}

	// manually add a request to the queue of one of the uploaders we're about to expire
	responseChan := make(chan sectorUploadResp, 1)
	for _, ul := range ul.uploaders {
		if ul.fcid == hNew.metadata.ID {
			ul.mu.Lock()
			ul.queue = append(ul.queue, &sectorUploadReq{responseChan: responseChan, sector: &sectorUpload{ctx: context.Background()}})
			ul.mu.Unlock()
			break
		}
	}

	// upload data again but now with a blockheight that should expire most uploaders
	params.bh = c1.WindowEnd
	ul.Upload(context.Background(), bytes.NewReader(data), contracts, params, lockingPriorityUpload)

	// assert we only have one uploader left
	if len(ul.uploaders) != 1 {
		t.Fatalf("unexpected number of uploaders, %v != %v", len(ul.uploaders), 1)
	}

	// assert all queued requests failed with an error indicating the underlying
	// contract expired
	res := <-responseChan
	if !errors.Is(res.err, errContractExpired) {
		t.Fatal("expected contract expired error", res.err)
	}
}

func TestUploadRegression(t *testing.T) {
	// create test worker
	w := newTestWorker(t)

	// add hosts to worker
	w.AddHosts(testRedundancySettings.TotalShards)

	// convenience variables
	os := w.os
	dl := w.downloadManager

	// create test data
	data := frand.Bytes(128)

	// create upload params
	params := testParameters(t.Name())

	// make sure the memory manager blocks
	unblock := w.BlockUploads()

	// upload data
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := w.upload(ctx, params.bucket, params.path, bytes.NewReader(data), w.Contracts(), testOpts()...)
	if !errors.Is(err, errUploadInterrupted) {
		t.Fatal(err)
	}

	// unblock the memory manager
	unblock()

	// upload data
	_, err = w.upload(context.Background(), params.bucket, params.path, bytes.NewReader(data), w.Contracts(), testOpts()...)
	if err != nil {
		t.Fatal(err)
	}

	// grab the object
	o, err := os.Object(context.Background(), testBucket, t.Name(), api.GetObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// download data for good measure
	var buf bytes.Buffer
	err = dl.DownloadObject(context.Background(), &buf, *o.Object.Object, 0, uint64(o.Object.Size), w.Contracts())
	if err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(data, buf.Bytes()) {
		t.Fatal("data mismatch", data, buf.Bytes())
	}
}

func TestUploadSingleSectorSlowHosts(t *testing.T) {
	// create test worker
	w := newTestWorker(t)

	// add hosts to worker
	minShards := 10
	totalShards := 30
	slowHosts := 5
	w.uploadManager.maxOverdrive = uint64(slowHosts)
	w.uploadManager.overdriveTimeout = time.Second
	hosts := w.AddHosts(totalShards + slowHosts)

	for i := 0; i < slowHosts; i++ {
		hosts[i].uploadDelay = time.Hour
	}

	// create test data
	data := frand.Bytes(rhpv2.SectorSize * minShards)

	// create upload params
	params := testParameters(t.Name())
	params.rs.MinShards = minShards
	params.rs.TotalShards = totalShards

	// upload data
	_, _, err := w.uploadManager.Upload(context.Background(), bytes.NewReader(data), w.Contracts(), params, lockingPriorityUpload)
	if err != nil {
		t.Fatal(err)
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

func testOpts() []UploadOption {
	return []UploadOption{
		WithContractSet(testContractSet),
		WithRedundancySettings(testRedundancySettings),
	}
}
