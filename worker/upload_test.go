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
	w.addHosts(testRedundancySettings.TotalShards * 2)

	// convenience variables
	os := w.os
	dl := w.downloadManager
	ul := w.uploadManager

	// create test data
	data := make([]byte, 128)
	if _, err := frand.Read(data); err != nil {
		t.Fatal(err)
	}

	// create upload params
	params := testParameters(t.Name())

	// upload data
	_, _, err := ul.Upload(context.Background(), bytes.NewReader(data), w.contracts(), params, lockingPriorityUpload)
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
	err = dl.DownloadObject(context.Background(), &buf, *o.Object.Object, 0, uint64(o.Object.Size), w.contracts())
	if err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(data, buf.Bytes()) {
		t.Fatal("data mismatch")
	}

	// filter contracts to have (at most) min shards used contracts
	var n int
	var filtered []api.ContractMetadata
	for _, md := range w.contracts() {
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
	_, _, err = ul.Upload(context.Background(), bytes.NewReader(data), w.contracts(), params, lockingPriorityUpload)
	if !errors.Is(err, api.ErrBucketNotFound) {
		t.Fatal("expected bucket not found error", err)
	}

	// upload data using a cancelled context - assert we don't hang
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err = ul.Upload(ctx, bytes.NewReader(data), w.contracts(), params, lockingPriorityUpload)
	if err == nil || !errors.Is(err, errUploadInterrupted) {
		t.Fatal(err)
	}
}

func TestUploadPackedSlab(t *testing.T) {
	// create test worker
	w := newTestWorker(t)

	// add hosts to worker
	w.addHosts(testRedundancySettings.TotalShards * 2)

	// convenience variables
	os := w.os
	mm := w.ulmm
	dl := w.downloadManager
	ul := w.uploadManager

	// create test data
	data := make([]byte, 128)
	if _, err := frand.Read(data); err != nil {
		t.Fatal(err)
	}

	// create upload params
	params := testParameters(t.Name())
	params.packing = true

	// upload data
	_, _, err := ul.Upload(context.Background(), bytes.NewReader(data), w.contracts(), params, lockingPriorityUpload)
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
	err = dl.DownloadObject(context.Background(), &buf, *o.Object.Object, 0, uint64(o.Object.Size), w.contracts())
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
	err = ul.UploadPackedSlab(context.Background(), params.rs, ps, mem, w.contracts(), 0, lockingPriorityUpload)
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
	err = dl.DownloadObject(context.Background(), &buf, *o.Object.Object, 0, uint64(o.Object.Size), w.contracts())
	if err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(data, buf.Bytes()) {
		t.Fatal("data mismatch")
	}
}

func TestUploadShards(t *testing.T) {
	// create test worker
	w := newTestWorker(t)

	// add hosts to worker
	w.addHosts(testRedundancySettings.TotalShards * 2)

	// convenience variables
	os := w.os
	mm := w.ulmm
	dl := w.downloadManager
	ul := w.uploadManager

	// create test data
	data := make([]byte, 128)
	if _, err := frand.Read(data); err != nil {
		t.Fatal(err)
	}

	// create upload params
	params := testParameters(t.Name())

	// upload data
	_, _, err := ul.Upload(context.Background(), bytes.NewReader(data), w.contracts(), params, lockingPriorityUpload)
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
	shards, _, err := dl.DownloadSlab(context.Background(), slab.Slab, w.contracts())
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
	for _, c := range w.contracts() {
		_, used := usedHosts[c.HostKey]
		_, bad := badHosts[c.HostKey]
		if !used && !bad {
			contracts = append(contracts, c)
		}
	}

	// migrate those shards away from bad hosts
	mem := mm.AcquireMemory(context.Background(), uint64(len(badIndices))*rhpv2.SectorSize)
	err = ul.UploadShards(context.Background(), &o.Object.Object.Slabs[0].Slab, badIndices, shards, testContractSet, contracts, 0, lockingPriorityUpload, mem)
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
	for _, c := range w.contracts() {
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
	w.addHosts(testRedundancySettings.TotalShards)

	// convenience variables
	ul := w.uploadManager
	cs := w.cs
	hm := w.hm

	// create test data
	data := make([]byte, 128)
	if _, err := frand.Read(data); err != nil {
		t.Fatal(err)
	}

	// create upload params
	params := testParameters(t.Name())

	// upload data
	contracts := w.contracts()
	_, err := w.upload(context.Background(), bytes.NewReader(data), contracts, params)
	if err != nil {
		t.Fatal(err)
	}

	// assert we have the expected number of uploaders
	if len(ul.uploaders) != len(contracts) {
		t.Fatalf("unexpected number of uploaders, %v != %v", len(ul.uploaders), len(contracts))
	}

	// renew the first contract
	c1 := contracts[0]
	c1Renewed := w.renewContract(c1.HostKey)

	// remove the host from the second contract
	c2 := contracts[1]
	delete(hm.hosts, c2.HostKey)
	delete(cs.contracts, c2.ID)

	// add a new host/contract
	hNew := w.addHost()

	// upload data
	contracts = w.contracts()
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
	w.addHosts(testRedundancySettings.TotalShards)

	// convenience variables
	os := w.os
	dl := w.downloadManager

	// create test data
	data := make([]byte, 128)
	if _, err := frand.Read(data); err != nil {
		t.Fatal(err)
	}

	// create upload params
	params := testParameters(t.Name())

	// make sure the memory manager blocks
	unblock := w.blockUploads()

	// upload data
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := w.upload(ctx, bytes.NewReader(data), w.contracts(), params)
	if !errors.Is(err, errUploadInterrupted) {
		t.Fatal(err)
	}

	// unblock the memory manager
	unblock()

	// upload data
	_, err = w.upload(context.Background(), bytes.NewReader(data), w.contracts(), params)
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
	err = dl.DownloadObject(context.Background(), &buf, *o.Object.Object, 0, uint64(o.Object.Size), w.contracts())
	if err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(data, buf.Bytes()) {
		t.Fatal("data mismatch", data, buf.Bytes())
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
