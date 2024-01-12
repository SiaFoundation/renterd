package worker

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/object"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

var (
	testBucket = "testbucket"
)

func TestUploadDownload(t *testing.T) {
	// create upload params
	params := testParameters(testBucket, t.Name())

	// create test hosts and contracts
	hosts := newMockHosts(params.rs.TotalShards * 2)
	contracts := newMockContracts(hosts)

	// mock dependencies
	cl := newMockContractLocker(contracts)
	hm := newMockHostManager(hosts)
	os := newMockObjectStore()
	mm := &mockMemoryManager{}

	// create managers
	dl := newDownloadManager(context.Background(), hm, mm, os, 0, 0, zap.NewNop().Sugar())
	ul := newUploadManager(context.Background(), hm, mm, os, cl, 0, 0, time.Minute, zap.NewNop().Sugar())

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

func testParameters(bucket, path string) uploadParameters {
	return uploadParameters{
		bucket: bucket,
		path:   path,

		ec:               object.GenerateEncryptionKey(), // random key
		encryptionOffset: 0,                              // from the beginning

		rs: api.RedundancySettings{MinShards: 2, TotalShards: 6},
	}
}
