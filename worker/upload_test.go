package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	rhpv4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/v2/api"
	"go.sia.tech/renterd/v2/internal/download"
	"go.sia.tech/renterd/v2/internal/test"
	"go.sia.tech/renterd/v2/internal/upload"
	"go.sia.tech/renterd/v2/object"
	"lukechampine.com/frand"
)

var (
	testBucket             = "testbucket"
	testRedundancySettings = api.RedundancySettings{MinShards: 2, TotalShards: 6}
)

func TestUpload(t *testing.T) {
	// create test worker
	w := newTestWorker(t, newTestWorkerCfg())

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
	_, _, err := ul.Upload(context.Background(), bytes.NewReader(data), w.UploadHosts(), params)
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
	for _, shard := range o.Object.Slabs[0].Shards {
		for hk := range shard.Contracts {
			used[hk] = struct{}{}
		}
	}

	// download the data and assert it matches
	var buf bytes.Buffer
	err = dl.DownloadObject(context.Background(), &buf, *o.Object, 0, uint64(o.Size), w.UsableHosts())
	if err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(data, buf.Bytes()) {
		t.Fatal("data mismatch")
	}

	// filter contracts to have (at most) min shards used contracts
	var n int
	var filtered []api.HostInfo
	for _, h := range w.UploadHosts() {
		// add unused contracts
		host, err := w.bus.Host(context.Background(), h.PublicKey)
		if err != nil {
			t.Fatal(err)
		}
		if _, used := used[h.PublicKey]; !used {
			filtered = append(filtered, host.Info())
			continue
		}

		// add min shards used contracts
		if n < int(params.RS.MinShards) {
			filtered = append(filtered, host.Info())
			n++
		}
	}

	// download the data again and assert it matches
	buf.Reset()
	err = dl.DownloadObject(context.Background(), &buf, *o.Object, 0, uint64(o.Size), filtered)
	if err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(data, buf.Bytes()) {
		t.Fatal("data mismatch")
	}

	// filter out one contract - expect download to fail
	for i, h := range filtered {
		if _, used := used[h.PublicKey]; used {
			filtered = append(filtered[:i], filtered[i+1:]...)
			break
		}
	}

	// download the data again and assert it fails
	buf.Reset()
	err = dl.DownloadObject(context.Background(), &buf, *o.Object, 0, uint64(o.Size), filtered)
	if !errors.Is(err, download.ErrDownloadNotEnoughHosts) {
		t.Fatal("expected not enough hosts error", err)
	}

	// try and upload into a bucket that does not exist
	params.Bucket = "doesnotexist"
	_, _, err = ul.Upload(context.Background(), bytes.NewReader(data), w.UploadHosts(), params)
	if !errors.Is(err, api.ErrBucketNotFound) {
		t.Fatal("expected bucket not found error", err)
	}

	// upload data using a cancelled context - assert we don't hang
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err = ul.Upload(ctx, bytes.NewReader(data), w.UploadHosts(), params)
	if err == nil || !errors.Is(err, upload.ErrUploadCancelled) {
		t.Fatal(err)
	}
}

func TestUploadPackedSlab(t *testing.T) {
	// create test worker
	w := newTestWorker(t, newTestWorkerCfg())

	// add hosts to worker
	w.AddHosts(testRedundancySettings.TotalShards)

	// convenience variables
	os := w.os
	mm := w.ulmm
	dl := w.downloadManager
	ul := w.uploadManager

	// create upload params
	params := testParameters(t.Name())
	params.Packing = true

	// create test data
	data := frand.Bytes(128)

	// upload data
	_, _, err := ul.Upload(context.Background(), bytes.NewReader(data), w.UploadHosts(), params)
	if err != nil {
		t.Fatal(err)
	}

	// assert our object store contains a partial slab
	if os.NumPartials() != 1 {
		t.Fatal("expected 1 partial slab")
	}

	// grab the object
	o, err := os.Object(context.Background(), testBucket, t.Name(), api.GetObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// download the data and assert it matches
	var buf bytes.Buffer
	err = dl.DownloadObject(context.Background(), &buf, *o.Object, 0, uint64(o.Size), w.UsableHosts())
	if err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(data, buf.Bytes()) {
		t.Fatal("data mismatch")
	}

	// fetch packed slabs for upload
	pss, err := os.PackedSlabsForUpload(context.Background(), time.Minute, uint8(params.RS.MinShards), uint8(params.RS.TotalShards), 1)
	if err != nil {
		t.Fatal(err)
	} else if len(pss) != 1 {
		t.Fatal("expected 1 packed slab")
	}
	ps := pss[0]

	// upload the packed slab
	mem := mm.AcquireMemory(context.Background(), params.RS.SlabSize())
	err = ul.UploadPackedSlab(context.Background(), params.RS, ps, mem, w.UploadHosts(), 0)
	if err != nil {
		t.Fatal(err)
	}

	// assert our object store contains zero partial slabs
	if os.NumPartials() != 0 {
		t.Fatal("expected no partial slabs")
	}

	// re-grab the object
	o, err = os.Object(context.Background(), testBucket, t.Name(), api.GetObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// download the data again and assert it matches
	buf.Reset()
	err = dl.DownloadObject(context.Background(), &buf, *o.Object, 0, uint64(o.Size), w.UsableHosts())
	if err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(data, buf.Bytes()) {
		t.Fatal("data mismatch")
	}

	// define a helper that counts packed slabs
	packedSlabsCount := func() int {
		t.Helper()
		cnt := os.NumPartials()
		return cnt
	}

	// define a helper that uploads data using the worker
	var c int
	uploadBytes := func(n int) {
		t.Helper()
		params.Key = fmt.Sprintf("%s_%d", t.Name(), c)
		_, err := w.upload(context.Background(), params.Bucket, params.Key, testRedundancySettings, bytes.NewReader(frand.Bytes(n)), w.UploadHosts(), upload.WithPacking(true))
		if err != nil {
			t.Fatal(err)
		}
		c++
	}

	// block aysnc packed slab uploads
	w.BlockAsyncPackedSlabUploads(params)

	// configure max buffer size
	os.SetSlabBufferMaxSizeSoft(128)

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
	w := newTestWorker(t, newTestWorkerCfg())

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
	_, _, err := ul.Upload(context.Background(), bytes.NewReader(data), w.UploadHosts(), params)
	if err != nil {
		t.Fatal(err)
	}

	// grab the slab
	o, err := os.Object(context.Background(), testBucket, t.Name(), api.GetObjectOptions{})
	if err != nil {
		t.Fatal(err)
	} else if len(o.Object.Slabs) != 1 {
		t.Fatal("expected 1 slab")
	}
	slab := o.Object.Slabs[0]

	// build usedHosts hosts
	usedHosts := make(map[types.PublicKey]struct{})
	for _, shard := range slab.Shards {
		for hk := range shard.Contracts {
			usedHosts[hk] = struct{}{}
		}
	}

	// assume the host of the first shard lost its sector
	var badHost types.PublicKey
	for hk := range slab.Shards[0].Contracts {
		badHost = hk
		break
	}
	badContract := slab.Shards[0].Contracts[badHost][0]
	err = os.DeleteHostSector(context.Background(), badHost, slab.Shards[0].Root)
	if err != nil {
		t.Fatal(err)
	}

	// download the slab
	shards, err := dl.DownloadSlab(context.Background(), slab.Slab, w.UsableHosts())
	if err != nil {
		t.Fatal(err)
	}

	// encrypt the shards
	o.Object.Slabs[0].Slab.Encrypt(shards)

	// filter it down to the shards we need to migrate
	shards = shards[:1]

	// recreate upload hosts
	hosts := make([]upload.HostInfo, 0)
	for _, h := range w.UploadHosts() {
		_, used := usedHosts[h.PublicKey]
		if !used && h.PublicKey != badHost {
			hosts = append(hosts, h)
		}
	}

	// re-grab the slab
	o, err = os.Object(context.Background(), testBucket, t.Name(), api.GetObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// migrate the shard away from the bad host
	mem := mm.AcquireMemory(context.Background(), rhpv4.SectorSize)
	err = ul.UploadShards(context.Background(), o.Object.Slabs[0].Slab, shards, hosts, 0, mem)
	if err != nil {
		t.Fatal(err)
	}

	// re-grab the slab
	o, err = os.Object(context.Background(), testBucket, t.Name(), api.GetObjectOptions{})
	if err != nil {
		t.Fatal(err)
	} else if len(o.Object.Slabs) != 1 {
		t.Fatal("expected 1 slab")
	}
	slab = o.Object.Slabs[0]

	// assert the bad shard is on a good host now
	shard := slab.Shards[0]
	if len(shard.Contracts) != 1 {
		t.Fatal("expected 1 contract")
	}
	for hk := range shard.Contracts {
		if hk == badHost {
			t.Fatal("shard is on bad host")
		}
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
	w := newTestWorker(t, newTestWorkerCfg())

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
	_, _, err := ul.Upload(context.Background(), bytes.NewReader(data), w.UploadHosts(), params)
	if err != nil {
		t.Fatal(err)
	}

	// grab the slab
	o, err := os.Object(context.Background(), testBucket, t.Name(), api.GetObjectOptions{})
	if err != nil {
		t.Fatal(err)
	} else if len(o.Object.Slabs) != 1 {
		t.Fatal("expected 1 slab")
	}
	slab := o.Object.Slabs[0]

	// build usedHosts hosts
	usedHosts := make(map[types.PublicKey]struct{})
	for _, shard := range slab.Shards {
		for hk := range shard.Contracts {
			usedHosts[hk] = struct{}{}
		}
	}

	// mark odd shards as bad
	var badIndices []int
	badHosts := make(map[types.PublicKey]struct{})
	for i, shard := range slab.Shards {
		if i%2 != 0 {
			badIndices = append(badIndices, i)
			for hk := range shard.Contracts {
				badHosts[hk] = struct{}{}
			}
		}
	}

	// download the slab
	shards, err := dl.DownloadSlab(context.Background(), slab.Slab, w.UsableHosts())
	if err != nil {
		t.Fatal(err)
	}

	// encrypt the shards
	o.Object.Slabs[0].Slab.Encrypt(shards)

	// filter it down to the shards we need to migrate
	for i, si := range badIndices {
		shards[i] = shards[si]
	}
	shards = shards[:len(badIndices)]

	// recreate upload hosts
	hosts := make([]upload.HostInfo, 0)
	for _, h := range w.UploadHosts() {
		_, used := usedHosts[h.PublicKey]
		_, bad := badHosts[h.PublicKey]
		if !used && !bad {
			hosts = append(hosts, h)
		}
	}

	// migrate those shards away from bad hosts
	mem := mm.AcquireMemory(context.Background(), uint64(len(badIndices))*rhpv4.SectorSize)
	err = ul.UploadShards(context.Background(), o.Object.Slabs[0].Slab, shards, hosts, 0, mem)
	if err != nil {
		t.Fatal(err)
	}

	// re-grab the slab
	o, err = os.Object(context.Background(), testBucket, t.Name(), api.GetObjectOptions{})
	if err != nil {
		t.Fatal(err)
	} else if len(o.Object.Slabs) != 1 {
		t.Fatal("expected 1 slab")
	}
	slab = o.Object.Slabs[0]

	// assert every shard that was on a bad host was migrated
	for i, shard := range slab.Shards {
		if i%2 == 0 && len(shard.Contracts) != 1 {
			t.Fatalf("expected 1 contract, got %v", len(shard.Contracts))
		} else if i%2 != 0 && len(shard.Contracts) != 2 {
			t.Fatalf("expected 2 contracts, got %v", len(shard.Contracts))
		}
		var bc, gc int
		for hk := range shard.Contracts {
			if _, bad := badHosts[hk]; bad {
				bc++
			} else {
				gc++
			}
		}
		if i%2 == 0 && bc != 0 && gc != 1 {
			t.Fatal("expected shard to be one 1 good host")
		} else if i%2 != 0 && bc != 1 && gc != 1 {
			t.Fatal("expected shard to be on 1 bad host and 1 good host")
		}
	}

	// create download contracts
	var infos []api.HostInfo
	for _, h := range w.UploadHosts() {
		if _, bad := badHosts[h.PublicKey]; !bad {
			host, err := w.bus.Host(context.Background(), h.PublicKey)
			if err != nil {
				t.Fatal(err)
			}
			infos = append(infos, host.Info())
		}
	}

	// download the data and assert it matches
	var buf bytes.Buffer
	err = dl.DownloadObject(context.Background(), &buf, *o.Object, 0, uint64(o.Size), infos)
	if err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(data, buf.Bytes()) {
		t.Fatal("data mismatch")
	}
}

func TestUploadSingleSectorSlowHosts(t *testing.T) {
	// create test worker
	cfg := newTestWorkerCfg()
	slowHosts := 5
	cfg.UploadMaxOverdrive = uint64(slowHosts)
	cfg.UploadOverdriveTimeout = time.Second

	w := newTestWorker(t, cfg)

	// add hosts to worker
	minShards := 10
	totalShards := 30
	hosts := w.AddHosts(totalShards + slowHosts)
	for i := 0; i < slowHosts; i++ {
		hosts[i].uploadDelay = time.Hour
	}

	// create test data
	data := frand.Bytes(rhpv4.SectorSize * minShards)

	// create upload params
	params := testParameters(t.Name())
	params.RS.MinShards = minShards
	params.RS.TotalShards = totalShards

	// upload data
	_, _, err := w.uploadManager.Upload(context.Background(), bytes.NewReader(data), w.UploadHosts(), params)
	if err != nil {
		t.Fatal(err)
	}
}

func TestUploadRegression(t *testing.T) {
	// create test worker
	w := newTestWorker(t, newTestWorkerCfg())

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
	_, err := w.upload(ctx, params.Bucket, params.Key, testRedundancySettings, bytes.NewReader(data), w.UploadHosts())
	if !errors.Is(err, upload.ErrUploadCancelled) {
		t.Fatal(err)
	}

	// unblock the memory manager
	unblock()

	// upload data
	_, err = w.upload(context.Background(), params.Bucket, params.Key, testRedundancySettings, bytes.NewReader(data), w.UploadHosts())
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
	err = dl.DownloadObject(context.Background(), &buf, *o.Object, 0, uint64(o.Size), w.UsableHosts())
	if err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(data, buf.Bytes()) {
		t.Fatal("data mismatch", data, buf.Bytes())
	}
}

func testParameters(key string) upload.Parameters {
	return upload.Parameters{
		Bucket: testBucket,
		Key:    key,

		EC:               object.GenerateEncryptionKey(object.EncryptionKeyTypeBasic), // random key
		EncryptionOffset: 0,                                                           // from the beginning

		RS: testRedundancySettings,
	}
}

func TestPinnedObject(t *testing.T) {
	// create test worker
	w := newTestWorker(t, newTestWorkerCfg())

	// add hosts to worker
	w.AddHosts(testRedundancySettings.TotalShards * 2)

	// convenience variables
	ul := w.uploadManager

	// create test data
	data := frand.Bytes(128)

	// create upload params
	params := testParameters(t.Name())

	// upload data
	_, _, err := ul.Upload(context.Background(), bytes.NewReader(data), w.UploadHosts(), params)
	if err != nil {
		t.Fatal(err)
	}

	// grab the object
	obj, err := w.os.Object(context.Background(), testBucket, t.Name(), api.GetObjectOptions{})
	if err != nil {
		t.Fatal(err)
	} else if len(obj.Object.Slabs) != 1 {
		t.Fatal("expected 1 slab")
	}

	po, err := w.PinnedObject(context.Background(), testBucket, t.Name())
	if err != nil {
		t.Fatal(err)
	} else if len(po.Slabs) != len(obj.Slabs) {
		t.Fatal("pinned object slabs do not match original object slabs")
	}
	for i, ps := range po.Slabs {
		slab := obj.Object.Slabs[i]
		if ps.Offset != slab.Offset || ps.Length != slab.Length {
			t.Fatal("pinned slab offset or length does not match original slab")
		} else if len(ps.Sectors) != len(slab.Shards) {
			t.Fatal("pinned slab sectors do not match original slab shards")
		}

		for j, sector := range ps.Sectors {
			shard := slab.Shards[j]
			if _, ok := shard.Contracts[sector.HostKey]; !ok {
				t.Fatal("pinned slab sector host key does not match original slab shard host key")
			} else if shard.Root != sector.Root {
				t.Fatal("pinned slab sector root does not match original slab shard root")
			}
		}
	}

	convertKey := func(key object.RawEncryptionKey) (imported object.EncryptionKey) {
		// unmarshals the key as a basic key rather than a salted key
		if err := imported.UnmarshalText(fmt.Appendf(nil, "key:%x", key)); err != nil {
			panic(fmt.Sprintf("failed to unmarshal key: %v", err))
		}
		return
	}

	imported := object.Object{
		Key: convertKey(po.EncryptionKey),
	}
	for i, ps := range po.Slabs {
		importedSlab := object.SlabSlice{
			Slab: object.Slab{
				EncryptionKey: convertKey(ps.EncryptionKey),
				MinShards:     ps.MinShards,
			},
			Offset: ps.Offset,
			Length: ps.Length,
		}
		for j, sector := range ps.Sectors {
			importedSlab.Shards = append(importedSlab.Shards, object.Sector{
				Root: sector.Root,
				Contracts: map[types.PublicKey][]types.FileContractID{
					sector.HostKey: obj.Slabs[i].Shards[j].Contracts[sector.HostKey], // need to grab the contract IDs from the original object
				},
			})
		}
		imported.Slabs = append(imported.Slabs, importedSlab)
	}

	if err := w.bus.AddObject(context.Background(), testBucket, "foo", imported, api.AddObjectOptions{}); err != nil {
		t.Fatal(err)
	}

	resp, err := w.GetObject(context.Background(), testBucket, "foo", api.DownloadObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}

	buf, err := io.ReadAll(resp.Content)
	if err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(buf, data) {
		t.Fatal("downloaded data does not match original data")
	}
}
