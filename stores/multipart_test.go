package stores

import (
	"context"
	"encoding/hex"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/klauspost/reedsolomon"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/object"
	"lukechampine.com/frand"
)

func TestMultipartUploadWithUploadPackingRegression(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	// create 30 hosts
	hks, err := ss.addTestHosts(30)
	if err != nil {
		t.Fatal(err)
	}

	// create one contract for each host.
	fcids, _, err := ss.addTestContracts(hks)
	if err != nil {
		t.Fatal(err)
	}

	usedContracts := make(map[types.PublicKey]types.FileContractID)
	for i := range hks {
		usedContracts[hks[i]] = fcids[i]
	}

	ctx := context.Background()
	minShards := uint8(10)
	totalShards := uint8(30)
	objName := "/foo"
	partSize := 8 * 1 << 20
	nParts := 6
	totalSize := int64(nParts * partSize)

	// Upload parts until we have enough data for 2 buffers.
	resp, err := ss.CreateMultipartUpload(ctx, api.DefaultBucketName, objName, object.NoOpKey, testMimeType, testMetadata)
	if err != nil {
		t.Fatal(err)
	}
	var parts []api.MultipartCompletedPart
	for i := 1; i <= nParts; i++ {
		partialSlabs, _, err := ss.AddPartialSlab(ctx, frand.Bytes(partSize), minShards, totalShards, testContractSet)
		if err != nil {
			t.Fatal(err)
		}
		etag := hex.EncodeToString(frand.Bytes(16))
		err = ss.AddMultipartPart(ctx, api.DefaultBucketName, objName, testContractSet, etag, resp.UploadID, i, partialSlabs)
		if err != nil {
			t.Fatal(err)
		}
		parts = append(parts, api.MultipartCompletedPart{
			PartNumber: i,
			ETag:       etag,
		})
	}

	// Assert metadata was persisted and is linked to the multipart upload
	var metadatas []dbObjectUserMetadata
	if err := ss.db.Model(&dbObjectUserMetadata{}).Find(&metadatas).Error; err != nil {
		t.Fatal(err)
	} else if len(metadatas) != len(testMetadata) {
		t.Fatal("expected metadata to be persisted")
	}
	for _, m := range metadatas {
		if m.DBMultipartUploadID == nil || m.DBObjectID != nil {
			t.Fatal("unexpected")
		}
	}

	// Complete the upload. Check that the number of slices stays the same.
	ts := time.Now()
	var nSlicesBefore int64
	var nSlicesAfter int64
	if err := ss.db.Model(&dbSlice{}).Count(&nSlicesBefore).Error; err != nil {
		t.Fatal(err)
	} else if nSlicesBefore == 0 {
		t.Fatal("expected some slices")
	} else if _, err = ss.CompleteMultipartUpload(ctx, api.DefaultBucketName, objName, resp.UploadID, parts, api.CompleteMultipartOptions{}); err != nil {
		t.Fatal(err)
	} else if err := ss.db.Model(&dbSlice{}).Count(&nSlicesAfter).Error; err != nil {
		t.Fatal(err)
	} else if nSlicesBefore != nSlicesAfter {
		t.Fatalf("expected number of slices to stay the same, but got %v before and %v after", nSlicesBefore, nSlicesAfter)
	} else if err := ss.waitForPruneLoop(ts); err != nil {
		t.Fatal(err)
	}

	// Fetch the object.
	obj, err := ss.Object(ctx, api.DefaultBucketName, objName)
	if err != nil {
		t.Fatal(err)
	} else if obj.Size != int64(totalSize) {
		t.Fatalf("expected object size to be %v, got %v", totalSize, obj.Size)
	} else if obj.TotalSize() != totalSize {
		t.Fatalf("expected object total size to be %v, got %v", totalSize, obj.TotalSize())
	}

	// Assert it has the metadata
	if !reflect.DeepEqual(obj.Metadata, testMetadata) {
		t.Fatal("meta mismatch", cmp.Diff(obj.Metadata, testMetadata))
	}

	// Assert metadata was converted and the multipart upload id was nullified
	if err := ss.db.Model(&dbObjectUserMetadata{}).Find(&metadatas).Error; err != nil {
		t.Fatal(err)
	} else if len(metadatas) != len(testMetadata) {
		t.Fatal("expected metadata to be persisted")
	}
	for _, m := range metadatas {
		if m.DBMultipartUploadID != nil || m.DBObjectID == nil {
			t.Fatal("unexpected")
		}
	}

	// Upload buffers.
	upload := func(ps api.PackedSlab) api.UploadedPackedSlab {
		ups := api.UploadedPackedSlab{
			BufferID: ps.BufferID,
		}
		rs, _ := reedsolomon.New(int(minShards), int(totalShards-minShards))
		splitData, err := rs.Split(ps.Data)
		if err != nil {
			t.Fatal(err)
		}
		err = rs.Encode(splitData)
		if err != nil {
			t.Fatal(err)
		}
		for i, shard := range splitData {
			ups.Shards = append(ups.Shards, newTestShard(hks[i], fcids[i], types.HashBytes(shard)))
		}
		return ups
	}
	packedSlabs, err := ss.PackedSlabsForUpload(ctx, time.Hour, minShards, totalShards, testContractSet, 2)
	if err != nil {
		t.Fatal(err)
	}
	var uploadedPackedSlabs []api.UploadedPackedSlab
	for _, ps := range packedSlabs {
		uploadedPackedSlabs = append(uploadedPackedSlabs, upload(ps))
	}
	if err := ss.MarkPackedSlabsUploaded(ctx, uploadedPackedSlabs); err != nil {
		t.Fatal(err)
	}

	// Fetch the object again.
	obj, err = ss.Object(ctx, api.DefaultBucketName, objName)
	if err != nil {
		t.Fatal(err)
	} else if obj.Size != int64(totalSize) {
		t.Fatalf("expected object size to be %v, got %v", totalSize, obj.Size)
	} else if obj.TotalSize() != totalSize {
		for _, f := range obj.Slabs {
			t.Log("slice", f.Length, f.IsPartial())
		}
		t.Fatalf("expected object total size to be %v, got %v", totalSize, obj.TotalSize())
	}
}

func TestMultipartUploads(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	// create 3 multipart uploads, the first 2 have the same path
	resp1, err := ss.CreateMultipartUpload(context.Background(), api.DefaultBucketName, "/foo", object.NoOpKey, testMimeType, testMetadata)
	if err != nil {
		t.Fatal(err)
	}
	resp2, err := ss.CreateMultipartUpload(context.Background(), api.DefaultBucketName, "/foo", object.NoOpKey, testMimeType, testMetadata)
	if err != nil {
		t.Fatal(err)
	}
	resp3, err := ss.CreateMultipartUpload(context.Background(), api.DefaultBucketName, "/foo2", object.NoOpKey, testMimeType, testMetadata)
	if err != nil {
		t.Fatal(err)
	}

	// prepare the expected order of uploads returned by MultipartUploads
	orderedUploads := []struct {
		uploadID string
		objectID string
	}{
		{uploadID: resp1.UploadID, objectID: "/foo"},
		{uploadID: resp2.UploadID, objectID: "/foo"},
		{uploadID: resp3.UploadID, objectID: "/foo2"},
	}
	sort.Slice(orderedUploads, func(i, j int) bool {
		if orderedUploads[i].objectID != orderedUploads[j].objectID {
			return strings.Compare(orderedUploads[i].objectID, orderedUploads[j].objectID) < 0
		}
		return strings.Compare(orderedUploads[i].uploadID, orderedUploads[j].uploadID) < 0
	})

	// fetch uploads
	mur, err := ss.MultipartUploads(context.Background(), api.DefaultBucketName, "", "", "", 3)
	if err != nil {
		t.Fatal(err)
	} else if len(mur.Uploads) != 3 {
		t.Fatal("expected 3 uploads")
	} else if mur.Uploads[0].UploadID != orderedUploads[0].uploadID {
		t.Fatal("unexpected upload id")
	} else if mur.Uploads[1].UploadID != orderedUploads[1].uploadID {
		t.Fatal("unexpected upload id")
	} else if mur.Uploads[2].UploadID != orderedUploads[2].uploadID {
		t.Fatal("unexpected upload id")
	}

	// fetch uploads with prefix
	mur, err = ss.MultipartUploads(context.Background(), api.DefaultBucketName, "/foo", "", "", 3)
	if err != nil {
		t.Fatal(err)
	} else if len(mur.Uploads) != 3 {
		t.Fatal("expected 3 uploads")
	} else if mur.Uploads[0].UploadID != orderedUploads[0].uploadID {
		t.Fatal("unexpected upload id")
	} else if mur.Uploads[1].UploadID != orderedUploads[1].uploadID {
		t.Fatal("unexpected upload id")
	} else if mur.Uploads[2].UploadID != orderedUploads[2].uploadID {
		t.Fatal("unexpected upload id")
	}
	mur, err = ss.MultipartUploads(context.Background(), api.DefaultBucketName, "/foo2", "", "", 3)
	if err != nil {
		t.Fatal(err)
	} else if len(mur.Uploads) != 1 {
		t.Fatal("expected 1 upload")
	} else if mur.Uploads[0].UploadID != orderedUploads[2].uploadID {
		t.Fatal("unexpected upload id")
	}

	// paginate through them one-by-one
	keyMarker := ""
	uploadIDMarker := ""
	hasMore := true
	for hasMore {
		mur, err = ss.MultipartUploads(context.Background(), api.DefaultBucketName, "", keyMarker, uploadIDMarker, 1)
		if err != nil {
			t.Fatal(err)
		} else if len(mur.Uploads) != 1 {
			t.Fatal("expected 1 upload")
		} else if mur.Uploads[0].UploadID != orderedUploads[0].uploadID {
			t.Fatalf("unexpected upload id: %v != %v", mur.Uploads[0].UploadID, orderedUploads[0].uploadID)
		}
		orderedUploads = orderedUploads[1:]
		keyMarker = mur.NextPathMarker
		uploadIDMarker = mur.NextUploadIDMarker
		hasMore = mur.HasMore
	}
	if len(orderedUploads) != 0 {
		t.Fatal("expected 3 iterations")
	}
}
