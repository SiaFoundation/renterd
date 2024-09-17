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
	resp, err := ss.CreateMultipartUpload(ctx, testBucket, objName, object.NoOpKey, testMimeType, testMetadata)
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
		err = ss.AddMultipartPart(ctx, testBucket, objName, testContractSet, etag, resp.UploadID, i, partialSlabs)
		if err != nil {
			t.Fatal(err)
		}
		parts = append(parts, api.MultipartCompletedPart{
			PartNumber: i,
			ETag:       etag,
		})
	}

	type oum struct {
		MultipartUploadID *int64
		ObjectID          *int64
	}
	fetchUserMD := func() (metadatas []oum) {
		rows, err := ss.DB().Query(context.Background(), "SELECT db_multipart_upload_id, db_object_id FROM object_user_metadata")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		for rows.Next() {
			var md oum
			if err := rows.Scan(&md.MultipartUploadID, &md.ObjectID); err != nil {
				t.Fatal(err)
			}
			metadatas = append(metadatas, md)
		}
		return
	}

	// Assert metadata was persisted and is linked to the multipart upload
	metadatas := fetchUserMD()
	if len(metadatas) != len(testMetadata) {
		t.Fatal("expected metadata to be persisted")
	}
	for _, m := range metadatas {
		if m.MultipartUploadID == nil || m.ObjectID != nil {
			t.Fatal("unexpected")
		}
	}

	// Complete the upload. Check that the number of slices stays the same.
	if nSlicesBefore := ss.Count("slices"); nSlicesBefore == 0 {
		t.Fatal("expected some slices")
	} else if _, err = ss.CompleteMultipartUpload(ctx, testBucket, objName, resp.UploadID, parts, api.CompleteMultipartOptions{}); err != nil {
		t.Fatal(err)
	} else if nSlicesAfter := ss.Count("slices"); nSlicesAfter != nSlicesBefore {
		t.Fatalf("expected number of slices to stay the same, but got %v before and %v after", nSlicesBefore, nSlicesAfter)
	}

	// Fetch the object.
	obj, err := ss.Object(ctx, testBucket, objName)
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
	metadatas = fetchUserMD()
	if len(metadatas) != len(testMetadata) {
		t.Fatal("expected metadata to be persisted")
	}
	for _, m := range metadatas {
		if m.MultipartUploadID != nil || m.ObjectID == nil {
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
	obj, err = ss.Object(ctx, testBucket, objName)
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
	resp1, err := ss.CreateMultipartUpload(context.Background(), testBucket, "/foo", object.NoOpKey, testMimeType, testMetadata)
	if err != nil {
		t.Fatal(err)
	}
	resp2, err := ss.CreateMultipartUpload(context.Background(), testBucket, "/foo", object.NoOpKey, testMimeType, testMetadata)
	if err != nil {
		t.Fatal(err)
	}
	resp3, err := ss.CreateMultipartUpload(context.Background(), testBucket, "/foo2", object.NoOpKey, testMimeType, testMetadata)
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
	mur, err := ss.MultipartUploads(context.Background(), testBucket, "", "", "", 3)
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
	mur, err = ss.MultipartUploads(context.Background(), testBucket, "/foo", "", "", 3)
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
	mur, err = ss.MultipartUploads(context.Background(), testBucket, "/foo2", "", "", 3)
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
		mur, err = ss.MultipartUploads(context.Background(), testBucket, "", keyMarker, uploadIDMarker, 1)
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

func TestMultipartUploadEmptyObjects(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	// create 2 multipart parts
	resp1, err := ss.CreateMultipartUpload(context.Background(), testBucket, "/foo1", object.NoOpKey, testMimeType, testMetadata)
	if err != nil {
		t.Fatal(err)
	}
	resp2, err := ss.CreateMultipartUpload(context.Background(), testBucket, "/foo2", object.NoOpKey, testMimeType, testMetadata)
	if err != nil {
		t.Fatal(err)
	}

	// complete uploads in reverse order
	cmu1, err := ss.CompleteMultipartUpload(context.Background(), testBucket, "/foo2", resp2.UploadID, []api.MultipartCompletedPart{}, api.CompleteMultipartOptions{})
	if err != nil {
		t.Fatal(err)
	}
	cmu2, err := ss.CompleteMultipartUpload(context.Background(), testBucket, "/foo1", resp1.UploadID, []api.MultipartCompletedPart{}, api.CompleteMultipartOptions{})
	if err != nil {
		t.Fatal(err)
	}

	foo1, err := ss.ObjectMetadata(context.Background(), testBucket, "/foo1")
	if err != nil {
		t.Fatal(err)
	} else if foo1.ETag != cmu1.ETag {
		t.Fatal("unexpected etag")
	}
	foo2, err := ss.ObjectMetadata(context.Background(), testBucket, "/foo2")
	if err != nil {
		t.Fatal(err)
	} else if foo2.ETag != cmu2.ETag {
		t.Fatal("unexpected etag")
	}
}
