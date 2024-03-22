package e2e

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/minio/minio-go/v7"
	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/gofakes3"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/test"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

var (
	errBucketNotExists = errors.New("specified bucket does not exist")
)

func TestS3Basic(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	start := time.Now()
	cluster := newTestCluster(t, testClusterOptions{
		hosts: test.RedundancySettings.TotalShards,
	})
	defer cluster.Shutdown()

	// delete default bucket before testing.
	s3 := cluster.S3
	tt := cluster.tt
	if err := cluster.Bus.DeleteBucket(context.Background(), api.DefaultBucketName); err != nil {
		t.Fatal(err)
	}

	// create bucket
	bucket := "bucket"
	objPath := "obj#ct" // special char to check escaping
	tt.OK(s3.MakeBucket(context.Background(), bucket, minio.MakeBucketOptions{}))

	// list buckets
	buckets, err := s3.ListBuckets(context.Background())
	tt.OK(err)
	if len(buckets) != 1 {
		t.Fatalf("unexpected number of buckets, %d != 1", len(buckets))
	} else if buckets[0].Name != bucket {
		t.Fatalf("unexpected bucket name, %s != %s", buckets[0].Name, bucket)
	} else if buckets[0].CreationDate.IsZero() {
		t.Fatal("expected non-zero creation date")
	}

	// exist buckets
	exists, err := s3.BucketExists(context.Background(), bucket)
	tt.OK(err)
	if !exists {
		t.Fatal("expected bucket to exist")
	}
	exists, err = s3.BucketExists(context.Background(), bucket+"nonexistent")
	tt.OK(err)
	if exists {
		t.Fatal("expected bucket to not exist")
	}

	// add object to the bucket
	data := frand.Bytes(10)
	etag := md5.Sum(data)
	uploadInfo, err := s3.PutObject(context.Background(), bucket, objPath, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{})
	tt.OK(err)
	if uploadInfo.ETag != hex.EncodeToString(etag[:]) {
		t.Fatalf("expected ETag %v, got %v", hex.EncodeToString(etag[:]), uploadInfo.ETag)
	}
	busObject, err := cluster.Bus.Object(context.Background(), bucket, objPath, api.GetObjectOptions{})
	tt.OK(err)
	if busObject.Object == nil {
		t.Fatal("expected object to exist")
	} else if busObject.Object.ETag != uploadInfo.ETag {
		t.Fatalf("expected ETag %q, got %q", uploadInfo.ETag, busObject.Object.ETag)
	}

	_, err = s3.PutObject(context.Background(), bucket+"nonexistent", objPath, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{})
	tt.AssertIs(err, errBucketNotExists)

	// get object
	obj, err := s3.GetObject(context.Background(), bucket, objPath, minio.GetObjectOptions{})
	tt.OK(err)
	if b, err := io.ReadAll(obj); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(b, data) {
		t.Fatal("data mismatch")
	} else if info, err := obj.Stat(); err != nil {
		t.Fatal(err)
	} else if info.ETag != uploadInfo.ETag {
		t.Fatal("unexpected ETag:", info.ETag, uploadInfo.ETag)
	}

	// stat object
	info, err := s3.StatObject(context.Background(), bucket, objPath, minio.StatObjectOptions{})
	tt.OK(err)
	if info.Size != int64(len(data)) {
		t.Fatal("size mismatch")
	} else if info.ETag != uploadInfo.ETag {
		t.Fatal("unexpected ETag:", info.ETag)
	}

	// stat object that doesn't exist
	_, err = s3.StatObject(context.Background(), bucket, "nonexistent", minio.StatObjectOptions{})
	if err == nil || !strings.Contains(err.Error(), "The specified key does not exist") {
		t.Fatal(err)
	}

	// add another bucket
	tt.OK(s3.MakeBucket(context.Background(), bucket+"2", minio.MakeBucketOptions{}))

	// copy our object into the new bucket.
	res, err := s3.CopyObject(context.Background(), minio.CopyDestOptions{
		Bucket: bucket + "2",
		Object: objPath,
	}, minio.CopySrcOptions{
		Bucket: bucket,
		Object: objPath,
	})
	tt.OK(err)
	if res.LastModified.IsZero() {
		t.Fatal("expected LastModified to be non-zero")
	} else if !res.LastModified.After(start.UTC()) {
		t.Fatal("expected LastModified to be after the start of our test")
	} else if res.ETag == "" {
		t.Fatal("expected ETag to be set")
	}

	// get copied object
	obj, err = s3.GetObject(context.Background(), bucket+"2", objPath, minio.GetObjectOptions{})
	tt.OK(err)
	if b, err := io.ReadAll(obj); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(b, data) {
		t.Fatal("data mismatch")
	}

	// assert deleting the bucket fails because it's not empty
	err = s3.RemoveBucket(context.Background(), bucket)
	tt.AssertIs(err, gofakes3.ErrBucketNotEmpty)

	// assert deleting the bucket fails because it doesn't exist
	err = s3.RemoveBucket(context.Background(), bucket+"nonexistent")
	tt.AssertIs(err, errBucketNotExists)

	// remove the object
	tt.OK(s3.RemoveObject(context.Background(), bucket, objPath, minio.RemoveObjectOptions{}))

	// try to get object
	obj, err = s3.GetObject(context.Background(), bucket, objPath, minio.GetObjectOptions{})
	tt.OK(err)
	_, err = io.ReadAll(obj)
	tt.AssertContains(err, "The specified key does not exist")

	// add a few objects to the bucket.
	tt.OKAll(s3.PutObject(context.Background(), bucket, "dir/", bytes.NewReader(frand.Bytes(10)), 10, minio.PutObjectOptions{}))
	tt.OKAll(s3.PutObject(context.Background(), bucket, "dir/file", bytes.NewReader(frand.Bytes(10)), 10, minio.PutObjectOptions{}))

	// delete them using the multi delete endpoint.
	objectsCh := make(chan minio.ObjectInfo, 3)
	objectsCh <- minio.ObjectInfo{Key: "dir/file"}
	objectsCh <- minio.ObjectInfo{Key: "dir/"}
	close(objectsCh)
	results := s3.RemoveObjects(context.Background(), bucket, objectsCh, minio.RemoveObjectsOptions{})
	for res := range results {
		tt.OK(res.Err)
	}

	// delete bucket
	tt.OK(s3.RemoveBucket(context.Background(), bucket))
	exists, err = s3.BucketExists(context.Background(), bucket)
	tt.OK(err)
	if exists {
		t.Fatal("expected bucket to not exist")
	}
}

func TestS3ObjectMetadata(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	// create cluster
	opts := testClusterOptions{
		hosts:  test.RedundancySettings.TotalShards,
		logger: zap.NewNop(),
	}
	cluster := newTestCluster(t, opts)
	defer cluster.Shutdown()

	// convenience variables
	s3 := cluster.S3
	tt := cluster.tt

	// create dummy metadata
	metadata := map[string]string{
		"Foo": "bar",
		"Baz": "qux",
	}

	// add object to the bucket
	_, err := s3.PutObject(context.Background(), api.DefaultBucketName, t.Name(), bytes.NewReader([]byte(t.Name())), int64(len([]byte(t.Name()))), minio.PutObjectOptions{UserMetadata: metadata})
	tt.OK(err)

	// create helper to assert metadata is present
	assertMetadata := func(want map[string]string, got minio.StringMap) {
		t.Helper()
		for k, wantt := range want {
			if gott, ok := got[k]; !ok || gott != wantt {
				t.Fatalf("unexpected metadata, '%v' != '%v' (found: %v)", gott, wantt, ok)
			}
		}
	}

	// perform GET request
	obj, err := s3.GetObject(context.Background(), api.DefaultBucketName, t.Name(), minio.GetObjectOptions{})
	tt.OK(err)

	// assert metadata is set
	get, err := obj.Stat()
	tt.OK(err)
	assertMetadata(metadata, get.UserMetadata)

	// perform HEAD request
	head, err := s3.StatObject(context.Background(), api.DefaultBucketName, t.Name(), minio.StatObjectOptions{})
	tt.OK(err)
	assertMetadata(metadata, head.UserMetadata)

	// perform metadata update (same src/dst copy)
	metadata["Baz"] = "updated"
	_, err = s3.CopyObject(
		context.Background(),
		minio.CopyDestOptions{Bucket: api.DefaultBucketName, Object: t.Name(), UserMetadata: metadata, ReplaceMetadata: true},
		minio.CopySrcOptions{Bucket: api.DefaultBucketName, Object: t.Name()},
	)
	tt.OK(err)

	// perform HEAD request
	head, err = s3.StatObject(context.Background(), api.DefaultBucketName, t.Name(), minio.StatObjectOptions{})
	tt.OK(err)
	assertMetadata(metadata, head.UserMetadata)

	// perform copy
	metadata["Baz"] = "copied"
	_, err = s3.CopyObject(
		context.Background(),
		minio.CopyDestOptions{Bucket: api.DefaultBucketName, Object: t.Name() + "copied", UserMetadata: metadata, ReplaceMetadata: true},
		minio.CopySrcOptions{Bucket: api.DefaultBucketName, Object: t.Name()},
	)
	tt.OK(err)

	// perform HEAD request
	head, err = s3.StatObject(context.Background(), api.DefaultBucketName, t.Name()+"copied", minio.StatObjectOptions{})
	tt.OK(err)
	assertMetadata(metadata, head.UserMetadata)

	// assert the original object's metadata is unchanged
	metadata["Baz"] = "updated"
	head, err = s3.StatObject(context.Background(), api.DefaultBucketName, t.Name(), minio.StatObjectOptions{})
	tt.OK(err)
	assertMetadata(metadata, head.UserMetadata)

	// upload a file using multipart upload
	core := cluster.S3Core
	uid, err := core.NewMultipartUpload(context.Background(), api.DefaultBucketName, "multi", minio.PutObjectOptions{
		UserMetadata: map[string]string{
			"New": "1",
		},
	})
	tt.OK(err)
	data := frand.Bytes(3)

	part, err := core.PutObjectPart(context.Background(), api.DefaultBucketName, "foo", uid, 1, bytes.NewReader(data), int64(len(data)), minio.PutObjectPartOptions{})
	tt.OK(err)
	_, err = core.CompleteMultipartUpload(context.Background(), api.DefaultBucketName, "multi", uid, []minio.CompletePart{
		{
			PartNumber: part.PartNumber,
			ETag:       part.ETag,
		},
	}, minio.PutObjectOptions{
		UserMetadata: map[string]string{
			"Complete": "2",
		},
	})
	tt.OK(err)

	// check metadata
	head, err = s3.StatObject(context.Background(), api.DefaultBucketName, "multi", minio.StatObjectOptions{})
	tt.OK(err)
	assertMetadata(map[string]string{
		"New":      "1",
		"Complete": "2",
	}, head.UserMetadata)
}

func TestS3Authentication(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	cluster := newTestCluster(t, clusterOptsDefault)
	defer cluster.Shutdown()
	tt := cluster.tt

	assertAuth := func(c *minio.Core, shouldWork bool) {
		t.Helper()
		_, err := c.ListObjectsV2(api.DefaultBucketName, "/", "", "", "", 100)
		if shouldWork && err != nil {
			t.Fatal(err)
		} else if !shouldWork && err == nil {
			t.Fatal("expected error")
		} else if !shouldWork && err != nil && !strings.Contains(err.Error(), "AccessDenied") {
			t.Fatal("wrong error")
		}
	}

	// Create client.
	url := cluster.S3.EndpointURL().Host
	s3Unauthenticated, err := minio.NewCore(url, &minio.Options{
		Creds: nil, // no authentication
	})
	tt.OK(err)

	// List bucket. Shouldn't work.
	assertAuth(s3Unauthenticated, false)

	// Create client with credentials and try again..
	s3Authenticated, err := minio.NewCore(url, &minio.Options{
		Creds: test.S3Credentials,
	})
	tt.OK(err)

	// List buckets. Should work.
	assertAuth(s3Authenticated, true)

	// Update the policy of the bucket to allow public read access.
	tt.OK(cluster.Bus.UpdateBucketPolicy(context.Background(), api.DefaultBucketName, api.BucketPolicy{
		PublicReadAccess: true,
	}))

	// Check that the policy was updated.
	b, err := cluster.Bus.Bucket(context.Background(), api.DefaultBucketName)
	tt.OK(err)
	if b.Policy.PublicReadAccess != true {
		t.Fatal("expected public read access")
	}

	// Listing should work now.
	assertAuth(s3Unauthenticated, true)

	// Update the policy again to disable access.
	tt.OK(cluster.Bus.UpdateBucketPolicy(context.Background(), api.DefaultBucketName, api.BucketPolicy{
		PublicReadAccess: false,
	}))

	// Check that the policy was updated.
	b, err = cluster.Bus.Bucket(context.Background(), api.DefaultBucketName)
	tt.OK(err)
	if b.Policy.PublicReadAccess == true {
		t.Fatal("expected no public read access")
	}

	// Listing should not work now.
	assertAuth(s3Unauthenticated, false)
}

func TestS3List(t *testing.T) {
	cluster := newTestCluster(t, testClusterOptions{
		hosts:         test.RedundancySettings.TotalShards,
		uploadPacking: true,
	})
	defer cluster.Shutdown()

	s3 := cluster.S3
	core := cluster.S3Core
	tt := cluster.tt

	// create bucket
	tt.OK(s3.MakeBucket(context.Background(), "bucket", minio.MakeBucketOptions{}))

	// manually create the 'a/' object as a directory. It should also be
	// possible to call StatObject on it without errors.
	tt.OKAll(s3.PutObject(context.Background(), "bucket", "a/", bytes.NewReader(nil), 0, minio.PutObjectOptions{}))
	so, err := s3.StatObject(context.Background(), "bucket", "a/", minio.StatObjectOptions{})
	tt.OK(err)
	if so.Key != "a/" {
		t.Fatal("unexpected key:", so.Key)
	}

	objects := []string{
		"a/a/a",
		"a/b",
		"b",
		"c/a",
		"d",
		"ab",
		"y/",
		"y/y/y/y",
	}
	for _, object := range objects {
		data := frand.Bytes(10)
		tt.OKAll(s3.PutObject(context.Background(), "bucket", object, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{}))
	}

	flatten := func(res minio.ListBucketResult) []string {
		var objs []string
		for _, obj := range res.Contents {
			if !strings.HasSuffix(obj.Key, "/") && obj.LastModified.IsZero() {
				t.Fatal("expected non-zero LastModified", obj.Key)
			}
			objs = append(objs, obj.Key)
		}
		for _, cp := range res.CommonPrefixes {
			objs = append(objs, cp.Prefix)
		}
		return objs
	}

	tests := []struct {
		delimiter string
		prefix    string
		marker    string
		want      []string
	}{
		{
			delimiter: "/",
			prefix:    "",
			marker:    "",
			want:      []string{"ab", "b", "d", "a/", "c/", "y/"},
		},
		{
			delimiter: "/",
			prefix:    "a",
			marker:    "",
			want:      []string{"ab", "a/"},
		},
		{
			delimiter: "/",
			prefix:    "a/a",
			marker:    "",
			want:      []string{"a/a/"},
		},
		{
			delimiter: "/",
			prefix:    "",
			marker:    "b",
			want:      []string{"d", "c/", "y/"},
		},
		{
			delimiter: "/",
			prefix:    "z",
			marker:    "",
			want:      nil,
		},
		{
			delimiter: "/",
			prefix:    "a",
			marker:    "a/",
			want:      []string{"ab"},
		},
		{
			delimiter: "/",
			prefix:    "y/",
			marker:    "",
			want:      []string{"y/y/"},
		},
		{
			delimiter: "",
			prefix:    "y/",
			marker:    "",
			want:      []string{"y/", "y/y/y/y"},
		},
		{
			delimiter: "",
			prefix:    "y/y",
			marker:    "",
			want:      []string{"y/y/y/y"},
		},
		{
			delimiter: "",
			prefix:    "y/y/",
			marker:    "",
			want:      []string{"y/y/y/y"},
		},
	}
	for i, test := range tests {
		result, err := core.ListObjects("bucket", test.prefix, test.marker, test.delimiter, 1000)
		if err != nil {
			t.Fatal(err)
		}
		got := flatten(result)
		if !cmp.Equal(test.want, got) {
			t.Errorf("test %d: unexpected response, want %v got %v", i, test.want, got)
		}
		for _, obj := range result.Contents {
			if obj.ETag == "" {
				t.Fatal("expected non-empty ETag")
			} else if obj.LastModified.IsZero() {
				t.Fatal("expected non-zero LastModified")
			}
		}
	}

	// use pagination to loop over objects one-by-one
	marker := ""
	expectedOrder := []string{"a/", "a/a/a", "a/b", "ab", "b", "c/a", "d", "y/", "y/y/y/y"}
	hasMore := true
	for i := 0; hasMore; i++ {
		result, err := core.ListObjectsV2("bucket", "", "", marker, "", 1)
		if err != nil {
			t.Fatal(err)
		} else if len(result.Contents) != 1 {
			t.Fatalf("unexpected number of objects, %d != 1", len(result.Contents))
		} else if result.Contents[0].Key != expectedOrder[i] {
			t.Errorf("unexpected object, %s != %s", result.Contents[0].Key, expectedOrder[i])
		}
		marker = result.NextContinuationToken
		hasMore = result.IsTruncated
	}
}

func TestS3MultipartUploads(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	cluster := newTestCluster(t, testClusterOptions{
		hosts:         test.RedundancySettings.TotalShards,
		uploadPacking: true,
	})
	defer cluster.Shutdown()
	s3 := cluster.S3
	core := cluster.S3Core
	tt := cluster.tt

	// Create bucket.
	tt.OK(s3.MakeBucket(context.Background(), "multipart", minio.MakeBucketOptions{}))

	// Start a new multipart upload.
	uploadID, err := core.NewMultipartUpload(context.Background(), "multipart", "foo", minio.PutObjectOptions{})
	tt.OK(err)
	if uploadID == "" {
		t.Fatal("expected non-empty upload ID")
	}

	// Start another one in the default bucket. This should not show up when
	// listing the uploads in the 'multipart' bucket.
	tt.OKAll(core.NewMultipartUpload(context.Background(), api.DefaultBucketName, "foo", minio.PutObjectOptions{}))

	// List uploads
	lmu, err := core.ListMultipartUploads(context.Background(), "multipart", "", "", "", "", 0)
	tt.OK(err)
	if len(lmu.Uploads) != 1 {
		t.Fatal("expected 1 upload", len(lmu.Uploads))
	} else if upload := lmu.Uploads[0]; upload.UploadID != uploadID || upload.Key != "foo" {
		t.Fatal("unexpected upload:", upload.UploadID, upload.Key)
	}

	// delete default bucket for the remainder of the test. This makes sure we
	// can delete the bucket even though it contains a multipart upload.
	tt.OK(cluster.Bus.DeleteBucket(context.Background(), api.DefaultBucketName))

	// Add 3 parts out of order to make sure the object is reconstructed
	// correctly.
	putPart := func(partNum int, data []byte) string {
		t.Helper()
		part, err := core.PutObjectPart(context.Background(), "multipart", "foo", uploadID, partNum, bytes.NewReader(data), int64(len(data)), minio.PutObjectPartOptions{})
		tt.OK(err)
		if part.ETag == "" {
			t.Fatal("expected non-empty ETag")
		}
		return part.ETag
	}
	etag2 := putPart(2, []byte("world"))
	etag1 := putPart(1, []byte("hello"))
	etag3 := putPart(3, []byte("!"))

	// List parts
	lop, err := core.ListObjectParts(context.Background(), "multipart", "foo", uploadID, 0, 0)
	tt.OK(err)
	if lop.Bucket != "multipart" || lop.Key != "foo" || lop.UploadID != uploadID || len(lop.ObjectParts) != 3 {
		t.Fatal("unexpected response:", lop)
	} else if part1 := lop.ObjectParts[0]; part1.PartNumber != 1 || part1.Size != 5 || part1.ETag == "" {
		t.Fatal("unexpected part:", part1)
	} else if part2 := lop.ObjectParts[1]; part2.PartNumber != 2 || part2.Size != 5 || part2.ETag == "" {
		t.Fatal("unexpected part:", part2)
	} else if part3 := lop.ObjectParts[2]; part3.PartNumber != 3 || part3.Size != 1 || part3.ETag == "" {
		t.Fatal("unexpected part:", part3)
	}

	// Complete upload
	ui, err := core.CompleteMultipartUpload(context.Background(), "multipart", "foo", uploadID, []minio.CompletePart{
		{
			PartNumber: 1,
			ETag:       etag1,
		},
		{
			PartNumber: 2,
			ETag:       etag2,
		},
		{
			PartNumber: 3,
			ETag:       etag3,
		},
	}, minio.PutObjectOptions{})
	tt.OK(err)
	if ui.Bucket != "multipart" || ui.Key != "foo" || ui.ETag == "" {
		t.Fatal("unexpected response:", ui)
	}

	// Download object
	expectedData := []byte("helloworld!")
	downloadedObj, err := s3.GetObject(context.Background(), "multipart", "foo", minio.GetObjectOptions{})
	tt.OK(err)
	if data, err := io.ReadAll(downloadedObj); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(data, expectedData) {
		t.Fatal("unexpected data:", string(data))
	} else if info, err := downloadedObj.Stat(); err != nil {
		t.Fatal(err)
	} else if info.ETag != ui.ETag {
		t.Fatal("unexpected ETag:", info.ETag)
	} else if info.Size != int64(len(expectedData)) {
		t.Fatal("unexpected size:", info.Size)
	}

	// Stat object
	if info, err := s3.StatObject(context.Background(), "multipart", "foo", minio.StatObjectOptions{}); err != nil {
		t.Fatal(err)
	} else if info.ETag != ui.ETag {
		t.Fatal("unexpected ETag:", info.ETag)
	} else if info.Size != int64(len(expectedData)) {
		t.Fatal("unexpected size:", info.Size)
	}

	// Download again with range request.
	b := make([]byte, 5)
	downloadedObj, err = s3.GetObject(context.Background(), "multipart", "foo", minio.GetObjectOptions{})
	tt.OK(err)
	if _, err = downloadedObj.ReadAt(b, 5); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(b, []byte("world")) {
		t.Fatal("unexpected data:", string(b))
	}

	// Start a second multipart upload.
	uploadID, err = core.NewMultipartUpload(context.Background(), "multipart", "bar", minio.PutObjectOptions{})
	tt.OK(err)

	// Add a part.
	putPart(1, []byte("bar"))

	// Abort upload
	tt.OK(core.AbortMultipartUpload(context.Background(), "multipart", "bar", uploadID))

	// List it.
	res, err := core.ListMultipartUploads(context.Background(), "multipart", "", "", "", "", 0)
	tt.OK(err)
	if len(res.Uploads) != 0 {
		t.Fatal("expected 0 uploads")
	}
}

// TestS3MultipartPruneSlabs is a regression test for an edge case where a
// packed slab is referenced by both a regular upload as well as a part of a
// multipart upload. Deleting the regularly uploaded object by e.g. overwriting
// it, the following call to 'pruneSlabs' would fail. That's because it didn't
// account for references of multipart uploads when deleting slabs.
func TestS3MultipartPruneSlabs(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	cluster := newTestCluster(t, testClusterOptions{
		hosts:         test.RedundancySettings.TotalShards,
		uploadPacking: true,
	})
	defer cluster.Shutdown()

	s3 := cluster.S3
	core := cluster.S3Core
	bucket := "multipart"
	tt := cluster.tt

	// delete default bucket before testing.
	tt.OK(cluster.Bus.DeleteBucket(context.Background(), api.DefaultBucketName))

	// Create bucket.
	tt.OK(s3.MakeBucket(context.Background(), bucket, minio.MakeBucketOptions{}))

	// Start a new multipart upload.
	uploadID, err := core.NewMultipartUpload(context.Background(), bucket, "foo", minio.PutObjectOptions{})
	tt.OK(err)
	if uploadID == "" {
		t.Fatal("expected non-empty upload ID")
	}

	// Add 1 part to the upload.
	data := frand.Bytes(5)
	tt.OKAll(core.PutObjectPart(context.Background(), bucket, "foo", uploadID, 1, bytes.NewReader(data), int64(len(data)), minio.PutObjectPartOptions{}))

	// Upload 1 regular object. It will share the same packed slab, cause the
	// packed slab to be complete and start a new one.
	data = frand.Bytes(test.RedundancySettings.MinShards*rhpv2.SectorSize - 1)
	tt.OKAll(s3.PutObject(context.Background(), bucket, "bar", bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{}))

	// Block until the buffer is uploaded.
	tt.Retry(100, 100*time.Millisecond, func() error {
		buffers, err := cluster.Bus.SlabBuffers()
		tt.OK(err)
		if len(buffers) != 1 {
			return fmt.Errorf("expected 1 slab buffer, got %d", len(buffers))
		}
		return nil
	})

	// Upload another object that overwrites the first one, triggering a call to
	// 'pruneSlabs'.
	data = frand.Bytes(5)
	tt.OKAll(s3.PutObject(context.Background(), bucket, "bar", bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{}))
}

func TestS3SpecialChars(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	cluster := newTestCluster(t, testClusterOptions{
		hosts:         test.RedundancySettings.TotalShards,
		uploadPacking: true,
	})
	defer cluster.Shutdown()
	s3 := cluster.S3
	tt := cluster.tt

	// manually create the 'a/' object as a directory. It should also be
	// possible to call StatObject on it without errors.
	objectKey := "foo/höst (1).log"
	tt.OKAll(s3.PutObject(context.Background(), api.DefaultBucketName, objectKey, bytes.NewReader([]byte("bar")), 0, minio.PutObjectOptions{}))
	so, err := s3.StatObject(context.Background(), api.DefaultBucketName, objectKey, minio.StatObjectOptions{})
	tt.OK(err)
	if so.Key != objectKey {
		t.Fatal("unexpected key:", so.Key)
	}
	for res := range s3.ListObjects(context.Background(), api.DefaultBucketName, minio.ListObjectsOptions{Prefix: "foo/"}) {
		tt.OK(res.Err)
		if res.Key != objectKey {
			t.Fatal("unexpected key:", res.Key)
		}
	}

	// delete it and verify its gone.
	tt.OK(s3.RemoveObject(context.Background(), api.DefaultBucketName, objectKey, minio.RemoveObjectOptions{}))
	so, err = s3.StatObject(context.Background(), api.DefaultBucketName, objectKey, minio.StatObjectOptions{})
	if err == nil {
		t.Fatal("shouldn't exist", err)
	}
	for res := range s3.ListObjects(context.Background(), api.DefaultBucketName, minio.ListObjectsOptions{Prefix: "foo/"}) {
		tt.OK(res.Err)
		if res.Key == objectKey {
			t.Fatal("unexpected key:", res.Key)
		}
	}
}

func TestS3SettingsValidate(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	cluster := newTestCluster(t, clusterOptsDefault)
	defer cluster.Shutdown()

	tests := []struct {
		id         string
		key        string
		shouldFail bool
	}{
		{

			id:         strings.Repeat("a", api.S3MinAccessKeyLen),
			key:        strings.Repeat("a", api.S3SecretKeyLen),
			shouldFail: false,
		},
		{
			id:         strings.Repeat("a", api.S3MaxAccessKeyLen),
			key:        strings.Repeat("a", api.S3SecretKeyLen),
			shouldFail: false,
		},
		{
			id:         strings.Repeat("a", api.S3MinAccessKeyLen-1),
			key:        strings.Repeat("a", api.S3SecretKeyLen),
			shouldFail: true,
		},
		{
			id:         strings.Repeat("a", api.S3MaxAccessKeyLen+1),
			key:        strings.Repeat("a", api.S3SecretKeyLen),
			shouldFail: true,
		},
		{
			id:         "",
			key:        strings.Repeat("a", api.S3SecretKeyLen),
			shouldFail: true,
		},
		{
			id:         strings.Repeat("a", api.S3MinAccessKeyLen),
			key:        "",
			shouldFail: true,
		},
		{
			id:         strings.Repeat("a", api.S3MinAccessKeyLen),
			key:        strings.Repeat("a", api.S3SecretKeyLen+1),
			shouldFail: true,
		},
	}
	for i, test := range tests {
		err := cluster.Bus.UpdateSetting(context.Background(), api.SettingS3Authentication, api.S3AuthenticationSettings{
			V4Keypairs: map[string]string{
				test.id: test.key,
			},
		})
		if err != nil && !test.shouldFail {
			t.Errorf("%d: unexpected error: %v", i, err)
		} else if err == nil && test.shouldFail {
			t.Errorf("%d: expected error", i)
		}
	}
}
