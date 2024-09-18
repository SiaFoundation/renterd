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

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	s3aws "github.com/aws/aws-sdk-go/service/s3"
	"github.com/google/go-cmp/cmp"
	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/gofakes3"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/test"
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
	tt := cluster.tt
	if err := cluster.Bus.DeleteBucket(context.Background(), testBucket); err != nil {
		t.Fatal(err)
	}

	// create bucket
	bucket := "bucket"
	objPath := "obj#ct" // special char to check escaping
	tt.OKAll(cluster.S3.CreateBucket(bucket))

	// list buckets
	lbo, err := cluster.S3.ListBuckets()
	tt.OK(err)
	if buckets := lbo.buckets; len(buckets) != 1 {
		t.Fatalf("unexpected number of buckets, %d != 1", len(buckets))
	} else if buckets[0].name != bucket {
		t.Fatalf("unexpected bucket name, %s != %s", buckets[0].name, bucket)
	} else if buckets[0].creationDate.IsZero() {
		t.Fatal("expected non-zero creation date")
	}

	// exist buckets
	err = cluster.S3.HeadBucket(bucket)
	tt.OK(err)
	err = cluster.S3.HeadBucket("nonexistent")
	tt.AssertContains(err, "NotFound")

	// add object to the bucket
	data := frand.Bytes(10)
	etag := md5.Sum(data)
	uploadInfo, err := cluster.S3.PutObject(bucket, objPath, bytes.NewReader(data), putObjectOptions{})
	tt.OK(err)
	if uploadInfo.etag != api.FormatETag(hex.EncodeToString(etag[:])) {
		t.Fatalf("expected ETag %v, got %v", hex.EncodeToString(etag[:]), uploadInfo.etag)
	}
	busObject, err := cluster.Bus.Object(context.Background(), bucket, objPath, api.GetObjectOptions{})
	tt.OK(err)
	if busObject.Object == nil {
		t.Fatal("expected object to exist")
	} else if api.FormatETag(busObject.ETag) != *&uploadInfo.etag {
		t.Fatalf("expected ETag %v, got %v", uploadInfo.etag, busObject.ETag)
	} else if api.FormatETag(busObject.ETag) != uploadInfo.etag {
		t.Fatalf("expected ETag %v, got %v", uploadInfo.etag, busObject.ETag)
	}

	_, err = cluster.S3.PutObject("nonexistent", objPath, bytes.NewReader(data), putObjectOptions{})
	tt.AssertIs(err, errBucketNotExists)

	// get object
	obj, err := cluster.S3.GetObject(bucket, objPath, getObjectOptions{})
	tt.OK(err)
	if b, err := io.ReadAll(obj.body); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(b, data) {
		t.Fatal("data mismatch")
	} else if obj.etag != uploadInfo.etag {
		t.Fatal("unexpected ETag:", obj.etag, uploadInfo.etag)
	}

	// stat object
	info, err := cluster.S3.HeadObject(bucket, objPath)
	tt.OK(err)
	if info.contentLength != int64(len(data)) {
		t.Fatal("size mismatch")
	} else if info.etag != uploadInfo.etag {
		t.Fatal("unexpected ETag:", info.etag)
	}

	// stat object that doesn't exist
	info, err = cluster.S3.HeadObject("nonexistent", objPath)
	tt.AssertContains(err, "NotFound")

	// add another bucket
	bucket2 := "bucket2"
	tt.OKAll(cluster.S3.CreateBucket(bucket2))

	// copy our object into the new bucket.
	res, err := cluster.S3.CopyObject(bucket, bucket2, objPath, objPath, putObjectOptions{})
	tt.OK(err)
	if res.lastModified.IsZero() {
		t.Fatal("expected LastModified to be non-zero")
	} else if !res.lastModified.After(start.UTC()) {
		t.Fatal("expected LastModified to be after the start of our test")
	} else if res.etag != uploadInfo.etag {
		t.Fatal("expected correct ETag to be set")
	}

	// get copied object
	obj, err = cluster.S3.GetObject(bucket2, objPath, getObjectOptions{})
	tt.OK(err)
	if b, err := io.ReadAll(obj.body); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(b, data) {
		t.Fatal("data mismatch")
	}

	// assert deleting the bucket fails because it's not empty
	err = cluster.S3.DeleteBucket(bucket)
	tt.AssertIs(err, gofakes3.ErrBucketNotEmpty)

	// assert deleting the bucket fails because it doesn't exist
	err = cluster.S3.DeleteBucket("nonexistent")
	tt.AssertIs(err, errBucketNotExists)

	// remove the object
	tt.OKAll(cluster.S3.DeleteObject(bucket, objPath))

	// try to get object
	obj, err = cluster.S3.GetObject(bucket, objPath, getObjectOptions{})
	tt.AssertContains(err, "NoSuchKey")

	// add a few objects to the bucket.
	tmpObj1 := "dir/"
	body := frand.Bytes(10)
	tt.OKAll(cluster.S3.PutObject(bucket, tmpObj1, bytes.NewReader(body), putObjectOptions{}))
	tmpObj2 := "dir/file"
	tt.OKAll(cluster.S3.PutObject(bucket, tmpObj2, bytes.NewReader(body), putObjectOptions{}))

	// delete them using the multi delete endpoint.
	tt.OKAll(cluster.S3.DeleteObject(bucket, tmpObj1))
	tt.OKAll(cluster.S3.DeleteObject(bucket, tmpObj2))

	// delete bucket
	err = cluster.S3.DeleteBucket(bucket)
	tt.OK(err)
	err = cluster.S3.HeadBucket(bucket)
	tt.AssertContains(err, "NotFound")
}

func TestS3ObjectMetadata(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	// create cluster
	opts := testClusterOptions{
		hosts: test.RedundancySettings.TotalShards,
	}
	cluster := newTestCluster(t, opts)
	defer cluster.Shutdown()

	// convenience variables
	tt := cluster.tt

	// create dummy metadata
	metadata := map[string]string{
		"Foo": "bar",
		"Baz": "qux",
	}

	// add object to the bucket
	_, err := cluster.S3.PutObject(testBucket, t.Name(), bytes.NewReader([]byte(t.Name())), putObjectOptions{metadata: metadata})
	tt.OK(err)

	// create helper to assert metadata is present
	assertMetadata := func(want map[string]string, got map[string]string) {
		t.Helper()
		for k, wantt := range want {
			if gott, ok := got[k]; !ok || gott != wantt {
				t.Fatalf("unexpected metadata, '%v' != '%v' (found: %v)", gott, wantt, ok)
			}
		}
	}

	// perform GET request
	obj, err := cluster.S3.GetObject(testBucket, t.Name(), getObjectOptions{})
	tt.OK(err)
	assertMetadata(metadata, obj.metadata)

	// assert metadata is set on HEAD request
	get, err := cluster.S3.HeadObject(testBucket, t.Name())
	tt.OK(err)
	assertMetadata(metadata, get.metadata)

	// perform HEAD request
	head, err := cluster.S3.HeadObject(testBucket, t.Name())
	tt.OK(err)
	assertMetadata(metadata, head.metadata)

	// perform metadata update (same src/dst copy)
	metadata["Baz"] = "updated"
	_, err = cluster.S3.CopyObject(
		testBucket,
		testBucket,
		t.Name(),
		t.Name(),
		putObjectOptions{metadata: metadata},
	)
	tt.OK(err)

	// perform HEAD request
	head, err = cluster.S3.HeadObject(testBucket, t.Name())
	tt.OK(err)
	assertMetadata(metadata, head.metadata)

	// perform copy
	metadata["Baz"] = "copied"
	_, err = cluster.S3.CopyObject(
		testBucket,
		testBucket,
		t.Name(),
		t.Name()+"copied",
		putObjectOptions{
			metadata: metadata,
		},
	)
	tt.OK(err)

	// perform HEAD request
	head, err = cluster.S3.HeadObject(testBucket, t.Name()+"copied")
	tt.OK(err)
	assertMetadata(metadata, metadata)

	// assert the original object's metadata is unchanged
	metadata["Baz"] = "updated"
	head, err = cluster.S3.HeadObject(testBucket, t.Name())
	tt.OK(err)
	assertMetadata(metadata, metadata)

	// upload a file using multipart upload
	uid, err := cluster.S3.NewMultipartUpload(testBucket, "multi", putObjectOptions{
		metadata: map[string]string{
			"New": "1",
		},
	})
	tt.OK(err)
	data := frand.Bytes(3)

	part, err := cluster.S3.PutObjectPart(testBucket, "foo", uid, 1, bytes.NewReader(data), putObjectPartOptions{})
	tt.OK(err)
	_, err = cluster.S3.CompleteMultipartUpload(testBucket, "multi", uid, []completePart{
		{
			partNumber: 1,
			etag:       part.etag,
		},
	}, putObjectOptions{})
	tt.OK(err)

	// check metadata
	head, err = cluster.S3.HeadObject(testBucket, "multi")
	tt.OK(err)
	assertMetadata(map[string]string{
		"New": "1",
	}, head.metadata)
}

func TestS3Authentication(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	cluster := newTestCluster(t, clusterOptsDefault)
	defer cluster.Shutdown()
	tt := cluster.tt

	assertAuth := func(c *s3aws.S3, shouldWork bool) {
		t.Helper()
		bucket := testBucket
		_, err := c.ListObjectsV2(&s3aws.ListObjectsV2Input{
			Bucket: &bucket,
		})
		if shouldWork && err != nil {
			t.Fatal(err)
		} else if !shouldWork && err == nil {
			t.Fatal("expected error")
		} else if !shouldWork && err != nil && !strings.Contains(err.Error(), "AccessDenied") {
			t.Fatal("wrong error", shouldWork, err)
		}
	}

	// Create client that is not authenticated
	cfg := cluster.S3.Config()
	cfg.Credentials = credentials.NewCredentials(&credentials.StaticProvider{
		Value: credentials.Value{
			AccessKeyID:     "unknownkey",
			SecretAccessKey: "somesecret",
		},
	})

	mySession := session.Must(session.NewSession())
	s3Unauthenticated := s3aws.New(mySession, &cfg)

	// List bucket. Shouldn't work.
	assertAuth(s3Unauthenticated, false)

	// Use authenticated client
	s3Authenticated := cluster.S3.s3

	// List buckets. Should work.
	assertAuth(s3Authenticated, true)

	// Update the policy of the bucket to allow public read access.
	tt.OK(cluster.Bus.UpdateBucketPolicy(context.Background(), testBucket, api.BucketPolicy{
		PublicReadAccess: true,
	}))

	// Check that the policy was updated.
	b, err := cluster.Bus.Bucket(context.Background(), testBucket)
	tt.OK(err)
	if b.Policy.PublicReadAccess != true {
		t.Fatal("expected public read access")
	}

	// Listing should work now.
	assertAuth(s3Unauthenticated, true)

	// Update the policy again to disable access.
	tt.OK(cluster.Bus.UpdateBucketPolicy(context.Background(), testBucket, api.BucketPolicy{
		PublicReadAccess: false,
	}))

	// Check that the policy was updated.
	b, err = cluster.Bus.Bucket(context.Background(), testBucket)
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

	tt := cluster.tt

	// create bucket
	tt.OK(cluster.S3.CreateBucket("bucket"))

	// manually create the 'a/' object as a directory. It should also be
	// possible to call StatObject on it without errors.
	tt.OKAll(cluster.S3.PutObject("bucket", "a/", bytes.NewReader(nil), putObjectOptions{}))
	so, err := cluster.S3.HeadObject("bucket", "a/")
	tt.OK(err)
	if so.key != "a/" {
		t.Fatal("unexpected key:", so.key)
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
		tt.OKAll(cluster.S3.PutObject("bucket", object, bytes.NewReader(data), putObjectOptions{}))
	}

	flatten := func(res listObjectsResponse) []string {
		var objs []string
		for _, obj := range res.contents {
			if !strings.HasSuffix(obj.key, "/") && obj.lastModified.IsZero() {
				t.Fatal("expected non-zero LastModified", obj.key)
			}
			objs = append(objs, obj.key)
		}
		for _, cp := range res.commonPrefixes {
			objs = append(objs, cp)
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
		result, err := cluster.S3.ListObjects("bucket", listObjectsOptions{
			prefix:    test.prefix,
			marker:    test.marker,
			delimiter: test.delimiter,
			maxKeys:   1000,
		})
		if err != nil {
			t.Fatal(err)
		}
		got := flatten(result)
		if !cmp.Equal(test.want, got) {
			t.Errorf("test %d: unexpected response, want %v got %v", i, test.want, got)
		}
		for _, obj := range result.contents {
			if obj.etag == "" {
				t.Fatal("expected non-empty ETag")
			} else if obj.lastModified.IsZero() {
				t.Fatal("expected non-zero LastModified")
			}
		}
	}

	// use pagination to loop over objects one-by-one
	marker := ""
	expectedOrder := []string{"a/", "a/a/a", "a/b", "ab", "b", "c/a", "d", "y/", "y/y/y/y"}
	hasMore := true
	for i := 0; hasMore; i++ {
		result, err := cluster.S3.ListObjects("bucket", listObjectsOptions{
			marker:  marker,
			maxKeys: 1,
		})
		if err != nil {
			t.Fatal(err)
		} else if len(result.contents) != 1 {
			t.Fatalf("unexpected number of objects, %d != 1", len(result.contents))
		} else if result.contents[0].key != expectedOrder[i] {
			t.Errorf("unexpected object, %s != %s", result.contents[0].key, expectedOrder[i])
		}
		marker = result.nextMarker
		hasMore = result.truncated
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
	tt := cluster.tt

	// Create bucket.
	tt.OK(cluster.S3.CreateBucket("multipart"))

	// Start a new multipart upload.
	uploadID, err := cluster.S3.NewMultipartUpload("multipart", "foo", putObjectOptions{})
	tt.OK(err)
	if uploadID == "" {
		t.Fatal("expected non-empty upload ID")
	}

	// Start another one in the default bucket. This should not show up when
	// listing the uploads in the 'multipart' bucket.
	tt.OKAll(cluster.S3.NewMultipartUpload(testBucket, "foo", putObjectOptions{}))

	// List uploads
	uploads, err := cluster.S3.ListMultipartUploads("multipart")
	tt.OK(err)
	if len(uploads) != 1 {
		t.Fatal("expected 1 upload", len(uploads))
	} else if upload := uploads[0]; upload.uploadID != uploadID || upload.key != "foo" {
		t.Fatal("unexpected upload:", upload.uploadID, upload.key)
	}

	// delete default bucket for the remainder of the test. This makes sure we
	// can delete the bucket even though it contains a multipart upload.
	tt.OK(cluster.Bus.DeleteBucket(context.Background(), testBucket))

	// Add 3 parts out of order to make sure the object is reconstructed
	// correctly.
	putPart := func(partNum int64, data []byte) string {
		t.Helper()
		part, err := cluster.S3.PutObjectPart("multipart", "foo", uploadID, partNum, bytes.NewReader(data), putObjectPartOptions{})
		tt.OK(err)
		if part.etag == "" {
			t.Fatal("expected non-empty ETag")
		}
		return part.etag
	}
	etag2 := putPart(2, []byte("world"))
	etag1 := putPart(1, []byte("hello"))
	etag3 := putPart(3, []byte("!"))

	// List parts
	lop, err := cluster.S3.ListObjectParts("multipart", "foo", uploadID)
	tt.OK(err)
	if lop.bucket != "multipart" || lop.key != "foo" || lop.uploadId != uploadID || len(lop.objectParts) != 3 {
		t.Fatal("unexpected response:", lop)
	} else if part1 := lop.objectParts[0]; part1.partNumber != 1 || part1.size != 5 || part1.etag == "" {
		t.Fatal("unexpected part:", part1)
	} else if part2 := lop.objectParts[1]; part2.partNumber != 2 || part2.size != 5 || part2.etag == "" {
		t.Fatal("unexpected part:", part2)
	} else if part3 := lop.objectParts[2]; part3.partNumber != 3 || part3.size != 1 || part3.etag == "" {
		t.Fatal("unexpected part:", part3)
	}

	// Complete upload
	ui, err := cluster.S3.CompleteMultipartUpload("multipart", "foo", uploadID, []completePart{
		{
			partNumber: 1,
			etag:       etag1,
		},
		{
			partNumber: 2,
			etag:       etag2,
		},
		{
			partNumber: 3,
			etag:       etag3,
		},
	}, putObjectOptions{})
	tt.OK(err)
	if ui.bucket != "multipart" || ui.key != "foo" || ui.etag == "" {
		t.Fatal("unexpected response:", ui)
	}

	// Download object
	expectedData := []byte("helloworld!")
	downloadedObj, err := cluster.S3.GetObject("multipart", "foo", getObjectOptions{})
	tt.OK(err)
	if data, err := io.ReadAll(downloadedObj.body); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(data, expectedData) {
		t.Fatal("unexpected data:", string(data))
	} else if downloadedObj.etag != ui.etag {
		t.Fatal("unexpected ETag:", downloadedObj.etag)
	}

	// Stat object
	if info, err := cluster.S3.HeadObject("multipart", "foo"); err != nil {
		t.Fatal(err)
	} else if info.etag != ui.etag {
		t.Fatal("unexpected ETag:", info.etag)
	} else if info.contentLength != int64(len(expectedData)) {
		t.Fatal("unexpected size:", info.contentLength)
	}

	// Download again with range request.
	downloadedObj, err = cluster.S3.GetObject("multipart", "foo", getObjectOptions{
		offset: 5,
		length: 5,
	})
	tt.OK(err)
	if data, err := io.ReadAll(downloadedObj.body); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(data, []byte("world")) {
		t.Fatal("unexpected data:", string(data))
	}

	// Start a second multipart upload.
	uploadID, err = cluster.S3.NewMultipartUpload("multipart", "bar", putObjectOptions{})
	tt.OK(err)

	// Add a part.
	putPart(1, []byte("bar"))

	// Abort upload
	tt.OK(cluster.S3.AbortMultipartUpload("multipart", "bar", uploadID))

	// List it.
	uploads, err = cluster.S3.ListMultipartUploads("multipart")
	tt.OK(err)
	if len(uploads) != 0 {
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

	bucket := "multipart"
	tt := cluster.tt

	// delete default bucket before testing.
	tt.OK(cluster.Bus.DeleteBucket(context.Background(), testBucket))

	// Create bucket.
	tt.OK(cluster.S3.CreateBucket(bucket))

	// Start a new multipart upload.
	uploadID, err := cluster.S3.NewMultipartUpload(bucket, "foo", putObjectOptions{})
	tt.OK(err)
	if uploadID == "" {
		t.Fatal("expected non-empty upload ID")
	}

	// Add 1 part to the upload.
	data := frand.Bytes(5)
	tt.OKAll(cluster.S3.PutObjectPart(bucket, "foo", uploadID, 1, bytes.NewReader(data), putObjectPartOptions{}))

	// Upload 1 regular object. It will share the same packed slab, cause the
	// packed slab to be complete and start a new one.
	data = frand.Bytes(test.RedundancySettings.MinShards*rhpv2.SectorSize - 1)
	tt.OKAll(cluster.S3.PutObject(bucket, "bar", bytes.NewReader(data), putObjectOptions{}))

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
	tt.OKAll(cluster.S3.PutObject(bucket, "bar", bytes.NewReader(data), putObjectOptions{}))
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
	tt := cluster.tt

	// manually create the 'a/' object as a directory. It should also be
	// possible to call StatObject on it without errors.
	objectKey := "foo/höst (1).log"
	tt.OKAll(cluster.S3.PutObject(testBucket, objectKey, bytes.NewReader([]byte("bar")), putObjectOptions{}))
	so, err := cluster.S3.HeadObject(testBucket, objectKey)
	tt.OK(err)
	if so.key != objectKey {
		t.Fatal("unexpected key:", so.key)
	}
	lor, err := cluster.S3.ListObjects(testBucket, listObjectsOptions{prefix: "foo/"})
	tt.OK(err)
	for _, res := range lor.contents {
		if res.key != objectKey {
			t.Fatal("unexpected key:", res.key)
		}
	}

	// delete it and verify its gone.
	tt.OK(cluster.S3.DeleteObject(testBucket, objectKey))
	so, err = cluster.S3.HeadObject(testBucket, objectKey)
	if err == nil {
		t.Fatal("shouldn't exist", err)
	}
	lor, err = cluster.S3.ListObjects(testBucket, listObjectsOptions{prefix: "foo/"})
	for _, res := range lor.contents {
		tt.OK(err)
		if res.key == objectKey {
			t.Fatal("unexpected key:", res.key)
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
		err := cluster.Bus.UpdateS3Settings(context.Background(), api.S3Settings{
			Authentication: api.S3AuthenticationSettings{
				V4Keypairs: map[string]string{
					test.id: test.key,
				},
			},
		})
		if err != nil && !test.shouldFail {
			t.Errorf("%d: unexpected error: %v", i, err)
		} else if err == nil && test.shouldFail {
			t.Errorf("%d: expected error", i)
		}
	}
}
