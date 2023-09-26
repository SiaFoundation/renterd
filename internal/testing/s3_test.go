package testing

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/SiaFoundation/gofakes3"
	"github.com/google/go-cmp/cmp"
	"github.com/minio/minio-go/v7"
	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/s3"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

var (
	errBucketNotExists = errors.New("specified bucket does not exist")
	errBucketNotFound  = errors.New("bucket not found")
)

func TestS3Basic(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	start := time.Now()
	cluster, err := newTestCluster(t.TempDir(), newTestLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := cluster.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	}()

	// delete default bucket before testing.
	s3 := cluster.S3
	if err := cluster.Bus.DeleteBucket(context.Background(), api.DefaultBucketName); err != nil {
		t.Fatal(err)
	}

	// add hosts
	if _, err := cluster.AddHostsBlocking(testRedundancySettings.TotalShards); err != nil {
		t.Fatal(err)
	}

	// create bucket
	bucket := "bucket"
	err = s3.MakeBucket(context.Background(), bucket, minio.MakeBucketOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// list buckets
	buckets, err := s3.ListBuckets(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if len(buckets) != 1 {
		t.Fatalf("unexpected number of buckets, %d != 1", len(buckets))
	} else if buckets[0].Name != bucket {
		t.Fatalf("unexpected bucket name, %s != %s", buckets[0].Name, bucket)
	} else if buckets[0].CreationDate.IsZero() {
		t.Fatal("expected non-zero creation date")
	}

	// exist buckets
	exists, err := s3.BucketExists(context.Background(), bucket)
	if err != nil {
		t.Fatal(err)
	} else if !exists {
		t.Fatal("expected bucket to exist")
	}
	exists, err = s3.BucketExists(context.Background(), bucket+"nonexistent")
	if err != nil {
		t.Fatal(err)
	} else if exists {
		t.Fatal("expected bucket to not exist")
	}

	// add object to the bucket
	data := frand.Bytes(10)
	_, err = s3.PutObject(context.Background(), bucket, "object", bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}

	_, err = s3.PutObject(context.Background(), bucket+"nonexistent", "object", bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{})
	if err == nil ||
		!(strings.Contains(err.Error(), errBucketNotExists.Error()) ||
			strings.Contains(err.Error(), errBucketNotFound.Error())) {
		t.Fatal(err)
	}

	// get object
	obj, err := s3.GetObject(context.Background(), bucket, "object", minio.GetObjectOptions{})
	if err != nil {
		t.Fatal(err)
	} else if b, err := io.ReadAll(obj); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(b, data) {
		t.Fatal("data mismatch")
	}

	// stat object
	info, err := s3.StatObject(context.Background(), bucket, "object", minio.StatObjectOptions{})
	if err != nil {
		t.Fatal(err)
	} else if info.Size != int64(len(data)) {
		t.Fatal("size mismatch")
	}

	// add another bucket
	err = s3.MakeBucket(context.Background(), bucket+"2", minio.MakeBucketOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// copy our object into the new bucket.
	res, err := s3.CopyObject(context.Background(), minio.CopyDestOptions{
		Bucket: bucket + "2",
		Object: "object",
	}, minio.CopySrcOptions{
		Bucket: bucket,
		Object: "object",
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.LastModified.IsZero() {
		t.Fatal("expected LastModified to be non-zero")
	} else if !res.LastModified.After(start.UTC()) {
		t.Fatal("expected LastModified to be after the start of our test")
	}

	// get copied object
	obj, err = s3.GetObject(context.Background(), bucket+"2", "object", minio.GetObjectOptions{})
	if err != nil {
		t.Fatal(err)
	} else if b, err := io.ReadAll(obj); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(b, data) {
		t.Fatal("data mismatch")
	}

	// assert deleting the bucket fails because it's not empty
	err = s3.RemoveBucket(context.Background(), bucket)
	if err == nil || !strings.Contains(err.Error(), gofakes3.ErrBucketNotEmpty.Error()) {
		t.Fatal(err)
	}

	// assert deleting the bucket fails because it doesn't exist
	err = s3.RemoveBucket(context.Background(), bucket+"nonexistent")
	if err == nil || !strings.Contains(err.Error(), errBucketNotExists.Error()) {
		t.Fatal(err)
	}

	// remove the object
	err = s3.RemoveObject(context.Background(), bucket, "object", minio.RemoveObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// try to get object
	obj, err = s3.GetObject(context.Background(), bucket, "object", minio.GetObjectOptions{})
	if err != nil {
		t.Fatal(err)
	} else if _, err := io.ReadAll(obj); err == nil || !strings.Contains(err.Error(), "The specified key does not exist") {
		t.Fatal(err)
	}

	// add a few objects to the bucket.
	_, err = s3.PutObject(context.Background(), bucket, "dir/", bytes.NewReader(frand.Bytes(10)), 10, minio.PutObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s3.PutObject(context.Background(), bucket, "dir/file", bytes.NewReader(frand.Bytes(10)), 10, minio.PutObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// delete them using the multi delete endpoint.
	objectsCh := make(chan minio.ObjectInfo, 3)
	objectsCh <- minio.ObjectInfo{Key: "dir/file"}
	objectsCh <- minio.ObjectInfo{Key: "dir/"}
	close(objectsCh)
	results := s3.RemoveObjects(context.Background(), bucket, objectsCh, minio.RemoveObjectsOptions{})
	for res := range results {
		if res.Err != nil {
			t.Fatal(res.Err)
		}
	}

	// delete bucket
	err = s3.RemoveBucket(context.Background(), bucket)
	if err != nil {
		t.Fatal(err)
	}
	exists, err = s3.BucketExists(context.Background(), bucket)
	if err != nil {
		t.Fatal(err)
	} else if exists {
		t.Fatal("expected bucket to not exist")
	}
}

func TestS3Authentication(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	cluster, err := newTestCluster(t.TempDir(), newTestLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := cluster.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	}()

	// Create a new S3 server that connects to the cluster.
	s3Listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s3Handler, err := s3.New(cluster.Bus, cluster.Worker, zap.NewNop().Sugar(), s3.Opts{
		AuthKeyPairs: testS3AuthPairs,
	})
	if err != nil {
		t.Fatal(err)
	}
	s3Server := http.Server{
		Handler: s3Handler,
	}
	go s3Server.Serve(s3Listener)

	// Create client.
	s3Client, err := minio.New(s3Listener.Addr().String(), &minio.Options{
		Creds: nil, // no authentication
	})
	if err != nil {
		t.Fatal(err)
	}

	// List buckets. Shouldn't work.
	if _, err := s3Client.ListBuckets(context.Background()); err == nil || !strings.Contains(err.Error(), "unsupported algorithm") {
		t.Fatal(err)
	}

	// Create client with credentials and try again..
	s3Client, err = minio.New(s3Listener.Addr().String(), &minio.Options{
		Creds: testS3Credentials,
	})
	if err != nil {
		t.Fatal(err)
	}

	// List buckets. Should work.
	if _, err := s3Client.ListBuckets(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestS3List(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	cluster, err := newTestCluster(t.TempDir(), newTestLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := cluster.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	}()
	s3 := cluster.S3
	core := cluster.S3Core

	// enable upload packing to speed up test
	err = cluster.Bus.UpdateSetting(context.Background(), api.SettingUploadPacking, api.UploadPackingSettings{
		Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	// add hosts
	if _, err := cluster.AddHostsBlocking(testRedundancySettings.TotalShards); err != nil {
		t.Fatal(err)
	}

	// create bucket
	err = s3.MakeBucket(context.Background(), "bucket", minio.MakeBucketOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// manually create the 'a/' object as a directory. It should also be
	// possible to call StatObject on it without errors.
	_, err = s3.PutObject(context.Background(), "bucket", "a/", bytes.NewReader(nil), 0, minio.PutObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	so, err := s3.StatObject(context.Background(), "bucket", "a/", minio.StatObjectOptions{})
	if err != nil {
		t.Fatal(err)
	} else if so.Key != "a/" {
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
		_, err = s3.PutObject(context.Background(), "bucket", object, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{})
		if err != nil {
			t.Fatal(err)
		}
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
	}
}

func TestS3MultipartUploads(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	cluster, err := newTestCluster(t.TempDir(), newTestLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := cluster.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	}()
	s3 := cluster.S3
	core := cluster.S3Core

	// delete default bucket before testing.
	if err := cluster.Bus.DeleteBucket(context.Background(), api.DefaultBucketName); err != nil {
		t.Fatal(err)
	}

	// Enable upload packing to speed up test.
	err = cluster.Bus.UpdateSetting(context.Background(), api.SettingUploadPacking, api.UploadPackingSettings{
		Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	// add hosts
	if _, err := cluster.AddHostsBlocking(testRedundancySettings.TotalShards); err != nil {
		t.Fatal(err)
	}

	// Create bucket.
	err = s3.MakeBucket(context.Background(), "multipart", minio.MakeBucketOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// Start a new multipart upload.
	uploadID, err := core.NewMultipartUpload(context.Background(), "multipart", "foo", minio.PutObjectOptions{})
	if err != nil {
		t.Fatal(err)
	} else if uploadID == "" {
		t.Fatal("expected non-empty upload ID")
	}

	// List uploads
	lmu, err := core.ListMultipartUploads(context.Background(), "multipart", "", "", "", "", 0)
	if err != nil {
		t.Fatal(err)
	} else if len(lmu.Uploads) != 1 {
		t.Fatal("expected 1 upload")
	} else if upload := lmu.Uploads[0]; upload.UploadID != uploadID || upload.Key != "foo" {
		t.Fatal("unexpected upload:", upload.UploadID, upload.Key)
	}

	// Add 3 parts out of order to make sure the object is reconstructed
	// correctly.
	putPart := func(partNum int, data []byte) string {
		t.Helper()
		part, err := core.PutObjectPart(context.Background(), "multipart", "foo", uploadID, partNum, bytes.NewReader(data), int64(len(data)), minio.PutObjectPartOptions{})
		if err != nil {
			t.Fatal(err)
		} else if part.ETag == "" {
			t.Fatal("expected non-empty ETag")
		}
		return part.ETag
	}
	etag2 := putPart(2, []byte("world"))
	etag1 := putPart(1, []byte("hello"))
	etag3 := putPart(3, []byte("!"))

	// List parts
	lop, err := core.ListObjectParts(context.Background(), "multipart", "foo", uploadID, 0, 0)
	if err != nil {
		t.Fatal(err)
	} else if lop.Bucket != "multipart" || lop.Key != "foo" || lop.UploadID != uploadID || len(lop.ObjectParts) != 3 {
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
	if err != nil {
		t.Fatal(err)
	} else if ui.Bucket != "multipart" || ui.Key != "foo" || ui.ETag == "" {
		t.Fatal("unexpected response:", ui)
	}

	// Download object
	downloadedObj, err := s3.GetObject(context.Background(), "multipart", "foo", minio.GetObjectOptions{})
	if err != nil {
		t.Fatal(err)
	} else if data, err := io.ReadAll(downloadedObj); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(data, []byte("helloworld!")) {
		t.Fatal("unexpected data:", string(data))
	}

	// Start a second multipart upload.
	uploadID, err = core.NewMultipartUpload(context.Background(), "multipart", "bar", minio.PutObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// Add a part.
	putPart(1, []byte("bar"))

	// Abort upload
	err = core.AbortMultipartUpload(context.Background(), "multipart", "bar", uploadID)
	if err != nil {
		t.Fatal(err)
	}

	// List it.
	res, err := core.ListMultipartUploads(context.Background(), "multipart", "", "", "", "", 0)
	if err != nil {
		t.Fatal(err)
	} else if len(res.Uploads) != 0 {
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

	cluster, err := newTestCluster(t.TempDir(), newTestLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := cluster.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	}()
	s3 := cluster.S3
	core := cluster.S3Core
	bucket := "multipart"

	// delete default bucket before testing.
	if err := cluster.Bus.DeleteBucket(context.Background(), api.DefaultBucketName); err != nil {
		t.Fatal(err)
	}

	// This test requires upload packing.
	err = cluster.Bus.UpdateSetting(context.Background(), api.SettingUploadPacking, api.UploadPackingSettings{
		Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	// add hosts
	if _, err := cluster.AddHostsBlocking(testRedundancySettings.TotalShards); err != nil {
		t.Fatal(err)
	}

	// Create bucket.
	err = s3.MakeBucket(context.Background(), bucket, minio.MakeBucketOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// Start a new multipart upload.
	uploadID, err := core.NewMultipartUpload(context.Background(), bucket, "foo", minio.PutObjectOptions{})
	if err != nil {
		t.Fatal(err)
	} else if uploadID == "" {
		t.Fatal("expected non-empty upload ID")
	}

	// Add 1 part to the upload.
	data := frand.Bytes(5)
	_, err = core.PutObjectPart(context.Background(), bucket, "foo", uploadID, 1, bytes.NewReader(data), int64(len(data)), minio.PutObjectPartOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// Upload 1 regular object. It will share the same packed slab, cause the
	// packed slab to be complete and start a new one.
	data = frand.Bytes(testRedundancySettings.MinShards*rhpv2.SectorSize - 1)
	_, err = s3.PutObject(context.Background(), bucket, "bar", bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// Block until the buffer is uploaded.
	err = Retry(100, 100*time.Millisecond, func() error {
		buffers, err := cluster.Bus.SlabBuffers()
		if err != nil {
			t.Fatal(err)
		} else if len(buffers) != 1 {
			return fmt.Errorf("expected 1 slab buffer, got %d", len(buffers))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Upload another object that overwrites the first one, triggering a call to
	// 'pruneSlabs'.
	data = frand.Bytes(5)
	_, err = s3.PutObject(context.Background(), bucket, "bar", bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
}

func TestS3SpecialChars(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	cluster, err := newTestCluster(t.TempDir(), newTestLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := cluster.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	}()
	s3 := cluster.S3

	// enable upload packing to speed up test
	err = cluster.Bus.UpdateSetting(context.Background(), api.SettingUploadPacking, api.UploadPackingSettings{
		Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	// add hosts
	if _, err := cluster.AddHostsBlocking(testRedundancySettings.TotalShards); err != nil {
		t.Fatal(err)
	}

	// manually create the 'a/' object as a directory. It should also be
	// possible to call StatObject on it without errors.
	objectKey := "foo/höst (1).log"
	_, err = s3.PutObject(context.Background(), api.DefaultBucketName, objectKey, bytes.NewReader([]byte("bar")), 0, minio.PutObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	so, err := s3.StatObject(context.Background(), api.DefaultBucketName, objectKey, minio.StatObjectOptions{})
	if err != nil {
		t.Fatal(err)
	} else if so.Key != objectKey {
		t.Fatal("unexpected key:", so.Key)
	}
	for res := range s3.ListObjects(context.Background(), api.DefaultBucketName, minio.ListObjectsOptions{Prefix: "foo/"}) {
		if res.Err != nil {
			t.Fatal(err)
		}
		if res.Key != objectKey {
			t.Fatal("unexpected key:", res.Key)
		}
	}

	// delete it and verify its gone.
	err = s3.RemoveObject(context.Background(), api.DefaultBucketName, objectKey, minio.RemoveObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	so, err = s3.StatObject(context.Background(), api.DefaultBucketName, objectKey, minio.StatObjectOptions{})
	if err == nil {
		t.Fatal("shouldn't exist", err)
	}
	for res := range s3.ListObjects(context.Background(), api.DefaultBucketName, minio.ListObjectsOptions{Prefix: "foo/"}) {
		if res.Err != nil {
			t.Fatal(err)
		}
		if res.Key == objectKey {
			t.Fatal("unexpected key:", res.Key)
		}
	}
}
