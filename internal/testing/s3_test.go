package testing

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"

	"github.com/SiaFoundation/gofakes3"
	"github.com/google/go-cmp/cmp"
	"github.com/minio/minio-go/v7"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/s3"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

func TestS3(t *testing.T) {
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

	// delete default bucket before testing.
	s3 := cluster.S3
	if err := cluster.Bus.DeleteBucket(context.Background(), api.DefaultBucketName); err != nil {
		t.Fatal(err)
	}

	// add hosts
	if _, err := cluster.AddHostsBlocking(testRedundancySettings.TotalShards); err != nil {
		t.Fatal(err)
	}

	// Create bucket.
	err = s3.MakeBucket(context.Background(), "bucket1", minio.MakeBucketOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// List bucket.
	buckets, err := s3.ListBuckets(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(buckets) != 1 {
		t.Fatal("expected 1 bucket")
	}
	if buckets[0].Name != "bucket1" {
		t.Fatal("expected bucket1", buckets[0].Name)
	} else if buckets[0].CreationDate.IsZero() {
		t.Fatal("expected non-zero creation date")
	}

	// Exists bucket.
	exists, err := s3.BucketExists(context.Background(), "bucket1")
	if err != nil {
		t.Fatal(err)
	} else if !exists {
		t.Fatal("expected bucket1 to exist")
	}
	exists, err = s3.BucketExists(context.Background(), "bucket2")
	if err != nil {
		t.Fatal(err)
	} else if exists {
		t.Fatal("expected bucket2 to not exist")
	}

	// Create bucket 2.
	err = s3.MakeBucket(context.Background(), "bucket2", minio.MakeBucketOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// PutObject into bucket.
	data := frand.Bytes(10)
	_, err = s3.PutObject(context.Background(), "bucket1", "object1", bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// Copy object into other bucket.
	_, err = s3.CopyObject(context.Background(), minio.CopyDestOptions{
		Bucket: "bucket2",
		Object: "object1",
	}, minio.CopySrcOptions{
		Bucket: "bucket1",
		Object: "object1",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Get object.
	obj, err := s3.GetObject(context.Background(), "bucket1", "object1", minio.GetObjectOptions{})
	if err != nil {
		t.Fatal(err)
	} else if b, err := io.ReadAll(obj); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(b, data) {
		t.Fatal("data mismatch")
	}

	// Get copied object.
	obj, err = s3.GetObject(context.Background(), "bucket2", "object1", minio.GetObjectOptions{})
	if err != nil {
		t.Fatal(err)
	} else if b, err := io.ReadAll(obj); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(b, data) {
		t.Fatal("data mismatch")
	}

	// Try to delete full bucket.
	err = s3.RemoveBucket(context.Background(), "bucket1")
	if err == nil || !strings.Contains(err.Error(), gofakes3.ErrBucketNotEmpty.Error()) {
		t.Fatal(err)
	}

	// Remove object.
	err = s3.RemoveObject(context.Background(), "bucket1", "object1", minio.RemoveObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// Try to get object that doesn't exist anymore.
	obj, err = s3.GetObject(context.Background(), "bucket1", "object1", minio.GetObjectOptions{})
	if err != nil {
		t.Fatal(err)
	} else if _, err := io.ReadAll(obj); err == nil || !strings.Contains(err.Error(), "The specified key does not exist") {
		t.Fatal(err)
	}

	// Delete bucket.
	err = s3.RemoveBucket(context.Background(), "bucket1")
	if err != nil {
		t.Fatal(err)
	}
	exists, err = s3.BucketExists(context.Background(), "bucket1")
	if err != nil {
		t.Fatal(err)
	} else if exists {
		t.Fatal("expected bucket1 to exist")
	}
	err = s3.RemoveBucket(context.Background(), "bucket3")
	if err == nil || !strings.Contains(err.Error(), "The specified bucket does not exist") {
		t.Fatal(err)
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
	err = s3.MakeBucket(context.Background(), "bucket", minio.MakeBucketOptions{})
	if err != nil {
		t.Fatal(err)
	}

	objects := []string{
		"sample.jpg",
		"photos/2006/January/sample.jpg",
		"photos2/2006/February/sample3.jpg",
		"photos/2006/February/sample2.jpg",
		"photos2/2006/February/sample4.jpg",
	}
	for _, object := range objects {
		data := frand.Bytes(10)
		_, err = s3.PutObject(context.Background(), "bucket", object, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{})
		if err != nil {
			t.Fatal(err)
		}
	}

	tests := []struct {
		prefix string
		result []string
	}{
		{
			prefix: "",
			result: []string{
				"sample.jpg",
				"photos/",
				"photos2/",
			},
		},
		{
			prefix: "photos/2006/Feb",
			result: []string{
				"photos/2006/February/", // @reviewer: not sure if this is correct
			},
		},
	}
	for i, test := range tests {
		var response []string
		for objInfo := range s3.ListObjects(context.Background(), "bucket", minio.ListObjectsOptions{
			Prefix: test.prefix,
		}) {
			if objInfo.Err != nil {
				t.Fatal(err)
			}
			response = append(response, objInfo.Key)
		}
		if !cmp.Equal(test.result, response) {
			t.Errorf("test %d: unexpected response: %v", i, cmp.Diff(test.result, response))
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

	// Create a core client for lower-level operations.
	url := s3.EndpointURL()
	core, err := minio.NewCore(url.Host+url.Path, &minio.Options{
		Creds: testS3Credentials,
	})
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
	putPart := func(partNum int, data []byte) {
		t.Helper()
		part, err := core.PutObjectPart(context.Background(), "multipart", "foo", uploadID, partNum, bytes.NewReader(data), int64(len(data)), minio.PutObjectPartOptions{})
		if err != nil {
			t.Fatal(err)
		} else if part.ETag == "" {
			t.Fatal("expected non-empty ETag")
		}
	}
	putPart(2, []byte("world"))
	putPart(1, []byte("hello"))
	putPart(3, []byte("!"))

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

	// TODO: complete upload

	// TODO: download object
}
