package testing

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	"github.com/Mikubill/gofakes3"
	"github.com/google/go-cmp/cmp"
	"github.com/minio/minio-go/v7"
	"go.sia.tech/renterd/api"
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
	s3 := cluster.S3

	// delete default bucket before testing.
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

	// PutOBject into bucket.
	data := frand.Bytes(10)
	_, err = s3.PutObject(context.Background(), "bucket1", "object1", bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s3.PutObject(context.Background(), "bucket2", "object2", bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{})
	if err == nil || !strings.Contains(err.Error(), "The specified bucket does not exist") {
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

	// Try to get object.
	_, err = s3.GetObject(context.Background(), "bucket1", "object1", minio.GetObjectOptions{})
	if err != nil {
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
	err = s3.RemoveBucket(context.Background(), "bucket2")
	if err == nil || !strings.Contains(err.Error(), "The specified bucket does not exist") {
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
