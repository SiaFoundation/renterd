package testing

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/Mikubill/gofakes3"
	"github.com/minio/minio-go/v7"
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
	} else if buckets[0].CreationDate != time.Unix(0, 0).UTC() {
		t.Fatal("expected unix 0", buckets[0].CreationDate, time.Unix(0, 0))
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
