package testing

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"go.uber.org/zap/zapcore"
)

func TestS3(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	cluster, err := newTestCluster(t.TempDir(), newTestLoggerCustom(zapcore.DebugLevel))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := cluster.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	}()

	s3 := cluster.S3

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
