package testing

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/Mikubill/gofakes3"
	"github.com/google/go-cmp/cmp"
	"github.com/minio/minio-go/v7"
	"go.sia.tech/renterd/api"
	"lukechampine.com/frand"
)

var (
	errBucketNotExists = errors.New("specified bucket does not exist")
)

func TestS3Basic(t *testing.T) {
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

	// delete default bucket
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
	if err == nil || !strings.Contains(err.Error(), errBucketNotExists.Error()) {
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

	// add another bucket
	err = s3.MakeBucket(context.Background(), bucket+"2", minio.MakeBucketOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// copy our object into the new bucket.
	_, err = s3.CopyObject(context.Background(), minio.CopyDestOptions{
		Bucket: bucket + "2",
		Object: "object",
	}, minio.CopySrcOptions{
		Bucket: bucket,
		Object: "object",
	})
	if err != nil {
		t.Fatal(err)
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

	objects := []string{
		"a/a/a",
		"a/b",
		"b",
		"c/a",
		"d",
		"ab",
	}
	for _, object := range objects {
		data := frand.Bytes(10)
		_, err = s3.PutObject(context.Background(), "bucket", object, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{})
		if err != nil {
			t.Fatal(err)
		}
	}

	flatten := func(res <-chan minio.ObjectInfo) []string {
		var objs []string
		for obj := range res {
			if obj.Err != nil {
				t.Fatal(err)
			}
			objs = append(objs, obj.Key)
		}
		return objs
	}

	tests := []struct {
		prefix string
		marker string
		want   []string
	}{
		// {
		// 	prefix: "",
		// 	marker: "",
		// 	want:   []string{"ab", "b", "d", "a/", "c/"},
		// },
		// {
		// 	prefix: "a",
		// 	marker: "",
		// 	want:   []string{"ab", "a/"},
		// },
		// {
		// 	prefix: "",
		// 	marker: "b",
		// 	want:   []string{"d", "c/"},
		// },
		// {
		// 	prefix: "e",
		// 	marker: "",
		// 	want:   nil,
		// },
		{
			prefix: "a",
			marker: "a/",
			want:   []string{"ab"},
		},
	}
	for i, test := range tests {
		got := flatten(s3.ListObjects(context.Background(), "bucket", minio.ListObjectsOptions{Prefix: test.prefix, StartAfter: test.marker}))
		if !cmp.Equal(test.want, got) {
			t.Errorf("test %d: unexpected response, want %v got %v", i, test.want, got)
		}
	}
}
