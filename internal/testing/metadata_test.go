package testing

import (
	"bytes"
	"context"
	"reflect"
	"testing"

	"go.sia.tech/renterd/api"
	"go.uber.org/zap"
)

func TestObjectMetadata(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	// create cluster
	cluster := newTestCluster(t, testClusterOptions{
		hosts:  testRedundancySettings.TotalShards,
		logger: zap.NewNop(),
	})
	defer cluster.Shutdown()

	// convenience variables
	w := cluster.Worker
	b := cluster.Bus

	// create options to pass metadata
	opts := api.UploadObjectOptions{
		Metadata: api.ObjectUserMetadata{"Foo": "bar", "Baz": "quux"},
	}

	// upload the object
	_, err := w.UploadObject(context.Background(), bytes.NewReader([]byte(t.Name())), api.DefaultBucketName, t.Name(), opts)
	if err != nil {
		t.Fatal(err)
	}

	// get the object from the bus and assert it has the metadata
	ress, err := b.Object(context.Background(), api.DefaultBucketName, t.Name(), api.GetObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(ress.Object.Metadata, opts.Metadata) {
		t.Fatal("metadata mismatch", ress.Object.Metadata)
	}

	// get the object from the worker and assert it has the metadata
	res, err := w.GetObject(context.Background(), api.DefaultBucketName, t.Name(), api.DownloadObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(res.Metadata, opts.Metadata) {
		t.Fatal("metadata mismatch", res.Metadata)
	}

	// re-upload the object
	_, err = w.UploadObject(context.Background(), bytes.NewReader([]byte(t.Name())), api.DefaultBucketName, t.Name(), api.UploadObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// assert metadata was removed
	res, err = w.GetObject(context.Background(), api.DefaultBucketName, t.Name(), api.DownloadObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Metadata) > 0 {
		t.Fatal("unexpected metadata", res.Metadata)
	}
}
