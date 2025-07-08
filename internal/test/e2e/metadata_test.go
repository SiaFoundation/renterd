package e2e

import (
	"bytes"
	"context"
	"net/http"
	"reflect"
	"testing"
	"time"

	"go.sia.tech/renterd/v2/api"
	"go.sia.tech/renterd/v2/internal/test"
)

func TestObjectMetadata(t *testing.T) {
	// create cluster
	cluster := newTestCluster(t, testClusterOptions{
		hosts:  test.RedundancySettings.TotalShards,
		logger: newTestLogger(true),
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
	data := []byte(t.Name())
	_, err := w.UploadObject(context.Background(), bytes.NewReader(data), testBucket, t.Name(), opts)
	if err != nil {
		t.Fatal(err)
	}

	// get the object from the bus and assert it has the metadata
	or, err := b.Object(context.Background(), testBucket, t.Name(), api.GetObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(or.Metadata, opts.Metadata) {
		t.Fatal("metadata mismatch", or.Metadata)
	}

	// get the object from the worker and assert it has the metadata
	gor, err := w.GetObject(context.Background(), testBucket, t.Name(), api.DownloadObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gor.Metadata, opts.Metadata) {
		t.Fatal("metadata mismatch", gor.Metadata)
	} else if gor.Etag == "" {
		t.Fatal("missing etag")
	}

	// HeadObject retrieves the modtime from a http header so it's not as
	// accurate as the modtime from the object GET endpoint which returns it in
	// the body.
	orModtime, err := time.Parse(http.TimeFormat, or.ModTime.Std().Format(http.TimeFormat))
	if err != nil {
		t.Fatal(err)
	}

	// perform a HEAD request and assert the headers are all present
	hor, err := w.HeadObject(context.Background(), testBucket, t.Name(), api.HeadObjectOptions{Range: &api.DownloadRange{Offset: 1, Length: 1}})
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(hor, &api.HeadObjectResponse{
		ContentType:  or.ContentType(),
		Etag:         gor.Etag,
		LastModified: api.TimeRFC3339(orModtime),
		Range:        &api.ContentRange{Offset: 1, Length: 1, Size: int64(len(data))},
		Size:         int64(len(data)),
		Metadata:     gor.Metadata,
	}) {
		t.Fatalf("unexpected response: %+v", hor)
	}

	// re-upload the object
	_, err = w.UploadObject(context.Background(), bytes.NewReader([]byte(t.Name())), testBucket, t.Name(), api.UploadObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// assert metadata was removed
	gor, err = w.GetObject(context.Background(), testBucket, t.Name(), api.DownloadObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(gor.Metadata) > 0 {
		t.Fatal("unexpected metadata", gor.Metadata)
	}
}
