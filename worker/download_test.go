package worker

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"testing"

	"github.com/gotd/contrib/http_range"
	"go.sia.tech/renterd/v2/api"
	"lukechampine.com/frand"
)

func TestDownloadObjectRange(t *testing.T) {
	// create test worker
	w := newTestWorker(t, newTestWorkerCfg())
	w.AddHosts(testRedundancySettings.TotalShards)

	// upload data
	data := frand.Bytes(128)
	params := testParameters(t.Name())
	if _, _, err := w.uploadManager.Upload(context.Background(), bytes.NewReader(data), w.UploadHosts(), params); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		offset, length int64
		expected       []byte
	}{
		{offset: 0, length: -1, expected: data},
		{offset: 0, length: int64(len(data)), expected: data},
		{offset: 10, length: 20, expected: data[10:30]},
		{offset: 100, length: -1, expected: data[100:]},
		{offset: int64(len(data)), length: -1, expected: nil},
		{offset: int64(len(data)), length: 0, expected: nil},
		{offset: int64(len(data)) + 1, length: -1},
		{offset: 0, length: int64(len(data)) + 1},
		{offset: 1, length: int64(len(data))},
		{offset: 0, length: -2},
		{offset: -1, length: 1},
		{offset: 0, length: math.MaxInt64},
		{offset: 1, length: math.MaxInt64},
		{offset: math.MaxInt64, length: 1},
		{offset: math.MaxInt64, length: -1},
		{offset: math.MaxInt64, length: math.MaxInt64},
	}
	for _, test := range tests {
		dr := api.DownloadRange{Offset: test.offset, Length: test.length}
		res, err := w.GetObject(context.Background(), testBucket, t.Name(), api.DownloadObjectOptions{Range: &dr})
		if test.expected == nil && (test.offset != int64(len(data)) || test.length > 0) {
			if !errors.Is(err, http_range.ErrInvalid) {
				t.Fatalf("range %v: expected %v, got %v", dr, http_range.ErrInvalid, err)
			}
			continue
		} else if err != nil {
			t.Fatalf("range %v: %v", dr, err)
		}
		buf, err := io.ReadAll(res.Content)
		if err != nil {
			t.Fatalf("range %v: %v", dr, err)
		} else if !bytes.Equal(buf, test.expected) {
			t.Fatalf("range %v: unexpected data", dr)
		}
	}
}
