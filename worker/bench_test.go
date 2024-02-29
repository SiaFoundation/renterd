package worker

import (
	"bytes"
	"context"
	"io"
	"testing"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/renterd/api"
	"lukechampine.com/frand"
)

// zeroReader is a reader that leaves the buffer unchanged and returns no error.
// It's useful for benchmarks that need to produce data for uploading and should
// be used together with a io.LimitReader.
type zeroReader struct{}

func (z *zeroReader) Read(p []byte) (n int, err error) {
	return len(p), nil
}

// BenchmarkDownlaoderSingleObject benchmarks downloading a single, slab-sized
// object.
// 1036.74 MB/s | M2 Pro | c9dc1b6
func BenchmarkDownloaderSingleObject(b *testing.B) {
	w := newTestWorker(b)

	up := testParameters(b.TempDir())
	up.rs.MinShards = 10
	up.rs.TotalShards = 30
	up.packing = false
	w.AddHosts(up.rs.TotalShards)

	data := bytes.NewReader(frand.Bytes(int(up.rs.SlabSizeNoRedundancy())))
	_, _, err := w.uploadManager.Upload(context.Background(), data, w.Contracts(), up, lockingPriorityUpload)
	if err != nil {
		b.Fatal(err)
	}
	o, err := w.os.Object(context.Background(), testBucket, up.path, api.GetObjectOptions{})
	if err != nil {
		b.Fatal(err)
	}

	b.SetBytes(o.Object.Size)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err = w.downloadManager.DownloadObject(context.Background(), io.Discard, *o.Object.Object, 0, uint64(o.Object.Size), w.Contracts())
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkUploaderSingleObject benchmarks uploading a single object.
//
// Speed       | CPU    | Commit
// 433.86 MB/s | M2 Pro | bae6e77
func BenchmarkUploaderSingleObject(b *testing.B) {
	w := newTestWorker(b)

	up := testParameters(b.TempDir())
	up.rs.MinShards = 10
	up.rs.TotalShards = 30
	up.packing = false
	w.AddHosts(up.rs.TotalShards)

	data := io.LimitReader(&zeroReader{}, int64(b.N*rhpv2.SectorSize*up.rs.MinShards))
	b.SetBytes(int64(rhpv2.SectorSize * up.rs.MinShards))
	b.ResetTimer()

	_, _, err := w.uploadManager.Upload(context.Background(), data, w.Contracts(), up, lockingPriorityUpload)
	if err != nil {
		b.Fatal(err)
	}
}

// BenchmarkUploaderSingleObject benchmarks uploading one object per slab.
//
// Speed       | CPU    | Commit
// 282.47 MB/s | M2 Pro | bae6e77
func BenchmarkUploaderMultiObject(b *testing.B) {
	w := newTestWorker(b)

	up := testParameters(b.TempDir())
	up.rs.MinShards = 10
	up.rs.TotalShards = 30
	up.packing = false
	w.AddHosts(up.rs.TotalShards)

	b.SetBytes(int64(rhpv2.SectorSize * up.rs.MinShards))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		data := io.LimitReader(&zeroReader{}, int64(rhpv2.SectorSize*up.rs.MinShards))
		_, _, err := w.uploadManager.Upload(context.Background(), data, w.Contracts(), up, lockingPriorityUpload)
		if err != nil {
			b.Fatal(err)
		}
	}
}
