package worker

import (
	"bytes"
	"context"
	"io"
	"testing"

	rhpv4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/renterd/v2/api"
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
	w := newTestWorker(b, newTestWorkerCfg())

	up := testParameters(b.TempDir())
	up.RS.MinShards = 10
	up.RS.TotalShards = 30
	up.Packing = false
	w.AddHosts(up.RS.TotalShards)

	data := bytes.NewReader(frand.Bytes(int(up.RS.SlabSizeNoRedundancy())))
	_, _, err := w.uploadManager.Upload(context.Background(), data, w.UploadHosts(), up)
	if err != nil {
		b.Fatal(err)
	}
	o, err := w.os.Object(context.Background(), testBucket, up.Key, api.GetObjectOptions{})
	if err != nil {
		b.Fatal(err)
	}

	b.SetBytes(o.Size)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err = w.downloadManager.DownloadObject(context.Background(), io.Discard, *o.Object, 0, uint64(o.Size), w.UsableHosts())
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
	w := newTestWorker(b, newTestWorkerCfg())

	up := testParameters(b.TempDir())
	up.RS.MinShards = 10
	up.RS.TotalShards = 30
	up.Packing = false
	w.AddHosts(up.RS.TotalShards)

	data := io.LimitReader(&zeroReader{}, int64(b.N*rhpv4.SectorSize*up.RS.MinShards))
	b.SetBytes(int64(rhpv4.SectorSize * up.RS.MinShards))
	b.ResetTimer()

	_, _, err := w.uploadManager.Upload(context.Background(), data, w.UploadHosts(), up)
	if err != nil {
		b.Fatal(err)
	}
}

// BenchmarkUploaderSingleObject benchmarks uploading one object per slab.
//
// Speed       | CPU    | Commit
// 282.47 MB/s | M2 Pro | bae6e77
func BenchmarkUploaderMultiObject(b *testing.B) {
	w := newTestWorker(b, newTestWorkerCfg())

	up := testParameters(b.TempDir())
	up.RS.MinShards = 10
	up.RS.TotalShards = 30
	up.Packing = false
	w.AddHosts(up.RS.TotalShards)

	b.SetBytes(int64(rhpv4.SectorSize * up.RS.MinShards))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		data := io.LimitReader(&zeroReader{}, int64(rhpv4.SectorSize*up.RS.MinShards))
		_, _, err := w.uploadManager.Upload(context.Background(), data, w.UploadHosts(), up)
		if err != nil {
			b.Fatal(err)
		}
	}
}
