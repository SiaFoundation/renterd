package worker

import (
	"bytes"
	"context"
	"io"
	"sync"
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
	w := newMockWorker()

	up := testParameters(b.TempDir())
	up.rs.MinShards = 10
	up.rs.TotalShards = 30
	up.packing = false
	w.addHosts(up.rs.TotalShards)

	data := bytes.NewReader(frand.Bytes(int(up.rs.SlabSizeNoRedundancy())))
	_, _, err := w.ul.Upload(context.Background(), data, w.contracts(), up, lockingPriorityUpload)
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
		err = w.dl.DownloadObject(context.Background(), io.Discard, *o.Object.Object, 0, uint64(o.Object.Size), w.contracts())
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
	w := newMockWorker()

	up := testParameters(b.TempDir())
	up.rs.MinShards = 10
	up.rs.TotalShards = 30
	up.packing = false
	w.addHosts(up.rs.TotalShards)

	data := io.LimitReader(&zeroReader{}, int64(b.N*rhpv2.SectorSize*up.rs.MinShards))
	b.SetBytes(int64(rhpv2.SectorSize * up.rs.MinShards))
	b.ResetTimer()

	_, _, err := w.ul.Upload(context.Background(), data, w.contracts(), up, lockingPriorityUpload)
	if err != nil {
		b.Fatal(err)
	}
}

// BenchmarkUploaderSingleObject benchmarks uploading one object per slab.
//
// Speed       | CPU    | Commit
// 282.47 MB/s | M2 Pro | bae6e77
func BenchmarkUploaderMultiObject(b *testing.B) {
	w := newMockWorker()

	up := testParameters(b.TempDir())
	up.rs.MinShards = 10
	up.rs.TotalShards = 30
	up.packing = false
	w.addHosts(up.rs.TotalShards)

	b.SetBytes(int64(rhpv2.SectorSize * up.rs.MinShards))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		data := io.LimitReader(&zeroReader{}, int64(rhpv2.SectorSize*up.rs.MinShards))
		_, _, err := w.ul.Upload(context.Background(), data, w.contracts(), up, lockingPriorityUpload)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSectorRoot30Goroutines benchmarks the SectorRoot function with 30
// goroutines processing roots in parallel to simulate sequential uploads of
// slabs.
//
// Speed        | CPU    | Commit
// 1658.49 MB/s | M2 Pro | bae6e77
func BenchmarkSectorRoot30Goroutines(b *testing.B) {
	data := make([]byte, rhpv2.SectorSize)
	b.SetBytes(int64(rhpv2.SectorSize))

	// spin up workers
	c := make(chan struct{})
	work := func() {
		for range c {
			rhpv2.SectorRoot((*[rhpv2.SectorSize]byte)(data))
		}
	}
	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			work()
			wg.Done()
		}()
	}
	b.ResetTimer()

	// run the benchmark
	for i := 0; i < b.N; i++ {
		c <- struct{}{}
	}
	close(c)
	wg.Wait()
}

// BenchmarkSectorRootSingleGoroutine benchmarks the SectorRoot function.
//
// Speed       | CPU    | Commit
// 177.33 MB/s | M2 Pro | bae6e77
func BenchmarkSectorRootSingleGoroutine(b *testing.B) {
	data := make([]byte, rhpv2.SectorSize)
	b.SetBytes(rhpv2.SectorSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rhpv2.SectorRoot((*[rhpv2.SectorSize]byte)(data))
	}
}
