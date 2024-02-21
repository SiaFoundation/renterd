package worker

import (
	"context"
	"io"
	"sync"
	"testing"

	rhpv2 "go.sia.tech/core/rhp/v2"
)

// zeroReader is a reader that leaves the buffer unchanged and returns no error.
// It's useful for benchmarks that need to produce data for uploading and should
// be used together with a io.LimitReader.
type zeroReader struct{}

func (z *zeroReader) Read(p []byte) (n int, err error) {
	return len(p), nil
}

// BenchmarkUploaderPacking benchmarks the Upload function with packing
// disabled.
func BenchmarkUploaderNoPacking(b *testing.B) {
	w := newMockWorker()

	minDataPieces := 10
	totalDataPieces := 30

	w.addHosts(totalDataPieces)

	// create a reader that returns dev/null
	data := io.LimitReader(&zeroReader{}, int64(b.N*rhpv2.SectorSize*minDataPieces))

	up := testParameters(b.TempDir())
	up.rs.MinShards = minDataPieces
	up.rs.TotalShards = totalDataPieces
	up.packing = false

	b.ResetTimer()

	_, _, err := w.ul.Upload(context.Background(), data, w.contracts(), up, lockingPriorityUpload)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(rhpv2.SectorSize * minDataPieces))
}

// BenchmarkSectorRoot30Goroutines benchmarks the SectorRoot function with 30
// goroutines processing roots in parallel to simulate sequential uploads of
// slabs.
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
func BenchmarkSectorRootSingleGoroutine(b *testing.B) {
	data := make([]byte, rhpv2.SectorSize)
	b.SetBytes(rhpv2.SectorSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rhpv2.SectorRoot((*[rhpv2.SectorSize]byte)(data))
	}
}
