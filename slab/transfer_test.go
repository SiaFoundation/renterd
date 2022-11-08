package slab_test

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"testing"

	"go.sia.tech/renterd/internal/observability"
	"go.sia.tech/renterd/internal/slabutil"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"go.sia.tech/renterd/slab"
	"lukechampine.com/frand"
)

func TestSlabs(t *testing.T) {
	ctx := testCtx()

	// generate data
	data := frand.Bytes(1000000)

	// upload slabs
	var hosts []slab.Host
	for i := 0; i < 10; i++ {
		hosts = append(hosts, slabutil.NewMockHost())
	}
	s, err := slab.UploadSlab(ctx, bytes.NewReader(data), 3, 10, hosts)
	if err != nil {
		t.Fatal(err)
	}

	// download various ranges
	checkDownload := func(offset, length uint32) {
		t.Helper()
		got, err := slab.DownloadSlab(ctx, slab.Slice{s, offset, length}, hosts)
		if err != nil {
			t.Error(err)
			return
		}
		exp := data[offset:][:length]
		if !bytes.Equal(got, exp) {
			if len(got) > 20 {
				t.Errorf("download(%v, %v):\nexpected: %x...%x (%v)\ngot:      %x...%x (%v)",
					offset, length,
					exp[:20], exp[len(exp)-20:], len(exp),
					got[:20], got[len(got)-20:], len(got))
			} else {
				t.Errorf("download(%v, %v):\nexpected: %x (%v)\ngot:      %x (%v)",
					offset, length, exp, len(exp), got, len(got))
			}
		}
	}
	checkDownload(0, 0)
	checkDownload(0, 1)
	checkDownload(rhpv2.LeafSize*10, rhpv2.LeafSize*20)
	checkDownload(0, uint32(len(data)/2))
	checkDownload(0, uint32(len(data)))
	checkDownload(uint32(len(data)/2), uint32(len(data)/2))
	checkDownload(84923, uint32(len(data[84923:])-53219))

	checkDownloadFail := func(offset, length uint32) {
		t.Helper()
		if _, err := slab.DownloadSlab(ctx, slab.Slice{s, offset, length}, hosts); err == nil {
			t.Error("expected error, got nil")
		}
	}
	checkDownloadFail(0, math.MaxUint32)
	checkDownloadFail(math.MaxUint32, 1)

	// migrate to 5 new hosts
	from := hosts[5:]
	to := hosts[5:]
	for i := 0; i < 5; i++ {
		to = append(to, slabutil.NewMockHost())
	}
	old := fmt.Sprint(s)
	if err := slab.MigrateSlab(ctx, &s, from, to); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(s) == old {
		t.Error("no change to s after migration")
	}
	checkDownload(0, 0)
	checkDownload(0, 1)
	checkDownload(rhpv2.LeafSize*10, rhpv2.LeafSize*20)
	checkDownload(0, uint32(len(data)/2))
	checkDownload(0, uint32(len(data)))
	checkDownload(uint32(len(data)/2), uint32(len(data)/2))
	checkDownload(84923, uint32(len(data[84923:])-53219))

	// delete
	if err := slab.DeleteSlabs(ctx, []slab.Slab{s}, to); err != nil {
		t.Fatal(err)
	}

	// downloads should now fail
	checkDownloadFail(0, uint32(len(data)))
	checkDownloadFail(0, 1)
}

func testCtx() context.Context {
	return observability.ContextWithMetricsRecorder(context.Background())
}
