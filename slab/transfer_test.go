package slab_test

import (
	"bytes"
	"fmt"
	"testing"

	"go.sia.tech/renterd/internal/slabutil"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"go.sia.tech/renterd/slab"
	"lukechampine.com/frand"
)

func TestSlabs(t *testing.T) {
	// generate data
	data := frand.Bytes(1000000)

	// upload slabs
	var hosts []slab.Host
	for i := 0; i < 10; i++ {
		hosts = append(hosts, slabutil.NewMockHost())
	}
	slabs, err := slab.UploadSlabs(bytes.NewReader(data), 3, 10, hosts)
	if err != nil {
		t.Fatal(err)
	} else if len(slabs) != 1 {
		t.Fatal(len(slabs))
	}
	ss := []slab.Slice{{
		Slab:   slabs[0],
		Offset: 0,
		Length: uint32(len(data)),
	}}

	// download various ranges
	checkDownload := func(offset, length int) {
		t.Helper()
		var buf bytes.Buffer
		if err := slab.DownloadSlabs(&buf, ss, int64(offset), int64(length), hosts); err != nil {
			t.Error(err)
			return
		}
		exp := data[offset:][:length]
		got := buf.Bytes()
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
	checkDownload(0, len(data)/2)
	checkDownload(0, len(data))
	checkDownload(len(data)/2, len(data)/2)
	checkDownload(84923, len(data[84923:])-53219)

	checkDownloadFail := func(offset, length int) {
		t.Helper()
		var buf bytes.Buffer
		if err := slab.DownloadSlabs(&buf, ss, int64(offset), int64(length), hosts); err == nil {
			t.Error("expected error, got nil")
		}
	}
	checkDownloadFail(0, -1)
	checkDownloadFail(-1, 0)
	checkDownloadFail(0, len(data)+1)
	checkDownloadFail(len(data), 1)

	// migrate to 5 new hosts
	from := hosts[5:]
	to := hosts[5:]
	for i := 0; i < 5; i++ {
		to = append(to, slabutil.NewMockHost())
	}
	old := fmt.Sprint(slabs)
	if err := slab.MigrateSlabs(slabs, from, to); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(slabs) == old {
		t.Error("no change to slabs after migration")
	}
	checkDownload(0, 0)
	checkDownload(0, 1)
	checkDownload(rhpv2.LeafSize*10, rhpv2.LeafSize*20)
	checkDownload(0, len(data)/2)
	checkDownload(0, len(data))
	checkDownload(len(data)/2, len(data)/2)
	checkDownload(84923, len(data[84923:])-53219)

	// delete
	if err := slab.DeleteSlabs(slabs, to); err != nil {
		t.Fatal(err)
	}

	// downloads should now fail
	checkDownloadFail(0, len(data))
	checkDownloadFail(0, 1)
}
