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
	hs := slabutil.NewMockHostSet()
	for i := 0; i < 10; i++ {
		hs.AddHost()
	}
	ssu := slab.SerialSlabsUploader{SlabUploader: slab.SerialSlabUploader{Hosts: hs.Uploaders()}}
	slabs, err := ssu.UploadSlabs(bytes.NewReader(data), 3, 10)
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
		ssd := slab.SerialSlabsDownloader{SlabDownloader: slab.SerialSlabDownloader{Hosts: hs.Downloaders()}}
		if err := ssd.DownloadSlabs(&buf, ss, int64(offset), int64(length)); err != nil {
			t.Error(err)
			return
		}
		exp := data[offset:][:length]
		got := buf.Bytes()
		if !bytes.Equal(got, exp) {
			t.Errorf("download(%v, %v):\nexpected: %x...%x (%v)\ngot:      %x...%x (%v)",
				offset, length,
				exp[:20], exp[len(exp)-20:], len(exp),
				got[:20], got[len(got)-20:], len(got))
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
		ssd := slab.SerialSlabsDownloader{SlabDownloader: slab.SerialSlabDownloader{Hosts: hs.Downloaders()}}
		if err := ssd.DownloadSlabs(&buf, ss, int64(offset), int64(length)); err == nil {
			t.Error("expected error, got nil")
		}
	}
	checkDownloadFail(0, -1)
	checkDownloadFail(-1, 0)
	checkDownloadFail(0, len(data)+1)
	checkDownloadFail(len(data), 1)

	// migrate to 5 new hosts
	for i := 0; i < 5; i++ {
		hs.AddHost()
	}
	rsc := slab.NewRSCode(3, 10)
	shards := make([][]byte, 10)
	for i := range shards {
		shards[i] = make([]byte, 0, rhpv2.SectorSize)
	}
	rsc.Encode(data, shards)
	for i := range shards {
		if i < 5 {
			shards[i] = nil
		} else {
			shards[i] = shards[i][:rhpv2.SectorSize]
		}
	}
	ssm := slab.SerialSlabMigrator{Hosts: hs.Uploaders()}
	old := fmt.Sprint(slabs[0])
	if err := ssm.MigrateSlab(&slabs[0], shards); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(slabs[0]) == old {
		t.Error("no change to slab after migration")
	}
	checkDownload(0, 0)
	checkDownload(0, 1)
	checkDownload(0, len(data))

	// delete
	ssd := slab.SerialSlabsDeleter{Hosts: hs.Deleters()}
	if err := ssd.DeleteSlabs(slabs); err != nil {
		t.Fatal(err)
	}

	// downloads should now fail
	checkDownloadFail(0, len(data))
	checkDownloadFail(0, 1)
}
