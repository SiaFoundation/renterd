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
	ssu := slab.SerialSlabsUploader{Uploader: slab.SerialSlabUploader{Hosts: hs.Uploaders()}}
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
		ssd := slab.SerialSlabsDownloader{Downloader: slab.SerialSlabDownloader{Hosts: hs.Downloaders()}}
		if err := ssd.DownloadSlabs(&buf, ss, int64(offset), int64(length)); err != nil {
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
		ssd := slab.SerialSlabsDownloader{Downloader: slab.SerialSlabDownloader{Hosts: hs.Downloaders()}}
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
		for h := range hs.Hosts {
			delete(hs.Hosts, h)
			break
		}
	}
	for i := 0; i < 5; i++ {
		hs.AddHost()
	}
	from := hs.Downloaders()
	to := hs.Uploaders()
	ssm := slab.SerialSlabMigrator{
		From: from,
		To:   to,
	}
	old := fmt.Sprint(slabs[0])
	if err := ssm.MigrateSlab(&slabs[0]); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(slabs[0]) == old {
		t.Error("no change to slab after migration")
	}
	checkDownload(0, 0)
	checkDownload(0, 1)
	checkDownload(rhpv2.LeafSize*10, rhpv2.LeafSize*20)
	checkDownload(0, len(data)/2)
	checkDownload(0, len(data))
	checkDownload(len(data)/2, len(data)/2)
	checkDownload(84923, len(data[84923:])-53219)

	// delete
	ssd := slab.SerialSlabsDeleter{Hosts: hs.Deleters()}
	if err := ssd.DeleteSlabs(slabs); err != nil {
		t.Fatal(err)
	}

	// downloads should now fail
	checkDownloadFail(0, len(data))
	checkDownloadFail(0, 1)
}
