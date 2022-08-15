package object_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	"go.sia.tech/renterd/internal/objectutil"
	"go.sia.tech/renterd/internal/slabutil"
	"go.sia.tech/renterd/object"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"go.sia.tech/renterd/slab"
	"lukechampine.com/frand"
)

func TestObject(t *testing.T) {
	// generate data and encryption key
	data := frand.Bytes(1000000)
	key := object.GenerateEncryptionKey()

	// upload slabs
	hs := slabutil.NewMockHostSet()
	for i := 0; i < 10; i++ {
		hs.AddHost()
	}
	su := slab.SerialSlabsUploader{SlabUploader: hs.SlabUploader()}
	slabs, err := su.UploadSlabs(context.Background(), key.Encrypt(bytes.NewReader(data)), 3, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(slabs) != 1 {
		t.Fatal(len(slabs))
	}

	// construct object
	o := object.Object{
		Key:   key,
		Slabs: make([]slab.Slice, len(slabs)),
	}
	for i, s := range slabs {
		o.Slabs[i] = slab.Slice{
			Slab:   s,
			Offset: 0,
			Length: uint32(len(data)),
		}
	}

	// store object
	es := objectutil.NewEphemeralStore()
	if err := es.Put("foo", o); err != nil {
		t.Fatal(err)
	}

	// retrieve object
	o, err = es.Get("foo")
	if err != nil {
		t.Fatal(err)
	}

	// download various ranges
	checkDownload := func(offset, length int) {
		t.Helper()
		var buf bytes.Buffer
		ssd := slab.SerialSlabsDownloader{SlabDownloader: hs.SlabDownloader()}
		if err := ssd.DownloadSlabs(context.Background(), key.Decrypt(&buf, int64(offset)), o.Slabs, int64(offset), int64(length)); err != nil {
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

	checkInvalidRange := func(offset, length int) {
		t.Helper()
		var buf bytes.Buffer
		ssd := slab.SerialSlabsDownloader{SlabDownloader: hs.SlabDownloader()}
		if err := ssd.DownloadSlabs(context.Background(), &buf, o.Slabs, int64(offset), int64(length)); err == nil {
			t.Error("expected error, got nil")
		}
	}
	checkInvalidRange(0, -1)
	checkInvalidRange(-1, 0)
	checkInvalidRange(0, len(data)+1)
	checkInvalidRange(len(data), 1)
}

func TestMultipleObjects(t *testing.T) {
	data := [][]byte{
		frand.Bytes(111),
		frand.Bytes(222),
		make([]byte, rhpv2.SectorSize*5), // will require multiple slabs
		frand.Bytes(333),
		frand.Bytes(444),
	}
	keys := make([]object.EncryptionKey, len(data))
	for i := range keys {
		keys[i] = object.GenerateEncryptionKey()
	}
	rs := make([]io.Reader, len(data))
	for i := range rs {
		rs[i] = keys[i].Encrypt(bytes.NewReader(data[i]))
	}
	r := io.MultiReader(rs...)

	// upload
	hs := slabutil.NewMockHostSet()
	for i := 0; i < 10; i++ {
		hs.AddHost()
	}
	ssu := slab.SerialSlabsUploader{SlabUploader: hs.SlabUploader()}
	slabs, err := ssu.UploadSlabs(context.Background(), r, 3, 10)
	if err != nil {
		t.Fatal(err)
	}

	// construct objects
	os := make([]object.Object, len(data))
	lengths := make([]int, len(data))
	for i := range data {
		lengths[i] = len(data[i])
	}
	ss := object.SplitSlabs(slabs, lengths)
	for i := range os {
		os[i] = object.Object{
			Key:   keys[i],
			Slabs: ss[i],
		}
	}

	// download
	checkDownload := func(data []byte, o object.Object, offset, length int) {
		t.Helper()
		var buf bytes.Buffer
		ssd := slab.SerialSlabsDownloader{SlabDownloader: hs.SlabDownloader()}
		if err := ssd.DownloadSlabs(context.Background(), o.Key.Decrypt(&buf, int64(offset)), o.Slabs, int64(offset), int64(length)); err != nil {
			t.Error(err)
			return
		}
		exp := data[offset:][:length]
		got := buf.Bytes()
		if !bytes.Equal(got, exp) {
			if len(exp) > 20 {
				exp = exp[:20]
			}
			if len(got) > 20 {
				got = got[:20]
			}
			t.Errorf("download(%v, %v):\nexpected: %x (%v)\ngot:      %x (%v)",
				offset, length,
				exp, len(exp),
				got, len(got))
		}
	}

	for i, o := range os {
		for _, r := range []struct{ offset, length int }{
			{0, 0},
			{0, 1},
			{0, len(data[i]) / 2},
			{len(data[i]) / 2, len(data[i]) / 2},
			{len(data[i]) - 1, 1},
			{0, len(data[i])},
		} {
			checkDownload(data[i], o, r.offset, r.length)
		}
	}
}
