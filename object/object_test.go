package object_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	"go.sia.tech/renterd/internal/observability"
	"go.sia.tech/renterd/internal/slabutil"
	"go.sia.tech/renterd/object"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"go.sia.tech/renterd/slab"
	"lukechampine.com/frand"
)

func TestMultipleObjects(t *testing.T) {
	ctx := observability.ContextWithMetricsRecorder(context.Background())

	// generate object data
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
	var hosts []slab.Host
	for i := 0; i < 10; i++ {
		hosts = append(hosts, slabutil.NewMockHost())
	}
	var slabs []slab.Slab
	for {
		s, err := slab.UploadSlab(ctx, r, 3, 10, hosts)
		if err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		slabs = append(slabs, s)
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
		dst := o.Key.Decrypt(&buf, int64(offset))
		ss := slab.SlabsForDownload(o.Slabs, int64(offset), int64(length))
		for _, s := range ss {
			if err := slab.DownloadSlab(ctx, dst, s, hosts); err != nil {
				t.Error(err)
				return
			}
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
