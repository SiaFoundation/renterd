package worker

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/object"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"lukechampine.com/frand"
)

type mockHost struct {
	contractID types.FileContractID
	publicKey  types.PublicKey
	sectors    map[types.Hash256][]byte
}

func (h *mockHost) Contract() types.FileContractID {
	return h.contractID
}

func (h *mockHost) PublicKey() types.PublicKey {
	return h.publicKey
}

func (h *mockHost) UploadSector(_ context.Context, sector *[rhpv2.SectorSize]byte) (types.Hash256, error) {
	root := rhpv2.SectorRoot(sector)
	h.sectors[root] = append([]byte(nil), sector[:]...)
	return root, nil
}

func (h *mockHost) DownloadSector(_ context.Context, w io.Writer, root types.Hash256, offset, length uint32) error {
	sector, ok := h.sectors[root]
	if !ok {
		return errors.New("unknown root")
	} else if uint64(offset)+uint64(length) > rhpv2.SectorSize {
		return errors.New("offset+length out of bounds")
	}
	_, err := w.Write(sector[offset:][:length])
	return err
}

func (h *mockHost) DeleteSectors(_ context.Context, roots []types.Hash256) error {
	for _, root := range roots {
		delete(h.sectors, root)
	}
	return nil
}

func newMockHost() *mockHost {
	var contractID types.FileContractID
	frand.Read(contractID[:])
	return &mockHost{
		contractID: contractID,
		publicKey:  types.GeneratePrivateKey().PublicKey(),
		sectors:    make(map[types.Hash256][]byte),
	}
}

func TestMultipleObjects(t *testing.T) {
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
	var hosts []sectorStore
	for i := 0; i < 10; i++ {
		hosts = append(hosts, newMockHost())
	}
	var slabs []object.Slab
	for {
		s, _, err := uploadSlab(context.Background(), r, 3, 10, hosts)
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
		ss := slabsForDownload(o.Slabs, int64(offset), int64(length))
		for _, s := range ss {
			if err := downloadSlab(context.Background(), dst, s, hosts); err != nil {
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
