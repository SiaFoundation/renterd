package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"

	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/object"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

type mockHost struct {
	account    rhpv3.Account
	contractID types.FileContractID
	publicKey  types.PublicKey
	sectors    map[types.Hash256][]byte
}

func (h *mockHost) Contract() types.FileContractID {
	return h.contractID
}

func (h *mockHost) HostKey() types.PublicKey {
	return h.publicKey
}

func (h *mockHost) UploadSector(_ context.Context, sector *[rhpv2.SectorSize]byte) (types.Hash256, error) {
	root := rhpv2.SectorRoot(sector)
	h.sectors[root] = append([]byte(nil), sector[:]...)
	return root, nil
}

func (h *mockHost) DownloadSector(_ context.Context, w io.Writer, root types.Hash256, offset, length uint64) error {
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

type mockUploader struct {
	locker    contractLocker
	contracts []api.ContractMetadata
	idx       map[uploadID]int
}

func (u *mockUploader) total() int                    { return len(u.contracts) }
func (u *mockUploader) finish(uploadID)               {}
func (u *mockUploader) update([]api.ContractMetadata) {}
func (u *mockUploader) schedule(_ context.Context, id uploadID, _ int, _ int, _ map[types.PublicKey]struct{}) (api.ContractMetadata, chan error, chan error, error) {
	signal := make(chan error, 1)
	done := make(chan error, 1)
	close(signal)

	release, _ := u.locker.AcquireContract(context.Background(), types.FileContractID{}, 0)
	go func() {
		err := <-done
		if err != nil {
			fmt.Println(err)
		}
		release.Release(context.Background())
	}()

	if u.idx[id] >= len(u.contracts) {
		return api.ContractMetadata{}, nil, nil, errors.New("no uploader available")
	}

	contract := u.contracts[u.idx[id]]
	u.idx[id]++
	return contract, signal, done, nil
}

func newMockUploader(locker contractLocker, contracts []api.ContractMetadata) *mockUploader {
	return &mockUploader{
		locker:    locker,
		contracts: contracts,
		idx:       make(map[uploadID]int),
	}
}

type mockContractLocker struct {
	mu       sync.Mutex
	acquired int
	released int
}

type mockReleaser struct {
	l *mockContractLocker
}

func (r *mockReleaser) Release(ctx context.Context) error {
	r.l.mu.Lock()
	defer r.l.mu.Unlock()
	r.l.released++
	return nil
}

func (l *mockContractLocker) AcquireContract(ctx context.Context, fcid types.FileContractID, priority int) (lock contractReleaser, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.acquired++
	return &mockReleaser{
		l: l,
	}, nil
}

type mockStoreProvider struct {
	hosts map[types.PublicKey]sectorStore
}

func newMockStoreProvider(hosts []sectorStore) *mockStoreProvider {
	sp := &mockStoreProvider{
		hosts: make(map[types.PublicKey]sectorStore),
	}
	for _, h := range hosts {
		sp.hosts[h.HostKey()] = h
	}
	return sp
}

func (sp *mockStoreProvider) withHostV2(ctx context.Context, contractID types.FileContractID, hostKey types.PublicKey, hostIP string, f func(sectorStore) error) (err error) {
	h, exists := sp.hosts[hostKey]
	if !exists {
		panic("doesn't exist")
	}
	return f(h)
}

func (sp *mockStoreProvider) withHostV3(ctx context.Context, contractID types.FileContractID, hostKey types.PublicKey, siamuxAddr string, f func(sectorStore) error) (err error) {
	h, exists := sp.hosts[hostKey]
	if !exists {
		panic("doesn't exist")
	}
	return f(h)
}

// TODO PJ: fix this test
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

	// Prepare hosts.
	var hosts []sectorStore
	for i := 0; i < 10; i++ {
		hosts = append(hosts, newMockHost())
	}
	sp := newMockStoreProvider(hosts)
	var contracts []api.ContractMetadata
	for _, h := range hosts {
		contracts = append(contracts, api.ContractMetadata{ID: h.Contract(), HostKey: h.HostKey()})
	}

	// Prepare uploader.
	mockLocker := &mockContractLocker{}
	ul := newMockUploader(mockLocker, contracts)

	// upload
	var slabs []object.Slab
	for {
		s, _, err := uploadSlab(context.Background(), sp, ul, r, 3, 10, 0, 0, zap.NewNop().Sugar())
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
			if _, err := downloadSlab(context.Background(), sp, dst, s, contracts, 0, 0, zap.NewNop().Sugar()); err != nil {
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

	mockLocker.mu.Lock()
	if mockLocker.acquired == 0 {
		t.Errorf("should have acquired")
	}
	if mockLocker.released == 0 {
		t.Errorf("should have released")
	}
	mockLocker.mu.Unlock()
}
