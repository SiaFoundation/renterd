package worker

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/hostdb"
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

func (h *mockHost) UploadSector(_ context.Context, sector *[rhpv2.SectorSize]byte, rev types.FileContractRevision) (types.Hash256, error) {
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

func (sp *mockHost) Close() error { return nil }

func (h *mockHost) FetchPriceTable(ctx context.Context, rev *types.FileContractRevision) (hpt hostdb.HostPriceTable, err error) {
	panic("not implemented")
}
func (h *mockHost) FetchRevision(ctx context.Context, fetchTimeout time.Duration, blockHeight uint64) (_ types.FileContractRevision, _ error) {
	panic("not implemented")
}
func (h *mockHost) FundAccount(ctx context.Context, balance types.Currency, rev *types.FileContractRevision) error {
	panic("not implemented")
}
func (h *mockHost) Renew(ctx context.Context, rrr api.RHPRenewRequest) (_ rhpv2.ContractRevision, _ []types.Transaction, err error) {
	panic("not implemented")
}
func (h *mockHost) SyncAccount(ctx context.Context, rev *types.FileContractRevision) error {
	panic("not implemented")
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

type mockRevisionLocker struct {
	mu    sync.Mutex
	calls int
}

func (l *mockRevisionLocker) withRevision(ctx context.Context, _ time.Duration, contractID types.FileContractID, hk types.PublicKey, siamuxAddr string, lockPriority int, blockHeight uint64, fn func(rev types.FileContractRevision) error) error {
	l.mu.Lock()
	l.calls++
	l.mu.Unlock()
	return fn(types.FileContractRevision{})
}

type mockHostProvider struct {
	hosts map[types.PublicKey]hostV3
}

func newMockHostProvider(hosts []hostV3) *mockHostProvider {
	sp := &mockHostProvider{
		hosts: make(map[types.PublicKey]hostV3),
	}
	for _, h := range hosts {
		sp.hosts[h.HostKey()] = h
	}
	return sp
}

func (sp *mockHostProvider) newHostV3(contractID types.FileContractID, hostKey types.PublicKey, siamuxAddr string) hostV3 {
	h, exists := sp.hosts[hostKey]
	if !exists {
		panic("doesn't exist")
	}
	return h
}

func TestMultipleObjects(t *testing.T) {
	mockLocker := &mockRevisionLocker{}

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
	rr := make([]io.Reader, len(data))
	for i := range rr {
		rr[i] = keys[i].Encrypt(bytes.NewReader(data[i]))
	}
	r := io.MultiReader(rr...)

	// prepare hosts
	var hosts []hostV3
	for i := 0; i < 10; i++ {
		hosts = append(hosts, newMockHost())
	}
	hp := newMockHostProvider(hosts)
	var contracts []api.ContractMetadata
	for _, h := range hosts {
		contracts = append(contracts, api.ContractMetadata{ID: h.Contract(), HostKey: h.HostKey()})
	}

	// prepare upload manager
	mgr := newUploadManager(hp, mockLocker, 0, 0, zap.NewNop().Sugar())
	upload, err := mgr.newUpload(10, contracts, 0)
	if err != nil {
		t.Fatal(err)
	}

	rs := api.RedundancySettings{MinShards: 3, TotalShards: 10}
	dataChan := make(chan []byte)
	go func() {
		for {
			<-upload.nextSlabTrigger

			// read slab data
			buf := make([]byte, int(rs.MinShards)*rhpv2.SectorSize)
			_, err := io.ReadFull(r, buf)
			if err == io.EOF {
				close(dataChan)
				return
			} else if err != nil && err != io.ErrUnexpectedEOF {
				t.Error(err)
			}
			dataChan <- buf
		}
	}()

	// upload
	var slabs []object.Slab
	for data := range dataChan {
		s := object.Slab{
			Key:       object.GenerateEncryptionKey(),
			MinShards: uint8(rs.MinShards),
		}

		shards := make([][]byte, rs.TotalShards)
		s.Encode(data, shards)
		s.Encrypt(shards)

		s.Shards, err = upload.uploadShards(context.Background(), shards)
		if err != nil {
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
			if _, err := downloadSlab(context.Background(), hp, dst, s, contracts, 0, 0, zap.NewNop().Sugar()); err != nil {
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
	if mockLocker.calls == 0 {
		t.Errorf("should have called the locker")
	}
	mockLocker.mu.Unlock()
}
