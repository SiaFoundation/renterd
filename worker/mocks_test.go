package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
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

type (
	contractsMap map[types.PublicKey]api.ContractMetadata

	mockContract struct {
		rev types.FileContractRevision

		mu      sync.Mutex
		sectors map[types.Hash256]*[rhpv2.SectorSize]byte
	}

	mockContractStore struct {
		contracts map[types.FileContractID]*mockContract
	}

	mockHost struct {
		hk types.PublicKey

		mu sync.Mutex
		c  *mockContract

		hpt          hostdb.HostPriceTable
		hptBlockChan chan struct{}
	}

	mockHostManager struct {
		hosts map[types.PublicKey]Host
	}

	mockMemory        struct{}
	mockMemoryManager struct{}

	mockObjectStore struct {
		mu           sync.Mutex
		objects      map[string]map[string]object.Object
		partials     map[string]mockPackedSlab
		bufferIDCntr uint // allows marking packed slabs as uploaded
	}

	mockPackedSlab struct {
		parameterKey string // ([minshards]-[totalshards]-[contractset])
		bufferID     uint
		slabKey      object.EncryptionKey
		data         []byte
	}

	mockWorker struct {
		cs *mockContractStore
		hm *mockHostManager
		mm *mockMemoryManager
		os *mockObjectStore

		dl *downloadManager
		ul *uploadManager

		contracts contractsMap
	}
)

var (
	_ ContractStore = (*mockContractStore)(nil)
	_ Host          = (*mockHost)(nil)
	_ HostManager   = (*mockHostManager)(nil)
	_ Memory        = (*mockMemory)(nil)
	_ MemoryManager = (*mockMemoryManager)(nil)
	_ ObjectStore   = (*mockObjectStore)(nil)
)

var (
	errBucketNotFound    = errors.New("bucket not found")
	errContractNotFound  = errors.New("contract not found")
	errObjectNotFound    = errors.New("object not found")
	errSlabNotFound      = errors.New("slab not found")
	errSectorOutOfBounds = errors.New("sector out of bounds")
)

func (c contractsMap) values() []api.ContractMetadata {
	var contracts []api.ContractMetadata
	for _, contract := range c {
		contracts = append(contracts, contract)
	}
	return contracts
}

func (m *mockMemory) Release()           {}
func (m *mockMemory) ReleaseSome(uint64) {}

func (mm *mockMemoryManager) Limit(amt uint64) (MemoryManager, error) {
	return &mockMemoryManager{}, nil
}
func (mm *mockMemoryManager) Status() api.MemoryStatus { return api.MemoryStatus{} }
func (mm *mockMemoryManager) AcquireMemory(ctx context.Context, amt uint64) Memory {
	return &mockMemory{}
}

func (os *mockContractStore) RenewedContract(ctx context.Context, renewedFrom types.FileContractID) (api.ContractMetadata, error) {
	return api.ContractMetadata{}, api.ErrContractNotFound
}

func (os *mockObjectStore) AddMultipartPart(ctx context.Context, bucket, path, contractSet, ETag, uploadID string, partNumber int, slices []object.SlabSlice) (err error) {
	return nil
}

func (os *mockObjectStore) AddUploadingSector(ctx context.Context, uID api.UploadID, id types.FileContractID, root types.Hash256) error {
	return nil
}

func (os *mockObjectStore) TrackUpload(ctx context.Context, uID api.UploadID) error { return nil }

func (os *mockObjectStore) FinishUpload(ctx context.Context, uID api.UploadID) error { return nil }

func (os *mockObjectStore) DeleteHostSector(ctx context.Context, hk types.PublicKey, root types.Hash256) error {
	return nil
}

func (os *mockObjectStore) DeleteObject(ctx context.Context, bucket, path string, opts api.DeleteObjectOptions) error {
	return nil
}

func (os *mockObjectStore) AddObject(ctx context.Context, bucket, path, contractSet string, o object.Object, opts api.AddObjectOptions) error {
	os.mu.Lock()
	defer os.mu.Unlock()

	// check if the bucket exists
	if _, exists := os.objects[bucket]; !exists {
		return errBucketNotFound
	}

	os.objects[bucket][path] = o
	return nil
}

func (os *mockObjectStore) AddPartialSlab(ctx context.Context, data []byte, minShards, totalShards uint8, contractSet string) (slabs []object.SlabSlice, slabBufferMaxSizeSoftReached bool, err error) {
	os.mu.Lock()
	defer os.mu.Unlock()

	// check if given data is too big
	slabSize := int(minShards) * int(rhpv2.SectorSize)
	if len(data) > slabSize {
		return nil, false, fmt.Errorf("data size %v exceeds size of a slab %v", len(data), slabSize)
	}

	// create slab
	ec := object.GenerateEncryptionKey()
	ss := object.SlabSlice{
		Slab:   object.NewPartialSlab(ec, minShards),
		Offset: 0,
		Length: uint32(len(data)),
	}

	// update store
	os.partials[ec.String()] = mockPackedSlab{
		parameterKey: fmt.Sprintf("%d-%d-%v", minShards, totalShards, contractSet),
		bufferID:     os.bufferIDCntr,
		slabKey:      ec,
		data:         data,
	}
	os.bufferIDCntr++

	return []object.SlabSlice{ss}, false, nil
}

func (os *mockObjectStore) Object(ctx context.Context, bucket, path string, opts api.GetObjectOptions) (api.ObjectsResponse, error) {
	os.mu.Lock()
	defer os.mu.Unlock()

	// check if the bucket exists
	if _, exists := os.objects[bucket]; !exists {
		return api.ObjectsResponse{}, errBucketNotFound
	}

	// check if the object exists
	if _, exists := os.objects[bucket][path]; !exists {
		return api.ObjectsResponse{}, errObjectNotFound
	}

	// clone to ensure the store isn't unwillingly modified
	var o object.Object
	if b, err := json.Marshal(os.objects[bucket][path]); err != nil {
		panic(err)
	} else if err := json.Unmarshal(b, &o); err != nil {
		panic(err)
	}

	return api.ObjectsResponse{Object: &api.Object{
		ObjectMetadata: api.ObjectMetadata{Name: path, Size: o.TotalSize()},
		Object:         o,
	}}, nil
}

func (os *mockObjectStore) FetchPartialSlab(ctx context.Context, key object.EncryptionKey, offset, length uint32) ([]byte, error) {
	os.mu.Lock()
	defer os.mu.Unlock()

	packedSlab, exists := os.partials[key.String()]
	if !exists {
		return nil, errSlabNotFound
	}
	if offset+length > uint32(len(packedSlab.data)) {
		return nil, errors.New("offset out of bounds")
	}

	return packedSlab.data[offset : offset+length], nil
}

func (os *mockObjectStore) Slab(ctx context.Context, key object.EncryptionKey) (slab object.Slab, err error) {
	os.mu.Lock()
	defer os.mu.Unlock()

	os.forEachObject(func(bucket, path string, o object.Object) {
		for _, s := range o.Slabs {
			if s.Slab.Key.String() == key.String() {
				slab = s.Slab
				return
			}
		}
		err = errSlabNotFound
	})
	return
}

func (os *mockObjectStore) UpdateSlab(ctx context.Context, s object.Slab, contractSet string) error {
	os.mu.Lock()
	defer os.mu.Unlock()

	os.forEachObject(func(bucket, path string, o object.Object) {
		for i, slab := range o.Slabs {
			if slab.Key.String() == s.Key.String() {
				os.objects[bucket][path].Slabs[i].Slab = s
				return
			}
		}
	})

	return nil
}

func (os *mockObjectStore) PackedSlabsForUpload(ctx context.Context, lockingDuration time.Duration, minShards, totalShards uint8, set string, limit int) (pss []api.PackedSlab, _ error) {
	os.mu.Lock()
	defer os.mu.Unlock()

	parameterKey := fmt.Sprintf("%d-%d-%v", minShards, totalShards, set)
	for _, ps := range os.partials {
		if ps.parameterKey == parameterKey {
			pss = append(pss, api.PackedSlab{
				BufferID: ps.bufferID,
				Data:     ps.data,
				Key:      ps.slabKey,
			})
		}
	}
	return
}

func (os *mockObjectStore) MarkPackedSlabsUploaded(ctx context.Context, slabs []api.UploadedPackedSlab) error {
	os.mu.Lock()
	defer os.mu.Unlock()

	bufferIDToKey := make(map[uint]string)
	for key, ps := range os.partials {
		bufferIDToKey[ps.bufferID] = key
	}

	slabKeyToSlab := make(map[string]*object.Slab)
	os.forEachObject(func(bucket, path string, o object.Object) {
		for i, slab := range o.Slabs {
			slabKeyToSlab[slab.Slab.Key.String()] = &os.objects[bucket][path].Slabs[i].Slab
		}
	})

	for _, slab := range slabs {
		key := bufferIDToKey[slab.BufferID]
		slabKeyToSlab[key].Shards = slab.Shards
		delete(os.partials, key)
	}

	return nil
}

func (os *mockObjectStore) forEachObject(fn func(bucket, path string, o object.Object)) {
	for bucket, objects := range os.objects {
		for path, object := range objects {
			fn(bucket, path, object)
		}
	}
}

func (h *mockHost) DownloadSector(ctx context.Context, w io.Writer, root types.Hash256, offset, length uint32, overpay bool) error {
	sector, exist := h.c.sectors[root]
	if !exist {
		return errSectorNotFound
	}
	if offset+length > rhpv2.SectorSize {
		return errSectorOutOfBounds
	}
	_, err := w.Write(sector[offset : offset+length])
	return err
}

func (h *mockHost) UploadSector(ctx context.Context, sector *[rhpv2.SectorSize]byte, rev types.FileContractRevision) (types.Hash256, error) {
	root := rhpv2.SectorRoot(sector)
	h.c.sectors[root] = sector
	return root, nil
}

func (h *mockHost) FetchRevision(ctx context.Context, fetchTimeout time.Duration, blockHeight uint64) (rev types.FileContractRevision, _ error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	rev = h.c.rev
	return
}

func (h *mockHost) FetchPriceTable(ctx context.Context, rev *types.FileContractRevision) (hostdb.HostPriceTable, error) {
	<-h.hptBlockChan
	return h.hpt, nil
}

func (h *mockHost) FundAccount(ctx context.Context, balance types.Currency, rev *types.FileContractRevision) error {
	return nil
}

func (h *mockHost) RenewContract(ctx context.Context, rrr api.RHPRenewRequest) (_ rhpv2.ContractRevision, _ []types.Transaction, _ types.Currency, err error) {
	return
}

func (h *mockHost) SyncAccount(ctx context.Context, rev *types.FileContractRevision) error {
	return nil
}

func (hp *mockHostManager) Host(hk types.PublicKey, fcid types.FileContractID, siamuxAddr string) Host {
	if _, ok := hp.hosts[hk]; !ok {
		panic("host not found")
	}
	return hp.hosts[hk]
}

func (cs *mockContractStore) AcquireContract(ctx context.Context, fcid types.FileContractID, priority int, d time.Duration) (lockID uint64, err error) {
	if lock, ok := cs.contracts[fcid]; !ok {
		return 0, errContractNotFound
	} else {
		lock.mu.Lock()
	}

	return 0, nil
}

func (cs *mockContractStore) ReleaseContract(ctx context.Context, fcid types.FileContractID, lockID uint64) (err error) {
	if lock, ok := cs.contracts[fcid]; !ok {
		return errContractNotFound
	} else {
		lock.mu.Unlock()
	}
	return nil
}

func (cs *mockContractStore) KeepaliveContract(ctx context.Context, fcid types.FileContractID, lockID uint64, d time.Duration) (err error) {
	return nil
}

func newMockHosts(n int) []*mockHost {
	hosts := make([]*mockHost, n)
	for i := range hosts {
		hosts[i] = newMockHost(types.PublicKey{byte(i)}, newTestHostPriceTable(time.Now().Add(time.Minute)), nil)
	}
	return hosts
}

func newMockHost(hk types.PublicKey, hpt hostdb.HostPriceTable, c *mockContract) *mockHost {
	return &mockHost{
		hk: hk,
		c:  c,

		hpt: hpt,
	}
}

func newMockContracts(hosts []*mockHost) []*mockContract {
	contracts := make([]*mockContract, len(hosts))
	for i := range contracts {
		contracts[i] = newMockContract(types.FileContractID{byte(i)})
		hosts[i].c = contracts[i]
	}
	return contracts
}

func newMockContract(fcid types.FileContractID) *mockContract {
	return &mockContract{
		rev:     types.FileContractRevision{ParentID: fcid},
		sectors: make(map[types.Hash256]*[rhpv2.SectorSize]byte),
	}
}

func newMockContractStore(contracts []*mockContract) *mockContractStore {
	cs := &mockContractStore{contracts: make(map[types.FileContractID]*mockContract)}
	for _, c := range contracts {
		cs.contracts[c.rev.ParentID] = c
	}
	return cs
}

func newMockHostManager(hosts []*mockHost) *mockHostManager {
	hm := &mockHostManager{hosts: make(map[types.PublicKey]Host)}
	for _, h := range hosts {
		hm.hosts[h.hk] = h
	}
	return hm
}

func newMockObjectStore() *mockObjectStore {
	os := &mockObjectStore{objects: make(map[string]map[string]object.Object), partials: make(map[string]mockPackedSlab)}
	os.objects[testBucket] = make(map[string]object.Object)
	return os
}

func newMockSector() (*[rhpv2.SectorSize]byte, types.Hash256) {
	var sector [rhpv2.SectorSize]byte
	frand.Read(sector[:])
	return &sector, rhpv2.SectorRoot(&sector)
}

func newMockWorker(numHosts int) *mockWorker {
	// create hosts and contracts
	hosts := newMockHosts(numHosts)
	contracts := newMockContracts(hosts)

	// create dependencies
	cs := newMockContractStore(contracts)
	hm := newMockHostManager(hosts)
	os := newMockObjectStore()
	mm := &mockMemoryManager{}

	dl := newDownloadManager(context.Background(), hm, mm, os, 0, 0, zap.NewNop().Sugar())
	ul := newUploadManager(context.Background(), hm, mm, os, cs, 0, 0, time.Minute, zap.NewNop().Sugar())

	// create contract metadata
	metadatas := make(contractsMap)
	for _, h := range hosts {
		metadatas[h.hk] = api.ContractMetadata{
			ID:      h.c.rev.ParentID,
			HostKey: h.hk,
		}
	}

	return &mockWorker{
		hm: hm,
		mm: mm,
		os: os,

		dl: dl,
		ul: ul,

		contracts: metadatas,
	}
}

func newTestHostPriceTable(expiry time.Time) hostdb.HostPriceTable {
	var uid rhpv3.SettingsID
	frand.Read(uid[:])

	return hostdb.HostPriceTable{
		HostPriceTable: rhpv3.HostPriceTable{UID: uid, Validity: time.Minute},
		Expiry:         expiry,
	}
}
