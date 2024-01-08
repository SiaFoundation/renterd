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
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/object"
	"lukechampine.com/frand"
)

type (
	mockContract struct {
		rev types.FileContractRevision

		mu      sync.Mutex
		sectors map[types.Hash256]*[rhpv2.SectorSize]byte
	}

	mockContractLocker struct {
		contracts map[types.FileContractID]*mockContract
	}

	mockHost struct {
		hk types.PublicKey

		mu sync.Mutex
		c  *mockContract
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
)

var (
	_ ContractLocker = (*mockContractLocker)(nil)
	_ Host           = (*mockHost)(nil)
	_ HostManager    = (*mockHostManager)(nil)
	_ Memory         = (*mockMemory)(nil)
	_ MemoryManager  = (*mockMemoryManager)(nil)
	_ ObjectStore    = (*mockObjectStore)(nil)
)

var (
	errBucketNotFound    = errors.New("bucket not found")
	errContractNotFound  = errors.New("contract not found")
	errObjectNotFound    = errors.New("object not found")
	errSlabNotFound      = errors.New("slab not found")
	errSectorOutOfBounds = errors.New("sector out of bounds")
)

func (m *mockMemory) Release()           {}
func (m *mockMemory) ReleaseSome(uint64) {}

func (mm *mockMemoryManager) Limit(amt uint64) (MemoryManager, error) {
	return &mockMemoryManager{}, nil
}
func (mm *mockMemoryManager) Status() api.MemoryStatus { return api.MemoryStatus{} }
func (mm *mockMemoryManager) AcquireMemory(ctx context.Context, amt uint64) Memory {
	return &mockMemory{}
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

func (os *mockObjectStore) Slab(ctx context.Context, key object.EncryptionKey) (object.Slab, error) {
	os.mu.Lock()
	defer os.mu.Unlock()

	for _, objects := range os.objects {
		for _, object := range objects {
			for _, slab := range object.Slabs {
				if slab.Slab.Key.String() == key.String() {
					return slab.Slab, nil
				}
			}
		}
	}
	return object.Slab{}, errSlabNotFound
}

func (os *mockObjectStore) UpdateSlab(ctx context.Context, s object.Slab, contractSet string) error {
	os.mu.Lock()
	defer os.mu.Unlock()

	for bucket, objects := range os.objects {
		for path, object := range objects {
			for i, slab := range object.Slabs {
				if slab.Key.String() == s.Key.String() {
					os.objects[bucket][path].Slabs[i].Slab = s
					return nil
				}
			}
		}
	}
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
	for bucket, objects := range os.objects {
		for path, object := range objects {
			for i, slab := range object.Slabs {
				slabKeyToSlab[slab.Slab.Key.String()] = &os.objects[bucket][path].Slabs[i].Slab
			}
		}
	}

	for _, slab := range slabs {
		key := bufferIDToKey[slab.BufferID]
		slabKeyToSlab[key].Shards = slab.Shards
		delete(os.partials, key)
	}

	return nil
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

func (h *mockHost) FetchPriceTable(ctx context.Context, rev *types.FileContractRevision) (hpt hostdb.HostPriceTable, err error) {
	return
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

func (cl *mockContractLocker) AcquireContract(ctx context.Context, fcid types.FileContractID, priority int, d time.Duration) (lockID uint64, err error) {
	if lock, ok := cl.contracts[fcid]; !ok {
		return 0, errContractNotFound
	} else {
		lock.mu.Lock()
	}

	return 0, nil
}

func (cl *mockContractLocker) ReleaseContract(ctx context.Context, fcid types.FileContractID, lockID uint64) (err error) {
	if lock, ok := cl.contracts[fcid]; !ok {
		return errContractNotFound
	} else {
		lock.mu.Unlock()
	}
	return nil
}

func (cl *mockContractLocker) KeepaliveContract(ctx context.Context, fcid types.FileContractID, lockID uint64, d time.Duration) (err error) {
	return nil
}

func newMockHosts(n int) []*mockHost {
	hosts := make([]*mockHost, n)
	for i := range hosts {
		hosts[i] = newMockHost(types.PublicKey{byte(i)}, nil)
	}
	return hosts
}

func newMockHost(hk types.PublicKey, c *mockContract) *mockHost {
	return &mockHost{
		hk: hk,
		c:  c,
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

func newMockContractLocker(contracts []*mockContract) *mockContractLocker {
	cl := &mockContractLocker{contracts: make(map[types.FileContractID]*mockContract)}
	for _, c := range contracts {
		cl.contracts[c.rev.ParentID] = c
	}
	return cl
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
