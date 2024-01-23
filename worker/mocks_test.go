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
	mockContract struct {
		rev      types.FileContractRevision
		metadata api.ContractMetadata

		mu      sync.Mutex
		sectors map[types.Hash256]*[rhpv2.SectorSize]byte
	}

	mockContractStore struct {
		mu    sync.Mutex
		locks map[types.FileContractID]*sync.Mutex
	}

	mockHost struct {
		hk types.PublicKey

		mu sync.Mutex
		c  *mockContract

		hpt          hostdb.HostPriceTable
		hptBlockChan chan struct{}
	}

	mockHostManager struct {
		mu    sync.Mutex
		hosts map[types.PublicKey]*mockHost
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

		mu       sync.Mutex
		hkCntr   uint
		fcidCntr uint
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

type (
	mockHosts     []*mockHost
	mockContracts []*mockContract
)

func (hosts mockHosts) contracts() mockContracts {
	contracts := make([]*mockContract, len(hosts))
	for i, host := range hosts {
		contracts[i] = host.c
	}
	return contracts
}

func (contracts mockContracts) metadata() []api.ContractMetadata {
	metadata := make([]api.ContractMetadata, len(contracts))
	for i, contract := range contracts {
		metadata[i] = contract.metadata
	}
	return metadata
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

func newMockContractStore() *mockContractStore {
	return &mockContractStore{
		locks: make(map[types.FileContractID]*sync.Mutex),
	}
}

func (cs *mockContractStore) AcquireContract(ctx context.Context, fcid types.FileContractID, priority int, d time.Duration) (lockID uint64, err error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if lock, ok := cs.locks[fcid]; !ok {
		return 0, errContractNotFound
	} else {
		lock.Lock()
	}
	return 0, nil
}

func (cs *mockContractStore) ReleaseContract(ctx context.Context, fcid types.FileContractID, lockID uint64) (err error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if lock, ok := cs.locks[fcid]; !ok {
		return errContractNotFound
	} else {
		lock.Unlock()
	}
	return nil
}

func (cs *mockContractStore) KeepaliveContract(ctx context.Context, fcid types.FileContractID, lockID uint64, d time.Duration) (err error) {
	return nil
}

func (os *mockContractStore) RenewedContract(ctx context.Context, renewedFrom types.FileContractID) (api.ContractMetadata, error) {
	return api.ContractMetadata{}, api.ErrContractNotFound
}

func newMockObjectStore() *mockObjectStore {
	os := &mockObjectStore{
		objects:  make(map[string]map[string]object.Object),
		partials: make(map[string]mockPackedSlab),
	}
	os.objects[testBucket] = make(map[string]object.Object)
	return os
}

func (cs *mockContractStore) addContract(c *mockContract) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.locks[c.metadata.ID] = new(sync.Mutex)
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

func newMockHost(hk types.PublicKey) *mockHost {
	return &mockHost{
		hk:  hk,
		hpt: newTestHostPriceTable(time.Now().Add(time.Minute)),
	}
}

func (h *mockHost) DownloadSector(ctx context.Context, w io.Writer, root types.Hash256, offset, length uint32, overpay bool) error {
	sector, exist := h.contract().sector(root)
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
	return h.contract().addSector(sector), nil
}

func (h *mockHost) FetchRevision(ctx context.Context, fetchTimeout time.Duration) (rev types.FileContractRevision, _ error) {
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
	return rhpv2.ContractRevision{}, nil, types.ZeroCurrency, nil
}

func (h *mockHost) SyncAccount(ctx context.Context, rev *types.FileContractRevision) error {
	return nil
}

func (h *mockHost) contract() (c *mockContract) {
	h.mu.Lock()
	c = h.c
	h.mu.Unlock()

	if c == nil {
		panic("host does not have a contract")
	}
	return
}

func newMockContract(hk types.PublicKey, fcid types.FileContractID) *mockContract {
	return &mockContract{
		metadata: api.ContractMetadata{
			ID:          fcid,
			HostKey:     hk,
			WindowStart: 0,
			WindowEnd:   10,
		},
		rev:     types.FileContractRevision{ParentID: fcid},
		sectors: make(map[types.Hash256]*[rhpv2.SectorSize]byte),
	}
}

func (c *mockContract) addSector(sector *[rhpv2.SectorSize]byte) (root types.Hash256) {
	root = rhpv2.SectorRoot(sector)
	c.mu.Lock()
	c.sectors[root] = sector
	c.mu.Unlock()
	return
}

func (c *mockContract) sector(root types.Hash256) (sector *[rhpv2.SectorSize]byte, found bool) {
	c.mu.Lock()
	sector, found = c.sectors[root]
	c.mu.Unlock()
	return
}

func newMockHostManager() *mockHostManager {
	return &mockHostManager{
		hosts: make(map[types.PublicKey]*mockHost),
	}
}

func (hm *mockHostManager) Host(hk types.PublicKey, fcid types.FileContractID, siamuxAddr string) Host {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	if _, ok := hm.hosts[hk]; !ok {
		panic("host not found")
	}
	return hm.hosts[hk]
}

func (hm *mockHostManager) addHost(h *mockHost) {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	if _, ok := hm.hosts[h.hk]; ok {
		panic("host already exists")
	}

	hm.hosts[h.hk] = h
}

func (hm *mockHostManager) host(hk types.PublicKey) *mockHost {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	return hm.hosts[hk]
}

func newMockSector() (*[rhpv2.SectorSize]byte, types.Hash256) {
	var sector [rhpv2.SectorSize]byte
	frand.Read(sector[:])
	return &sector, rhpv2.SectorRoot(&sector)
}

func newMockWorker() *mockWorker {
	cs := newMockContractStore()
	hm := newMockHostManager()
	os := newMockObjectStore()
	mm := &mockMemoryManager{}

	return &mockWorker{
		cs: cs,
		hm: hm,
		mm: mm,
		os: os,

		dl: newDownloadManager(context.Background(), hm, mm, os, 0, 0, zap.NewNop().Sugar()),
		ul: newUploadManager(context.Background(), hm, mm, os, cs, 0, 0, time.Minute, zap.NewNop().Sugar()),
	}
}

func (w *mockWorker) addHosts(n int) {
	for i := 0; i < n; i++ {
		w.addHost()
	}
}

func (w *mockWorker) addHost() *mockHost {
	host := newMockHost(w.newHostKey())
	w.hm.addHost(host)
	w.formContractWithHost(host.hk)
	return host
}

func (w *mockWorker) formContractWithHost(hk types.PublicKey) *mockContract {
	host := w.hm.host(hk)
	if host == nil {
		panic("host not found")
	} else if host.c != nil {
		panic("host already has contract, use renew")
	}

	host.c = newMockContract(host.hk, w.newFileContractID())
	w.cs.addContract(host.c)
	return host.c
}

func (w *mockWorker) renewContractWithHost(hk types.PublicKey) *mockContract {
	host := w.hm.host(hk)
	if host == nil {
		panic("host not found")
	} else if host.c == nil {
		panic("host does not have a contract to renew")
	}

	curr := host.c.metadata
	update := newMockContract(host.hk, w.newFileContractID())
	update.metadata.RenewedFrom = curr.ID
	update.metadata.WindowStart = curr.WindowEnd
	update.metadata.WindowEnd = update.metadata.WindowStart + (curr.WindowEnd - curr.WindowStart)
	host.c = update

	w.cs.addContract(host.c)
	return host.c
}

func (w *mockWorker) contracts() (metadatas []api.ContractMetadata) {
	for _, h := range w.hm.hosts {
		metadatas = append(metadatas, h.c.metadata)
	}
	return
}

func (w *mockWorker) newHostKey() (hk types.PublicKey) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.hkCntr++
	hk = types.PublicKey{byte(w.hkCntr)}
	return
}

func (w *mockWorker) newFileContractID() (fcid types.FileContractID) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.fcidCntr++
	fcid = types.FileContractID{byte(w.fcidCntr)}
	return
}

func newTestHostPriceTable(expiry time.Time) hostdb.HostPriceTable {
	var uid rhpv3.SettingsID
	frand.Read(uid[:])

	return hostdb.HostPriceTable{
		HostPriceTable: rhpv3.HostPriceTable{UID: uid, Validity: time.Minute},
		Expiry:         expiry,
	}
}
