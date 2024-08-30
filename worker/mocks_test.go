package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/alerts"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/gouging"
	"go.sia.tech/renterd/object"
	"go.sia.tech/renterd/webhooks"
)

type accountsMock struct{}

func (*accountsMock) Accounts(context.Context, string) ([]api.Account, error) {
	return nil, nil
}

func (*accountsMock) UpdateAccounts(context.Context, []api.Account) error {
	return nil
}

var _ alerts.Alerter = (*alerterMock)(nil)

type alerterMock struct{}

func (*alerterMock) Alerts(_ context.Context, opts alerts.AlertsOpts) (resp alerts.AlertsResponse, err error) {
	return alerts.AlertsResponse{}, nil
}
func (*alerterMock) RegisterAlert(context.Context, alerts.Alert) error     { return nil }
func (*alerterMock) DismissAlerts(context.Context, ...types.Hash256) error { return nil }

var _ gouging.ConsensusState = (*chainMock)(nil)

type chainMock struct {
	cs api.ConsensusState
}

func (c *chainMock) ConsensusState(ctx context.Context) (api.ConsensusState, error) {
	return c.cs, nil
}

type busMock struct {
	*alerterMock
	*accountsMock
	*chainMock
	*contractLockerMock
	*contractStoreMock
	*hostStoreMock
	*objectStoreMock
	*settingStoreMock
	*syncerMock
	*s3Mock
	*walletMock
	*webhookBroadcasterMock
	*webhookStoreMock
}

func newBusMock(cs *contractStoreMock, hs *hostStoreMock, os *objectStoreMock) *busMock {
	return &busMock{
		alerterMock:            &alerterMock{},
		accountsMock:           &accountsMock{},
		chainMock:              &chainMock{},
		contractLockerMock:     newContractLockerMock(),
		contractStoreMock:      cs,
		hostStoreMock:          hs,
		objectStoreMock:        os,
		settingStoreMock:       &settingStoreMock{},
		syncerMock:             &syncerMock{},
		walletMock:             &walletMock{},
		webhookBroadcasterMock: &webhookBroadcasterMock{},
	}
}

type contractMock struct {
	rev      types.FileContractRevision
	metadata api.ContractMetadata

	mu      sync.Mutex
	sectors map[types.Hash256]*[rhpv2.SectorSize]byte
}

func newContractMock(hk types.PublicKey, fcid types.FileContractID) *contractMock {
	return &contractMock{
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

func (c *contractMock) AddSector(root types.Hash256, sector *[rhpv2.SectorSize]byte) {
	c.mu.Lock()
	c.sectors[root] = sector
	c.mu.Unlock()
}

func (c *contractMock) Sector(root types.Hash256) (sector *[rhpv2.SectorSize]byte, found bool) {
	c.mu.Lock()
	sector, found = c.sectors[root]
	c.mu.Unlock()
	return
}

var _ ContractLocker = (*contractLockerMock)(nil)

type contractLockerMock struct {
	mu    sync.Mutex
	locks map[types.FileContractID]*sync.Mutex
}

func newContractLockerMock() *contractLockerMock {
	return &contractLockerMock{
		locks: make(map[types.FileContractID]*sync.Mutex),
	}
}

func (cs *contractLockerMock) AcquireContract(_ context.Context, fcid types.FileContractID, _ int, _ time.Duration) (uint64, error) {
	cs.mu.Lock()
	lock, exists := cs.locks[fcid]
	if !exists {
		cs.locks[fcid] = new(sync.Mutex)
		lock = cs.locks[fcid]
	}
	cs.mu.Unlock()

	lock.Lock()
	return 0, nil
}

func (cs *contractLockerMock) ReleaseContract(_ context.Context, fcid types.FileContractID, _ uint64) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	cs.locks[fcid].Unlock()
	return nil
}

func (*contractLockerMock) KeepaliveContract(context.Context, types.FileContractID, uint64, time.Duration) error {
	return nil
}

var _ ContractStore = (*contractStoreMock)(nil)

type contractStoreMock struct {
	mu         sync.Mutex
	contracts  map[types.FileContractID]*contractMock
	hosts2fcid map[types.PublicKey]types.FileContractID
	fcidCntr   uint
}

func newContractStoreMock() *contractStoreMock {
	return &contractStoreMock{
		contracts:  make(map[types.FileContractID]*contractMock),
		hosts2fcid: make(map[types.PublicKey]types.FileContractID),
	}
}

func (*contractStoreMock) RenewedContract(context.Context, types.FileContractID) (api.ContractMetadata, error) {
	return api.ContractMetadata{}, nil
}

func (*contractStoreMock) Contract(context.Context, types.FileContractID) (api.ContractMetadata, error) {
	return api.ContractMetadata{}, nil
}

func (*contractStoreMock) ContractSize(context.Context, types.FileContractID) (api.ContractSize, error) {
	return api.ContractSize{}, nil
}

func (*contractStoreMock) ContractRoots(context.Context, types.FileContractID) ([]types.Hash256, []types.Hash256, error) {
	return nil, nil, nil
}

func (cs *contractStoreMock) Contracts(context.Context, api.ContractsOpts) (metadatas []api.ContractMetadata, _ error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	for _, c := range cs.contracts {
		metadatas = append(metadatas, c.metadata)
	}
	return
}

func (cs *contractStoreMock) addContract(hk types.PublicKey) *contractMock {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	fcid := cs.newFileContractID()
	cs.contracts[fcid] = newContractMock(hk, fcid)
	cs.hosts2fcid[hk] = fcid
	return cs.contracts[fcid]
}

func (cs *contractStoreMock) renewContract(hk types.PublicKey) (*contractMock, error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	curr := cs.hosts2fcid[hk]
	c := cs.contracts[curr]
	if c == nil {
		return nil, errors.New("host does not have a contract to renew")
	}
	delete(cs.contracts, curr)

	renewal := newContractMock(hk, cs.newFileContractID())
	renewal.metadata.RenewedFrom = c.metadata.ID
	renewal.metadata.WindowStart = c.metadata.WindowEnd
	renewal.metadata.WindowEnd = renewal.metadata.WindowStart + (c.metadata.WindowEnd - c.metadata.WindowStart)
	cs.contracts[renewal.metadata.ID] = renewal
	cs.hosts2fcid[hk] = renewal.metadata.ID
	return renewal, nil
}

func (cs *contractStoreMock) newFileContractID() types.FileContractID {
	cs.fcidCntr++
	return types.FileContractID{byte(cs.fcidCntr)}
}

var errSectorOutOfBounds = errors.New("sector out of bounds")

type hostMock struct {
	hk types.PublicKey
	hi api.Host
}

func newHostMock(hk types.PublicKey) *hostMock {
	return &hostMock{
		hk: hk,
		hi: api.Host{
			PublicKey: hk,
			Scanned:   true,
		},
	}
}

var _ HostStore = (*hostStoreMock)(nil)

type hostStoreMock struct {
	mu     sync.Mutex
	hosts  map[types.PublicKey]*hostMock
	hkCntr uint
}

func newHostStoreMock() *hostStoreMock {
	return &hostStoreMock{hosts: make(map[types.PublicKey]*hostMock)}
}

func (hs *hostStoreMock) Host(ctx context.Context, hostKey types.PublicKey) (api.Host, error) {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	h, ok := hs.hosts[hostKey]
	if !ok {
		return api.Host{}, api.ErrHostNotFound
	}
	return h.hi, nil
}

func (hs *hostStoreMock) RecordHostScans(ctx context.Context, scans []api.HostScan) error {
	return nil
}

func (hs *hostStoreMock) RecordPriceTables(ctx context.Context, priceTableUpdate []api.HostPriceTableUpdate) error {
	return nil
}

func (hs *hostStoreMock) RecordContractSpending(ctx context.Context, records []api.ContractSpendingRecord) error {
	return nil
}

func (hs *hostStoreMock) addHost() *hostMock {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	hs.hkCntr++
	hk := types.PublicKey{byte(hs.hkCntr)}
	hs.hosts[hk] = newHostMock(hk)
	return hs.hosts[hk]
}

var (
	_ MemoryManager = (*memoryManagerMock)(nil)
	_ Memory        = (*memoryMock)(nil)
)

type (
	memoryMock        struct{}
	memoryManagerMock struct{ memBlockChan chan struct{} }
)

func newMemoryManagerMock() *memoryManagerMock {
	mm := &memoryManagerMock{memBlockChan: make(chan struct{})}
	close(mm.memBlockChan)
	return mm
}

func (m *memoryMock) Release()           {}
func (m *memoryMock) ReleaseSome(uint64) {}

func (mm *memoryManagerMock) Limit(amt uint64) (MemoryManager, error) {
	return mm, nil
}

func (mm *memoryManagerMock) Status() api.MemoryStatus { return api.MemoryStatus{} }

func (mm *memoryManagerMock) AcquireMemory(ctx context.Context, amt uint64) Memory {
	<-mm.memBlockChan
	return &memoryMock{}
}

var _ ObjectStore = (*objectStoreMock)(nil)

type (
	objectStoreMock struct {
		mu                    sync.Mutex
		objects               map[string]map[string]object.Object
		partials              map[string]*packedSlabMock
		slabBufferMaxSizeSoft int
		bufferIDCntr          uint // allows marking packed slabs as uploaded
	}

	packedSlabMock struct {
		parameterKey string // ([minshards]-[totalshards]-[contractset])
		bufferID     uint
		slabKey      object.EncryptionKey
		data         []byte
		lockedUntil  time.Time
	}
)

func newObjectStoreMock(bucket string) *objectStoreMock {
	os := &objectStoreMock{
		objects:               make(map[string]map[string]object.Object),
		partials:              make(map[string]*packedSlabMock),
		slabBufferMaxSizeSoft: math.MaxInt64,
	}
	os.objects[bucket] = make(map[string]object.Object)
	return os
}

func (os *objectStoreMock) AddMultipartPart(ctx context.Context, bucket, path, contractSet, eTag, uploadID string, partNumber int, slices []object.SlabSlice) (err error) {
	return nil
}

func (os *objectStoreMock) AddUploadingSector(ctx context.Context, uID api.UploadID, id types.FileContractID, root types.Hash256) error {
	return nil
}

func (os *objectStoreMock) TrackUpload(ctx context.Context, uID api.UploadID) error { return nil }

func (os *objectStoreMock) FinishUpload(ctx context.Context, uID api.UploadID) error { return nil }

func (os *objectStoreMock) DeleteHostSector(ctx context.Context, hk types.PublicKey, root types.Hash256) error {
	os.mu.Lock()
	defer os.mu.Unlock()

	for _, objects := range os.objects {
		for _, object := range objects {
			for _, slab := range object.Slabs {
				for _, shard := range slab.Slab.Shards {
					if shard.Root == root {
						delete(shard.Contracts, hk)
					}
				}
			}
		}
	}

	return nil
}

func (os *objectStoreMock) DeleteObject(ctx context.Context, bucket, path string, opts api.DeleteObjectOptions) error {
	return nil
}

func (os *objectStoreMock) AddObject(ctx context.Context, bucket, path, contractSet string, o object.Object, opts api.AddObjectOptions) error {
	os.mu.Lock()
	defer os.mu.Unlock()

	// check if the bucket exists
	if _, exists := os.objects[bucket]; !exists {
		return api.ErrBucketNotFound
	}

	os.objects[bucket][path] = o
	return nil
}

func (os *objectStoreMock) AddPartialSlab(ctx context.Context, data []byte, minShards, totalShards uint8, contractSet string) (slabs []object.SlabSlice, slabBufferMaxSizeSoftReached bool, err error) {
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
	os.partials[ec.String()] = &packedSlabMock{
		parameterKey: fmt.Sprintf("%d-%d-%v", minShards, totalShards, contractSet),
		bufferID:     os.bufferIDCntr,
		slabKey:      ec,
		data:         data,
	}
	os.bufferIDCntr++

	return []object.SlabSlice{ss}, os.totalSlabBufferSize() > os.slabBufferMaxSizeSoft, nil
}

func (os *objectStoreMock) Object(ctx context.Context, bucket, path string, opts api.GetObjectOptions) (api.ObjectsResponse, error) {
	os.mu.Lock()
	defer os.mu.Unlock()

	// check if the bucket exists
	if _, exists := os.objects[bucket]; !exists {
		return api.ObjectsResponse{}, api.ErrBucketNotFound
	}

	// check if the object exists
	if _, exists := os.objects[bucket][path]; !exists {
		return api.ObjectsResponse{}, api.ErrObjectNotFound
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
		Object:         &o,
	}}, nil
}

func (os *objectStoreMock) FetchPartialSlab(ctx context.Context, key object.EncryptionKey, offset, length uint32) ([]byte, error) {
	os.mu.Lock()
	defer os.mu.Unlock()

	packedSlab, exists := os.partials[key.String()]
	if !exists {
		return nil, api.ErrSlabNotFound
	}
	if offset+length > uint32(len(packedSlab.data)) {
		return nil, errors.New("offset out of bounds")
	}

	return packedSlab.data[offset : offset+length], nil
}

func (os *objectStoreMock) Slab(ctx context.Context, key object.EncryptionKey) (slab object.Slab, err error) {
	os.mu.Lock()
	defer os.mu.Unlock()

	os.forEachObject(func(bucket, path string, o object.Object) {
		for _, s := range o.Slabs {
			if s.Slab.Key.String() == key.String() {
				slab = s.Slab
				return
			}
		}
		err = api.ErrSlabNotFound
	})
	return
}

func (os *objectStoreMock) UpdateSlab(ctx context.Context, s object.Slab, contractSet string) error {
	os.mu.Lock()
	defer os.mu.Unlock()

	os.forEachObject(func(bucket, path string, o object.Object) {
		for i, slab := range o.Slabs {
			if slab.Key.String() != s.Key.String() {
				continue
			}
			// update slab
			shards := os.objects[bucket][path].Slabs[i].Slab.Shards
			for sI := range shards {
				// overwrite latest host
				shards[sI].LatestHost = s.Shards[sI].LatestHost

				// merge contracts for each shard
				existingContracts := make(map[types.FileContractID]struct{})
				for _, fcids := range shards[sI].Contracts {
					for _, fcid := range fcids {
						existingContracts[fcid] = struct{}{}
					}
				}
				for hk, fcids := range s.Shards[sI].Contracts {
					for _, fcid := range fcids {
						if _, exists := existingContracts[fcid]; exists {
							continue
						}
						shards[sI].Contracts[hk] = append(shards[sI].Contracts[hk], fcids...)
					}
				}
			}
			os.objects[bucket][path].Slabs[i].Slab.Shards = shards
			return
		}
	})

	return nil
}

func (os *objectStoreMock) PackedSlabsForUpload(ctx context.Context, lockingDuration time.Duration, minShards, totalShards uint8, set string, limit int) (pss []api.PackedSlab, _ error) {
	os.mu.Lock()
	defer os.mu.Unlock()

	if limit == -1 {
		limit = math.MaxInt
	}

	parameterKey := fmt.Sprintf("%d-%d-%v", minShards, totalShards, set)
	for _, ps := range os.partials {
		if ps.parameterKey == parameterKey && time.Now().After(ps.lockedUntil) {
			ps.lockedUntil = time.Now().Add(lockingDuration)
			pss = append(pss, api.PackedSlab{
				BufferID: ps.bufferID,
				Data:     ps.data,
				Key:      ps.slabKey,
			})
			if len(pss) == limit {
				break
			}
		}
	}
	return
}

func (os *objectStoreMock) MarkPackedSlabsUploaded(ctx context.Context, slabs []api.UploadedPackedSlab) error {
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

func (os *objectStoreMock) Bucket(_ context.Context, bucket string) (api.Bucket, error) {
	return api.Bucket{}, nil
}

func (os *objectStoreMock) MultipartUpload(ctx context.Context, uploadID string) (resp api.MultipartUpload, err error) {
	return api.MultipartUpload{}, nil
}

func (os *objectStoreMock) totalSlabBufferSize() (total int) {
	for _, p := range os.partials {
		if time.Now().After(p.lockedUntil) {
			total += len(p.data)
		}
	}
	return
}

func (os *objectStoreMock) setSlabBufferMaxSizeSoft(n int) {
	os.mu.Lock()
	defer os.mu.Unlock()
	os.slabBufferMaxSizeSoft = n
}

func (os *objectStoreMock) forEachObject(fn func(bucket, path string, o object.Object)) {
	for bucket, objects := range os.objects {
		for path, object := range objects {
			fn(bucket, path, object)
		}
	}
}

type s3Mock struct{}

func (*s3Mock) CreateBucket(context.Context, string, api.CreateBucketOptions) error {
	return nil
}

func (*s3Mock) DeleteBucket(context.Context, string) error {
	return nil
}

func (*s3Mock) ListBuckets(context.Context) (buckets []api.Bucket, err error) {
	return nil, nil
}

func (*s3Mock) CopyObject(context.Context, string, string, string, string, api.CopyObjectOptions) (om api.ObjectMetadata, err error) {
	return api.ObjectMetadata{}, nil
}

func (*s3Mock) ListObjects(context.Context, string, api.ListObjectOptions) (resp api.ObjectsListResponse, err error) {
	return api.ObjectsListResponse{}, nil
}

func (*s3Mock) AbortMultipartUpload(context.Context, string, string, string) (err error) {
	return nil
}

func (*s3Mock) CompleteMultipartUpload(context.Context, string, string, string, []api.MultipartCompletedPart, api.CompleteMultipartOptions) (_ api.MultipartCompleteResponse, err error) {
	return api.MultipartCompleteResponse{}, nil
}

func (*s3Mock) CreateMultipartUpload(context.Context, string, string, api.CreateMultipartOptions) (api.MultipartCreateResponse, error) {
	return api.MultipartCreateResponse{}, nil
}

func (*s3Mock) MultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker string, maxUploads int) (resp api.MultipartListUploadsResponse, _ error) {
	return api.MultipartListUploadsResponse{}, nil
}

func (*s3Mock) MultipartUploadParts(ctx context.Context, bucket, object string, uploadID string, marker int, limit int64) (resp api.MultipartListPartsResponse, _ error) {
	return api.MultipartListPartsResponse{}, nil
}

func (*s3Mock) S3AuthenticationSettings(context.Context) (as api.S3AuthenticationSettings, err error) {
	return api.S3AuthenticationSettings{}, nil
}

func (*s3Mock) UpdateSetting(context.Context, string, interface{}) error {
	return nil
}

var _ SettingStore = (*settingStoreMock)(nil)

type settingStoreMock struct{}

func (*settingStoreMock) GougingParams(context.Context) (api.GougingParams, error) {
	return api.GougingParams{}, nil
}

func (*settingStoreMock) UploadParams(context.Context) (api.UploadParams, error) {
	return api.UploadParams{}, nil
}

var _ Syncer = (*syncerMock)(nil)

type syncerMock struct{}

func (*syncerMock) BroadcastTransaction(context.Context, []types.Transaction) error {
	return nil
}

func (*syncerMock) SyncerPeers(context.Context) ([]string, error) {
	return nil, nil
}

var _ Wallet = (*walletMock)(nil)

type walletMock struct{}

func (*walletMock) WalletDiscard(context.Context, types.Transaction) error {
	return nil
}

func (*walletMock) WalletFund(context.Context, *types.Transaction, types.Currency, bool) ([]types.Hash256, []types.Transaction, error) {
	return nil, nil, nil
}

func (*walletMock) WalletSign(context.Context, *types.Transaction, []types.Hash256, types.CoveredFields) error {
	return nil
}

var _ webhooks.Broadcaster = (*webhookBroadcasterMock)(nil)

type webhookBroadcasterMock struct{}

func (*webhookBroadcasterMock) BroadcastAction(context.Context, webhooks.Event) error {
	return nil
}

var _ WebhookStore = (*webhookStoreMock)(nil)

type webhookStoreMock struct{}

func (*webhookStoreMock) RegisterWebhook(ctx context.Context, webhook webhooks.Webhook) error {
	return nil
}

func (*webhookStoreMock) UnregisterWebhook(ctx context.Context, webhook webhooks.Webhook) error {
	return nil
}
