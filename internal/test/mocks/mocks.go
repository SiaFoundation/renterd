package mocks

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"sync"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/alerts"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/gouging"
	"go.sia.tech/renterd/internal/memory"
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

var _ gouging.ConsensusState = (*Chain)(nil)

type Chain struct {
	cs api.ConsensusState
}

func NewChain(cs api.ConsensusState) *Chain {
	return &Chain{cs: cs}
}

func (c *Chain) ConsensusState(ctx context.Context) (api.ConsensusState, error) {
	return c.cs, nil
}

func (c *Chain) UpdateHeight(bh uint64) {
	c.cs.BlockHeight = bh
}

type busMock struct {
	*alerterMock
	*accountsMock
	*Chain
	*contractLockerMock
	*ContractStore
	*HostStore
	*ObjectStore
	*settingStoreMock
	*syncerMock
	*s3Mock
	*webhookBroadcasterMock
	*webhookStoreMock
}

func NewBus(cs *ContractStore, hs *HostStore, os *ObjectStore) *busMock {
	return &busMock{
		alerterMock:            &alerterMock{},
		accountsMock:           &accountsMock{},
		Chain:                  &Chain{},
		contractLockerMock:     newContractLockerMock(),
		ContractStore:          cs,
		HostStore:              hs,
		ObjectStore:            os,
		settingStoreMock:       &settingStoreMock{},
		syncerMock:             &syncerMock{},
		webhookBroadcasterMock: &webhookBroadcasterMock{},
	}
}

func (b *busMock) FundAccount(ctx context.Context, acc rhpv3.Account, fcid types.FileContractID, desired types.Currency) (types.Currency, error) {
	return types.ZeroCurrency, nil
}

type Contract struct {
	rev      types.FileContractRevision
	metadata api.ContractMetadata

	mu      sync.Mutex
	sectors map[types.Hash256]*[rhpv2.SectorSize]byte
}

func NewContract(hk types.PublicKey, fcid types.FileContractID) *Contract {
	return &Contract{
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

func (c *Contract) AddSector(root types.Hash256, sector *[rhpv2.SectorSize]byte) {
	c.mu.Lock()
	c.sectors[root] = sector
	c.mu.Unlock()
}

func (c *Contract) ID() types.FileContractID {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.metadata.ID
}

func (c *Contract) Metadata() api.ContractMetadata {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.metadata
}

func (c *Contract) Revision() types.FileContractRevision {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rev
}

func (c *Contract) Sector(root types.Hash256) (sector *[rhpv2.SectorSize]byte, found bool) {
	c.mu.Lock()
	sector, found = c.sectors[root]
	c.mu.Unlock()
	return
}

var ErrSectorOutOfBounds = errors.New("sector out of bounds")

type Host struct {
	hk types.PublicKey
	hi api.Host
}

func NewHost(hk types.PublicKey) *Host {
	return &Host{
		hk: hk,
		hi: api.Host{
			PublicKey: hk,
			Scanned:   true,
		},
	}
}

func (h *Host) UpdatePriceTable(pt api.HostPriceTable) {
	h.hi.PriceTable = pt
}

func (h *Host) PriceTable() api.HostPriceTable {
	return h.hi.PriceTable
}

func (h *Host) PublicKey() types.PublicKey {
	return h.hk
}

type (
	Memory        struct{}
	MemoryManager struct{ memBlockChan chan struct{} }
)

func NewMemoryManager() *MemoryManager {
	mm := &MemoryManager{memBlockChan: make(chan struct{})}
	close(mm.memBlockChan)
	return mm
}

func (mm *MemoryManager) Block() func() {
	select {
	case <-mm.memBlockChan:
		log.Fatal("already blocking")
	default:
	}
	blockChan := make(chan struct{})
	mm.memBlockChan = blockChan
	return func() { close(blockChan) }
}

func (m *Memory) Release()           {}
func (m *Memory) ReleaseSome(uint64) {}

func (mm *MemoryManager) Limit(amt uint64) (memory.MemoryManager, error) {
	return mm, nil
}

func (mm *MemoryManager) Status() memory.Status { return memory.Status{} }

func (mm *MemoryManager) AcquireMemory(ctx context.Context, amt uint64) memory.Memory {
	<-mm.memBlockChan
	return &Memory{}
}

type (
	ObjectStore struct {
		cs *ContractStore // TODO: remove

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

func NewObjectStore(bucket string, cs *ContractStore) *ObjectStore {
	os := &ObjectStore{
		cs:                    cs,
		objects:               make(map[string]map[string]object.Object),
		partials:              make(map[string]*packedSlabMock),
		slabBufferMaxSizeSoft: math.MaxInt64,
	}
	os.objects[bucket] = make(map[string]object.Object)
	return os
}

func (os *ObjectStore) AddMultipartPart(ctx context.Context, bucket, path, contractSet, eTag, uploadID string, partNumber int, slices []object.SlabSlice) (err error) {
	return nil
}

func (os *ObjectStore) AddUploadingSector(ctx context.Context, uID api.UploadID, id types.FileContractID, root types.Hash256) error {
	return nil
}

func (os *ObjectStore) NumPartials() int {
	os.mu.Lock()
	defer os.mu.Unlock()
	return len(os.partials)
}

func (os *ObjectStore) TrackUpload(ctx context.Context, uID api.UploadID) error { return nil }

func (os *ObjectStore) FinishUpload(ctx context.Context, uID api.UploadID) error { return nil }

func (os *ObjectStore) DeleteHostSector(ctx context.Context, hk types.PublicKey, root types.Hash256) error {
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

func (os *ObjectStore) DeleteObject(ctx context.Context, bucket, key string) error {
	return nil
}

func (os *ObjectStore) AddObject(ctx context.Context, bucket, path, contractSet string, o object.Object, opts api.AddObjectOptions) error {
	os.mu.Lock()
	defer os.mu.Unlock()

	// check if the bucket exists
	if _, exists := os.objects[bucket]; !exists {
		return api.ErrBucketNotFound
	}

	os.objects[bucket][path] = o
	return nil
}

func (os *ObjectStore) AddPartialSlab(ctx context.Context, data []byte, minShards, totalShards uint8, contractSet string) (slabs []object.SlabSlice, slabBufferMaxSizeSoftReached bool, err error) {
	os.mu.Lock()
	defer os.mu.Unlock()

	// check if given data is too big
	slabSize := int(minShards) * int(rhpv2.SectorSize)
	if len(data) > slabSize {
		return nil, false, fmt.Errorf("data size %v exceeds size of a slab %v", len(data), slabSize)
	}

	// create slab
	ec := object.GenerateEncryptionKey(object.EncryptionKeyTypeSalted)
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

func (os *ObjectStore) Object(ctx context.Context, bucket, key string, opts api.GetObjectOptions) (api.Object, error) {
	os.mu.Lock()
	defer os.mu.Unlock()

	// check if the bucket exists
	if _, exists := os.objects[bucket]; !exists {
		return api.Object{}, api.ErrBucketNotFound
	}

	// check if the object exists
	if _, exists := os.objects[bucket][key]; !exists {
		return api.Object{}, api.ErrObjectNotFound
	}

	// clone to ensure the store isn't unwillingly modified
	var o object.Object
	if b, err := json.Marshal(os.objects[bucket][key]); err != nil {
		panic(err)
	} else if err := json.Unmarshal(b, &o); err != nil {
		panic(err)
	}

	return api.Object{
		ObjectMetadata: api.ObjectMetadata{Key: key, Size: o.TotalSize()},
		Object:         &o,
	}, nil
}

func (os *ObjectStore) FetchPartialSlab(ctx context.Context, key object.EncryptionKey, offset, length uint32) ([]byte, error) {
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

func (os *ObjectStore) Slab(ctx context.Context, key object.EncryptionKey) (slab object.Slab, err error) {
	os.mu.Lock()
	defer os.mu.Unlock()

	os.forEachObject(func(bucket, objKey string, o object.Object) {
		for _, s := range o.Slabs {
			if s.Slab.EncryptionKey.String() == key.String() {
				slab = s.Slab
				return
			}
		}
		err = api.ErrSlabNotFound
	})
	return
}

func (os *ObjectStore) UpdateSlab(ctx context.Context, key object.EncryptionKey, sectors []api.UploadedSector) error {
	os.mu.Lock()
	defer os.mu.Unlock()

	updated := make(map[types.Hash256]types.FileContractID)
	for _, sector := range sectors {
		_, exists := updated[sector.Root]
		if exists {
			return errors.New("duplicate sector")
		}
		updated[sector.Root] = sector.ContractID
	}

	var err error
	os.forEachObject(func(bucket, objKey string, o object.Object) {
		for i, slab := range o.Slabs {
			if slab.EncryptionKey.String() != key.String() {
				continue
			}

			shards := os.objects[bucket][objKey].Slabs[i].Slab.Shards
			for _, shard := range shards {
				if contract, ok := updated[shard.Root]; !ok {
					continue // not updated
				} else {
					var hk types.PublicKey
					hk, err = os.hostForContract(ctx, contract)
					if err != nil {
						return
					}
					shard.Contracts[hk] = append(shard.Contracts[hk], contract)
				}
			}
			os.objects[bucket][objKey].Slabs[i].Slab.Shards = shards
			return
		}
	})

	return err
}

func (os *ObjectStore) PackedSlabsForUpload(ctx context.Context, lockingDuration time.Duration, minShards, totalShards uint8, set string, limit int) (pss []api.PackedSlab, _ error) {
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
				BufferID:      ps.bufferID,
				Data:          ps.data,
				EncryptionKey: ps.slabKey,
			})
			if len(pss) == limit {
				break
			}
		}
	}
	return
}

func (os *ObjectStore) Objects(ctx context.Context, prefix string, opts api.ListObjectOptions) (resp api.ObjectsResponse, err error) {
	return api.ObjectsResponse{}, nil
}

func (os *ObjectStore) MarkPackedSlabsUploaded(ctx context.Context, slabs []api.UploadedPackedSlab) error {
	os.mu.Lock()
	defer os.mu.Unlock()

	bufferIDToKey := make(map[uint]string)
	for key, ps := range os.partials {
		bufferIDToKey[ps.bufferID] = key
	}

	slabKeyToSlab := make(map[string]*object.Slab)
	os.forEachObject(func(bucket, objKey string, o object.Object) {
		for i, slab := range o.Slabs {
			slabKeyToSlab[slab.Slab.EncryptionKey.String()] = &os.objects[bucket][objKey].Slabs[i].Slab
		}
	})

	for _, slab := range slabs {
		var sectors []object.Sector
		for _, shard := range slab.Shards {
			hk, err := os.hostForContract(ctx, shard.ContractID)
			if err != nil {
				return err
			}
			sectors = append(sectors, object.Sector{
				Contracts: map[types.PublicKey][]types.FileContractID{hk: {shard.ContractID}},
				Root:      shard.Root,
			})
		}
		key := bufferIDToKey[slab.BufferID]
		slabKeyToSlab[key].Shards = sectors
		delete(os.partials, key)
	}

	return nil
}

func (os *ObjectStore) Bucket(_ context.Context, bucket string) (api.Bucket, error) {
	return api.Bucket{}, nil
}

func (os *ObjectStore) MultipartUpload(ctx context.Context, uploadID string) (resp api.MultipartUpload, err error) {
	return api.MultipartUpload{}, nil
}

func (os *ObjectStore) RemoveObjects(ctx context.Context, bucket, prefix string) error {
	return nil
}

func (os *ObjectStore) totalSlabBufferSize() (total int) {
	for _, p := range os.partials {
		if time.Now().After(p.lockedUntil) {
			total += len(p.data)
		}
	}
	return
}

func (os *ObjectStore) SetSlabBufferMaxSizeSoft(n int) {
	os.mu.Lock()
	defer os.mu.Unlock()
	os.slabBufferMaxSizeSoft = n
}

func (os *ObjectStore) forEachObject(fn func(bucket, key string, o object.Object)) {
	for bucket, objects := range os.objects {
		for path, object := range objects {
			fn(bucket, path, object)
		}
	}
}

func (os *ObjectStore) hostForContract(ctx context.Context, fcid types.FileContractID) (types.PublicKey, error) {
	c, err := os.cs.Contract(ctx, fcid)
	if err != nil && !errors.Is(err, api.ErrContractNotFound) {
		return types.PublicKey{}, err
	} else if err == nil {
		return c.HostKey, nil
	}

	c, err = os.cs.RenewedContract(ctx, fcid)
	if err != nil {
		return types.PublicKey{}, err
	}
	return c.HostKey, nil
}

type settingStoreMock struct{}

func (*settingStoreMock) GougingParams(context.Context) (api.GougingParams, error) {
	return api.GougingParams{}, nil
}

func (*settingStoreMock) UploadParams(context.Context) (api.UploadParams, error) {
	return api.UploadParams{}, nil
}

type syncerMock struct{}

func (*syncerMock) BroadcastTransaction(context.Context, []types.Transaction) error {
	return nil
}

func (*syncerMock) SyncerPeers(context.Context) ([]string, error) {
	return nil, nil
}

var _ webhooks.Broadcaster = (*webhookBroadcasterMock)(nil)

type webhookBroadcasterMock struct{}

func (*webhookBroadcasterMock) BroadcastAction(context.Context, webhooks.Event) error {
	return nil
}

type webhookStoreMock struct{}

func (*webhookStoreMock) RegisterWebhook(ctx context.Context, webhook webhooks.Webhook) error {
	return nil
}

func (*webhookStoreMock) UnregisterWebhook(ctx context.Context, webhook webhooks.Webhook) error {
	return nil
}
