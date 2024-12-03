package mocks

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
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/object"
)

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
		parameterKey string // ([minshards]-[totalshards])
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

func (os *ObjectStore) AddMultipartPart(ctx context.Context, bucket, path, eTag, uploadID string, partNumber int, slices []object.SlabSlice) (err error) {
	return nil
}

func (os *ObjectStore) AddUploadingSectors(ctx context.Context, uID api.UploadID, root []types.Hash256) error {
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

func (os *ObjectStore) AddObject(ctx context.Context, bucket, path string, o object.Object, opts api.AddObjectOptions) error {
	os.mu.Lock()
	defer os.mu.Unlock()

	// check if the bucket exists
	if _, exists := os.objects[bucket]; !exists {
		return api.ErrBucketNotFound
	}

	os.objects[bucket][path] = o
	return nil
}

func (os *ObjectStore) AddPartialSlab(ctx context.Context, data []byte, minShards, totalShards uint8) (slabs []object.SlabSlice, slabBufferMaxSizeSoftReached bool, err error) {
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
		parameterKey: fmt.Sprintf("%d-%d", minShards, totalShards),
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

func (os *ObjectStore) PackedSlabsForUpload(ctx context.Context, lockingDuration time.Duration, minShards, totalShards uint8, limit int) (pss []api.PackedSlab, _ error) {
	os.mu.Lock()
	defer os.mu.Unlock()

	if limit == -1 {
		limit = math.MaxInt
	}

	parameterKey := fmt.Sprintf("%d-%d", minShards, totalShards)
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
