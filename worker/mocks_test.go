package worker

import (
	"context"
	"errors"
	"sync"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/object"
)

type (
	mockMemory        struct{}
	mockMemoryManager struct{}
	mockObjectStore   struct {
		mu      sync.Mutex
		objects map[string]map[string]object.Object
	}
)

var (
	_ Memory        = (*mockMemory)(nil)
	_ MemoryManager = (*mockMemoryManager)(nil)
	_ ObjectStore   = (*mockObjectStore)(nil)
)

var (
	errBucketNotFound = errors.New("bucket not found")
	errObjectNotFound = errors.New("object not found")
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
	return nil, false, nil
}

func (os *mockObjectStore) AddUploadingSector(ctx context.Context, uID api.UploadID, id types.FileContractID, root types.Hash256) error {
	return nil
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

	object := os.objects[bucket][path]
	return api.ObjectsResponse{Object: &api.Object{
		ObjectMetadata: api.ObjectMetadata{Name: path, Size: object.TotalSize()},
		Object:         object,
	}}, nil
}

func (os *mockObjectStore) DeleteHostSector(ctx context.Context, hk types.PublicKey, root types.Hash256) error {
	return nil
}
func (os *mockObjectStore) DeleteObject(ctx context.Context, bucket, path string, opts api.DeleteObjectOptions) error {
	return nil
}
func (os *mockObjectStore) FetchPartialSlab(ctx context.Context, key object.EncryptionKey, offset, length uint32) ([]byte, error) {
	return nil, nil
}
func (os *mockObjectStore) Slab(ctx context.Context, key object.EncryptionKey) (object.Slab, error) {
	return object.Slab{}, nil
}
func (os *mockObjectStore) UpdateSlab(ctx context.Context, s object.Slab, contractSet string) error {
	return nil
}
func (os *mockObjectStore) MarkPackedSlabsUploaded(ctx context.Context, slabs []api.UploadedPackedSlab) error {
	return nil
}
func (os *mockObjectStore) TrackUpload(ctx context.Context, uID api.UploadID) error  { return nil }
func (os *mockObjectStore) FinishUpload(ctx context.Context, uID api.UploadID) error { return nil }

func newMockContracts(hosts []*mockHost) []*mockContract {
	contracts := make([]*mockContract, len(hosts))
	for i := range contracts {
		contracts[i] = newMockContract(types.FileContractID{byte(i)})
		hosts[i].c = contracts[i]
	}
	return contracts
}

func newMockContractLocker(contracts []*mockContract) *mockContractLocker {
	cl := &mockContractLocker{contracts: make(map[types.FileContractID]*mockContract)}
	for _, c := range contracts {
		cl.contracts[c.rev.ParentID] = c
	}
	return cl
}

func newMockHosts(n int) []*mockHost {
	hosts := make([]*mockHost, n)
	for i := range hosts {
		hosts[i] = newMockHost(types.PublicKey{byte(i)}, nil)
	}
	return hosts
}

func newMockHostManager(hosts []*mockHost) *mockHostManager {
	hm := &mockHostManager{hosts: make(map[types.PublicKey]Host)}
	for _, h := range hosts {
		hm.hosts[h.hk] = h
	}
	return hm
}

func newMockObjectStore() *mockObjectStore {
	os := &mockObjectStore{objects: make(map[string]map[string]object.Object)}
	os.objects[testBucket] = make(map[string]object.Object)
	return os
}
