package worker

import (
	"context"
	"fmt"
	"testing"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.uber.org/zap"
	"golang.org/x/crypto/blake2b"
	"lukechampine.com/frand"
)

type (
	testWorker struct {
		t *testing.T
		*worker

		cs *contractStoreMock
		os *objectStoreMock
		hs *hostStoreMock

		dlmm *memoryManagerMock
		ulmm *memoryManagerMock

		hm *testHostManager
	}
)

func newTestWorker(t *testing.T) *testWorker {
	// create bus dependencies
	cs := newContractStoreMock()
	os := newObjectStoreMock(testBucket)
	hs := newHostStoreMock()

	// create worker dependencies
	b := newBusMock(cs, hs, os)
	dlmm := newMemoryManagerMock()
	ulmm := newMemoryManagerMock()

	// create worker
	w, err := New(blake2b.Sum256([]byte("testwork")), "test", b, time.Second, time.Second, time.Second, time.Second, 0, 0, 1, 1, false, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}

	// override managers
	hm := newTestHostManager(t)
	w.priceTables.hm = hm
	w.downloadManager.hm = hm
	w.downloadManager.mm = dlmm
	w.uploadManager.hm = hm
	w.uploadManager.mm = ulmm

	return &testWorker{
		t,
		w,
		cs,
		os,
		hs,
		dlmm,
		ulmm,
		hm,
	}
}

func (w *testWorker) addHosts(n int) (added []*testHost) {
	for i := 0; i < n; i++ {
		added = append(added, w.addHost())
	}
	return
}

func (w *testWorker) addHost() *testHost {
	h := w.hs.addHost()
	c := w.cs.addContract(h.hk)
	host := newTestHost(h, c)
	w.hm.addHost(host)
	return host
}

func (w *testWorker) blockUploads() func() {
	select {
	case <-w.ulmm.memBlockChan:
	case <-time.After(time.Second):
		w.t.Fatal("already blocking")
	}

	blockChan := make(chan struct{})
	w.ulmm.memBlockChan = blockChan
	return func() { close(blockChan) }
}

func (w *testWorker) blockAsyncPackedSlabUploads(up uploadParameters) {
	w.uploadsMu.Lock()
	defer w.uploadsMu.Unlock()
	key := fmt.Sprintf("%d-%d_%s", up.rs.MinShards, up.rs.TotalShards, up.contractSet)
	w.uploadingPackedSlabs[key] = struct{}{}
}

func (w *testWorker) unblockAsyncPackedSlabUploads(up uploadParameters) {
	w.uploadsMu.Lock()
	defer w.uploadsMu.Unlock()
	key := fmt.Sprintf("%d-%d_%s", up.rs.MinShards, up.rs.TotalShards, up.contractSet)
	delete(w.uploadingPackedSlabs, key)
}

func (w *testWorker) contracts() []api.ContractMetadata {
	metadatas, err := w.cs.Contracts(context.Background(), api.ContractsOpts{})
	if err != nil {
		w.t.Fatal(err)
	}
	return metadatas
}

func (w *testWorker) renewContract(hk types.PublicKey) *contractMock {
	h := w.hm.hosts[hk]
	if h == nil {
		w.t.Fatal("host not found")
	}

	renewal, err := w.cs.renewContract(hk)
	if err != nil {
		w.t.Fatal(err)
	}
	return renewal
}

func newTestSector() (*[rhpv2.SectorSize]byte, types.Hash256) {
	var sector [rhpv2.SectorSize]byte
	frand.Read(sector[:])
	return &sector, rhpv2.SectorRoot(&sector)
}
