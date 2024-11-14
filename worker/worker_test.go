package worker

import (
	"context"
	"fmt"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/config"
	"go.sia.tech/renterd/internal/test"
	"go.sia.tech/renterd/internal/test/mocks"
	"go.uber.org/zap"
	"golang.org/x/crypto/blake2b"
	"lukechampine.com/frand"
)

type (
	testWorker struct {
		tt test.TT
		*Worker

		cs *mocks.ContractStore
		os *mocks.ObjectStore
		hs *mocks.HostStore

		dlmm *mocks.MemoryManager
		ulmm *mocks.MemoryManager

		hm *testHostManager
	}
)

func newTestWorker(t test.TestingCommon) *testWorker {
	// create bus dependencies
	cs := mocks.NewContractStore()
	os := mocks.NewObjectStore(testBucket, cs)
	hs := mocks.NewHostStore()

	// create worker dependencies
	b := mocks.NewBus(cs, hs, os)
	dlmm := mocks.NewMemoryManager()
	ulmm := mocks.NewMemoryManager()

	// create worker
	w, err := New(newTestWorkerCfg(), blake2b.Sum256([]byte("testwork")), b, zap.NewNop())
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
		test.NewTT(t),
		w,
		cs,
		os,
		hs,
		dlmm,
		ulmm,
		hm,
	}
}

func (w *testWorker) AddHosts(n int) (added []*testHost) {
	for i := 0; i < n; i++ {
		added = append(added, w.AddHost())
	}
	return
}

func (w *testWorker) AddHost() *testHost {
	h := w.hs.AddHost()
	c := w.cs.AddContract(h.PublicKey())
	host := newTestHost(h, c)
	w.hm.addHost(host)
	return host
}

func (w *testWorker) BlockUploads() func() {
	return w.ulmm.Block()
}

func (w *testWorker) BlockAsyncPackedSlabUploads(up uploadParameters) {
	w.uploadsMu.Lock()
	defer w.uploadsMu.Unlock()
	key := fmt.Sprintf("%d-%d_%s", up.rs.MinShards, up.rs.TotalShards, up.contractSet)
	w.uploadingPackedSlabs[key] = struct{}{}
}

func (w *testWorker) UnblockAsyncPackedSlabUploads(up uploadParameters) {
	w.uploadsMu.Lock()
	defer w.uploadsMu.Unlock()
	key := fmt.Sprintf("%d-%d_%s", up.rs.MinShards, up.rs.TotalShards, up.contractSet)
	delete(w.uploadingPackedSlabs, key)
}

func (w *testWorker) Contracts() []api.ContractMetadata {
	metadatas, err := w.cs.Contracts(context.Background(), api.ContractsOpts{})
	if err != nil {
		w.tt.Fatal(err)
	}
	return metadatas
}

func (w *testWorker) RenewContract(hk types.PublicKey) *mocks.Contract {
	h := w.hm.hosts[hk]
	if h == nil {
		w.tt.Fatal("host not found")
	}

	renewal, err := w.cs.RenewContract(hk)
	if err != nil {
		w.tt.Fatal(err)
	}
	return renewal
}

func (w *testWorker) UsableHosts() (hosts []api.HostInfo) {
	metadatas, err := w.cs.Contracts(context.Background(), api.ContractsOpts{})
	if err != nil {
		w.tt.Fatal(err)
	}
	for _, md := range metadatas {
		hosts = append(hosts, api.HostInfo{
			PublicKey:  md.HostKey,
			SiamuxAddr: md.SiamuxAddr,
		})
	}
	return
}

func newTestWorkerCfg() config.Worker {
	return config.Worker{
		AccountsRefillInterval:   time.Second,
		ID:                       "test",
		BusFlushInterval:         time.Second,
		DownloadOverdriveTimeout: time.Second,
		UploadOverdriveTimeout:   time.Second,
		DownloadMaxMemory:        1 << 12, // 4 KiB
		UploadMaxMemory:          1 << 12, // 4 KiB
	}
}

func newTestSector() (*[rhpv2.SectorSize]byte, types.Hash256) {
	var sector [rhpv2.SectorSize]byte
	frand.Read(sector[:])
	return &sector, rhpv2.SectorRoot(&sector)
}
