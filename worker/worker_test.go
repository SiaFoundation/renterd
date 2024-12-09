package worker

import (
	"context"
	"fmt"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/config"
	"go.sia.tech/renterd/internal/download"
	"go.sia.tech/renterd/internal/test"
	"go.sia.tech/renterd/internal/test/mocks"
	"go.sia.tech/renterd/internal/utils"
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
	cfg := newTestWorkerCfg()
	mk := utils.MasterKey(blake2b.Sum256([]byte("testwork")))
	w, err := New(cfg, mk, b, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}

	// override managers
	hm := newTestHostManager(t)
	uploadKey := mk.DeriveUploadKey()
	w.downloadManager = download.NewManager(context.Background(), &uploadKey, hm, b, cfg.UploadMaxMemory, cfg.UploadMaxOverdrive, cfg.UploadOverdriveTimeout, zap.NewNop())
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
	key := fmt.Sprintf("%d-%d", up.rs.MinShards, up.rs.TotalShards)
	w.uploadingPackedSlabs[key] = struct{}{}
}

func (w *testWorker) UnblockAsyncPackedSlabUploads(up uploadParameters) {
	w.uploadsMu.Lock()
	defer w.uploadsMu.Unlock()
	key := fmt.Sprintf("%d-%d", up.rs.MinShards, up.rs.TotalShards)
	delete(w.uploadingPackedSlabs, key)
}

func (w *testWorker) Contracts() (hcs []hostContract) {
	hosts, err := w.hs.UsableHosts(context.Background())
	if err != nil {
		w.tt.Fatal(err)
	}
	hmap := make(map[types.PublicKey]api.HostInfo)
	for _, h := range hosts {
		hmap[h.PublicKey] = h
	}

	contracts, err := w.cs.Contracts(context.Background(), api.ContractsOpts{})
	if err != nil {
		w.tt.Fatal(err)
	}
	for _, c := range contracts {
		if h, ok := hmap[c.HostKey]; ok {
			hcs = append(hcs, hostContract{c, h})
		}
	}

	return
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

func (w *testWorker) UsableHosts() []api.HostInfo {
	hosts, err := w.hs.UsableHosts(context.Background())
	if err != nil {
		w.tt.Fatal(err)
	}
	return hosts
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
