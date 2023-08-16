package testing

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"lukechampine.com/frand"
)

type blockedReader struct {
	rs        api.RedundancySettings
	read      int
	data      *bytes.Buffer
	blockChan chan struct{}
	readChan  chan struct{}
}

func newBlockedReader(rs api.RedundancySettings) *blockedReader {
	data := make([]byte, rhpv2.SectorSize*rs.MinShards)
	frand.Read(data)
	return &blockedReader{
		rs:        rs,
		data:      bytes.NewBuffer(data),
		blockChan: make(chan struct{}),
		readChan:  make(chan struct{}),
	}
}

func (r *blockedReader) Read(buf []byte) (n int, err error) {
	select {
	case <-r.readChan:
		<-r.blockChan
	default:
	}

	n, err = r.data.Read(buf)
	if r.read += n; n > 0 && r.read == r.rs.MinShards*rhpv2.SectorSize {
		close(r.readChan)
	}
	return
}

func TestUploadingSectorsCache(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	cluster, err := newTestCluster(t.TempDir(), newTestLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := cluster.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	}()
	w := cluster.Worker
	b := cluster.Bus
	rs := testRedundancySettings

	// add hosts
	if _, err := cluster.AddHostsBlocking(rs.TotalShards); err != nil {
		t.Fatal(err)
	}

	// wait for accounts to be funded
	if _, err := cluster.WaitForAccounts(); err != nil {
		t.Fatal(err)
	}

	// upload an object using our custom reader
	br := newBlockedReader(rs)
	go func() {
		err = w.UploadObject(context.Background(), br, t.Name())
		if err != nil {
			t.Error(err)
		}
	}()

	// block until we've read all data
	select {
	case <-br.readChan:
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for upload to finish")
	}

	// fetch contracts
	contracts, err := b.Contracts(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	// fetch pending roots for all contracts
	pending := make(map[types.FileContractID][]types.Hash256)
	if err := Retry(10, time.Second, func() error {
		var n int
		for _, c := range contracts {
			_, uploading, err := b.ContractRoots(context.Background(), c.ID)
			if err != nil {
				t.Fatal(err)
			}
			pending[c.ID] = uploading
			n += len(uploading)
		}

		// expect all sectors to be uploading at one point
		if n != rs.TotalShards {
			return fmt.Errorf("expected %v uploading sectors, got %v", rs.TotalShards, n)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// unblock the upload
	close(br.blockChan)

	// fetch uploaded roots for all contracts
	uploaded := make(map[types.FileContractID]map[types.Hash256]struct{})
	if err := Retry(10, time.Second, func() error {
		var n int
		for _, c := range contracts {
			roots, _, err := b.ContractRoots(context.Background(), c.ID)
			if err != nil {
				t.Fatal(err)
			}
			uploaded[c.ID] = make(map[types.Hash256]struct{})
			for _, root := range roots {
				uploaded[c.ID][root] = struct{}{}
			}
			n += len(roots)
		}

		// expect all sectors to be uploaded at one point
		if n != rs.TotalShards {
			return fmt.Errorf("expected %v uploading sectors, got %v", rs.TotalShards, n)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if len(pending) != len(uploaded) {
		t.Fatal("unexpected number of contracts")
	}
	for id, roots := range pending {
		for _, root := range roots {
			_, found := uploaded[id][root]
			if !found {
				t.Fatalf("pending root %v not found in uploaded roots", root)
			}
		}

		cr, err := w.RHPContractRoots(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		expected := make(map[types.Hash256]struct{})
		for _, root := range cr {
			expected[root] = struct{}{}
		}
		for _, root := range roots {
			_, found := uploaded[id][root]
			if !found {
				t.Fatalf("uploaded root %v not found in contract roots", root)
			}
		}
	}
}
