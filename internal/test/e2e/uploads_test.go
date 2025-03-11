package e2e

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/v2/api"
	"go.sia.tech/renterd/v2/internal/test"
	"lukechampine.com/frand"
)

type blockedReader struct {
	remaining int
	data      *bytes.Buffer
	readChan  chan struct{}
	blockChan chan struct{}
}

func newBlockedReader(data []byte) *blockedReader {
	return &blockedReader{
		remaining: len(data),
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
	if r.remaining -= n; err == nil && r.remaining <= 0 {
		close(r.readChan)
	}
	return
}

func TestUploadingSectorsCache(t *testing.T) {
	cluster := newTestCluster(t, testClusterOptions{
		hosts: test.RedundancySettings.TotalShards,
	})
	defer cluster.Shutdown()
	w := cluster.Worker
	b := cluster.Bus
	rs := test.RedundancySettings
	tt := cluster.tt

	// generate some random data
	data := make([]byte, rhpv2.SectorSize*rs.MinShards)
	frand.Read(data)

	// upload an object using our custom reader
	br := newBlockedReader(data)
	go func() {
		_, err := w.UploadObject(context.Background(), br, testBucket, t.Name(), api.UploadObjectOptions{})
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
	contracts, err := b.Contracts(context.Background(), api.ContractsOpts{})
	tt.OK(err)

	// fetch pending roots for all contracts
	tt.Retry(10, time.Second, func() error {
		var n int
		for _, c := range contracts {
			roots, err := b.ContractRoots(context.Background(), c.ID)
			tt.OK(err)
			n += len(roots)
		}

		// expect all sectors to be uploading at one point
		if n != 0 {
			return fmt.Errorf("expected 0 roots, got %v", n)
		}
		return nil
	})

	// unblock the upload
	close(br.blockChan)

	// fetch uploaded roots for all contracts
	uploaded := make(map[types.FileContractID]map[types.Hash256]struct{})
	tt.Retry(10, time.Second, func() error {
		var n int
		for _, c := range contracts {
			roots, err := b.ContractRoots(context.Background(), c.ID)
			tt.OK(err)
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
	})

	// fetch the object and compare its roots against the uploaded ones
	obj, err := b.Object(context.Background(), testBucket, t.Name(), api.GetObjectOptions{})
	tt.OK(err)
	objRoots := make(map[types.FileContractID][]types.Hash256)
	for _, slab := range obj.Slabs {
		for _, shard := range slab.Shards {
			for _, fcids := range shard.Contracts {
				for _, fcid := range fcids {
					objRoots[fcid] = append(objRoots[fcid], shard.Root)
				}
			}
		}
	}

	for id, roots := range objRoots {
		for _, root := range roots {
			_, found := uploaded[id][root]
			if !found {
				t.Fatalf("pending root %v not found in uploaded roots", root)
			}
		}

		cr, err := cluster.ContractRoots(context.Background(), id)
		tt.OK(err)
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
