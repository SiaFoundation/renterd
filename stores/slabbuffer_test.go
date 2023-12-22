package stores

import (
	"context"
	"errors"
	"testing"

	"lukechampine.com/frand"
)

func TestRecordAppendToCompletedBuffer(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	mgr, err := newSlabBufferManager(ss.SQLStore, 1000, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	// get contract set for its id
	var set dbContractSet
	if err := ss.db.Where("name", testContractSet).Take(&set).Error; err != nil {
		t.Fatal(err)
	}

	// compute gid
	gid := bufferGID(1, 2, uint32(set.ID))

	// add a slab that immediately fills a buffer but has 100 bytes left
	minShards := uint8(1)
	totalShards := uint8(2)
	maxSize := bufferedSlabSize(minShards)
	_, _, err = mgr.AddPartialSlab(context.Background(), frand.Bytes(maxSize-100), minShards, totalShards, set.ID)
	if err != nil {
		t.Fatal(err)
	} else if len(mgr.completeBuffers[gid]) != 1 {
		t.Fatalf("expected 1 complete buffer, got %v", len(mgr.completeBuffers[gid]))
	} else if len(mgr.incompleteBuffers[gid]) != 0 {
		t.Fatalf("expected 0 incomplete buffers, got %v", len(mgr.incompleteBuffers[gid]))
	}

	// add a slab that should fit in the buffer but since the first buffer is
	// complete we ignore it
	_, _, err = mgr.AddPartialSlab(context.Background(), frand.Bytes(1), minShards, totalShards, set.ID)
	if err != nil {
		t.Fatal(err)
	} else if len(mgr.completeBuffers[gid]) != 1 {
		t.Fatalf("expected 1 complete buffer, got %v", len(mgr.completeBuffers[gid]))
	} else if len(mgr.incompleteBuffers[gid]) != 1 {
		t.Fatalf("expected 1 incomplete buffers, got %v", len(mgr.incompleteBuffers[gid]))
	}
}

func TestMarkBufferCompleteTwice(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	mgr, err := newSlabBufferManager(ss.SQLStore, 0, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	// get contract set for its id
	var set dbContractSet
	if err := ss.db.Where("name", testContractSet).Take(&set).Error; err != nil {
		t.Fatal(err)
	}

	// compute gid
	gid := bufferGID(1, 2, uint32(set.ID))

	// create an incomplete buffer
	_, _, err = mgr.AddPartialSlab(context.Background(), frand.Bytes(1), 1, 2, set.ID)
	if err != nil {
		t.Fatal(err)
	}

	// make sure there is only one incomplete buffer
	incompleteBuffers := mgr.incompleteBuffers[gid]
	if len(incompleteBuffers) != 1 {
		t.Fatalf("expected 1 incomplete buffer, got %v", len(incompleteBuffers))
	} else if len(mgr.completeBuffers[gid]) != 0 {
		t.Fatalf("expected 0 complete buffers, got %v", len(mgr.completeBuffers[gid]))
	}
	b := incompleteBuffers[0]

	// mark the buffer as complete twice which might happen
	if err := mgr.markBufferComplete(b, gid); err != nil {
		t.Fatal(err)
	}

	// there should only be one complete buffer
	if len(mgr.completeBuffers[gid]) != 1 {
		t.Fatalf("expected 1 complete buffer, got %v", len(mgr.completeBuffers[gid]))
	} else if len(mgr.incompleteBuffers[gid]) != 0 {
		t.Fatalf("expected 0 incomplete buffers, got %v", len(mgr.incompleteBuffers[gid]))
	}

	// try again - should fail
	if err := mgr.markBufferComplete(b, gid); !errors.Is(err, errBufferNotFound) {
		t.Fatal("expected error marking buffer complete twice", err)
	}
}
