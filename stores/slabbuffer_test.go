package stores

import (
	"context"
	"errors"
	"testing"

	"lukechampine.com/frand"
)

func (s *testSQLStore) ContractSetID(name string) (id int64) {
	if err := s.DB().QueryRow(context.Background(), "SELECT id FROM contract_sets WHERE name = ?", name).
		Scan(&id); err != nil {
		s.t.Fatal(err)
	}
	return
}

func TestRecordAppendToCompletedBuffer(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	completionThreshold := int64(1000)
	mgr, err := newSlabBufferManager(context.Background(), ss.alerts, ss.db, ss.logger.Desugar(), completionThreshold, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	// get contract set for its id
	csID := ss.ContractSetID(testContractSet)

	// compute gid
	gid := bufferGID(1, 2, uint32(csID))

	// add a slab that immediately fills a buffer but has 100 bytes left
	minShards := uint8(1)
	totalShards := uint8(2)
	maxSize := bufferedSlabSize(minShards)
	_, _, err = mgr.AddPartialSlab(context.Background(), frand.Bytes(maxSize-100), minShards, totalShards, testContractSet)
	if err != nil {
		t.Fatal(err)
	} else if len(mgr.completeBuffers[gid]) != 1 {
		t.Fatalf("expected 1 complete buffer, got %v", len(mgr.completeBuffers[gid]))
	} else if len(mgr.incompleteBuffers[gid]) != 0 {
		t.Fatalf("expected 0 incomplete buffers, got %v", len(mgr.incompleteBuffers[gid]))
	}

	// fetch the complete buffer and try to manually append to it - shouldn't happen
	cb := mgr.completeBuffers[gid][0]
	_, _, used, err := cb.recordAppend(frand.Bytes(1), false, 1, completionThreshold)
	if err != nil {
		t.Fatal(err)
	} else if used {
		t.Fatal("expected buffer to not be used")
	}

	// add a slab that should fit in the buffer but since the first buffer is
	// complete we ignore it
	_, _, err = mgr.AddPartialSlab(context.Background(), frand.Bytes(1), minShards, totalShards, testContractSet)
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

	mgr, err := newSlabBufferManager(context.Background(), ss.alerts, ss.db, ss.logger.Desugar(), 0, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	// get contract set for its id
	csID := ss.ContractSetID(testContractSet)

	// compute gid
	gid := bufferGID(1, 2, uint32(csID))

	// create an incomplete buffer
	_, _, err = mgr.AddPartialSlab(context.Background(), frand.Bytes(1), 1, 2, testContractSet)
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

	// mark the buffer as complete
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
