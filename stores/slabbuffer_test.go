package stores

import (
	"context"
	"testing"

	"lukechampine.com/frand"
)

func TestMarkBufferCompleteTwice(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	mgr, err := newSlabBufferManager(ss.SQLStore, 0, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	// get contract set for its id
	var set dbContractSet
	if err := ss.db.Where("name", testContractSet).Take(&set).Error; err != nil {
		t.Fatal(err)
	}

	// compute gid
	gid := bufferGID(1, 2, uint32(set.ID))

	// create an incomplete buffer by adding a slab
	for i := 0; i < 2; i++ {
		_, _, err = mgr.AddPartialSlab(context.Background(), frand.Bytes(1), 1, 2, set.ID)
		if err != nil {
			t.Fatal(err)
		}
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
	mgr.markBufferComplete(b, gid)
	mgr.markBufferComplete(b, gid)

	// there should only be one complete buffer
	if len(mgr.completeBuffers[gid]) != 1 {
		t.Fatalf("expected 1 complete buffer, got %v", len(mgr.completeBuffers[gid]))
	}
}
