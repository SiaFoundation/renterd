package stores

import (
	"bytes"
	"context"
	"encoding/hex"
	"math"
	"testing"
	"time"

	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/object"
	"lukechampine.com/frand"
)

func TestPruneSlabBuffer(t *testing.T) {
	minShards := uint8(10)
	totalShards := uint8(30)
	fullBufferSize := bufferedSlabSize(minShards)

	newObjectWithPartialSlab := func(s *testSQLStore, n int) (string, []byte) {
		data := frand.Bytes(n)
		slabs, _, err := s.AddPartialSlab(context.Background(), data, minShards, totalShards, testContractSet)
		if err != nil {
			t.Fatal(err)
		}
		objPath := hex.EncodeToString(frand.Bytes(32))
		err = s.UpdateObject(context.Background(), api.DefaultBucketName, objPath, testContractSet, "", "", object.Object{
			Key:          object.GenerateEncryptionKey(),
			Slabs:        []object.SlabSlice{}, // no full slabs
			PartialSlabs: slabs,
		})
		if err != nil {
			t.Fatal(err)
		}
		return objPath, data
	}

	runCase := func(name string, f func(t *testing.T, s *testSQLStore)) {
		t.Run(name, func(t *testing.T) {
			s := newTestSQLStore(t, defaultTestSQLStoreConfig)
			defer s.Close()
			f(t, s)
		})
	}

	// Case 1: 2 partial slabs filling half the buffer The second one gets
	// deleted and replaced by a third one.
	runCase("Case1", func(t *testing.T, s *testSQLStore) {
		_, data1 := newObjectWithPartialSlab(s, fullBufferSize/2)
		obj2, _ := newObjectWithPartialSlab(s, fullBufferSize/2)
		if err := s.RemoveObject(context.Background(), api.DefaultBucketName, obj2); err != nil {
			t.Fatal(err)
		}
		ps, err := s.PackedSlabsForUpload(context.Background(), time.Minute, minShards, totalShards, testContractSet, math.MaxInt32)
		if err != nil {
			t.Fatal(err)
		} else if len(ps) != 0 {
			t.Fatal("expected 0 packed slab", len(ps))
		}

		_, data3 := newObjectWithPartialSlab(s, fullBufferSize/2)
		ps, err = s.PackedSlabsForUpload(context.Background(), time.Minute, minShards, totalShards, testContractSet, math.MaxInt32)
		if err != nil {
			t.Fatal(err)
		} else if len(ps) != 1 {
			t.Fatal("expected 1 packed slab", len(ps))
		} else if !bytes.Equal(ps[0].Data, append(data1, data3...)) {
			t.Fatal("packed slab data does not match")
		}
	})

	// Case 2: same as case 1 but the other slab gets replaced.
	runCase("Case2", func(t *testing.T, s *testSQLStore) {
		obj1, _ := newObjectWithPartialSlab(s, fullBufferSize/2)
		_, data2 := newObjectWithPartialSlab(s, fullBufferSize/2)
		if err := s.RemoveObject(context.Background(), api.DefaultBucketName, obj1); err != nil {
			t.Fatal(err)
		}
		ps, err := s.PackedSlabsForUpload(context.Background(), time.Minute, minShards, totalShards, testContractSet, math.MaxInt32)
		if err != nil {
			t.Fatal(err)
		} else if len(ps) != 0 {
			t.Fatal("expected 0 packed slab", len(ps))
		}

		_, data3 := newObjectWithPartialSlab(s, fullBufferSize/2)
		ps, err = s.PackedSlabsForUpload(context.Background(), time.Minute, minShards, totalShards, testContractSet, math.MaxInt32)
		if err != nil {
			t.Fatal(err)
		} else if len(ps) != 1 {
			t.Fatal("expected 1 packed slab", len(ps))
		} else if !bytes.Equal(ps[0].Data, append(data2, data3...)) {
			t.Fatal("packed slab data does not match")
		}
	})

	// Case 3: 256 partial slabs and every other gets deleted.
	runCase("Case3", func(t *testing.T, s *testSQLStore) {
		nSlabs := 256
		var toDelete []string
		var prunedData []byte

		// fill the buffer
		for i := 0; i < nSlabs; i++ {
			obj, data := newObjectWithPartialSlab(s, fullBufferSize/nSlabs)
			if i%2 == 0 {
				toDelete = append(toDelete, obj)
			} else {
				prunedData = append(prunedData, data...)
			}
		}

		// remove every other slab
		for _, obj := range toDelete {
			if err := s.RemoveObject(context.Background(), api.DefaultBucketName, obj); err != nil {
				t.Fatal(err)
			}
		}

		// trigger the pruning
		ps, err := s.PackedSlabsForUpload(context.Background(), time.Minute, minShards, totalShards, testContractSet, math.MaxInt32)
		if err != nil {
			t.Fatal(err)
		} else if len(ps) != 0 {
			t.Fatal("expected 0 packed slab", len(ps))
		}

		// add new data to fill the buffer again
		for i := 0; i < nSlabs/2; i++ {
			_, data := newObjectWithPartialSlab(s, fullBufferSize/nSlabs)
			prunedData = append(prunedData, data...)
		}

		ps, err = s.PackedSlabsForUpload(context.Background(), time.Minute, minShards, totalShards, testContractSet, math.MaxInt32)
		if err != nil {
			t.Fatal(err)
		} else if len(ps) != 1 {
			t.Fatal("expected 1 packed slab", len(ps))
		} else if !bytes.Equal(ps[0].Data, prunedData) {
			t.Fatal("packed slab data does not match")
		}
	})

	// Case 4: all slabs get deleted
}
