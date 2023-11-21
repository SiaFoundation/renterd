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
	s := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer s.Close()

	minShards := uint8(10)
	totalShards := uint8(30)
	fullBufferSize := bufferedSlabSize(minShards)

	newObjectWithPartialSlab := func(n int) (string, []byte) {
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

	// Case 1: 2 partial slabs filling half the buffer The second one gets
	// deleted and replaced by a third one.
	_, data1 := newObjectWithPartialSlab(fullBufferSize / 2)
	obj2, _ := newObjectWithPartialSlab(fullBufferSize / 2)
	if err := s.RemoveObject(context.Background(), api.DefaultBucketName, obj2); err != nil {
		t.Fatal(err)
	}
	ps, err := s.PackedSlabsForUpload(context.Background(), time.Minute, minShards, totalShards, testContractSet, math.MaxInt32)
	if err != nil {
		t.Fatal(err)
	} else if len(ps) != 0 {
		t.Fatal("expected 0 packed slab", len(ps))
	}

	_, data3 := newObjectWithPartialSlab(fullBufferSize / 2)
	ps, err = s.PackedSlabsForUpload(context.Background(), time.Minute, minShards, totalShards, testContractSet, math.MaxInt32)
	if err != nil {
		t.Fatal(err)
	} else if len(ps) != 1 {
		t.Fatal("expected 1 packed slab", len(ps))
	} else if !bytes.Equal(ps[0].Data, append(data1, data3...)) {
		t.Fatal("packed slab data does not match")
	}
}
