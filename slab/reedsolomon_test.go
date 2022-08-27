package slab

import (
	"bytes"
	"io/ioutil"
	"reflect"
	"testing"

	"go.sia.tech/renterd/rhp/v2"
	"lukechampine.com/frand"
)

func encodeAlloc(rsc RSCode, data []byte) [][]byte {
	shards := make([][]byte, rsc.n)
	for i := range shards {
		shards[i] = make([]byte, len(data)/int(rsc.m))
	}
	rsc.Encode(data, shards)
	return shards
}

func checkRecover(rsc RSCode, shards [][]byte, data []byte) bool {
	var buf bytes.Buffer
	if err := rsc.Recover(&buf, shards, 0, len(data)); err != nil {
		return false
	}
	return bytes.Equal(buf.Bytes(), data)
}

func TestReedSolomon(t *testing.T) {
	// 3-of-10 code
	rsc := NewRSCode(3, 10)
	chunkSize := 3 * rhp.LeafSize
	data := frand.Bytes(chunkSize * 4)
	shards := encodeAlloc(rsc, data)
	// delete 7 random shards
	partialShards := make([][]byte, len(shards))
	for i := range partialShards {
		partialShards[i] = append([]byte(nil), shards[i]...)
	}
	for _, i := range frand.Perm(len(partialShards))[:7] {
		partialShards[i] = make([]byte, 0, len(partialShards[i]))
	}
	// reconstruct
	if err := rsc.Reconstruct(partialShards); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(shards, partialShards) {
		t.Error("failed to reconstruct shards")
	}

	// delete 7 random shards
	for _, i := range frand.Perm(len(partialShards))[:7] {
		partialShards[i] = make([]byte, 0, len(partialShards[i]))
	}
	// recover
	if !checkRecover(rsc, partialShards, data) {
		t.Error("failed to recover shards")
	}

	// 7-of-7 code (simple redundancy)
	rsc = NewRSCode(7, 7)
	chunkSize = 7 * rhp.LeafSize
	data = frand.Bytes(chunkSize * 10)
	shards = encodeAlloc(rsc, data)
	// delete a random shard
	partialShards = append([][]byte(nil), shards...)
	i := frand.Intn(len(partialShards))
	partialShards[i] = partialShards[i][:0]
	// reconstruct should fail
	if err := rsc.Reconstruct(partialShards); err == nil {
		t.Error("Reconstruct should have failed with missing shard")
	}

	// recover
	if checkRecover(rsc, partialShards, data) {
		t.Error("Recover should have failed with missing shard")
	}
	if !checkRecover(rsc, shards, data) {
		t.Error("failed to recover shards")
	}
}

func TestReedSolomonPartial(t *testing.T) {
	// 3-of-10 code
	rsc := NewRSCode(3, 10)
	const chunkSize = 3 * rhp.LeafSize
	data := frand.Bytes(chunkSize * 10)
	shards := encodeAlloc(rsc, data)

	// pick a random segment from three shards
	segIndex := frand.Intn(len(shards[0]) / rhp.LeafSize)
	partialShards := make([][]byte, len(shards))
	for i := range partialShards {
		partialShards[i] = make([]byte, 0, rhp.LeafSize)
	}
	for _, i := range frand.Perm(len(partialShards))[:3] {
		partialShards[i] = shards[i][segIndex*rhp.LeafSize:][:rhp.LeafSize]
	}

	// recover
	dataSeg := data[segIndex*chunkSize:][:chunkSize]
	if !checkRecover(rsc, partialShards, dataSeg) {
		t.Error("failed to recover shards")
	}
}

func BenchmarkReedSolomon(b *testing.B) {
	makeShards := func(m, n uint8) ([]byte, [][]byte) {
		chunkSize := int(m) * rhp.LeafSize
		data := frand.Bytes(chunkSize * (rhp.SectorSize / chunkSize))
		shards := make([][]byte, n)
		for i := range shards {
			shards[i] = make([]byte, len(data)/int(m))
		}
		return data, shards
	}

	benchEncode := func(m, n uint8) func(*testing.B) {
		data, shards := makeShards(m, n)
		rsc := NewRSCode(m, n)
		return func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				rsc.Encode(data, shards)
			}
		}
	}

	benchRecover := func(m, n, r uint8) func(*testing.B) {
		data, shards := makeShards(m, n)
		rsc := NewRSCode(m, n)
		rsc.Encode(data, shards)
		return func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				for j := range shards[:r] {
					shards[j] = shards[j][:0]
				}
				if err := rsc.Recover(ioutil.Discard, shards, 0, len(data)); err != nil {
					b.Fatal(err)
				}
			}
		}
	}

	benchReconstruct := func(m, n, r uint8) func(*testing.B) {
		data, shards := makeShards(m, n)
		rsc := NewRSCode(m, n)
		rsc.Encode(data, shards)
		return func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(shards[0])) * int64(r))
			for i := 0; i < b.N; i++ {
				for j := range shards[:r] {
					shards[j] = shards[j][:0]
				}
				if err := rsc.Reconstruct(shards); err != nil {
					b.Fatal(err)
				}
			}
		}
	}

	b.Run("encode-10-of-40", benchEncode(10, 40))
	b.Run("encode-20-of-10", benchEncode(20, 40))
	b.Run("encode-30-of-10", benchEncode(30, 40))
	b.Run("encode-10-of-10", benchEncode(10, 10))

	b.Run("recover-1-of-10-of-40", benchRecover(10, 40, 1))
	b.Run("recover-10-of-10-of-40", benchRecover(10, 40, 10))
	b.Run("recover-0-of-10-of-10", benchRecover(10, 10, 0))

	b.Run("reconstruct-1-of-10-of-40", benchReconstruct(10, 40, 1))
	b.Run("reconstruct-10-of-10-of-40", benchReconstruct(10, 40, 10))
}
