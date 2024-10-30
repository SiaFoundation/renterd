package object

import (
	"bytes"
	"io"
	"testing"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"lukechampine.com/frand"
)

func checkRecover(s Slab, shards [][]byte, data []byte) bool {
	ss := SlabSlice{s, 0, uint32(len(data))}
	var buf bytes.Buffer
	if err := ss.Recover(&buf, shards); err != nil {
		return false
	}
	return bytes.Equal(buf.Bytes(), data)
}

func TestReedSolomon(t *testing.T) {
	// 3-of-10 code
	s := Slab{MinShards: 3, Shards: make([]Sector, 10)}
	data := frand.Bytes(rhpv2.SectorSize * 3)
	shards := make([][]byte, 10)
	s.Encode(data, shards)

	// delete 7 random shards
	partialShards := make([][]byte, len(shards))
	for i := range partialShards {
		partialShards[i] = append([]byte(nil), shards[i]...)
	}
	for _, i := range frand.Perm(len(partialShards))[:7] {
		partialShards[i] = nil
	}
	// reconstruct
	if err := s.Reconstruct(partialShards); err != nil {
		t.Fatal(err)
	}
	for i := range shards {
		if !bytes.Equal(shards[i], partialShards[i]) {
			t.Error("failed to reconstruct shards")
			break
		}
	}

	// delete 7 random shards
	for _, i := range frand.Perm(len(partialShards))[:7] {
		partialShards[i] = nil
	}
	// recover
	if !checkRecover(s, partialShards, data) {
		t.Error("failed to recover shards")
	}

	// pick a random segment from 3 shards
	segIndex := frand.Intn(len(shards[0]) / rhpv2.LeafSize)
	for i := range partialShards {
		partialShards[i] = make([]byte, 0, rhpv2.LeafSize)
	}
	for _, i := range frand.Perm(len(partialShards))[:3] {
		partialShards[i] = shards[i][segIndex*rhpv2.LeafSize:][:rhpv2.LeafSize]
	}

	// recover
	chunkSize := rhpv2.LeafSize * int(s.MinShards)
	dataSeg := data[segIndex*chunkSize:][:chunkSize]
	if !checkRecover(s, partialShards, dataSeg) {
		t.Error("failed to recover shards")
	}
}

func BenchmarkReedSolomon(b *testing.B) {
	makeSlab := func(m, n uint8) (Slab, []byte, [][]byte) {
		return Slab{EncryptionKey: GenerateEncryptionKey(EncryptionKeyTypeSalted), MinShards: m, Shards: make([]Sector, n)},
			frand.Bytes(rhpv2.SectorSize * int(m)),
			make([][]byte, n)
	}

	benchEncode := func(m, n uint8) func(*testing.B) {
		s, data, shards := makeSlab(m, n)
		return func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				s.Encode(data, shards)
			}
		}
	}

	benchRecover := func(m, n, r uint8) func(*testing.B) {
		s, data, shards := makeSlab(m, n)
		s.Encode(data, shards)
		ss := SlabSlice{s, 0, uint32(len(data))}
		return func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				for j := range shards[:r] {
					shards[j] = shards[j][:0]
				}
				if err := ss.Recover(io.Discard, shards); err != nil {
					b.Fatal(err)
				}
			}
		}
	}

	benchReconstruct := func(m, n, r uint8) func(*testing.B) {
		s, data, shards := makeSlab(m, n)
		s.Encode(data, shards)
		return func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(shards[0])) * int64(r))
			for i := 0; i < b.N; i++ {
				for j := range shards[:r] {
					shards[j] = shards[j][:0]
				}
				if err := s.Reconstruct(shards); err != nil {
					b.Fatal(err)
				}
			}
		}
	}

	b.Run("encode-10-of-40", benchEncode(10, 40))
	b.Run("encode-20-of-40", benchEncode(20, 40))
	b.Run("encode-30-of-40", benchEncode(30, 40))
	b.Run("encode-10-of-10", benchEncode(10, 10))

	b.Run("recover-1-of-10-of-40", benchRecover(10, 40, 1))
	b.Run("recover-10-of-10-of-40", benchRecover(10, 40, 10))
	b.Run("recover-0-of-10-of-10", benchRecover(10, 10, 0))

	b.Run("reconstruct-1-of-10-of-40", benchReconstruct(10, 40, 1))
	b.Run("reconstruct-10-of-10-of-40", benchReconstruct(10, 40, 10))
}
