package rhp

import (
	"bytes"
	"math/bits"
	"testing"

	"go.sia.tech/core/types"
	"golang.org/x/crypto/blake2b"
	"lukechampine.com/frand"
)

func leafHash(seg []byte) types.Hash256 {
	return blake2b.Sum256(append([]byte{0}, seg...))
}

func nodeHash(left, right types.Hash256) types.Hash256 {
	return blake2b.Sum256(append([]byte{1}, append(left[:], right[:]...)...))
}

func refSectorRoot(sector *[SectorSize]byte) types.Hash256 {
	roots := make([]types.Hash256, LeavesPerSector)
	for i := range roots {
		roots[i] = leafHash(sector[i*LeafSize:][:LeafSize])
	}
	return recNodeRoot(roots)
}

func recNodeRoot(roots []types.Hash256) types.Hash256 {
	switch len(roots) {
	case 0:
		return types.Hash256{}
	case 1:
		return roots[0]
	default:
		// split at largest power of two
		split := 1 << (bits.Len(uint(len(roots)-1)) - 1)
		return nodeHash(
			recNodeRoot(roots[:split]),
			recNodeRoot(roots[split:]),
		)
	}
}

func TestSectorRoot(t *testing.T) {
	// test some known roots
	var sector [SectorSize]byte
	if SectorRoot(&sector).String() != "h:50ed59cecd5ed3ca9e65cec0797202091dbba45272dafa3faa4e27064eedd52c" {
		t.Error("wrong Merkle root for empty sector")
	}
	sector[0] = 1
	if SectorRoot(&sector).String() != "h:8c20a2c90a733a5139cc57e45755322e304451c3434b0c0a0aad87f2f89a44ab" {
		t.Error("wrong Merkle root for sector[0] = 1")
	}
	sector[0] = 0
	sector[SectorSize-1] = 1
	if SectorRoot(&sector).String() != "h:d0ab6691d76750618452e920386e5f6f98fdd1219a70a06f06ef622ac6c6373c" {
		t.Error("wrong Merkle root for sector[SectorSize-1] = 1")
	}

	// test some random roots against a reference implementation
	for i := 0; i < 5; i++ {
		frand.Read(sector[:])
		if SectorRoot(&sector) != refSectorRoot(&sector) {
			t.Error("SectorRoot does not match reference implementation")
		}
	}

	// SectorRoot should not allocate
	allocs := testing.AllocsPerRun(5, func() {
		_ = SectorRoot(&sector)
	})
	if allocs > 0 {
		t.Error("expected SectorRoot to allocate 0 times, got", allocs)
	}
}

func BenchmarkSectorRoot(b *testing.B) {
	b.ReportAllocs()
	var sector [SectorSize]byte
	b.SetBytes(SectorSize)
	for i := 0; i < b.N; i++ {
		_ = SectorRoot(&sector)
	}
}

func TestMetaRoot(t *testing.T) {
	// test some known roots
	if MetaRoot(nil) != (types.Hash256{}) {
		t.Error("wrong Merkle root for empty tree")
	}
	roots := make([]types.Hash256, 1)
	roots[0] = frand.Entropy256()
	if MetaRoot(roots) != roots[0] {
		t.Error("wrong Merkle root for single root")
	}
	roots = make([]types.Hash256, 32)
	if MetaRoot(roots).String() != "h:1c23727030051d1bba1c887273addac2054afbd6926daddef6740f4f8bf1fb7f" {
		t.Error("wrong Merkle root for 32 empty roots")
	}
	roots[0][0] = 1
	if MetaRoot(roots).String() != "h:c5da05749139505704ea18a5d92d46427f652ac79c5f5712e4aefb68e20dffb8" {
		t.Error("wrong Merkle root for roots[0][0] = 1")
	}

	// test some random roots against a reference implementation
	for i := 0; i < 5; i++ {
		for j := range roots {
			roots[j] = frand.Entropy256()
		}
		if MetaRoot(roots) != recNodeRoot(roots) {
			t.Error("MetaRoot does not match reference implementation")
		}
	}
	// test some random tree sizes
	for i := 0; i < 10; i++ {
		roots := make([]types.Hash256, frand.Intn(LeavesPerSector))
		if MetaRoot(roots) != recNodeRoot(roots) {
			t.Error("MetaRoot does not match reference implementation")
		}
	}

	roots = roots[:5]
	if MetaRoot(roots) != recNodeRoot(roots) {
		t.Error("MetaRoot does not match reference implementation")
	}

	allocs := testing.AllocsPerRun(10, func() {
		_ = MetaRoot(roots)
	})
	if allocs > 0 {
		t.Error("expected MetaRoot to allocate 0 times, got", allocs)
	}

	// test a massive number of roots, larger than a single stack can store
	const sectorsPerTerabyte = 262145
	roots = make([]types.Hash256, sectorsPerTerabyte)
	if MetaRoot(roots) != recNodeRoot(roots) {
		t.Error("MetaRoot does not match reference implementation")
	}
}

func BenchmarkMetaRoot1TB(b *testing.B) {
	const sectorsPerTerabyte = 262144
	roots := make([]types.Hash256, sectorsPerTerabyte)
	b.SetBytes(sectorsPerTerabyte * 32)
	for i := 0; i < b.N; i++ {
		_ = MetaRoot(roots)
	}
}

func TestProofAccumulator(t *testing.T) {
	var pa proofAccumulator

	// test some known roots
	if pa.root() != (types.Hash256{}) {
		t.Error("wrong root for empty accumulator")
	}

	roots := make([]types.Hash256, 32)
	for _, root := range roots {
		pa.insertNode(root, 0)
	}
	if pa.root().String() != "h:1c23727030051d1bba1c887273addac2054afbd6926daddef6740f4f8bf1fb7f" {
		t.Error("wrong root for 32 empty roots")
	}

	pa = proofAccumulator{}
	roots[0][0] = 1
	for _, root := range roots {
		pa.insertNode(root, 0)
	}
	if pa.root().String() != "h:c5da05749139505704ea18a5d92d46427f652ac79c5f5712e4aefb68e20dffb8" {
		t.Error("wrong root for roots[0][0] = 1")
	}

	// test some random roots against a reference implementation
	for i := 0; i < 5; i++ {
		var pa proofAccumulator
		for j := range roots {
			roots[j] = frand.Entropy256()
			pa.insertNode(roots[j], 0)
		}
		if pa.root() != recNodeRoot(roots) {
			t.Error("root does not match reference implementation")
		}
	}

	// test an odd number of roots
	pa = proofAccumulator{}
	roots = roots[:5]
	for _, root := range roots {
		pa.insertNode(root, 0)
	}
	refRoot := recNodeRoot([]types.Hash256{recNodeRoot(roots[:4]), roots[4]})
	if pa.root() != refRoot {
		t.Error("root does not match reference implementation")
	}
}

func TestReadSector(t *testing.T) {
	var expected [SectorSize]byte
	frand.Read(expected[:256])
	buf := bytes.NewBuffer(nil)
	buf.Write(expected[:])

	expectedRoot := refSectorRoot(&expected)
	root, sector, err := ReadSector(buf)
	if err != nil {
		t.Fatal(err)
	} else if expectedRoot != root {
		t.Fatalf("incorrect root: expected %s, got %s", expected, root)
	} else if !bytes.Equal(sector[:], expected[:]) {
		t.Fatalf("incorrect data: expected %v, got %v", expected, sector)
	}

	buf.Reset()
	buf.Write(expected[:len(expected)-100])
	_, _, err = ReadSector(buf)
	if err == nil {
		t.Fatal("expected read error")
	}
}

func BenchmarkReadSector(b *testing.B) {
	buf := bytes.NewBuffer(nil)
	buf.Grow(SectorSize)

	sector := make([]byte, SectorSize)
	frand.Read(sector[:256])

	b.SetBytes(SectorSize)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buf.Reset()
		buf.Write(sector)
		_, _, err := ReadSector(buf)
		if err != nil {
			b.Fatal(err)
		}
	}
}
