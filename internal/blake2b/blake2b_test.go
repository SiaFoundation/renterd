package blake2b

import (
	"testing"
	"unsafe"
)

func TestBLAKE2b(t *testing.T) {
	var leaves [4][64]byte
	for i := range leaves {
		for j := range leaves[i] {
			leaves[i][j] = byte(i*64 + j + 7)
		}
	}
	var refs [4][32]byte
	hashBlocksGeneric(&refs, &leaves, 0)
	var outs [4][32]byte
	SumLeaves(&outs, &leaves)
	for i := range outs {
		if outs[i] != refs[i] {
			t.Fatalf("mismatch %v:\nasm: %x\nref: %x", i, outs[i], refs[i])
		}
	}

	parents := (*[8][32]byte)(unsafe.Pointer(&leaves))
	hashBlocksGeneric(&refs, &leaves, 1)
	SumNodes(&outs, parents)
	for i := range outs {
		if outs[i] != refs[i] {
			t.Fatalf("mismatch %v:\nasm: %x\nref: %x", i, outs[i], refs[i])
		}
	}
}

func BenchmarkBLAKE2b(b *testing.B) {
	var leaves [4][64]byte
	var nodes [8][32]byte
	var outs [4][32]byte
	b.Run("SumLeaves", func(b *testing.B) {
		b.SetBytes(4 * 64)
		for i := 0; i < b.N; i++ {
			SumLeaves(&outs, &leaves)
		}
	})
	b.Run("SumNodes", func(b *testing.B) {
		b.SetBytes(8 * 32)
		for i := 0; i < b.N; i++ {
			SumNodes(&outs, &nodes)
		}
	})
	b.Run("hashBlocksGeneric", func(b *testing.B) {
		b.SetBytes(4 * 64)
		for i := 0; i < b.N; i++ {
			hashBlocksGeneric(&outs, &leaves, 0)
		}
	})
}
