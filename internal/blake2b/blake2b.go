// Package blake2b implements the BLAKE2b cryptographic hash function,
// with optimized variants for hashing Merkle tree inputs.
package blake2b

import (
	"unsafe"

	"golang.org/x/crypto/blake2b"
)

// from RFC 6962
const leafHashPrefix = 0
const nodeHashPrefix = 1

// SumLeaf computes the Merkle tree leaf hash of a single leaf.
func SumLeaf(leaf *[64]byte) [32]byte {
	return hashBlock(leaf, leafHashPrefix)
}

// SumPair computes the Merkle root of a pair of node hashes.
func SumPair(left, right [32]byte) [32]byte {
	return hashBlock((*[64]byte)(unsafe.Pointer(&[2][32]byte{left, right})), nodeHashPrefix)
}

// SumLeaves computes the Merkle tree leaf hash of four leaves, storing the
// results in outs.
func SumLeaves(outs *[4][32]byte, leaves *[4][64]byte) {
	hashBlocks(outs, leaves, leafHashPrefix)
}

// SumNodes computes the Merkle roots of four pairs of node hashes, storing the
// results in outs.
func SumNodes(outs *[4][32]byte, nodes *[8][32]byte) {
	hashBlocks(outs, (*[4][64]byte)(unsafe.Pointer(nodes)), nodeHashPrefix)
}

func hashBlockGeneric(msg *[64]byte, prefix uint64) [32]byte {
	var buf [65]byte
	buf[0] = byte(prefix)
	copy(buf[1:], msg[:])
	return blake2b.Sum256(buf[:])
}

func hashBlocksGeneric(outs *[4][32]byte, msgs *[4][64]byte, prefix uint64) {
	for i := range msgs {
		outs[i] = hashBlockGeneric(&msgs[i], prefix)
	}
}
