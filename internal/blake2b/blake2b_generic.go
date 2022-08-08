//go:build !amd64
// +build !amd64

package blake2b

func hashBlock(msg *[64]byte, prefix uint64) [32]byte {
	return hashBlockGeneric(msg, prefix)
}

func hashBlocks(outs *[4][32]byte, msgs *[4][64]byte, prefix uint64) {
	hashBlocksGeneric(outs, msgs, prefix)
}
