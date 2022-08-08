package blake2b

import "golang.org/x/sys/cpu"

//go:generate go run gen.go -out blake2b_amd64.s

func hashBlock(msg *[64]byte, prefix uint64) [32]byte {
	// TODO: asm
	return hashBlockGeneric(msg, prefix)
}

//go:noescape
func hashBlocksAVX2(outs *[4][32]byte, msgs *[4][64]byte, prefix uint64)

func hashBlocks(outs *[4][32]byte, msgs *[4][64]byte, prefix uint64) {
	switch {
	case cpu.X86.HasAVX2:
		hashBlocksAVX2(outs, msgs, prefix)
	default:
		hashBlocksGeneric(outs, msgs, prefix)
	}
}
