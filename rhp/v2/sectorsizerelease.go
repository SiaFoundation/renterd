//go:build !testing

package rhp

// Most of these algorithms are derived from "Streaming Merkle Proofs within
// Binary Numeral Trees", available at https://eprint.iacr.org/2021/038

const (
	// SectorSize is the size of one sector in bytes.
	SectorSize = 1 << 22 // 4 MiB

	// LeavesPerSector is the number of leaves in one sector.
	LeavesPerSector = SectorSize / LeafSize
)
