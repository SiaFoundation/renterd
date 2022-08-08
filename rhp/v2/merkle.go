package rhp

import (
	"bytes"
	"errors"
	"io"
	"math"
	"math/bits"
	"sort"
	"unsafe"

	"go.sia.tech/renterd/internal/blake2b"
	"go.sia.tech/renterd/internal/consensus"
)

// Most of these algorithms are derived from "Streaming Merkle Proofs within
// Binary Numeral Trees", available at https://eprint.iacr.org/2021/038

const (
	// SectorSize is the size of one sector in bytes.
	SectorSize = 1 << 22 // 4 MiB

	// LeafSize is the size of one leaf in bytes.
	LeafSize = 64

	// LeavesPerSector is the number of leaves in one sector.
	LeavesPerSector = SectorSize / LeafSize
)

// A proofAccumulator is a specialized accumulator for building and verifying
// Merkle proofs.
type proofAccumulator struct {
	trees     [64]consensus.Hash256
	numLeaves uint64
}

func (pa *proofAccumulator) hasNodeAtHeight(height int) bool {
	return pa.numLeaves&(1<<height) != 0
}

func (pa *proofAccumulator) insertNode(h consensus.Hash256, height int) {
	i := height
	for ; pa.hasNodeAtHeight(i); i++ {
		h = blake2b.SumPair(pa.trees[i], h)
	}
	pa.trees[i] = h
	pa.numLeaves += 1 << height
}

func (pa *proofAccumulator) root() consensus.Hash256 {
	i := bits.TrailingZeros64(pa.numLeaves)
	if i == 64 {
		return consensus.Hash256{}
	}
	root := pa.trees[i]
	for i++; i < len(pa.trees); i++ {
		if pa.hasNodeAtHeight(i) {
			root = blake2b.SumPair(pa.trees[i], root)
		}
	}
	return root
}

// sectorAccumulator is a specialized accumulator for computing the total root
// of a sector.
type sectorAccumulator struct {
	// Unlike proofAccumulator, the subtree roots are ordered largest-to-
	// smallest, and we store four roots per height. This ordering allows us to
	// cast two adjacent elements into a single [8][32]byte, which reduces
	// copying when hashing.
	trees [15][4][32]byte
	// Since we operate on 8 nodes at a time, we need a buffer to hold nodes
	// until we have enough. And since the buffer is adjacent to the trees in
	// memory, we can again avoid some copying.
	nodeBuf [4][32]byte
	// Like proofAccumulator, 'numLeaves' is both the number of subtree roots
	// appended and a bit vector that indicates which elements are active. We
	// also use it to determine how many nodes are in the buffer.
	numLeaves uint32
}

// We rely on the nodeBuf field immediately following the last element of the
// trees field. This should always be true -- there's no reason for a compiler
// to insert padding between them -- but it doesn't hurt to check.
var _ [unsafe.Offsetof(sectorAccumulator{}.nodeBuf)]struct{} = [unsafe.Sizeof(sectorAccumulator{}.trees)]struct{}{}

func (sa *sectorAccumulator) hasNodeAtHeight(i int) bool {
	// not as simple as in proofAccumulator; order is reversed, and sa.numLeaves
	// is "off" by a factor of 4
	return (sa.numLeaves>>2)&(1<<(len(sa.trees)-i-1)) != 0
}

func (sa *sectorAccumulator) appendNode(h consensus.Hash256) {
	sa.nodeBuf[sa.numLeaves%4] = h
	sa.numLeaves++
	if sa.numLeaves%4 == 0 {
		sa.numLeaves -= 4 // hack: offset mergeNodeBuf adding 4
		sa.mergeNodeBuf()
	}
}

func (sa *sectorAccumulator) appendLeaves(leaves []byte) {
	if len(leaves)%LeafSize != 0 {
		panic("appendLeaves: illegal input size")
	}
	rem := len(leaves) % (LeafSize * 4)
	for i := 0; i < len(leaves)-rem; i += LeafSize * 4 {
		blake2b.SumLeaves(&sa.nodeBuf, (*[4][64]byte)(unsafe.Pointer(&leaves[i])))
		sa.mergeNodeBuf()
	}
	for i := len(leaves) - rem; i < len(leaves); i += LeafSize {
		sa.appendNode(blake2b.SumLeaf((*[64]byte)(unsafe.Pointer(&leaves[i]))))
	}
}

func (sa *sectorAccumulator) mergeNodeBuf() {
	// same as in proofAccumulator, except that we operate on 8 nodes at a time,
	// exploiting the fact that the two groups of 4 are contiguous in memory
	nodes := &sa.nodeBuf
	i := len(sa.trees) - 1
	for ; sa.hasNodeAtHeight(i); i-- {
		blake2b.SumNodes(&sa.trees[i], (*[8][32]byte)(unsafe.Pointer(&sa.trees[i])))
		nodes = &sa.trees[i]
	}
	sa.trees[i] = *nodes
	sa.numLeaves += 4
}

func (sa *sectorAccumulator) root() consensus.Hash256 {
	if sa.numLeaves == 0 {
		return consensus.Hash256{}
	}

	// helper function for computing the root of four subtrees
	root4 := func(nodes [4][32]byte) consensus.Hash256 {
		// NOTE: it would be more efficient to mutate sa.trees directly, but
		// that would make root non-idempotent
		in := (*[8][32]byte)(unsafe.Pointer(&[2][4][32]byte{0: nodes}))
		out := (*[4][32]byte)(unsafe.Pointer(in))
		blake2b.SumNodes(out, in)
		blake2b.SumNodes(out, in)
		return out[0]
	}

	i := len(sa.trees) - 1 - bits.TrailingZeros32(sa.numLeaves>>2)
	var root consensus.Hash256
	switch sa.numLeaves % 4 {
	case 0:
		root = root4(sa.trees[i])
		i--
	case 1:
		root = sa.nodeBuf[0]
	case 2:
		root = blake2b.SumPair(sa.nodeBuf[0], sa.nodeBuf[1])
	case 3:
		root = blake2b.SumPair(blake2b.SumPair(sa.nodeBuf[0], sa.nodeBuf[1]), sa.nodeBuf[2])
	}
	for ; i >= 0; i-- {
		if sa.hasNodeAtHeight(i) {
			root = blake2b.SumPair(root4(sa.trees[i]), root)
		}
	}
	return root
}

// SectorRoot computes the Merkle root of a sector.
func SectorRoot(sector *[SectorSize]byte) consensus.Hash256 {
	var sa sectorAccumulator
	sa.appendLeaves(sector[:])
	return sa.root()
}

// ReaderRoot returns the Merkle root of the supplied stream, which must contain
// an integer multiple of leaves.
func ReaderRoot(r io.Reader) (consensus.Hash256, error) {
	var s sectorAccumulator
	leafBatch := make([]byte, LeafSize*16)
	for {
		n, err := io.ReadFull(r, leafBatch)
		if err == io.EOF {
			break
		} else if err == io.ErrUnexpectedEOF {
			if n%LeafSize != 0 {
				return consensus.Hash256{}, errors.New("stream does not contain integer multiple of leaves")
			}
		} else if err != nil {
			return consensus.Hash256{}, err
		}
		s.appendLeaves(leafBatch[:n])
	}
	return s.root(), nil
}

// ReadSector reads a single sector from r and calculates its root.
func ReadSector(r io.Reader) (consensus.Hash256, *[SectorSize]byte, error) {
	var sector [SectorSize]byte
	buf := bytes.NewBuffer(sector[:0])
	root, err := ReaderRoot(io.TeeReader(io.LimitReader(r, SectorSize), buf))
	if buf.Len() != SectorSize {
		return consensus.Hash256{}, nil, io.ErrUnexpectedEOF
	}
	return root, &sector, err
}

// MetaRoot calculates the root of a set of existing Merkle roots.
func MetaRoot(roots []consensus.Hash256) consensus.Hash256 {
	// sectorAccumulator is only designed to store one sector's worth of leaves,
	// so we'll panic if we insert more than leavesPerSector leaves. To
	// compensate, call MetaRoot recursively.
	if len(roots) <= LeavesPerSector {
		var sa sectorAccumulator
		for _, r := range roots {
			sa.appendNode(r)
		}
		return sa.root()
	}
	// split at largest power of two
	split := 1 << (bits.Len(uint(len(roots)-1)) - 1)
	return blake2b.SumPair(MetaRoot(roots[:split]), MetaRoot(roots[split:]))
}

// ProofSize returns the size of a Merkle proof for the leaf i within a tree
// containing n leaves.
func ProofSize(n, i uint64) uint64 {
	return RangeProofSize(n, i, i+1)
}

// RangeProofSize returns the size of a Merkle proof for the leaf range [start,
// end) within a tree containing n leaves.
func RangeProofSize(n, start, end uint64) uint64 {
	leftHashes := bits.OnesCount64(start)
	pathMask := uint64(1)<<bits.Len64((end-1)^(n-1)) - 1
	rightHashes := bits.OnesCount64(^(end - 1) & pathMask)
	return uint64(leftHashes + rightHashes)
}

// DiffProofSize returns the size of a Merkle diff proof for the specified
// actions within a tree containing n leaves.
func DiffProofSize(n uint64, actions []RPCWriteAction) uint64 {
	return 128 // TODO
}

// nextSubtreeSize returns the size of the subtree adjacent to start that does
// not overlap end.
func nextSubtreeSize(start, end uint64) uint64 {
	ideal := bits.TrailingZeros64(start)
	max := bits.Len64(end-start) - 1
	if ideal > max {
		return 1 << max
	}
	return 1 << ideal
}

// A RangeProofVerifier allows range proofs to be verified in streaming fashion.
type RangeProofVerifier struct {
	start, end uint64
	roots      []consensus.Hash256
}

// ReadFrom implements io.ReaderFrom.
func (rpv *RangeProofVerifier) ReadFrom(r io.Reader) (int64, error) {
	var total int64
	i, j := rpv.start, rpv.end
	for i < j {
		subtreeSize := nextSubtreeSize(i, j)
		n := int64(subtreeSize * LeafSize)
		root, err := ReaderRoot(io.LimitReader(r, n))
		if err != nil {
			return total, err
		}
		total += n
		rpv.roots = append(rpv.roots, root)
		i += subtreeSize
	}
	return total, nil
}

// Verify verifies the supplied proof, using the data ingested from ReadFrom.
func (rpv *RangeProofVerifier) Verify(proof []consensus.Hash256, root consensus.Hash256) bool {
	if uint64(len(proof)) != RangeProofSize(LeavesPerSector, rpv.start, rpv.end) {
		return false
	}
	var acc proofAccumulator
	consume := func(roots *[]consensus.Hash256, i, j uint64) {
		for i < j && len(*roots) > 0 {
			subtreeSize := nextSubtreeSize(i, j)
			height := bits.TrailingZeros64(subtreeSize) // log2
			acc.insertNode((*roots)[0], height)
			*roots = (*roots)[1:]
			i += subtreeSize
		}
	}
	consume(&proof, 0, rpv.start)
	consume(&rpv.roots, rpv.start, rpv.end)
	consume(&proof, rpv.end, LeavesPerSector)
	return acc.root() == root
}

// NewRangeProofVerifier returns a RangeProofVerifier for the sector range
// [start, end).
func NewRangeProofVerifier(start, end uint64) *RangeProofVerifier {
	return &RangeProofVerifier{
		start: start,
		end:   end,
	}
}

// VerifySectorRangeProof verifies a proof produced by BuildRangeProof.
func VerifySectorRangeProof(proof []consensus.Hash256, rangeRoots []consensus.Hash256, start, end, numRoots uint64, root consensus.Hash256) bool {
	if numRoots == 0 {
		return len(proof) == 0
	} else if uint64(len(rangeRoots)) != end-start {
		panic("VerifySectorRangeProof: number of roots does not match range")
	} else if end > numRoots || start > end || start == end {
		panic("VerifySectorRangeProof: illegal proof range")
	}
	if uint64(len(proof)) != RangeProofSize(numRoots, start, end) {
		return false
	}

	var acc proofAccumulator
	insertRange := func(i, j uint64) {
		for i < j && len(proof) > 0 {
			subtreeSize := nextSubtreeSize(i, j)
			height := bits.TrailingZeros64(subtreeSize) // log2
			acc.insertNode(proof[0], height)
			proof = proof[1:]
			i += subtreeSize
		}
	}

	insertRange(0, start)
	for _, h := range rangeRoots {
		acc.insertNode(h, 0)
	}
	insertRange(end, math.MaxUint64)
	return acc.root() == root
}

// VerifyAppendProof verifies a proof produced by BuildAppendProof.
func VerifyAppendProof(numLeaves uint64, treeHashes []consensus.Hash256, sectorRoot, oldRoot, newRoot consensus.Hash256) bool {
	acc := proofAccumulator{numLeaves: numLeaves}
	for i := range acc.trees {
		if acc.hasNodeAtHeight(i) && len(treeHashes) > 0 {
			acc.trees[i] = treeHashes[0]
			treeHashes = treeHashes[1:]
		}
	}
	if acc.root() != oldRoot {
		return false
	}
	acc.insertNode(sectorRoot, 0)
	return acc.root() == newRoot
}

func precomputeAppendRoots(actions []RPCWriteAction) []consensus.Hash256 {
	var roots []consensus.Hash256
	for _, action := range actions {
		if action.Type == RPCWriteActionAppend {
			roots = append(roots, SectorRoot((*[SectorSize]byte)(action.Data)))
		}
	}
	return roots
}

// VerifyDiffProof verifies a proof produced by BuildDiffProof. ActionUpdate is
// not supported. If appendRoots is non-nil, it is assumed to contain the
// precomputed SectorRoots of all Append actions.
func VerifyDiffProof(actions []RPCWriteAction, numLeaves uint64, treeHashes, leafHashes []consensus.Hash256, oldRoot, newRoot consensus.Hash256, appendRoots []consensus.Hash256) bool {
	verifyMulti := func(proofIndices []uint64, treeHashes, leafHashes []consensus.Hash256, numLeaves uint64, root consensus.Hash256) bool {
		var acc proofAccumulator
		insertRange := func(i, j uint64) {
			for i < j {
				subtreeSize := nextSubtreeSize(i, j)
				height := bits.TrailingZeros64(subtreeSize) // log2
				acc.insertNode(treeHashes[0], height)
				treeHashes = treeHashes[1:]
				i += subtreeSize
			}
		}

		var start uint64
		for i, end := range proofIndices {
			insertRange(start, end)
			start = end + 1
			acc.insertNode(leafHashes[i], 0)
		}
		insertRange(start, numLeaves)

		return acc.root() == root
	}

	// first use the original proof to construct oldRoot
	proofIndices := sectorsChanged(actions, numLeaves)
	if len(proofIndices) != len(leafHashes) {
		return false
	}
	if !verifyMulti(proofIndices, treeHashes, leafHashes, numLeaves, oldRoot) {
		return false
	}

	// then modify the proof according to actions and construct the newRoot
	newLeafHashes := modifyLeaves(leafHashes, actions, numLeaves, appendRoots)
	newProofIndices := modifyProofRanges(proofIndices, actions, numLeaves)
	numLeaves += uint64(len(newLeafHashes) - len(leafHashes))

	return verifyMulti(newProofIndices, treeHashes, newLeafHashes, numLeaves, newRoot)
}

func sectorsChanged(actions []RPCWriteAction, numSectors uint64) []uint64 {
	newNumSectors := numSectors
	sectorsChanged := make(map[uint64]struct{})
	for _, action := range actions {
		switch action.Type {
		case RPCWriteActionAppend:
			sectorsChanged[newNumSectors] = struct{}{}
			newNumSectors++

		case RPCWriteActionTrim:
			for i := uint64(0); i < action.A; i++ {
				newNumSectors--
				sectorsChanged[newNumSectors] = struct{}{}
			}

		case RPCWriteActionSwap:
			sectorsChanged[action.A] = struct{}{}
			sectorsChanged[action.B] = struct{}{}

		default:
			panic("unknown or unsupported action type: " + action.Type.String())
		}
	}

	var sectorIndices []uint64
	for index := range sectorsChanged {
		if index < numSectors {
			sectorIndices = append(sectorIndices, index)
		}
	}
	sort.Slice(sectorIndices, func(i, j int) bool {
		return sectorIndices[i] < sectorIndices[j]
	})
	return sectorIndices
}

// modifyProofRanges modifies the proof ranges produced by calculateProofRanges
// to verify a post-modification Merkle diff proof for the specified actions.
func modifyProofRanges(proofIndices []uint64, actions []RPCWriteAction, numSectors uint64) []uint64 {
	for _, action := range actions {
		switch action.Type {
		case RPCWriteActionAppend:
			proofIndices = append(proofIndices, numSectors)
			numSectors++

		case RPCWriteActionTrim:
			proofIndices = proofIndices[:uint64(len(proofIndices))-action.A]
			numSectors -= action.A

		case RPCWriteActionSwap:
		case RPCWriteActionUpdate:

		default:
			panic("unknown or unsupported action type: " + action.Type.String())
		}
	}
	return proofIndices
}

// modifyLeaves modifies the leaf hashes of a Merkle diff proof to verify a
// post-modification Merkle diff proof for the specified actions.
func modifyLeaves(leafHashes []consensus.Hash256, actions []RPCWriteAction, numSectors uint64, appendRoots []consensus.Hash256) []consensus.Hash256 {
	// determine which sector index corresponds to each leaf hash
	var indices []uint64
	for _, action := range actions {
		switch action.Type {
		case RPCWriteActionAppend:
			indices = append(indices, numSectors)
			numSectors++
		case RPCWriteActionTrim:
			for j := uint64(0); j < action.A; j++ {
				numSectors--
				indices = append(indices, numSectors)
			}
		case RPCWriteActionSwap:
			indices = append(indices, action.A, action.B)

		default:
			panic("unknown or unsupported action type: " + action.Type.String())
		}
	}
	sort.Slice(indices, func(i, j int) bool {
		return indices[i] < indices[j]
	})
	indexMap := make(map[uint64]uint64, len(leafHashes))
	for i, index := range indices {
		if i > 0 && index == indices[i-1] {
			continue // remove duplicates
		}
		indexMap[index] = uint64(len(indexMap))
	}
	leafHashes = append([]consensus.Hash256(nil), leafHashes...)
	for _, action := range actions {
		switch action.Type {
		case RPCWriteActionAppend:
			var root consensus.Hash256
			if len(appendRoots) > 0 {
				root, appendRoots = appendRoots[0], appendRoots[1:]
			} else {
				root = SectorRoot((*[SectorSize]byte)(action.Data))
			}
			leafHashes = append(leafHashes, root)

		case RPCWriteActionTrim:
			leafHashes = leafHashes[:uint64(len(leafHashes))-action.A]

		case RPCWriteActionSwap:
			i, j := indexMap[action.A], indexMap[action.B]
			leafHashes[i], leafHashes[j] = leafHashes[j], leafHashes[i]

		default:
			panic("unknown or unsupported action type: " + action.Type.String())
		}
	}
	return leafHashes
}
