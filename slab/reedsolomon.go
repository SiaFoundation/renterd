package slab

import (
	"bytes"
	"io"

	"github.com/klauspost/reedsolomon"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
)

// An RSCode encodes and decodes data to/from a set of shards. The encoding is
// piecewise, such that every leaf-sized chunk can be decoded individually.
type RSCode struct {
	enc  reedsolomon.Encoder
	m, n uint8
}

// Encode encodes data into shards. The resulting shards do not constitute a
// single matrix, but a series of matrices, each with a shard size of
// rhpv2.LeafSize. The supplied shards must each have a capacity of at least
// len(data)/m. Encode may alter the len of the shards.
func (rsc RSCode) Encode(data []byte, shards [][]byte) {
	// extend all shards to proper len
	chunkSize := int(rsc.m) * rhpv2.LeafSize
	numChunks := len(data) / chunkSize
	if len(data)%chunkSize != 0 {
		numChunks++
	}
	shardSize := numChunks * rhpv2.LeafSize
	for i := range shards {
		if cap(shards[i]) < shardSize {
			panic("each shard must have capacity of at least len(data)/m")
		}
		shards[i] = shards[i][:shardSize]
	}

	// split data into data shards
	stripedSplit(data, shards[:rsc.m])

	// encode parity shards
	if err := rsc.enc.Encode(shards); err != nil {
		panic(err)
	}
}

// Reconstruct recalculates any missing shards in the input. Missing
// shards must have the same capacity as a normal shard, but a length of
// zero.
func (rsc RSCode) Reconstruct(shards [][]byte) error {
	for i := range shards {
		if cap(shards[i]) != cap(shards[0]) {
			panic("all shards must have same capacity")
		}
	}
	return rsc.enc.Reconstruct(shards)
}

// Recover reconstructs all data shards and writes them to w, skipping the first
// off bytes and stopping after n bytes.
func (rsc RSCode) Recover(w io.Writer, shards [][]byte, off, n int) error {
	if err := rsc.enc.ReconstructData(shards); err != nil {
		return err
	}
	return stripedJoin(w, shards[:rsc.m], off, n)
}

// NewRSCode returns an m-of-n Reed-Solomon erasure code. It panics if m <= 0 or
// n < m.
func NewRSCode(m, n uint8) RSCode {
	rsc, err := reedsolomon.New(int(m), int(n-m))
	if err != nil {
		panic(err)
	}
	return RSCode{
		enc: rsc,
		m:   m,
		n:   n,
	}
}

// stripedSplit splits data into striped data shards, which must have sufficient
// capacity.
func stripedSplit(data []byte, dataShards [][]byte) {
	buf := bytes.NewBuffer(data)
	for off := 0; buf.Len() > 0; off += rhpv2.LeafSize {
		for _, shard := range dataShards {
			copy(shard[off:], buf.Next(rhpv2.LeafSize))
		}
	}
}

// stripedJoin joins the striped data shards, writing them to dst. The first 'skip'
// bytes of the recovered data are skipped, and 'writeLen' bytes are written in
// total.
func stripedJoin(dst io.Writer, dataShards [][]byte, skip, writeLen int) error {
	for off := 0; writeLen > 0; off += rhpv2.LeafSize {
		for _, shard := range dataShards {
			if len(shard[off:]) < rhpv2.LeafSize {
				return reedsolomon.ErrShortData
			}
			shard = shard[off:][:rhpv2.LeafSize]
			if skip >= len(shard) {
				skip -= len(shard)
				continue
			} else if skip > 0 {
				shard = shard[skip:]
				skip = 0
			}
			if writeLen < len(shard) {
				shard = shard[:writeLen]
			}
			n, err := dst.Write(shard)
			if err != nil {
				return err
			}
			writeLen -= n
		}
	}
	return nil
}

// EncodeSlab encodes slab data into sector-sized shards.
func EncodeSlab(s Slab, buf []byte, shards [][]byte) {
	for i := range shards {
		if cap(shards[i]) < rhpv2.SectorSize {
			shards[i] = make([]byte, 0, rhpv2.SectorSize)
		}
		shards[i] = shards[i][:rhpv2.SectorSize]
	}
	rsc := NewRSCode(s.MinShards, uint8(len(shards)))
	rsc.Encode(buf, shards)
	s.Key.EncryptShards(shards)
}

// RecoverSlab recovers a slice of slab data from the supplied shards.
func RecoverSlab(w io.Writer, s Slice, shards [][]byte) error {
	minChunkSize := (rhpv2.LeafSize * uint32(s.MinShards))
	skip := s.Offset % minChunkSize
	offset := (s.Offset / minChunkSize) * rhpv2.LeafSize
	s.Key.DecryptShards(shards, offset)
	rsc := NewRSCode(s.MinShards, uint8(len(shards)))
	return rsc.Recover(w, shards, int(skip), int(s.Length))
}

// ReconstructSlab reconstructs the missing shards of a slab.
func ReconstructSlab(s Slab, shards [][]byte) error {
	s.Key.DecryptShards(shards, 0)
	rsc := NewRSCode(s.MinShards, uint8(len(s.Shards)))
	if err := rsc.Reconstruct(shards); err != nil {
		return err
	}
	s.Key.EncryptShards(shards)
	return nil
}
