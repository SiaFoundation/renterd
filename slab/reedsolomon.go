package slab

import (
	"bytes"
	"io"

	"github.com/klauspost/reedsolomon"
	"go.sia.tech/renterd/rhp/v2"
)

// An RSCode encodes and decodes data to/from a set of shards. The encoding is
// piecewise, such that every leaf-sized chunk can be decoded individually.
type RSCode struct {
	enc  reedsolomon.Encoder
	m, n uint8
}

// Encode encodes data into shards. The resulting shards do not constitute a
// single matrix, but a series of matrices, each with a shard size of
// rhp.LeafSize. The supplied shards must each have a capacity of at least
// len(data)/m. Encode may alter the len of the shards.
func (rsc RSCode) Encode(data []byte, shards [][]byte) {
	// extend all shards to proper len
	chunkSize := int(rsc.m) * rhp.LeafSize
	numChunks := len(data) / chunkSize
	if len(data)%chunkSize != 0 {
		numChunks++
	}
	shardSize := numChunks * rhp.LeafSize
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
	for off := 0; buf.Len() > 0; off += rhp.LeafSize {
		for _, shard := range dataShards {
			copy(shard[off:], buf.Next(rhp.LeafSize))
		}
	}
}

// stripedJoin joins the striped data shards, writing them to dst. The first 'skip'
// bytes of the recovered data are skipped, and 'writeLen' bytes are written in
// total.
func stripedJoin(dst io.Writer, dataShards [][]byte, skip, writeLen int) error {
	for off := 0; writeLen > 0; off += rhp.LeafSize {
		for _, shard := range dataShards {
			if len(shard[off:]) < rhp.LeafSize {
				return reedsolomon.ErrShortData
			}
			shard = shard[off:][:rhp.LeafSize]
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
