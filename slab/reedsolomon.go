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

// A UniformSlabReader reads slabs from a single input stream.
type UniformSlabReader struct {
	r         io.Reader
	rsc       RSCode
	buf       []byte
	shards    [][]byte
	minShards uint8
}

// ReadSlab implements SlabReader.
func (usr *UniformSlabReader) ReadSlab() (Slab, [][]byte, error) {
	_, err := io.ReadFull(usr.r, usr.buf)
	if err != nil && err != io.ErrUnexpectedEOF {
		return Slab{}, nil, err
	}
	usr.rsc.Encode(usr.buf, usr.shards)
	key := NewEncryptionKey()
	for i := range usr.shards {
		key.XORKeyStream(usr.shards[i], uint8(i), 0)
	}
	return Slab{
		Key:       key,
		MinShards: usr.minShards,
	}, usr.shards, nil
}

// NewUniformSlabReader returns a UnformSlabReader with the specified erasure
// coding parameters.
func NewUniformSlabReader(r io.Reader, m, n uint8) *UniformSlabReader {
	shards := make([][]byte, n)
	for i := range shards {
		shards[i] = make([]byte, 0, rhpv2.SectorSize)
	}
	return &UniformSlabReader{
		r:         r,
		rsc:       NewRSCode(m, n),
		buf:       make([]byte, int(m)*rhpv2.SectorSize),
		shards:    shards,
		minShards: m,
	}
}

// RecoverSlab recovers a slice of slab data from the supplied shards.
func RecoverSlab(w io.Writer, ss Slice, shards [][]byte) error {
	rsc := NewRSCode(ss.MinShards, uint8(len(shards)))
	minChunkSize := rhpv2.LeafSize * uint32(ss.MinShards)
	for i := range shards {
		ss.Key.XORKeyStream(shards[i], uint8(i), ss.Offset/minChunkSize)
	}
	skip := ss.Offset % minChunkSize
	return rsc.Recover(w, shards, int(skip), int(ss.Length))
}
