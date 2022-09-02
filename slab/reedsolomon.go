package slab

import (
	"bytes"
	"io"

	"github.com/klauspost/reedsolomon"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
)

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

// Encode encodes slab data into sector-sized shards. The supplied shards should
// have a capacity of at least rhpv2.SectorSize, or they will be reallocated.
func (s Slab) Encode(buf []byte, shards [][]byte) {
	for i := range shards {
		if cap(shards[i]) < rhpv2.SectorSize {
			shards[i] = make([]byte, 0, rhpv2.SectorSize)
		}
		shards[i] = shards[i][:rhpv2.SectorSize]
	}
	stripedSplit(buf, shards[:s.MinShards])
	rsc, _ := reedsolomon.New(int(s.MinShards), len(shards)-int(s.MinShards))
	if err := rsc.Encode(shards); err != nil {
		panic(err)
	}
}

// Reconstruct reconstructs the missing shards of a slab. Missing shards must
// have a len of zero. All shards should have a capacity of at least
// rhpv2.SectorSize, or they will be reallocated.
func (s Slab) Reconstruct(shards [][]byte) error {
	for i := range shards {
		if len(shards[i]) != rhpv2.SectorSize && len(shards[i]) != 0 {
			panic("shards must have a len of either 0 or rhpv2.SectorSize")
		}
		if cap(shards[i]) < rhpv2.SectorSize {
			shards[i] = make([]byte, 0, rhpv2.SectorSize)
		}
		if len(shards[i]) != 0 {
			shards[i] = shards[i][:rhpv2.SectorSize]
		}
	}

	rsc, _ := reedsolomon.New(int(s.MinShards), len(shards)-int(s.MinShards))
	if err := rsc.Reconstruct(shards); err != nil {
		return err
	}
	return nil
}

// Recover recovers a slice of slab data from the supplied shards.
func (s Slice) Recover(w io.Writer, shards [][]byte) error {
	rsc, _ := reedsolomon.New(int(s.MinShards), len(shards)-int(s.MinShards))
	if err := rsc.ReconstructData(shards); err != nil {
		return err
	}
	skip := s.Offset % (rhpv2.LeafSize * uint32(s.MinShards))
	return stripedJoin(w, shards[:s.MinShards], int(skip), int(s.Length))
}
