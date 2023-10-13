package object

import (
	"bytes"
	"io"

	"github.com/klauspost/reedsolomon"
	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"golang.org/x/crypto/chacha20"
)

// A Sector uniquely identifies a sector stored on a particular host.
type Sector struct {
	Host types.PublicKey `json:"host"`
	Root types.Hash256   `json:"root"`
}

// A Slab is raw data that has been erasure-encoded into sector-sized shards,
// encrypted, and stored across a set of hosts. A distinct EncryptionKey should
// be used for each Slab, and should not be the same key used for the parent
// Object.
type Slab struct {
	Health    float64       `json:"health"`
	Key       EncryptionKey `json:"key"`
	MinShards uint8         `json:"minShards"`
	Shards    []Sector      `json:"shards"`
}

type PartialSlab struct {
	Key    EncryptionKey `json:"key"`
	Offset uint32        `json:"offset"`
	Length uint32        `json:"length"`
}

// NewSlab returns a new slab for the shards.
func NewSlab(minShards uint8) Slab {
	return Slab{
		Key:       GenerateEncryptionKey(),
		MinShards: minShards,
	}
}

// Length returns the length of the raw data stored in s.
func (s Slab) Length() int {
	return rhpv2.SectorSize * int(s.MinShards)
}

// Encrypt xors shards with the keystream derived from s.Key, using a
// different nonce for each shard.
func (s Slab) Encrypt(shards [][]byte) {
	for i, shard := range shards {
		nonce := [24]byte{1: byte(i)}
		c, _ := chacha20.NewUnauthenticatedCipher(s.Key.entropy[:], nonce[:])
		c.XORKeyStream(shard, shard)
	}
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

// ReconstructSome reconstructs the required shards of a slab.
func (s Slab) ReconstructSome(shards [][]byte, required []bool) error {
	for i := range shards {
		// Make sure shards are either empty or full.
		if len(shards[i]) != rhpv2.SectorSize && len(shards[i]) != 0 {
			panic("shards must have a len of either 0 or rhpv2.SectorSize")
		}
		// Every required shard needs to have a sector worth of capacity.
		if required[i] && cap(shards[i]) < rhpv2.SectorSize {
			shards[i] = reedsolomon.AllocAligned(1, rhpv2.SectorSize)[0][:0]
		}
	}
	// The size of the batch per shard that gets reconstructed.
	var buf [rhpv2.SectorSize]byte
	rsc, _ := reedsolomon.New(int(s.MinShards), len(shards)-int(s.MinShards))

	dstShards := make([][]byte, len(shards))
	for i, shard := range shards {
		if len(shard) != 0 {
			// keep shards that are already present
			dstShards[i] = shards[i]
		} else if required[i] {
			// reconstruct required shards into 'shards'
			dstShards[i] = shards[i][:0]
		} else {
			// reconstruct non-required shards into a temporary buffer
			dstShards[i] = buf[:0]
		}
	}
	if err := rsc.Reconstruct(dstShards); err != nil {
		return err
	}
	for i := range shards {
		if required[i] {
			shards[i] = shards[i][:rhpv2.SectorSize]
		}
	}
	return nil
}

// A SlabSlice is a contiguous region within a Slab. Note that the offset and
// length always refer to the reconstructed data, and therefore may not
// necessarily be aligned to a leaf or chunk boundary. Use the SectorRegion
// method to compute the chunk-aligned offset and length.
type SlabSlice struct {
	Slab   `json:"slab"`
	Offset uint32 `json:"offset"`
	Length uint32 `json:"length"`
}

// SectorRegion returns the offset and length of the sector region that must be
// downloaded in order to recover the data referenced by the SlabSlice.
func (ss SlabSlice) SectorRegion() (offset, length uint32) {
	minChunkSize := rhpv2.LeafSize * uint32(ss.MinShards)
	start := (ss.Offset / minChunkSize) * rhpv2.LeafSize
	end := ((ss.Offset + ss.Length) / minChunkSize) * rhpv2.LeafSize
	if (ss.Offset+ss.Length)%minChunkSize != 0 {
		end += rhpv2.LeafSize
	}
	return uint32(start), uint32(end - start)
}

// Decrypt xors shards with the keystream derived from s.Key (starting at the
// slice offset), using a different nonce for each shard.
func (ss SlabSlice) Decrypt(shards [][]byte) {
	offset := ss.Offset / (rhpv2.LeafSize * uint32(ss.MinShards))
	for i, shard := range shards {
		nonce := [24]byte{1: byte(i)}
		c, _ := chacha20.NewUnauthenticatedCipher(ss.Key.entropy[:], nonce[:])
		c.SetCounter(offset)
		c.XORKeyStream(shard, shard)
	}
}

// Recover recovers a slice of slab data from the supplied shards.
func (ss SlabSlice) Recover(w io.Writer, shards [][]byte) error {
	empty := true
	for _, s := range shards {
		empty = empty && len(s) == 0
	}
	if empty || len(shards) == 0 {
		return nil
	}
	rsc, _ := reedsolomon.New(int(ss.MinShards), len(shards)-int(ss.MinShards))
	if err := rsc.ReconstructData(shards); err != nil {
		return err
	}
	skip := ss.Offset % (rhpv2.LeafSize * uint32(ss.MinShards))
	return stripedJoin(w, shards[:ss.MinShards], int(skip), int(ss.Length))
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
