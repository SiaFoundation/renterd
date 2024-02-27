package object

import (
	"bytes"
	"io"
	"sync"

	"github.com/klauspost/reedsolomon"
	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"golang.org/x/crypto/chacha20"
)

// A Sector uniquely identifies a sector stored on a particular host.
type Sector struct {
	Contracts  map[types.PublicKey][]types.FileContractID `json:"contracts"`
	LatestHost types.PublicKey                            `json:"latestHost"`
	Root       types.Hash256                              `json:"root"`
}

// A Slab is raw data that has been erasure-encoded into sector-sized shards,
// encrypted, and stored across a set of hosts. A distinct EncryptionKey should
// be used for each Slab, and should not be the same key used for the parent
// Object.
type Slab struct {
	Health    float64       `json:"health"`
	Key       EncryptionKey `json:"key"`
	MinShards uint8         `json:"minShards"`
	Shards    []Sector      `json:"shards,omitempty"`
}

func (s Slab) IsPartial() bool {
	return len(s.Shards) == 0
}

// NewSlab returns a new slab for the shards.
func NewSlab(minShards uint8) Slab {
	return Slab{
		Key:       GenerateEncryptionKey(),
		MinShards: minShards,
	}
}

// NewPartialSlab returns a new partial slab.
func NewPartialSlab(ec EncryptionKey, minShards uint8) Slab {
	return Slab{
		Health:    1,
		Key:       ec,
		MinShards: minShards,
		Shards:    nil,
	}
}

// ContractsFromShards is a helper to extract all contracts used by a set of
// shards.
func ContractsFromShards(shards []Sector) map[types.PublicKey]map[types.FileContractID]struct{} {
	usedContracts := make(map[types.PublicKey]map[types.FileContractID]struct{})
	for _, shard := range shards {
		for h, fcids := range shard.Contracts {
			for _, fcid := range fcids {
				if _, exists := usedContracts[h]; !exists {
					usedContracts[h] = make(map[types.FileContractID]struct{})
				}
				usedContracts[h][fcid] = struct{}{}
			}
		}
	}
	return usedContracts
}

func (s Slab) Contracts() map[types.PublicKey]map[types.FileContractID]struct{} {
	return ContractsFromShards(s.Shards)
}

// Length returns the length of the raw data stored in s.
func (s Slab) Length() int {
	return rhpv2.SectorSize * int(s.MinShards)
}

// Encrypt xors shards with the keystream derived from s.Key, using a
// different nonce for each shard.
func (s Slab) Encrypt(shards [][]byte) {
	var wg sync.WaitGroup
	for i := range shards {
		wg.Add(1)
		go func(i int) {
			nonce := [24]byte{1: byte(i)}
			c, _ := chacha20.NewUnauthenticatedCipher(s.Key.entropy[:], nonce[:])
			c.XORKeyStream(shards[i], shards[i])
			wg.Done()
		}(i)
	}
	wg.Wait()
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
	var wg sync.WaitGroup
	for i := range shards {
		wg.Add(1)
		go func(i int) {
			nonce := [24]byte{1: byte(i)}
			c, _ := chacha20.NewUnauthenticatedCipher(ss.Key.entropy[:], nonce[:])
			c.SetCounter(offset)
			c.XORKeyStream(shards[i], shards[i])
			wg.Done()
		}(i)
	}
	wg.Wait()
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
