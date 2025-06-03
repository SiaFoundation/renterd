package object

import (
	"bytes"
	"io"
	"sync"

	"github.com/klauspost/reedsolomon"
	rhpv4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"golang.org/x/crypto/chacha20"
)

// A Sector uniquely identifies a sector stored on a particular host.
type Sector struct {
	Contracts map[types.PublicKey][]types.FileContractID `json:"contracts"`
	Root      types.Hash256                              `json:"root"`
}

// A Slab is raw data that has been erasure-encoded into sector-sized shards,
// encrypted, and stored across a set of hosts. A distinct EncryptionKey should
// be used for each Slab, and should not be the same key used for the parent
// Object.
type Slab struct {
	Health        float64       `json:"health"`
	EncryptionKey EncryptionKey `json:"encryptionKey"`
	MinShards     uint8         `json:"minShards"`
	Shards        []Sector      `json:"shards,omitempty"`
}

func (s Slab) IsPartial() bool {
	return len(s.Shards) == 0
}

// NewSlab returns a new slab for the shards.
func NewSlab(minShards uint8) Slab {
	return Slab{
		EncryptionKey: GenerateEncryptionKey(EncryptionKeyTypeSalted),
		MinShards:     minShards,
	}
}

// NewPartialSlab returns a new partial slab.
func NewPartialSlab(ec EncryptionKey, minShards uint8) Slab {
	return Slab{
		Health:        1,
		EncryptionKey: ec,
		MinShards:     minShards,
		Shards:        nil,
	}
}

func (s Slab) Contracts() []types.FileContractID {
	var usedContracts []types.FileContractID
	added := make(map[types.FileContractID]struct{})
	for _, shard := range s.Shards {
		for _, fcids := range shard.Contracts {
			for _, fcid := range fcids {
				if _, exists := added[fcid]; !exists {
					usedContracts = append(usedContracts, fcid)
					added[fcid] = struct{}{}
				}
			}
		}
	}
	return usedContracts
}

// Length returns the length of the raw data stored in s.
func (s Slab) Length() int {
	return rhpv4.SectorSize * int(s.MinShards)
}

// Encrypt xors shards with the keystream derived from s.Key, using a
// different nonce for each shard.
func (s Slab) Encrypt(shards [][]byte) {
	var wg sync.WaitGroup
	for i := range shards {
		wg.Add(1)
		go func(i int) {
			nonce := [24]byte{1: byte(i)}
			c, _ := chacha20.NewUnauthenticatedCipher(s.EncryptionKey.entropy[:], nonce[:])
			c.XORKeyStream(shards[i], shards[i])
			wg.Done()
		}(i)
	}
	wg.Wait()
}

// Encode encodes slab data into sector-sized shards. The supplied shards should
// have a capacity of at least rhpv4.SectorSize, or they will be reallocated.
func (s Slab) Encode(buf []byte, shards [][]byte) {
	for i := range shards {
		if cap(shards[i]) < rhpv4.SectorSize {
			shards[i] = make([]byte, 0, rhpv4.SectorSize)
		}
		shards[i] = shards[i][:rhpv4.SectorSize]
	}
	stripedSplit(buf, shards[:s.MinShards])
	rsc, _ := reedsolomon.New(int(s.MinShards), len(shards)-int(s.MinShards))
	if err := rsc.Encode(shards); err != nil {
		panic(err)
	}
}

// Reconstruct reconstructs the missing shards of a slab. Missing shards must
// have a len of zero. All shards should have a capacity of at least
// rhpv4.SectorSize, or they will be reallocated.
func (s Slab) Reconstruct(shards [][]byte) error {
	for i := range shards {
		if len(shards[i]) != rhpv4.SectorSize && len(shards[i]) != 0 {
			panic("shards must have a len of either 0 or rhpv4.SectorSize")
		}
		if cap(shards[i]) < rhpv4.SectorSize {
			shards[i] = make([]byte, 0, rhpv4.SectorSize)
		}
		if len(shards[i]) != 0 {
			shards[i] = shards[i][:rhpv4.SectorSize]
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
func (ss SlabSlice) SectorRegion() (offset, length uint64) {
	minChunkSize := rhpv4.LeafSize * uint32(ss.MinShards)
	start := (ss.Offset / minChunkSize) * rhpv4.LeafSize
	end := ((ss.Offset + ss.Length) / minChunkSize) * rhpv4.LeafSize
	if (ss.Offset+ss.Length)%minChunkSize != 0 {
		end += rhpv4.LeafSize
	}
	return uint64(start), uint64(end - start)
}

// Decrypt xors shards with the keystream derived from s.Key (starting at the
// slice offset), using a different nonce for each shard.
func (ss SlabSlice) Decrypt(shards [][]byte) {
	offset := ss.Offset / (rhpv4.LeafSize * uint32(ss.MinShards))
	var wg sync.WaitGroup
	for i := range shards {
		wg.Add(1)
		go func(i int) {
			nonce := [24]byte{1: byte(i)}
			c, _ := chacha20.NewUnauthenticatedCipher(ss.EncryptionKey.entropy[:], nonce[:])
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
	skip := ss.Offset % (rhpv4.LeafSize * uint32(ss.MinShards))
	return stripedJoin(w, shards[:ss.MinShards], int(skip), int(ss.Length))
}

type SlabSlices []SlabSlice

func (ss SlabSlices) Contracts() []types.FileContractID {
	var usedContracts []types.FileContractID
	added := make(map[types.FileContractID]struct{})
	for _, s := range ss {
		for _, shard := range s.Shards {
			for _, fcids := range shard.Contracts {
				for _, fcid := range fcids {
					if _, exists := added[fcid]; !exists {
						added[fcid] = struct{}{}
						usedContracts = append(usedContracts, fcid)
					}
				}
			}
		}
	}
	return usedContracts
}

// stripedSplit splits data into striped data shards, which must have sufficient
// capacity.
func stripedSplit(data []byte, dataShards [][]byte) {
	buf := bytes.NewBuffer(data)
	for off := 0; buf.Len() > 0; off += rhpv4.LeafSize {
		for _, shard := range dataShards {
			copy(shard[off:], buf.Next(rhpv4.LeafSize))
		}
	}
}

// stripedJoin joins the striped data shards, writing them to dst. The first 'skip'
// bytes of the recovered data are skipped, and 'writeLen' bytes are written in
// total.
func stripedJoin(dst io.Writer, dataShards [][]byte, skip, writeLen int) error {
	for off := 0; writeLen > 0; off += rhpv4.LeafSize {
		for _, shard := range dataShards {
			if len(shard[off:]) < rhpv4.LeafSize {
				return reedsolomon.ErrShortData
			}
			shard = shard[off:][:rhpv4.LeafSize]
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
