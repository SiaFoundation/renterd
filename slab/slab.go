package slab

import (
	"bytes"
	"encoding/hex"
	"errors"

	"go.sia.tech/renterd/internal/consensus"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"golang.org/x/crypto/chacha20"
)

// A EncryptionKey can encrypt and decrypt messages.
type EncryptionKey [32]byte

// MarshalJSON implements the json.Marshaler interface.
func (k EncryptionKey) MarshalJSON() ([]byte, error) {
	return []byte(`"key:` + hex.EncodeToString(k[:]) + `"`), nil
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (k *EncryptionKey) UnmarshalJSON(b []byte) error {
	if n, err := hex.Decode(k[:], bytes.TrimPrefix(bytes.Trim(b, `"`), []byte("key:"))); err != nil {
		return err
	} else if n != len(k) {
		return errors.New("wrong seed length")
	}
	return nil
}

// XORKeyStream xors msg with the keystream derived from s, using shardIndex as
// the nonce and startIndex as the starting offset within the stream.
func (k *EncryptionKey) XORKeyStream(msg []byte, shardIndex uint8, startIndex uint32) {
	if len(msg)%rhpv2.LeafSize != 0 {
		panic("message must be a multiple of leaf size")
	}
	nonce := [24]byte{1: shardIndex}
	c, _ := chacha20.NewUnauthenticatedCipher(k[:], nonce[:])
	c.SetCounter(startIndex)
	c.XORKeyStream(msg, msg)
}

// A Sector uniquely identifies a sector stored on a particular host.
type Sector struct {
	Host consensus.PublicKey
	Root consensus.Hash256
}

// A Slab is raw data that has been erasure-encoded into sector-sized shards,
// encrypted, and stored across aa set of hosts.
type Slab struct {
	Key       EncryptionKey
	MinShards uint8
	Shards    []Sector
}

// A SlabSlice is a contiguous region within a Slab. Note that the offset and
// length always refer to the reconstructed data, and therefore may not
// necessarily be aligned to a leaf or chunk boundary. Use the SectorRegion
// method to compute the chunk-aligned offset and length.
type SlabSlice struct {
	Slab
	Offset uint32
	Length uint32
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
	return start, end - start
}
