package slab

import (
	"bytes"
	"encoding/hex"
	"errors"
	"strings"

	"go.sia.tech/renterd/internal/consensus"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"golang.org/x/crypto/chacha20"
	"lukechampine.com/frand"
)

// A EncryptionKey can encrypt and decrypt messages.
type EncryptionKey struct {
	entropy *[32]byte
}

// String returns a hex-encoded representation of the key.
func (k EncryptionKey) String() (s string) {
	return "key:" + hex.EncodeToString(k.entropy[:])
}

// LoadString loads an EncryptionKey from a string.
func (k *EncryptionKey) LoadString(s string) error {
	k.entropy = new([32]byte)
	if n, err := hex.Decode(k.entropy[:], []byte(strings.TrimPrefix(s, "key:"))); err != nil {
		return err
	} else if n != len(k.entropy) {
		return errors.New("wrong seed length")
	}
	return nil
}

// MarshalJSON implements the json.Marshaler interface.
func (k EncryptionKey) MarshalJSON() ([]byte, error) {
	return []byte(`"` + k.String() + `"`), nil
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (k *EncryptionKey) UnmarshalJSON(b []byte) error {
	return k.LoadString(string(bytes.Trim(b, `"`)))
}

// GenerateEncryptionKey returns a random encryption key.
func GenerateEncryptionKey() EncryptionKey {
	key := EncryptionKey{entropy: new([32]byte)}
	frand.Read(key.entropy[:])
	return key
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

// A Slice is a contiguous region within a Slab. Note that the offset and length
// always refer to the reconstructed data, and therefore may not necessarily be
// aligned to a leaf or chunk boundary. Use the SectorRegion method to compute
// the chunk-aligned offset and length.
type Slice struct {
	Slab
	Offset uint32
	Length uint32
}

// SectorRegion returns the offset and length of the sector region that must be
// downloaded in order to recover the data referenced by the Slice.
func (s Slice) SectorRegion() (offset, length uint32) {
	minChunkSize := rhpv2.LeafSize * uint32(s.MinShards)
	start := (s.Offset / minChunkSize) * rhpv2.LeafSize
	end := ((s.Offset + s.Length) / minChunkSize) * rhpv2.LeafSize
	if (s.Offset+s.Length)%minChunkSize != 0 {
		end += rhpv2.LeafSize
	}
	return start, end - start
}

// Decrypt xors shards with the keystream derived from s.Key (starting at the
// slice offset), using a different nonce for each shard.
func (s Slice) Decrypt(shards [][]byte) {
	offset := s.Offset / (rhpv2.LeafSize * uint32(s.MinShards))
	for i, shard := range shards {
		nonce := [24]byte{1: byte(i)}
		c, _ := chacha20.NewUnauthenticatedCipher(s.Key.entropy[:], nonce[:])
		c.SetCounter(offset)
		c.XORKeyStream(shard, shard)
	}
}
