package object

import (
	"bytes"
	"crypto/cipher"
	"encoding/hex"
	"errors"
	"io"

	"golang.org/x/crypto/chacha20"
	"lukechampine.com/frand"
)

// A EncryptionKey can encrypt and decrypt messages.
type EncryptionKey struct {
	entropy *[32]byte
}

// MarshalText implements the encoding.TextMarshaler interface.
func (k EncryptionKey) MarshalText() ([]byte, error) {
	return []byte("key:" + hex.EncodeToString(k.entropy[:])), nil
}

// UnmarshalText implements the encoding.TextUnmarshaler interface.
func (k *EncryptionKey) UnmarshalText(b []byte) error {
	k.entropy = new([32]byte)
	if n, err := hex.Decode(k.entropy[:], []byte(bytes.TrimPrefix(b, []byte("key:")))); err != nil {
		return err
	} else if n != len(k.entropy) {
		return errors.New("wrong seed length")
	}
	return nil
}

// Encrypt returns a cipher.StreamReader that encrypts r with k.
func (k EncryptionKey) Encrypt(r io.Reader) cipher.StreamReader {
	c, _ := chacha20.NewUnauthenticatedCipher(k.entropy[:], make([]byte, 24))
	return cipher.StreamReader{S: c, R: r}
}

// Decrypt returns a cipher.StreamWriter that decrypts w with k, starting at the
// specified offset.
func (k EncryptionKey) Decrypt(w io.Writer, offset int64) cipher.StreamWriter {
	c, _ := chacha20.NewUnauthenticatedCipher(k.entropy[:], make([]byte, 24))
	c.SetCounter(uint32(offset / 64))
	var buf [64]byte
	c.XORKeyStream(buf[:offset%64], buf[:offset%64])
	return cipher.StreamWriter{S: c, W: w}
}

// GenerateEncryptionKey returns a random encryption key.
func GenerateEncryptionKey() EncryptionKey {
	key := EncryptionKey{entropy: new([32]byte)}
	frand.Read(key.entropy[:])
	return key
}

// An Object is a unit of data that has been stored on a host.
type Object struct {
	Key   EncryptionKey
	Slabs []SlabSlice
}

// Size returns the total size of the object.
func (o Object) Size() int64 {
	var n int64
	for _, ss := range o.Slabs {
		n += int64(ss.Length)
	}
	return n
}

// SplitSlabs splits a set of slabs into slices comprising objects with the
// specified lengths.
func SplitSlabs(slabs []Slab, lengths []int) [][]SlabSlice {
	s := slabs[0]
	slabs = slabs[1:]
	objects := make([][]SlabSlice, len(lengths))
	offset := 0
	for i, l := range lengths {
		for l > s.Length() {
			objects[i] = append(objects[i], SlabSlice{
				Slab:   s,
				Offset: uint32(offset),
				Length: uint32(s.Length() - offset),
			})
			l -= s.Length() - offset
			s, slabs = slabs[0], slabs[1:]
			offset = 0
		}
		objects[i] = append(objects[i], SlabSlice{
			Slab:   s,
			Offset: uint32(offset),
			Length: uint32(l),
		})
		offset += l
	}
	return objects
}

// SingleSlabs converts a set of slabs into slices comprising a single object
// with the specified length.
func SingleSlabs(slabs []Slab, length int) []SlabSlice {
	return SplitSlabs(slabs, []int{length})[0]
}
