package object

import (
	"bytes"
	"crypto/cipher"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"math"

	"golang.org/x/crypto/chacha20"
	"lukechampine.com/frand"
)

var NoOpKey = EncryptionKey{
	entropy: new([32]byte),
}

// A EncryptionKey can encrypt and decrypt messages.
type EncryptionKey struct {
	entropy *[32]byte `json:"-"`
}

func (k EncryptionKey) IsNoopKey() bool {
	return bytes.Equal(k.entropy[:], NoOpKey.entropy[:])
}

// String implements fmt.Stringer.
func (k EncryptionKey) String() string {
	return "key:" + hex.EncodeToString(k.entropy[:])
}

// MarshalText implements the encoding.TextMarshaler interface.
func (k EncryptionKey) MarshalText() ([]byte, error) {
	return []byte(k.String()), nil
}

// UnmarshalText implements the encoding.TextUnmarshaler interface.
func (k *EncryptionKey) UnmarshalText(b []byte) error {
	k.entropy = new([32]byte)
	if n, err := hex.Decode(k.entropy[:], []byte(bytes.TrimPrefix(b, []byte("key:")))); err != nil {
		return err
	} else if n != len(k.entropy) {
		return fmt.Errorf("wrong key length: expected %v, got %v", len(k.entropy), n)
	}
	return nil
}

// Encrypt returns a cipher.StreamReader that encrypts r with k starting at the
// given offset.
func (k EncryptionKey) Encrypt(r io.Reader, offset uint64) (cipher.StreamReader, error) {
	if offset%64 != 0 {
		return cipher.StreamReader{}, fmt.Errorf("offset must be a multiple of 64, got %v", offset)
	}
	if k.IsNoopKey() {
		return cipher.StreamReader{S: &noOpStream{}, R: r}, nil
	}
	nonce64 := offset / (64 * math.MaxUint32)
	offset %= 64 * math.MaxUint32

	nonce := make([]byte, 24)
	binary.LittleEndian.PutUint64(nonce[16:], nonce64)
	c, _ := chacha20.NewUnauthenticatedCipher(k.entropy[:], nonce)
	c.SetCounter(uint32(offset / 64))
	rs := &rekeyStream{key: k.entropy[:], c: c}
	return cipher.StreamReader{S: rs, R: r}, nil
}

// Decrypt returns a cipher.StreamWriter that decrypts w with k, starting at the
// specified offset.
func (k EncryptionKey) Decrypt(w io.Writer, offset uint64) cipher.StreamWriter {
	if k.IsNoopKey() {
		return cipher.StreamWriter{S: &noOpStream{}, W: w}
	}
	nonce64 := offset / (64 * math.MaxUint32)
	offset %= 64 * math.MaxUint32

	nonce := make([]byte, 24)
	binary.LittleEndian.PutUint64(nonce[16:], nonce64)
	c, _ := chacha20.NewUnauthenticatedCipher(k.entropy[:], nonce)
	c.SetCounter(uint32(offset / 64))

	var buf [64]byte
	c.XORKeyStream(buf[:offset%64], buf[:offset%64])
	rs := &rekeyStream{key: k.entropy[:], c: c, counter: offset, nonce: nonce64}
	return cipher.StreamWriter{S: rs, W: w}
}

// GenerateEncryptionKey returns a random encryption key.
func GenerateEncryptionKey() EncryptionKey {
	key := EncryptionKey{entropy: new([32]byte)}
	frand.Read(key.entropy[:])
	return key
}

// An Object is a unit of data that has been stored on a host.
type Object struct {
	Key          EncryptionKey `json:"key"`
	Slabs        []SlabSlice   `json:"slabs"`
	PartialSlabs []PartialSlab `json:"partialSlab,omitempty"`
}

// NewObject returns a new Object with a random key.
func NewObject(ec EncryptionKey) Object {
	return Object{
		Key: ec,
	}
}

// TotalSize returns the total size of the object.
func (o Object) TotalSize() int64 {
	var n int64
	for _, ss := range o.Slabs {
		n += int64(ss.Length)
	}
	for _, partialSlab := range o.PartialSlabs {
		n += int64(partialSlab.Length)
	}
	return n
}

// Encrypt wraps the given reader with a reader that encrypts the stream using
// the object's key.
func (o Object) Encrypt(r io.Reader, offset uint64) (cipher.StreamReader, error) {
	return o.Key.Encrypt(r, offset)
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

type rekeyStream struct {
	key []byte
	c   *chacha20.Cipher

	counter uint64
	nonce   uint64
}

func (rs *rekeyStream) XORKeyStream(dst, src []byte) {
	rs.counter += uint64(len(src))
	if rs.counter < 64*math.MaxUint32 {
		rs.c.XORKeyStream(dst, src)
		return
	}
	// counter overflow; xor remaining bytes, then increment nonce and xor again
	rem := 64*math.MaxUint32 - (rs.counter - uint64(len(src)))
	rs.counter -= 64 * math.MaxUint32
	rs.c.XORKeyStream(dst[:rem], src[:rem])
	// NOTE: we increment the last 8 bytes because XChaCha uses the
	// first 16 bytes to derive a new key; leaving them alone means
	// the key will be stable, which might be useful.
	rs.nonce++
	nonce := make([]byte, 24)
	binary.LittleEndian.PutUint64(nonce[16:], rs.nonce)
	rs.c, _ = chacha20.NewUnauthenticatedCipher(rs.key, nonce)
	rs.c.XORKeyStream(dst[rem:], src[rem:])
}

type noOpStream struct{}

func (noOpStream) XORKeyStream(dst, src []byte) {
	copy(dst, src)
}
