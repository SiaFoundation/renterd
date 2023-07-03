package object

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"math"

	"golang.org/x/crypto/chacha20"
	"lukechampine.com/frand"
)

// A EncryptionKey can encrypt and decrypt messages.
type EncryptionKey struct {
	entropy *[32]byte `json:"-"`
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
		return errors.New("wrong seed length")
	}
	return nil
}

// Encrypt returns a cipher.StreamReader that encrypts r with k.
func (k EncryptionKey) Encrypt(r io.Reader) *streamReader {
	c, _ := chacha20.NewUnauthenticatedCipher(k.entropy[:], make([]byte, 24))
	return &streamReader{key: k.entropy[:], c: c, r: r}
}

// Decrypt returns a cipher.StreamWriter that decrypts w with k, starting at the
// specified offset.
func (k EncryptionKey) Decrypt(w io.Writer, offset uint64) *streamWriter {
	var nonce uint64
	for offset > 64*math.MaxUint32 {
		offset -= 64 * math.MaxUint32
		nonce++
	}
	n := make([]byte, 24)
	binary.LittleEndian.PutUint64(n[16:], nonce)
	c, _ := chacha20.NewUnauthenticatedCipher(k.entropy[:], n)
	c.SetCounter(uint32(offset / 64))
	var buf [64]byte
	c.XORKeyStream(buf[:offset%64], buf[:offset%64])
	return &streamWriter{key: k.entropy[:], c: c, w: w, counter: offset, nonce: nonce}
}

// GenerateEncryptionKey returns a random encryption key.
func GenerateEncryptionKey() EncryptionKey {
	key := EncryptionKey{entropy: new([32]byte)}
	frand.Read(key.entropy[:])
	return key
}

// An Object is a unit of data that has been stored on a host.
type Object struct {
	Key   EncryptionKey `json:"key"`
	Slabs []SlabSlice   `json:"slabs"`
}

// NewObject returns a new Object with a random key.
func NewObject() Object {
	return Object{
		Key: GenerateEncryptionKey(),
	}
}

// Size returns the total size of the object.
func (o Object) Size() int64 {
	var n int64
	for _, ss := range o.Slabs {
		n += int64(ss.Length)
	}
	return n
}

// Encrypt wraps the given reader with a reader that encrypts the stream using
// the object's key.
func (o Object) Encrypt(r io.Reader) *streamReader {
	return o.Key.Encrypt(r)
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

type streamReader struct {
	key []byte
	c   *chacha20.Cipher
	r   io.Reader

	counter uint64 // bytes, not blocks
	nonce   uint64
}

func (sr *streamReader) Read(p []byte) (int, error) {
	n, err := sr.r.Read(p)
	sr.counter += uint64(n)
	if sr.counter < 64*math.MaxUint32 {
		sr.c.XORKeyStream(p[:n], p[:n])
		return n, err
	}
	// counter overflow; xor remaining bytes, then increment nonce and xor again
	rem := 64*math.MaxUint32 - (sr.counter - uint64(n))
	sr.counter -= 64 * math.MaxUint32
	sr.c.XORKeyStream(p[:rem], p[:rem])
	// NOTE: we increment the last 8 bytes because XChaCha uses the
	// first 16 bytes to derive a new key; leaving them alone means
	// the key will be stable, which might be useful.
	sr.nonce++
	nonce := make([]byte, 24)
	binary.LittleEndian.PutUint64(nonce[16:], sr.nonce)
	sr.c, _ = chacha20.NewUnauthenticatedCipher(sr.key, nonce)
	sr.c.XORKeyStream(p[rem:], p[rem:])
	return n, err
}

type streamWriter struct {
	key []byte
	c   *chacha20.Cipher
	w   io.Writer

	counter uint64
	nonce   uint64
}

func (sw *streamWriter) Write(src []byte) (n int, err error) {
	c := make([]byte, len(src))
	if sw.counter+uint64(len(src)) < 64*math.MaxUint32 {
		sw.c.XORKeyStream(c, src)
		n, err = sw.w.Write(c)
		sw.counter += uint64(n)
		if n != len(src) && err == nil { // should never happen
			err = io.ErrShortWrite
		}
		return
	}
	// counter overflow, xor remaining bytes, then increment nonce and xor again
	rem := 64*math.MaxUint32 - sw.counter
	sw.counter -= 64 * math.MaxUint32
	sw.c.XORKeyStream(c[:rem], src[:rem])
	// NOTE: we increment the last 8 bytes because XChaCha uses the
	// first 16 bytes to derive a new key; leaving them alone means
	// the key will be stable, which might be useful.
	sw.nonce++
	nonce := make([]byte, 24)
	binary.LittleEndian.PutUint64(nonce[16:], sw.nonce)
	sw.c, _ = chacha20.NewUnauthenticatedCipher(sw.key, nonce)
	sw.c.XORKeyStream(c[rem:], src[rem:])
	n, err = sw.w.Write(c)
	sw.counter += uint64(n)
	if n != len(src) && err == nil { // should never happen
		err = io.ErrShortWrite
	}
	return
}

// Close closes the underlying Writer and returns its Close return value, if the Writer
// is also an io.Closer. Otherwise it returns nil.
func (w *streamWriter) Close() error {
	if c, ok := w.w.(io.Closer); ok {
		return c.Close()
	}
	return nil
}
