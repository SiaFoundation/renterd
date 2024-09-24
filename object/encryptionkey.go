package object

import (
	"bytes"
	"crypto/cipher"
	"encoding"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"

	"golang.org/x/crypto/blake2b"
	"golang.org/x/crypto/chacha20"
	"lukechampine.com/frand"
)

type EncryptionKeyInterface interface {
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
	encoding.TextMarshaler
	encoding.TextUnmarshaler
	fmt.Stringer
}

var NoOpKey = EncryptionKey{
	entropy: new([32]byte),
}

type EncryptionKeyType int

const (
	EncryptionKeyTypeBasic = EncryptionKeyType(iota + 1)
	EncryptionKeyTypeSalted
)

// A EncryptionKey can encrypt and decrypt messages.
type EncryptionKey struct {
	entropy *[32]byte         `json:"-"`
	keyType EncryptionKeyType `json:"-"`
}

// GenerateEncryptionKey returns a random encryption key.
func GenerateEncryptionKey(t EncryptionKeyType) EncryptionKey {
	key := EncryptionKey{
		entropy: new([32]byte),
		keyType: t,
	}
	frand.Read(key.entropy[:])
	return key
}

func (k EncryptionKey) IsNoopKey() bool {
	return bytes.Equal(k.entropy[:], NoOpKey.entropy[:])
}

func (k EncryptionKey) String() string {
	if k.entropy == nil {
		return ""
	}
	var prefix string
	switch k.keyType {
	case EncryptionKeyTypeBasic:
		prefix = "key"
	case EncryptionKeyTypeSalted:
		prefix = "skey"
	default:
		return ""
	}
	return fmt.Sprintf("%s:%s", prefix, hex.EncodeToString(k.entropy[:]))
}

func (k EncryptionKey) Type() EncryptionKeyType {
	return k.keyType
}

// MarshalBinary implements encoding.BinaryMarshaler.
func (k EncryptionKey) MarshalBinary() ([]byte, error) {
	return append([]byte{}, k.entropy[:]...), nil
}

func (k *EncryptionKey) UnmarshalBinary(b []byte) error {
	k.entropy = new([32]byte)
	if len(b) != len(k.entropy) {
		return fmt.Errorf("wrong key length: expected %v, got %v", len(k.entropy), len(b))
	}
	copy(k.entropy[:], b)
	return nil
}

// MarshalText implements the encoding.TextMarshaler interface.
func (k EncryptionKey) MarshalText() ([]byte, error) {
	return []byte(k.String()), nil
}

// UnmarshalText implements the encoding.TextUnmarshaler interface.
func (k *EncryptionKey) UnmarshalText(b []byte) error {
	splits := bytes.Split(b, []byte(":"))
	if len(splits) != 2 {
		return fmt.Errorf("expected the key to have the form prefix:entropy but had %v pieces", len(splits))
	}
	// prefix
	switch string(splits[0]) {
	case "key":
		k.keyType = EncryptionKeyTypeBasic
	case "skey":
		k.keyType = EncryptionKeyTypeSalted
	default:
		return fmt.Errorf("invalid prefix for key: '%s'", splits[0])
	}

	// entropy
	k.entropy = new([32]byte)
	if n, err := hex.Decode(k.entropy[:], splits[1]); err != nil {
		return err
	} else if n != len(k.entropy) {
		return fmt.Errorf("wrong key length: expected %v, got %v", len(k.entropy), n)
	}
	return nil
}

type EncryptionOptions struct {
	Offset uint64
	Key    *[32]byte
}

var ErrKeyType = errors.New("invalid key type")
var ErrKeyRequired = errors.New("key required")

func (k *EncryptionKey) Encrypt(r io.Reader, opts EncryptionOptions) (cipher.StreamReader, error) {
	switch k.keyType {
	case EncryptionKeyTypeBasic:
		return (*encryptionKeyBasic)(k).Encrypt(r, opts.Offset)
	case EncryptionKeyTypeSalted:
		if opts.Key == nil {
			return cipher.StreamReader{}, ErrKeyRequired
		}
		return (*encryptionKeySalted)(k).Encrypt(r, opts.Offset, opts.Key)
	default:
		return cipher.StreamReader{}, fmt.Errorf("%w: %v", ErrKeyType, k.keyType)
	}
}

func (k *EncryptionKey) Decrypt(w io.Writer, opts EncryptionOptions) (cipher.StreamWriter, error) {
	switch k.keyType {
	case EncryptionKeyTypeBasic:
		return (*encryptionKeyBasic)(k).Decrypt(w, opts.Offset), nil
	case EncryptionKeyTypeSalted:
		if opts.Key == nil {
			return cipher.StreamWriter{}, ErrKeyRequired
		}
		return (*encryptionKeySalted)(k).Decrypt(w, opts.Offset, opts.Key), nil
	default:
		return cipher.StreamWriter{}, fmt.Errorf("%w: %v", ErrKeyType, k.keyType)
	}
}

type encryptionKeyBasic EncryptionKey

func (k *encryptionKeyBasic) Encrypt(r io.Reader, offset uint64) (cipher.StreamReader, error) {
	return encrypt(k.entropy, r, offset)
}
func (k *encryptionKeyBasic) Decrypt(w io.Writer, offset uint64) cipher.StreamWriter {
	return decrypt(k.entropy, w, offset)
}

type encryptionKeySalted EncryptionKey

func (k *encryptionKeySalted) deriveEncryptionKey(key *[32]byte) *[32]byte {
	entropy := append([]byte(nil), key[:]...)
	entropy = append(entropy, k.entropy[:]...)
	sum := blake2b.Sum256(entropy)
	return &sum
}

func (k *encryptionKeySalted) Encrypt(r io.Reader, offset uint64, key *[32]byte) (cipher.StreamReader, error) {
	return encrypt(k.deriveEncryptionKey(key), r, offset)
}
func (k *encryptionKeySalted) Decrypt(w io.Writer, offset uint64, key *[32]byte) cipher.StreamWriter {
	return decrypt(k.deriveEncryptionKey(key), w, offset)
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

// Encrypt returns a cipher.StreamReader that encrypts r with k starting at the
// given offset.
func encrypt(key *[32]byte, r io.Reader, offset uint64) (cipher.StreamReader, error) {
	if offset%64 != 0 {
		return cipher.StreamReader{}, fmt.Errorf("offset must be a multiple of 64, got %v", offset)
	}
	if bytes.Equal(key[:], NoOpKey.entropy[:]) {
		return cipher.StreamReader{S: &noOpStream{}, R: r}, nil
	}
	nonce64 := offset / (64 * math.MaxUint32)
	offset %= 64 * math.MaxUint32

	nonce := make([]byte, 24)
	binary.LittleEndian.PutUint64(nonce[16:], nonce64)
	c, _ := chacha20.NewUnauthenticatedCipher(key[:], nonce)
	c.SetCounter(uint32(offset / 64))
	rs := &rekeyStream{key: key[:], c: c}
	return cipher.StreamReader{S: rs, R: r}, nil
}

// Decrypt returns a cipher.StreamWriter that decrypts w with k, starting at the
// specified offset.
func decrypt(key *[32]byte, w io.Writer, offset uint64) cipher.StreamWriter {
	if bytes.Equal(key[:], NoOpKey.entropy[:]) {
		return cipher.StreamWriter{S: &noOpStream{}, W: w}
	}
	nonce64 := offset / (64 * math.MaxUint32)
	offset %= 64 * math.MaxUint32

	nonce := make([]byte, 24)
	binary.LittleEndian.PutUint64(nonce[16:], nonce64)
	c, _ := chacha20.NewUnauthenticatedCipher(key[:], nonce)
	c.SetCounter(uint32(offset / 64))

	var buf [64]byte
	c.XORKeyStream(buf[:offset%64], buf[:offset%64])
	rs := &rekeyStream{key: key[:], c: c, counter: offset, nonce: nonce64}
	return cipher.StreamWriter{S: rs, W: w}
}
