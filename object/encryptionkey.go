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

	"go.sia.tech/renterd/v2/internal/utils"
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
	keyType: EncryptionKeyTypeBasic,
}

var (
	ErrKeyType     = errors.New("invalid key type")
	ErrKeyRequired = errors.New("key required")
)

type EncryptionKeyType int

const (
	EncryptionKeyTypeBasic = EncryptionKeyType(iota + 1)
	EncryptionKeyTypeSalted
)

// A EncryptionKey can encrypt and decrypt messages.
type EncryptionKey struct {
	entropy *[32]byte
	keyType EncryptionKeyType
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

// EncryptionKey returns the encryption key for the given upload key.
// For basic keys, it returns the entropy directly.
// For salted keys, it derives the key using the upload key.
func (k EncryptionKey) EncryptionKey(uk *utils.UploadKey) [32]byte {
	if k.IsNoopKey() {
		return [32]byte{}
	}
	switch k.keyType {
	case EncryptionKeyTypeBasic:
		return *k.entropy
	case EncryptionKeyTypeSalted:
		return uk.DeriveKey(k.entropy)
	default:
		panic(fmt.Sprintf("unknown key type: %v", k.keyType))
	}
}

// Entropy returns the entropy of the encryption key.
func (k EncryptionKey) Entropy() [32]byte {
	if k.entropy == nil {
		return [32]byte{}
	}
	return *k.entropy
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
	var b [33]byte
	switch k.keyType {
	case EncryptionKeyTypeBasic:
		b[0] = 1
	case EncryptionKeyTypeSalted:
		b[0] = 2
	default:
		return nil, ErrKeyType
	}
	copy(b[1:], k.entropy[:])
	return b[:], nil
}

func (k *EncryptionKey) UnmarshalBinary(b []byte) error {
	k.entropy = new([32]byte)
	if len(b) != len(k.entropy)+1 {
		return fmt.Errorf("wrong key length: expected %v, got %v", len(k.entropy)+1, len(b))
	}
	switch b[0] {
	case 1:
		k.keyType = EncryptionKeyTypeBasic
	case 2:
		k.keyType = EncryptionKeyTypeSalted
	default:
		return ErrKeyType
	}
	copy(k.entropy[:], b[1:])
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
	Key    *utils.UploadKey
}

func (k *EncryptionKey) Encrypt(r io.Reader, opts EncryptionOptions) (cipher.StreamReader, error) {
	switch k.keyType {
	case EncryptionKeyTypeBasic:
		return (*encryptionKeyBasic)(k).Encrypt(r, opts.Offset), nil
	case EncryptionKeyTypeSalted:
		if opts.Key == nil {
			return cipher.StreamReader{}, ErrKeyRequired
		}
		return (*encryptionKeySalted)(k).Encrypt(r, opts.Offset, opts.Key), nil
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

func (k *encryptionKeyBasic) Encrypt(r io.Reader, offset uint64) cipher.StreamReader {
	return encrypt(k.entropy, r, offset)
}
func (k *encryptionKeyBasic) Decrypt(w io.Writer, offset uint64) cipher.StreamWriter {
	return decrypt(k.entropy, w, offset)
}

type encryptionKeySalted EncryptionKey

func (k *encryptionKeySalted) Encrypt(r io.Reader, offset uint64, key *utils.UploadKey) cipher.StreamReader {
	derivedKey := key.DeriveKey(k.entropy)
	return encrypt(&derivedKey, r, offset)
}
func (k *encryptionKeySalted) Decrypt(w io.Writer, offset uint64, key *utils.UploadKey) cipher.StreamWriter {
	derivedKey := key.DeriveKey(k.entropy)
	return decrypt(&derivedKey, w, offset)
}

const (
	// maximum amount of data we can encrypt with a single nonce because
	// counter is a uint32 and each tick is 64 bytes
	maxBytesPerNonce = 64 * math.MaxUint32
)

type rekeyStream struct {
	key  []byte
	c    *chacha20.Cipher
	skip int

	counter uint64
	nonce   uint64
}

func (rs *rekeyStream) XORKeyStream(dst, src []byte) {
	if len(src) == 0 {
		return
	}

	if rs.skip > 0 {
		// determine how many bytes we can process from the first block.
		n := min(64-rs.skip, len(src))

		// generate the full 64-byte keystream for the initial block.
		var keyStream [64]byte
		rs.c.XORKeyStream(keyStream[:], keyStream[:])

		// XOR the relevant part of the keystream with the source data
		for i := 0; i < n; i++ {
			dst[i] = src[i] ^ keyStream[rs.skip+i]
		}

		// update state and slice pointers for the rest of the operation
		rs.counter += uint64(n)
		src = src[n:]
		dst = dst[n:]
		// only run once
		rs.skip = 0
	}
	if len(src) == 0 {
		return
	}

	rs.counter += uint64(len(src))
	if rs.counter < maxBytesPerNonce {
		rs.c.XORKeyStream(dst, src)
		return
	}

	// counter overflow; xor remaining bytes, then increment nonce and xor again
	rem := maxBytesPerNonce - (rs.counter - uint64(len(src)))
	rs.c.XORKeyStream(dst[:rem], src[:rem])
	src = src[rem:]
	dst = dst[rem:]

	// reset counter and re-key the cipher with an incremented nonce
	rs.counter = uint64(len(src))
	rs.nonce++
	nonce := make([]byte, 24)
	binary.LittleEndian.PutUint64(nonce[16:], rs.nonce)
	rs.c, _ = chacha20.NewUnauthenticatedCipher(rs.key, nonce)

	rs.c.XORKeyStream(dst, src)
}

type noOpStream struct{}

func (noOpStream) XORKeyStream(dst, src []byte) {
	copy(dst, src)
}

func nonce(offset uint64) (nonce [24]byte, nonce64 uint64) {
	nonce64 = offset / maxBytesPerNonce
	binary.LittleEndian.PutUint64(nonce[16:], nonce64)
	return
}

// Encrypt returns a cipher.StreamReader that encrypts r with k starting at the
// given offset.
func encrypt(key *[32]byte, r io.Reader, offset uint64) cipher.StreamReader {
	if bytes.Equal(key[:], NoOpKey.entropy[:]) {
		return cipher.StreamReader{S: &noOpStream{}, R: r}
	}

	n, n64 := nonce(offset)
	offset %= maxBytesPerNonce
	skip := int(offset % 64)

	c, _ := chacha20.NewUnauthenticatedCipher(key[:], n[:])
	c.SetCounter(uint32(offset / 64))
	rs := &rekeyStream{key: key[:], c: c, counter: offset, nonce: n64, skip: skip}
	return cipher.StreamReader{S: rs, R: r}
}

// Decrypt returns a cipher.StreamWriter that decrypts w with k, starting at the
// specified offset.
func decrypt(key *[32]byte, w io.Writer, offset uint64) cipher.StreamWriter {
	if bytes.Equal(key[:], NoOpKey.entropy[:]) {
		return cipher.StreamWriter{S: &noOpStream{}, W: w}
	}
	n, n64 := nonce(offset)
	offset %= maxBytesPerNonce
	skip := int(offset % 64)

	c, _ := chacha20.NewUnauthenticatedCipher(key[:], n[:])
	c.SetCounter(uint32(offset / 64))
	rs := &rekeyStream{key: key[:], c: c, counter: offset, nonce: n64, skip: skip}
	return cipher.StreamWriter{S: rs, W: w}
}
