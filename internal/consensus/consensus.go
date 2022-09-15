package consensus

import (
	"bytes"
	"crypto/ed25519"
	"encoding/hex"
	"fmt"
	"io"

	"github.com/hdevalence/ed25519consensus"
	"go.sia.tech/siad/types"
	"lukechampine.com/frand"
)

// A Hash256 is a generic 256-bit cryptographic hash.
type Hash256 [32]byte

// A BlockID uniquely identifies a block.
type BlockID Hash256

// A ChainIndex pairs a block's height with its ID.
type ChainIndex struct {
	Height uint64
	ID     BlockID
}

// A PublicKey is an Ed25519 public key.
type PublicKey [32]byte

// A PrivateKey is an Ed25519 private key.
type PrivateKey []byte

// PublicKey returns the PublicKey corresponding to priv.
func (priv PrivateKey) PublicKey() (pk PublicKey) {
	copy(pk[:], priv[32:])
	return
}

// NewPrivateKeyFromSeed derives a private key from a seed.
func NewPrivateKeyFromSeed(seed []byte) PrivateKey {
	return PrivateKey(ed25519.NewKeyFromSeed(seed))
}

// GeneratePrivateKey creates a new private key from a secure entropy source.
func GeneratePrivateKey() PrivateKey {
	seed := make([]byte, ed25519.SeedSize)
	frand.Read(seed)
	pk := NewPrivateKeyFromSeed(seed)
	for i := range seed {
		seed[i] = 0
	}
	return pk
}

// A Signature is an Ed25519 signature.
type Signature [64]byte

// SignHash signs h, producing a Signature.
func (priv PrivateKey) SignHash(h Hash256) (s Signature) {
	copy(s[:], ed25519.Sign(ed25519.PrivateKey(priv), h[:]))
	return
}

// VerifyHash verifies that s is a valid signature of h by pk.
func (pk PublicKey) VerifyHash(h Hash256, s Signature) bool {
	return ed25519consensus.Verify(pk[:], h[:], s[:])
}

// State represents the full state of the chain as of a particular block.
type State struct {
	Index ChainIndex
}

// InputSigHash returns the signature hash for the i'th TransactionSignature in
// txn.
func (s State) InputSigHash(txn types.Transaction, i int) Hash256 {
	return Hash256(txn.SigHash(i, types.BlockHeight(s.Index.Height+1)))
}

// Implementations of fmt.Stringer, encoding.Text(Un)marshaler, and json.(Un)marshaler, etc.

func stringerHex(prefix string, data []byte) string {
	return prefix + ":" + hex.EncodeToString(data[:])
}

func marshalHex(prefix string, data []byte) ([]byte, error) {
	return []byte(stringerHex(prefix, data)), nil
}

func unmarshalHex(dst []byte, prefix string, data []byte) error {
	n, err := hex.Decode(dst, bytes.TrimPrefix(data, []byte(prefix+":")))
	if n < len(dst) {
		err = io.EOF
	}
	if err != nil {
		return fmt.Errorf("decoding %v:<hex> failed: %w", prefix, err)
	}
	return nil
}

func marshalJSONHex(prefix string, data []byte) ([]byte, error) {
	return []byte(`"` + stringerHex(prefix, data) + `"`), nil
}

func unmarshalJSONHex(dst []byte, prefix string, data []byte) error {
	return unmarshalHex(dst, prefix, bytes.Trim(data, `"`))
}

// String implements fmt.Stringer.
func (h Hash256) String() string { return stringerHex("h", h[:]) }

// MarshalText implements encoding.TextMarshaler.
func (h Hash256) MarshalText() ([]byte, error) { return marshalHex("h", h[:]) }

// UnmarshalText implements encoding.TextUnmarshaler.
func (h *Hash256) UnmarshalText(b []byte) error { return unmarshalHex(h[:], "h", b) }

// MarshalJSON implements json.Marshaler.
func (h Hash256) MarshalJSON() ([]byte, error) { return marshalJSONHex("h", h[:]) }

// UnmarshalJSON implements json.Unmarshaler.
func (h *Hash256) UnmarshalJSON(b []byte) error { return unmarshalJSONHex(h[:], "h", b) }

// String implements fmt.Stringer.
func (bid BlockID) String() string { return stringerHex("bid", bid[:]) }

// MarshalText implements encoding.TextMarshaler.
func (bid BlockID) MarshalText() ([]byte, error) { return marshalHex("bid", bid[:]) }

// UnmarshalText implements encoding.TextUnmarshaler.
func (bid *BlockID) UnmarshalText(b []byte) error { return unmarshalHex(bid[:], "bid", b) }

// MarshalJSON implements json.Marshaler.
func (bid BlockID) MarshalJSON() ([]byte, error) { return marshalJSONHex("bid", bid[:]) }

// UnmarshalJSON implements json.Unmarshaler.
func (bid *BlockID) UnmarshalJSON(b []byte) error { return unmarshalJSONHex(bid[:], "bid", b) }

// String implements fmt.Stringer.
func (pk PublicKey) String() string { return stringerHex("ed25519", pk[:]) }

// MarshalText implements encoding.TextMarshaler.
func (pk PublicKey) MarshalText() ([]byte, error) { return marshalHex("ed25519", pk[:]) }

// UnmarshalText implements encoding.TextUnmarshaler.
func (pk *PublicKey) UnmarshalText(b []byte) error { return unmarshalHex(pk[:], "ed25519", b) }

// MarshalJSON implements json.Marshaler.
func (pk PublicKey) MarshalJSON() ([]byte, error) { return marshalJSONHex("ed25519", pk[:]) }

// UnmarshalJSON implements json.Unmarshaler.
func (pk *PublicKey) UnmarshalJSON(b []byte) error { return unmarshalJSONHex(pk[:], "ed25519", b) }

// String implements fmt.Stringer.
func (priv PrivateKey) String() string { return stringerHex("key", priv[:ed25519.SeedSize]) }

// MarshalText implements encoding.TextMarshaler.
func (priv PrivateKey) MarshalText() ([]byte, error) {
	return marshalHex("key", priv[:ed25519.SeedSize])
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (priv *PrivateKey) UnmarshalText(b []byte) error {
	seed := make([]byte, ed25519.SeedSize)
	if err := unmarshalHex(seed, "key", b); err != nil {
		return err
	}
	*priv = NewPrivateKeyFromSeed(seed)
	return nil
}

// MarshalJSON implements json.Marshaler.
func (priv PrivateKey) MarshalJSON() ([]byte, error) {
	return marshalJSONHex("key", priv[:ed25519.SeedSize])
}

// UnmarshalJSON implements json.Unmarshaler.
func (priv *PrivateKey) UnmarshalJSON(b []byte) error {
	seed := make([]byte, ed25519.SeedSize)
	if err := unmarshalJSONHex(seed, "key", b); err != nil {
		return err
	}
	*priv = NewPrivateKeyFromSeed(seed)
	return nil
}

// String implements fmt.Stringer.
func (sig Signature) String() string { return stringerHex("sig", sig[:]) }

// MarshalText implements encoding.TextMarshaler.
func (sig Signature) MarshalText() ([]byte, error) { return marshalHex("sig", sig[:]) }

// UnmarshalText implements encoding.TextUnmarshaler.
func (sig *Signature) UnmarshalText(b []byte) error { return unmarshalHex(sig[:], "sig", b) }

// MarshalJSON implements json.Marshaler.
func (sig Signature) MarshalJSON() ([]byte, error) { return marshalJSONHex("sig", sig[:]) }

// UnmarshalJSON implements json.Unmarshaler.
func (sig *Signature) UnmarshalJSON(b []byte) error { return unmarshalJSONHex(sig[:], "sig", b) }
