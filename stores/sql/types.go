package sql

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"

	"go.sia.tech/core/types"
)

const (
	proofHashSize = 32
	secretKeySize = 32
)

type (
	Currency       types.Currency
	FileContractID types.FileContractID
	Hash256        types.Hash256
	MerkleProof    struct{ Hashes []types.Hash256 }
	PublicKey      types.PublicKey
	SecretKey      []byte
)

var (
	_ sql.Scanner = &Currency{}
	_ sql.Scanner = &FileContractID{}
	_ sql.Scanner = &Hash256{}
	_ sql.Scanner = &PublicKey{}
	_ sql.Scanner = &SecretKey{}
)

// Scan scan value into Currency, implements sql.Scanner interface.
func (c *Currency) Scan(value interface{}) error {
	var s string
	switch value := value.(type) {
	case string:
		s = value
	case []byte:
		s = string(value)
	default:
		return fmt.Errorf("failed to unmarshal Currency value: %v %t", value, value)
	}
	curr, err := types.ParseCurrency(s)
	if err != nil {
		return err
	}
	*c = Currency(curr)
	return nil
}

// Value returns a publicKey value, implements driver.Valuer interface.
func (c Currency) Value() (driver.Value, error) {
	return types.Currency(c).ExactString(), nil
}

// Scan scan value into fileContractID, implements sql.Scanner interface.
func (fcid *FileContractID) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New(fmt.Sprint("failed to unmarshal fcid value:", value))
	}
	if len(bytes) != len(FileContractID{}) {
		return fmt.Errorf("failed to unmarshal fcid value due to invalid number of bytes %v != %v: %v", len(bytes), len(FileContractID{}), value)
	}
	*fcid = *(*FileContractID)(bytes)
	return nil
}

// Value returns a fileContractID value, implements driver.Valuer interface.
func (fcid FileContractID) Value() (driver.Value, error) {
	return fcid[:], nil
}

// Scan scan value into address, implements sql.Scanner interface.
func (h *Hash256) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New(fmt.Sprint("failed to unmarshal Hash256 value:", value))
	}
	if len(bytes) != len(Hash256{}) {
		return fmt.Errorf("failed to unmarshal Hash256 value due to invalid number of bytes %v != %v: %v", len(bytes), len(Hash256{}), value)
	}
	*h = *(*Hash256)(bytes)
	return nil
}

// Value returns an addr value, implements driver.Valuer interface.
func (h Hash256) Value() (driver.Value, error) {
	return h[:], nil
}

// Scan scan value into publicKey, implements sql.Scanner interface.
func (pk *PublicKey) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New(fmt.Sprint("failed to unmarshal publicKey value:", value))
	}
	if len(bytes) != len(types.PublicKey{}) {
		return fmt.Errorf("failed to unmarshal publicKey value due invalid number of bytes %v != %v: %v", len(bytes), len(PublicKey{}), value)
	}
	*pk = *(*PublicKey)(bytes)
	return nil
}

// Value returns a publicKey value, implements driver.Valuer interface.
func (pk PublicKey) Value() (driver.Value, error) {
	return pk[:], nil
}

// Scan scans value into a MerkleProof, implements sql.Scanner interface.
func (mp *MerkleProof) Scan(value interface{}) error {
	b, ok := value.([]byte)
	if !ok {
		return errors.New(fmt.Sprint("failed to unmarshal MerkleProof value:", value))
	} else if len(b)%proofHashSize != 0 {
		return fmt.Errorf("failed to unmarshal MerkleProof value due to invalid number of bytes %v: %v", len(b), value)
	}

	mp.Hashes = make([]types.Hash256, len(b)/proofHashSize)
	for i := range mp.Hashes {
		copy(mp.Hashes[i][:], b[i*proofHashSize:])
	}
	return nil
}

// Value returns a MerkleProof value, implements driver.Valuer interface.
func (mp MerkleProof) Value() (driver.Value, error) {
	b := make([]byte, len(mp.Hashes)*proofHashSize)
	for i, h := range mp.Hashes {
		copy(b[i*proofHashSize:], h[:])
	}
	return b, nil
}

// String implements fmt.Stringer to prevent the key from getting leaked in
// logs.
func (k SecretKey) String() string {
	return "*****"
}

// Scan scans value into key, implements sql.Scanner interface.
func (k *SecretKey) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New(fmt.Sprint("failed to unmarshal secretKey value:", value))
	} else if len(bytes) != secretKeySize {
		return fmt.Errorf("failed to unmarshal secretKey value due to invalid number of bytes %v != %v: %v", len(bytes), secretKeySize, value)
	}
	*k = append(SecretKey{}, SecretKey(bytes)...)
	return nil
}
