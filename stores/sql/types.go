package sql

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"

	"go.sia.tech/core/types"
)

const (
	secretKeySize = 32
)

type (
	FileContractID types.FileContractID
	PublicKey      types.PublicKey
	SecretKey      []byte
)

var (
	_ sql.Scanner = &SecretKey{}
	_ sql.Scanner = &FileContractID{}
)

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
