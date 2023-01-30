package stores

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"

	"go.sia.tech/core/types"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
)

var zeroCurrency = currency(types.ZeroCurrency)

type (
	currency       types.Currency
	fileContractID types.FileContractID
	publicKey      types.PublicKey
	hostSettings   rhpv2.HostSettings
)

// GormDataType implements gorm.GormDataTypeInterface.
func (fileContractID) GormDataType() string {
	return "bytes"
}

// Scan scan value into fileContractID, implements sql.Scanner interface.
func (fcid *fileContractID) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New(fmt.Sprint("failed to unmarshal fcid value:", value))
	}
	if len(bytes) < len(fileContractID{}) {
		return fmt.Errorf("failed to unmarshal fcid value due to insufficient bytes %v < %v: %v", len(bytes), len(fileContractID{}), value)
	}
	*fcid = *(*fileContractID)(bytes)
	return nil
}

// Value returns a currency value, implements driver.Valuer interface.
func (fcid fileContractID) Value() (driver.Value, error) {
	return fcid[:], nil
}

func (currency) GormDataType() string {
	return "string"
}

// Scan scan value into currency, implements sql.Scanner interface.
func (c *currency) Scan(value interface{}) error {
	s, ok := value.(string)
	if !ok {
		return errors.New(fmt.Sprint("failed to unmarshal currency value:", value))
	}
	curr, err := types.ParseCurrency(s)
	if err != nil {
		return err
	}
	*c = currency(curr)
	return nil
}

// Value returns a publicKey value, implements driver.Valuer interface.
func (c currency) Value() (driver.Value, error) {
	return types.Currency(c).ExactString(), nil
}

func (publicKey) GormDataType() string {
	return "bytes"
}

// Scan scan value into publicKey, implements sql.Scanner interface.
func (pk *publicKey) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New(fmt.Sprint("failed to unmarshal publicKey value:", value))
	}
	if len(bytes) < len(types.PublicKey{}) {
		return fmt.Errorf("failed to unmarshal publicKey value due to insufficient bytes %v < %v: %v", len(bytes), len(publicKey{}), value)
	}
	*pk = *(*publicKey)(bytes)
	return nil
}

// Value returns a publicKey value, implements driver.Valuer interface.
func (pk publicKey) Value() (driver.Value, error) {
	return pk[:], nil
}

func (hostSettings) GormDataType() string {
	return "string"
}

// Scan scan value into hostSettings, implements sql.Scanner interface.
func (hs *hostSettings) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New(fmt.Sprint("failed to unmarshal hostSettings value:", value))
	}
	return json.Unmarshal(bytes, hs)
}

// Value returns a hostSettings value, implements driver.Valuer interface.
func (hs hostSettings) Value() (driver.Value, error) {
	return json.Marshal(hs)
}
