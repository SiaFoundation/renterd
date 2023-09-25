package stores

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
)

var zeroCurrency = currency(types.ZeroCurrency)

type (
	datetime       time.Time
	currency       types.Currency
	fileContractID types.FileContractID
	hash256        types.Hash256
	publicKey      types.PublicKey
	hostSettings   rhpv2.HostSettings
	hostPriceTable rhpv3.HostPriceTable
	balance        big.Int
)

// GormDataType implements gorm.GormDataTypeInterface.
func (hash256) GormDataType() string {
	return "bytes"
}

// Scan scan value into address, implements sql.Scanner interface.
func (h *hash256) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New(fmt.Sprint("failed to unmarshal hash256 value:", value))
	}
	if len(bytes) < len(hash256{}) {
		return fmt.Errorf("failed to unmarshal hash256 value due to insufficient bytes %v < %v: %v", len(bytes), len(fileContractID{}), value)
	}
	*h = *(*hash256)(bytes)
	return nil
}

// Value returns an addr value, implements driver.Valuer interface.
func (h hash256) Value() (driver.Value, error) {
	return h[:], nil
}

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
	var s string
	switch value.(type) {
	case string:
		s = value.(string)
	case []byte:
		s = string(value.([]byte))
	default:
		return fmt.Errorf("failed to unmarshal currency value: %v %t", value, value)
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

func (hs hostPriceTable) GormDataType() string {
	return "string"
}

// Scan scan value into hostPriceTable, implements sql.Scanner interface.
func (hpt *hostPriceTable) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New(fmt.Sprint("failed to unmarshal hostPriceTable value:", value))
	}
	return json.Unmarshal(bytes, hpt)
}

// Value returns a hostPriceTable value, implements driver.Valuer interface.
func (hs hostPriceTable) Value() (driver.Value, error) {
	return json.Marshal(hs)
}

func (balance) GormDataType() string {
	return "string"
}

// Scan scan value into balance, implements sql.Scanner interface.
func (hs *balance) Scan(value interface{}) error {
	var s string
	switch value.(type) {
	case string:
		s = value.(string)
	case []byte:
		s = string(value.([]byte))
	default:
		return fmt.Errorf("failed to unmarshal balance value: %v %t", value, value)
	}
	if _, success := (*big.Int)(hs).SetString(s, 10); !success {
		return errors.New(fmt.Sprint("failed to scan balance value", value))
	}
	return nil
}

// SQLiteTimestampFormats were taken from github.com/mattn/go-sqlite3 and are
// used when parsing a string to a date
var SQLiteTimestampFormats = []string{
	"2006-01-02 15:04:05.999999999-07:00",
	"2006-01-02T15:04:05.999999999-07:00",
	"2006-01-02 15:04:05.999999999",
	"2006-01-02T15:04:05.999999999",
	"2006-01-02 15:04:05",
	"2006-01-02T15:04:05",
	"2006-01-02 15:04",
	"2006-01-02T15:04",
	"2006-01-02",
}

// Value returns a balance value, implements driver.Valuer interface.
func (hs balance) Value() (driver.Value, error) {
	return (*big.Int)(&hs).String(), nil
}

// GormDataType implements gorm.GormDataTypeInterface.
func (datetime) GormDataType() string {
	return "string"
}

// Scan scan value into balance, implements sql.Scanner interface.
func (dt *datetime) Scan(value interface{}) error {
	var s string
	switch value := value.(type) {
	case string:
		s = value
	case []byte:
		s = string(value)
	case time.Time:
		*dt = datetime(value)
		return nil
	default:
		return fmt.Errorf("failed to unmarshal time.Time value: %v %T", value, value)
	}

	var ok bool
	var t time.Time
	s = strings.TrimSuffix(s, "Z")
	for _, format := range SQLiteTimestampFormats {
		if timeVal, err := time.ParseInLocation(format, s, time.UTC); err == nil {
			ok = true
			t = timeVal
			break
		}
	}
	if !ok {
		return fmt.Errorf("failed to parse datetime value: %v", s)
	}

	*dt = datetime(t)
	return nil
}

// Value returns a datetime value, implements driver.Valuer interface.
func (dt datetime) Value() (driver.Value, error) {
	return (time.Time)(dt).Format(SQLiteTimestampFormats[0]), nil
}
