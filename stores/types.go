package stores

import (
	"database/sql/driver"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
)

const (
	secretKeySize = 32
)

var zeroCurrency = currency(types.ZeroCurrency)

type (
	unixTimeMS     time.Time
	datetime       time.Time
	currency       types.Currency
	bCurrency      types.Currency
	fileContractID types.FileContractID
	hash256        types.Hash256
	publicKey      types.PublicKey
	hostSettings   rhpv2.HostSettings
	hostPriceTable rhpv3.HostPriceTable
	unsigned64     uint64 // used for storing large uint64 values in sqlite
	secretKey      []byte
	setting        string
)

// GormDataType implements gorm.GormDataTypeInterface.
func (setting) GormDataType() string {
	return "string"
}

// String implements fmt.Stringer to prevent "s3authentication" settings from
// getting leaked.
func (s setting) String() string {
	if strings.Contains(string(s), "v4Keypairs") {
		return "*****"
	}
	return string(s)
}

// Scan scans value into the setting
func (s *setting) Scan(value interface{}) error {
	switch value := value.(type) {
	case string:
		*s = setting(value)
	case []byte:
		*s = setting(value)
	default:
		return fmt.Errorf("failed to unmarshal setting value from type %t", value)
	}
	return nil
}

// Value returns a setting value, implements driver.Valuer interface.
func (s setting) Value() (driver.Value, error) {
	return string(s), nil
}

// GormDataType implements gorm.GormDataTypeInterface.
func (secretKey) GormDataType() string {
	return "bytes"
}

// String implements fmt.Stringer to prevent the key from getting leaked in
// logs.
func (k secretKey) String() string {
	return "*****"
}

// Scan scans value into key, implements sql.Scanner interface.
func (k *secretKey) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New(fmt.Sprint("failed to unmarshal secretKey value:", value))
	} else if len(bytes) != secretKeySize {
		return fmt.Errorf("failed to unmarshal secretKey value due to invalid number of bytes %v != %v: %v", len(bytes), secretKeySize, value)
	}
	*k = append(secretKey{}, secretKey(bytes)...)
	return nil
}

// Value returns an key value, implements driver.Valuer interface.
func (k secretKey) Value() (driver.Value, error) {
	return []byte(k), nil
}

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
	if len(bytes) != len(hash256{}) {
		return fmt.Errorf("failed to unmarshal hash256 value due to invalid number of bytes %v != %v: %v", len(bytes), len(fileContractID{}), value)
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
	if len(bytes) != len(fileContractID{}) {
		return fmt.Errorf("failed to unmarshal fcid value due to invalid number of bytes %v != %v: %v", len(bytes), len(fileContractID{}), value)
	}
	*fcid = *(*fileContractID)(bytes)
	return nil
}

// Value returns a fileContractID value, implements driver.Valuer interface.
func (fcid fileContractID) Value() (driver.Value, error) {
	return fcid[:], nil
}

func (currency) GormDataType() string {
	return "string"
}

// Scan scan value into currency, implements sql.Scanner interface.
func (c *currency) Scan(value interface{}) error {
	var s string
	switch value := value.(type) {
	case string:
		s = value
	case []byte:
		s = string(value)
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
	if len(bytes) != len(types.PublicKey{}) {
		return fmt.Errorf("failed to unmarshal publicKey value due invalid number of bytes %v != %v: %v", len(bytes), len(publicKey{}), value)
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

// GormDataType implements gorm.GormDataTypeInterface.
func (datetime) GormDataType() string {
	return "string"
}

// Scan scan value into datetime, implements sql.Scanner interface.
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

// GormDataType implements gorm.GormDataTypeInterface.
func (unixTimeMS) GormDataType() string {
	return "BIGINT"
}

// Scan scan value into unixTimeMS, implements sql.Scanner interface.
func (u *unixTimeMS) Scan(value interface{}) error {
	var msec int64
	var err error
	switch value := value.(type) {
	case int64:
		msec = value
	case []uint8:
		msec, err = strconv.ParseInt(string(value), 10, 64)
		if err != nil {
			return fmt.Errorf("failed to unmarshal unixTimeMS value: %v %T", value, value)
		}
	default:
		return fmt.Errorf("failed to unmarshal unixTimeMS value: %v %T", value, value)
	}

	*u = unixTimeMS(time.UnixMilli(msec))
	return nil
}

// Value returns a int64 value representing a unix timestamp in milliseconds,
// implements driver.Valuer interface.
func (u unixTimeMS) Value() (driver.Value, error) {
	return time.Time(u).UnixMilli(), nil
}

// GormDataType implements gorm.GormDataTypeInterface.
func (unsigned64) GormDataType() string {
	return "BIGINT"
}

// Scan scan value into unsigned64, implements sql.Scanner interface.
func (u *unsigned64) Scan(value interface{}) error {
	var n int64
	var err error
	switch value := value.(type) {
	case int64:
		n = value
	case []uint8:
		n, err = strconv.ParseInt(string(value), 10, 64)
		if err != nil {
			return fmt.Errorf("failed to unmarshal unsigned64 value: %v %T", value, value)
		}
	default:
		return fmt.Errorf("failed to unmarshal unsigned64 value: %v %T", value, value)
	}

	*u = unsigned64(n)
	return nil
}

// Value returns a datetime value, implements driver.Valuer interface.
func (u unsigned64) Value() (driver.Value, error) {
	return int64(u), nil
}

func (bCurrency) GormDataType() string {
	return "bytes"
}

// Scan implements the sql.Scanner interface.
func (sc *bCurrency) Scan(src any) error {
	buf, ok := src.([]byte)
	if !ok {
		return fmt.Errorf("cannot scan %T to Currency", src)
	} else if len(buf) != 16 {
		return fmt.Errorf("cannot scan %d bytes to Currency", len(buf))
	}

	sc.Hi = binary.BigEndian.Uint64(buf[:8])
	sc.Lo = binary.BigEndian.Uint64(buf[8:])
	return nil
}

// Value implements the driver.Valuer interface.
func (sc bCurrency) Value() (driver.Value, error) {
	buf := make([]byte, 16)
	binary.BigEndian.PutUint64(buf[:8], sc.Hi)
	binary.BigEndian.PutUint64(buf[8:], sc.Lo)
	return buf, nil
}
