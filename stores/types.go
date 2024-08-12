package stores

import (
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.sia.tech/core/types"
)

const (
	proofHashSize = 32
	secretKeySize = 32
)

type (
	unixTimeMS     time.Time
	bCurrency      types.Currency
	fileContractID types.FileContractID
	publicKey      types.PublicKey
	secretKey      []byte
	setting        string

	// NOTE: we have to wrap the proof here because Gorm can't scan bytes into
	// multiple slices, all bytes are scanned into the first row
	merkleProof struct{ proof []types.Hash256 }
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

// GormDataType implements gorm.GormDataTypeInterface.
func (mp *merkleProof) GormDataType() string {
	return "bytes"
}

// Scan scans value into mp, implements sql.Scanner interface.
func (mp *merkleProof) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New(fmt.Sprint("failed to unmarshal merkleProof value:", value))
	} else if len(bytes) == 0 || len(bytes)%proofHashSize != 0 {
		return fmt.Errorf("failed to unmarshal merkleProof value due to invalid number of bytes %v", len(bytes))
	}

	n := len(bytes) / proofHashSize
	mp.proof = make([]types.Hash256, n)
	for i := 0; i < n; i++ {
		copy(mp.proof[i][:], bytes[:proofHashSize])
		bytes = bytes[proofHashSize:]
	}
	return nil
}

// Value returns a merkle proof value, implements driver.Valuer interface.
func (mp merkleProof) Value() (driver.Value, error) {
	var i int
	out := make([]byte, len(mp.proof)*proofHashSize)
	for _, ph := range mp.proof {
		i += copy(out[i:], ph[:])
	}
	return out, nil
}
