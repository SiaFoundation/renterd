package sql

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/renterd/api"
)

const (
	proofHashSize = 32
	secretKeySize = 32
)

var (
	ZeroCurrency = Currency(types.ZeroCurrency)
)

type (
	AutopilotConfig api.AutopilotConfig
	BigInt          big.Int
	BusSetting      string
	Currency        types.Currency
	FileContractID  types.FileContractID
	Hash256         types.Hash256
	MerkleProof     struct{ Hashes []types.Hash256 }
	HostSettings    rhpv2.HostSettings
	PriceTable      rhpv3.HostPriceTable
	PublicKey       types.PublicKey
	SecretKey       []byte
	Uint64Str       uint64
	UnixTimeMS      time.Time
	UnixTimeNS      time.Time
	Unsigned64      uint64
)

type scannerValuer interface {
	driver.Valuer
	sql.Scanner
}

var (
	_ scannerValuer = (*AutopilotConfig)(nil)
	_ scannerValuer = (*BigInt)(nil)
	_ scannerValuer = (*BusSetting)(nil)
	_ scannerValuer = (*Currency)(nil)
	_ scannerValuer = (*FileContractID)(nil)
	_ scannerValuer = (*Hash256)(nil)
	_ scannerValuer = (*MerkleProof)(nil)
	_ scannerValuer = (*HostSettings)(nil)
	_ scannerValuer = (*PriceTable)(nil)
	_ scannerValuer = (*PublicKey)(nil)
	_ scannerValuer = (*SecretKey)(nil)
	_ scannerValuer = (*UnixTimeMS)(nil)
	_ scannerValuer = (*UnixTimeNS)(nil)
	_ scannerValuer = (*Unsigned64)(nil)
)

// Scan scan value into AutopilotConfig, implements sql.Scanner interface.
func (cfg *AutopilotConfig) Scan(value interface{}) error {
	var bytes []byte
	switch value := value.(type) {
	case string:
		bytes = []byte(value)
	case []byte:
		bytes = value
	default:
		return fmt.Errorf("failed to unmarshal AutopilotConfig value: %v %T", value, value)
	}
	return json.Unmarshal(bytes, cfg)
}

// Value returns a AutopilotConfig value, implements driver.Valuer interface.
func (cfg AutopilotConfig) Value() (driver.Value, error) {
	return json.Marshal(cfg)
}

// Scan scan value into BigInt, implements sql.Scanner interface.
func (b *BigInt) Scan(value interface{}) error {
	var s string
	switch value := value.(type) {
	case string:
		s = value
	case []byte:
		s = string(value)
	default:
		return fmt.Errorf("failed to unmarshal BigInt value: %v %t", value, value)
	}
	if _, success := (*big.Int)(b).SetString(s, 10); !success {
		return errors.New(fmt.Sprint("failed to scan BigInt value", value))
	}
	return nil
}

// Value returns a BigInt value, implements driver.Valuer interface.
func (b BigInt) Value() (driver.Value, error) {
	return (*big.Int)(&b).String(), nil
}

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

// Scan scan value into HostSettings, implements sql.Scanner interface.
func (hs *HostSettings) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New(fmt.Sprint("failed to unmarshal Settings value:", value))
	}
	return json.Unmarshal(bytes, hs)
}

// Value returns a HostSettings value, implements driver.Valuer interface.
func (hs HostSettings) Value() (driver.Value, error) {
	return json.Marshal(hs)
}

// Scan scan value into PriceTable, implements sql.Scanner interface.
func (pt *PriceTable) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New(fmt.Sprint("failed to unmarshal PriceTable value:", value))
	}
	return json.Unmarshal(bytes, pt)
}

// Value returns a PriceTable value, implements driver.Valuer interface.
func (pt PriceTable) Value() (driver.Value, error) {
	return json.Marshal(pt)
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

// Value returns an key value, implements driver.Valuer interface.
func (k SecretKey) Value() (driver.Value, error) {
	return []byte(k), nil
}

// String implements fmt.Stringer to prevent "s3authentication" settings from
// getting leaked.
func (s BusSetting) String() string {
	if strings.Contains(string(s), "v4Keypairs") {
		return "*****"
	}
	return string(s)
}

// Scan scans value into the BusSetting
func (s *BusSetting) Scan(value interface{}) error {
	switch value := value.(type) {
	case string:
		*s = BusSetting(value)
	case []byte:
		*s = BusSetting(value)
	default:
		return fmt.Errorf("failed to unmarshal BusSetting value from type %t", value)
	}
	return nil
}

// Value returns a BusSetting value, implements driver.Valuer interface.
func (s BusSetting) Value() (driver.Value, error) {
	return string(s), nil
}

// Scan scan value into unixTimeMS, implements sql.Scanner interface.
func (u *UnixTimeMS) Scan(value interface{}) error {
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

	*u = UnixTimeMS(time.UnixMilli(msec))
	return nil
}

// Value returns a int64 value representing a unix timestamp in milliseconds,
// implements driver.Valuer interface.
func (u UnixTimeMS) Value() (driver.Value, error) {
	return time.Time(u).UnixMilli(), nil
}

// Scan scan value into UnixTimeNS, implements sql.Scanner interface.
func (u *UnixTimeNS) Scan(value interface{}) error {
	var nsec int64
	var err error
	switch value := value.(type) {
	case int64:
		nsec = value
	case []uint8:
		nsec, err = strconv.ParseInt(string(value), 10, 64)
		if err != nil {
			return fmt.Errorf("failed to unmarshal UnixTimeNS value: %v %T", value, value)
		}
	default:
		return fmt.Errorf("failed to unmarshal UnixTimeNS value: %v %T", value, value)
	}

	if nsec == 0 {
		*u = UnixTimeNS{}
	} else {
		*u = UnixTimeNS(time.Unix(0, nsec))
	}
	return nil
}

// Value returns a int64 value representing a unix timestamp in milliseconds,
// implements driver.Valuer interface.
func (u UnixTimeNS) Value() (driver.Value, error) {
	return time.Time(u).UnixNano(), nil
}

// Scan scan value into Uint64, implements sql.Scanner interface.
func (u *Uint64Str) Scan(value interface{}) error {
	var s string
	switch value := value.(type) {
	case string:
		s = value
	case []byte:
		s = string(value)
	default:
		return fmt.Errorf("failed to unmarshal Uint64 value: %v %t", value, value)
	}
	var val uint64
	_, err := fmt.Sscan(s, &val)
	if err != nil {
		return fmt.Errorf("failed to scan Uint64 value: %v", err)
	}
	*u = Uint64Str(val)
	return nil
}

// Value returns a Uint64 value, implements driver.Valuer interface.
func (u Uint64Str) Value() (driver.Value, error) {
	return fmt.Sprint(u), nil
}

func UnmarshalEventData(b []byte, t string) (dst wallet.EventData, err error) {
	switch t {
	case wallet.EventTypeMinerPayout,
		wallet.EventTypeSiafundClaim,
		wallet.EventTypeFoundationSubsidy:
		dst = new(wallet.EventPayout)
	case wallet.EventTypeV1ContractResolution:
		dst = new(wallet.EventV1ContractResolution)
	case wallet.EventTypeV2ContractResolution:
		dst = new(wallet.EventV2ContractResolution)
	case wallet.EventTypeV1Transaction:
		dst = new(wallet.EventV1Transaction)
	case wallet.EventTypeV2Transaction:
		dst = new(wallet.EventV2Transaction)
	default:
		return nil, fmt.Errorf("unknown event type %v", t)
	}
	err = json.Unmarshal(b, dst)
	return
}

// Scan scan value into Unsigned64, implements sql.Scanner interface.
func (u *Unsigned64) Scan(value interface{}) error {
	var n int64
	var err error
	switch value := value.(type) {
	case int64:
		n = value
	case []uint8:
		n, err = strconv.ParseInt(string(value), 10, 64)
		if err != nil {
			return fmt.Errorf("failed to unmarshal Unsigned64 value: %v %T", value, value)
		}
	default:
		return fmt.Errorf("failed to unmarshal Unsigned64 value: %v %T", value, value)
	}

	*u = Unsigned64(n)
	return nil
}

// Value returns an Unsigned64 value, implements driver.Valuer interface.
func (u Unsigned64) Value() (driver.Value, error) {
	return int64(u), nil
}
