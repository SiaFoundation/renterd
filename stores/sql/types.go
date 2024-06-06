package sql

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
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
	Settings       rhpv2.HostSettings
	PriceTable     rhpv3.HostPriceTable
	PublicKey      types.PublicKey
	SecretKey      []byte
	EventData      []byte
	UnixTimeNS     time.Time
)

var (
	_ sql.Scanner = &Currency{}
	_ sql.Scanner = &FileContractID{}
	_ sql.Scanner = &Hash256{}
	_ sql.Scanner = &MerkleProof{}
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

// Scan scan value into Settings, implements sql.Scanner interface.
func (hs *Settings) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New(fmt.Sprint("failed to unmarshal Settings value:", value))
	}
	return json.Unmarshal(bytes, hs)
}

// Value returns a Settings value, implements driver.Valuer interface.
func (hs Settings) Value() (driver.Value, error) {
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

// Scan scans value into a EventData, implements sql.Scanner interface.
func (e *EventData) Scan(value interface{}) error {
	b, ok := value.([]byte)
	if !ok {
		return errors.New(fmt.Sprint("failed to unmarshal Transaction value:", value))
	}
	*e = b
	return nil
}

// Value returns a Transaction value, implements driver.Valuer interface.
func (e EventData) Value() (driver.Value, error) {
	return []byte(e), nil
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

func FromEventData(e EventData, t string) (wallet.EventData, error) {
	dec := json.NewDecoder(bytes.NewReader(e))
	switch t {
	case wallet.EventTypeMinerPayout:
		var payout wallet.EventMinerPayout
		if err := dec.Decode(&payout); err != nil {
			return nil, err
		}
		return payout, nil
	case wallet.EventTypeFoundationSubsidy:
		var subsidy wallet.EventFoundationSubsidy
		if err := dec.Decode(&subsidy); err != nil {
			return nil, err
		}
		return subsidy, nil
	case wallet.EventTypeV1Contract:
		var v1c wallet.EventV1ContractPayout
		if err := dec.Decode(&v1c); err != nil {
			return nil, err
		}
		return v1c, nil
	case wallet.EventTypeV1Transaction:
		var v1txn wallet.EventV1Transaction
		if err := dec.Decode(&v1txn); err != nil {
			return nil, err
		}
		return v1txn, nil
	case wallet.EventTypeV2Contract:
		var v2c wallet.EventV2ContractPayout
		if err := dec.Decode(&v2c); err != nil {
			return nil, err
		}
		return v2c, nil
	case wallet.EventTypeV2Transaction:
		var v2txn wallet.EventV2Transaction
		if err := dec.Decode(&v2txn); err != nil {
			return nil, err
		}
		return v2txn, nil
	default:
		return nil, fmt.Errorf("unknown event type %v", t)
	}
}

func ToEventData(e wallet.EventData) (EventData, error) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(e); err != nil {
		return nil, fmt.Errorf("failed to marshal Transaction value: %v", err)
	}
	return buf.Bytes(), nil
}