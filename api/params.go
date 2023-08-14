package api

import (
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"time"

	"go.sia.tech/core/types"
	"lukechampine.com/frand"
)

type (
	// ParamCurrency aliases types.Currency and marshal them in hastings.
	ParamCurrency types.Currency

	// ParamTime aliases time.Time to add marshaling functions that url escape
	// and format a time in the RFC3339 format.
	ParamTime time.Time

	// A ParamDurationHour aliases time.Duration to add marshaling functions
	// that format the duration in hours.
	ParamDurationHour time.Duration

	// A ParamDuration is a duration encoded as an integer number of
	// milliseconds.
	ParamDuration time.Duration

	// ParamString is a helper type since jape expects query params to
	// implement the TextMarshaler interface.
	ParamString string

	// A SlabID uniquely identifies a slab.
	SlabID uint

	// UploadID identifies an ongoing upload.
	UploadID [8]byte
)

func NewUploadID() (uID UploadID) {
	frand.Read(uID[:])
	return
}

// String implements fmt.Stringer.
func (c ParamCurrency) String() string { return types.Currency(c).ExactString() }

// MarshalText implements encoding.TextMarshaler.
func (c ParamCurrency) MarshalText() ([]byte, error) {
	return []byte(c.String()), nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (c *ParamCurrency) UnmarshalText(b []byte) error {
	curr, err := types.ParseCurrency(string(b))
	*c = ParamCurrency(curr)
	return err
}

// String implements fmt.Stringer.
func (s ParamString) String() string { return string(s) }

// MarshalText implements encoding.TextMarshaler.
func (s ParamString) MarshalText() ([]byte, error) {
	return []byte(s), nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (s *ParamString) UnmarshalText(b []byte) error {
	*s = ParamString(b)
	return nil
}

// String implements fmt.Stringer.
func (t ParamTime) String() string { return url.QueryEscape((time.Time)(t).Format(time.RFC3339)) }

// UnmarshalText implements encoding.TextUnmarshaler.
func (t *ParamTime) UnmarshalText(b []byte) error { return (*time.Time)(t).UnmarshalText(b) }

// MarshalJSON implements json.Marshaler.
func (t ParamTime) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"%s"`, (time.Time)(t).Format(time.RFC3339))), nil
}

// String implements fmt.Stringer.
func (d ParamDuration) String() string {
	return strconv.FormatInt(time.Duration(d).Milliseconds(), 10)
}

// MarshalText implements encoding.TextMarshaler.
func (d ParamDuration) MarshalText() ([]byte, error) {
	return []byte(d.String()), nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (d *ParamDuration) UnmarshalText(b []byte) error {
	ms, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return err
	}
	*d = ParamDuration(time.Duration(ms) * time.Millisecond)
	return nil
}

// MarshalJSON implements json.Marshaler.
func (d ParamDuration) MarshalJSON() ([]byte, error) {
	return []byte(strconv.FormatInt(time.Duration(d).Milliseconds(), 10)), nil
}

// UnmarshalJSON implements json.Unmarshaler.
func (d *ParamDuration) UnmarshalJSON(data []byte) error {
	return d.UnmarshalText(data)
}

// String implements fmt.Stringer.
func (d ParamDurationHour) String() string {
	return strconv.Itoa(int(time.Duration(d).Hours()))
}

// MarshalText implements encoding.TextMarshaler.
func (d ParamDurationHour) MarshalText() ([]byte, error) {
	return []byte(d.String()), nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (d *ParamDurationHour) UnmarshalText(b []byte) error {
	h, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return err
	}
	*d = ParamDurationHour(time.Duration(h) * time.Hour)
	return nil
}

// LoadString is implemented for jape's DecodeParam.
func (sid *SlabID) LoadString(s string) (err error) {
	var slabID uint
	_, err = fmt.Sscan(s, &slabID)
	*sid = SlabID(slabID)
	return
}

// String encodes the SlabID as a string.
func (sid SlabID) String() string {
	return fmt.Sprint(uint8(sid))
}

// String implements fmt.Stringer.
func (uID UploadID) String() string {
	return hex.EncodeToString(uID[:])
}

// MarshalText implements encoding.TextMarshaler.
func (uID UploadID) MarshalText() ([]byte, error) {
	return []byte(uID.String()), nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (uID *UploadID) UnmarshalText(b []byte) error {
	b, err := hex.DecodeString(string(b))
	if err != nil {
		return err
	} else if len(b) != 8 {
		return errors.New("invalid length")
	}

	copy(uID[:], b)
	return nil
}
