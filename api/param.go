package api

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"time"

	"go.sia.tech/core/types"
	"lukechampine.com/frand"
)

type (
	// ParamCurrency aliases types.Currency and marshal them in hastings.
	ParamCurrency types.Currency

	// TimeRFC3339 aliases time.Time to add marshaling functions that url escape
	// and format a time in the RFC3339 format.
	TimeRFC3339 time.Time

	// A DurationH aliases time.Duration to add marshaling functions
	// that format the duration in hours.
	DurationH time.Duration

	// A DurationMS is a duration encoded as an integer number of
	// milliseconds.
	DurationMS time.Duration

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

// CompareTimeRFC3339 is a comparer function to be used with cmp.Comparer.
func CompareTimeRFC3339(t1, t2 TimeRFC3339) bool {
	return time.Time(t1).UTC().Equal(time.Time(t2).UTC())
}

// TimeNow returns the current time as a TimeRFC3339.
func TimeNow() TimeRFC3339 {
	return TimeRFC3339(time.Now())
}

// Std converts a TimeRFC3339 to a time.Time.
func (t TimeRFC3339) Std() time.Time { return time.Time(t).UTC() }

// IsZero reports whether t represents the zero time instant, January 1, year 1,
// 00:00:00 UTC.
func (t TimeRFC3339) IsZero() bool { return (time.Time)(t).IsZero() }

// String implements fmt.Stringer.
func (t TimeRFC3339) String() string { return (time.Time)(t).UTC().Format(time.RFC3339Nano) }

// UnmarshalText implements encoding.TextUnmarshaler.
func (t *TimeRFC3339) UnmarshalText(b []byte) error {
	var stdTime time.Time
	err := stdTime.UnmarshalText(b)
	if err != nil {
		return err
	}
	*t = TimeRFC3339(stdTime.UTC())
	return nil
}

// MarshalJSON implements json.Marshaler.
func (t TimeRFC3339) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("%q", (time.Time)(t).UTC().Format(time.RFC3339Nano))), nil
}

// String implements fmt.Stringer.
func (d DurationMS) String() string {
	return strconv.FormatInt(time.Duration(d).Milliseconds(), 10)
}

// MarshalText implements encoding.TextMarshaler.
func (d DurationMS) MarshalText() ([]byte, error) {
	return []byte(d.String()), nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (d *DurationMS) UnmarshalText(b []byte) error {
	ms, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return err
	}
	*d = DurationMS(time.Duration(ms) * time.Millisecond)
	return nil
}

// MarshalJSON implements json.Marshaler.
func (d DurationMS) MarshalJSON() ([]byte, error) {
	return []byte(strconv.FormatInt(time.Duration(d).Milliseconds(), 10)), nil
}

// UnmarshalJSON implements json.Unmarshaler.
func (d *DurationMS) UnmarshalJSON(data []byte) error {
	return d.UnmarshalText(data)
}

// String implements fmt.Stringer.
func (d DurationH) String() string {
	return strconv.Itoa(int(time.Duration(d).Hours()))
}

// MarshalText implements encoding.TextMarshaler.
func (d DurationH) MarshalText() ([]byte, error) {
	return []byte(d.String()), nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (d *DurationH) UnmarshalText(b []byte) error {
	h, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return err
	}
	*d = DurationH(time.Duration(h) * time.Hour)
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
