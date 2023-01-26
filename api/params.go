package api

import (
	"fmt"
	"net/url"
	"strconv"
	"time"
)

type (
	// ParamTime aliases time.Time to add marshaling functions that url escape
	// and format a time in the RFC3339 format.
	ParamTime time.Time

	// A Duration is the elapsed time between two instants. Durations are encoded as
	// an integer number of milliseconds.
	Duration time.Duration

	// ParamString is a helper type since jape expects query params to
	// implement the TextMarshaler interface.
	ParamString string

	// A SlabID uniquely identifies a slab.
	SlabID uint
)

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

// String implements fmt.Stringer.
func (d Duration) String() string {
	return strconv.FormatInt(time.Duration(d).Milliseconds(), 10)
}

// MarshalText implements encoding.TextMarshaler.
func (d Duration) MarshalText() ([]byte, error) {
	return []byte(d.String()), nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (d *Duration) UnmarshalText(b []byte) error {
	ms, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return err
	}
	*d = Duration(time.Duration(ms) * time.Millisecond)
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
