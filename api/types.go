package api

import (
	"net/url"
	"strconv"
	"time"
)

type ParamTime time.Time

func (t ParamTime) String() string                { return url.QueryEscape((time.Time)(t).Format(time.RFC3339)) }
func (t *ParamTime) UnmarshalText(b []byte) error { return (*time.Time)(t).UnmarshalText(b) }

// A Duration is the elapsed time between two instants. Durations are encoded as
// an integer number of milliseconds.
type Duration time.Duration

// MarshalText implements encoding.TextMarshaler.
func (d Duration) MarshalText() ([]byte, error) {
	return []byte(strconv.FormatInt(time.Duration(d).Milliseconds(), 10)), nil
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
