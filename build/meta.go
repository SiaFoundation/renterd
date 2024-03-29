// Code generated by go generate; DO NOT EDIT.
// This file was generated by go generate at 2023-08-11T11:44:04+02:00.
package build

import (
	"time"
)

const (
	commit    = "?"
	version   = "?"
	buildTime = 0
)

// Commit returns the commit hash of hostd
func Commit() string {
	return commit
}

// Version returns the version of hostd
func Version() string {
	return version
}

// BuildTime returns the time at which the binary was built.
func BuildTime() time.Time {
	return time.Unix(buildTime, 0)
}
