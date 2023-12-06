package api

import (
	"errors"
)

const (
	DefaultBucketName = "default"
)

var (
	// ErrBucketExists is returned when trying to create a bucket that already
	// exists.
	ErrBucketExists = errors.New("bucket already exists")

	// ErrBucketNotEmpty is returned when trying to delete a bucket that is not
	// empty.
	ErrBucketNotEmpty = errors.New("bucket not empty")

	// ErrBucketNotFound is returned when an bucket can't be retrieved from the
	// database.
	ErrBucketNotFound = errors.New("bucket not found")
)

type (
	Bucket struct {
		CreatedAt TimeRFC3339  `json:"createdAt"`
		Name      string       `json:"name"`
		Policy    BucketPolicy `json:"policy"`
	}

	BucketPolicy struct {
		PublicReadAccess bool `json:"publicReadAccess"`
	}

	CreateBucketOptions struct {
		Policy BucketPolicy
	}
)

type (
	BucketCreateRequest struct {
		Name   string       `json:"name"`
		Policy BucketPolicy `json:"policy"`
	}

	BucketUpdatePolicyRequest struct {
		Policy BucketPolicy `json:"policy"`
	}
)
