package api

import (
	"errors"
	"regexp"
	"strings"
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

var validBucketExp = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$`)

func (req BucketCreateRequest) Validate() error {
	// make sure the bucket name complies with the restrictions for S3 transfer
	// acceleration which are the regular S3 conventions with the additional
	// restriction that the bucket name can't contain a period.
	if strings.HasPrefix(req.Name, "xn--") ||
		strings.HasSuffix(req.Name, "-s3alias") ||
		!validBucketExp.MatchString(req.Name) {
		return errors.New("the bucket name doesn't comply with the S3 bucket naming convention (https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html)")
	}
	return nil
}
