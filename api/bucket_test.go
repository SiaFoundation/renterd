package api

import (
	"strings"
	"testing"
)

func TestBucketNameValidation(t *testing.T) {
	tests := []struct {
		name  string
		valid bool
		desc  string
	}{
		{
			name:  "valid-bucket-name",
			valid: true,
			desc:  "valid bucket name",
		},
		{
			name:  strings.Repeat("x", 2),
			valid: false,
			desc:  "too short",
		},
		{
			name:  strings.Repeat("x", 64),
			valid: false,
			desc:  "too long",
		},
		{
			name:  "foo.bar",
			valid: false,
			desc:  "contains period",
		},
		{
			name:  "UPPERCASE",
			valid: false,
			desc:  "contains uppercase letter",
		},
		{
			name:  "xn--forbidden-prefix",
			valid: false,
			desc:  "forbidden prefix",
		},
		{
			name:  "forbidden-suffix-s3alias",
			valid: false,
			desc:  "forbidden suffix",
		},
		{
			name:  "$pecial-©haracters",
			valid: false,
			desc:  "special characters",
		},
		{
			name:  "with spaces",
			valid: false,
			desc:  "spaces",
		},
	}
	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			valid := BucketCreateRequest{Name: test.name}.Validate() == nil
			if valid != test.valid {
				t.Fatalf("'valid' should be %v but was %v", test.valid, valid)
			}
		})
	}
}
