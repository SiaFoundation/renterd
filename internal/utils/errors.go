package utils

import (
	"errors"
	"strings"
)

// IsErr can be used to compare an error to a target and also works when used on
// errors that haven't been wrapped since it will fall back to a string
// comparison. Useful to check errors returned over the network.
func IsErr(err error, target error) bool {
	if (err == nil) != (target == nil) {
		return false
	} else if errors.Is(err, target) {
		return true
	}
	return strings.Contains(err.Error(), target.Error())
}
