package autopilot

import (
	"errors"
	"strings"
)

func containsError(x, y error) bool {
	return strings.Contains(x.Error(), y.Error())
}

func errStr(err error) string {
	if err != nil {
		return err.Error()
	}
	return ""
}

func joinErrors(errs []error) error {
	filtered := errs[:0]
	for _, err := range errs {
		if err != nil {
			filtered = append(filtered, err)
		}
	}

	switch len(filtered) {
	case 0:
		return nil
	case 1:
		return filtered[0]
	default:
		strs := make([]string, len(filtered))
		for i := range strs {
			strs[i] = filtered[i].Error()
		}
		return errors.New(strings.Join(strs, ";"))
	}
}
