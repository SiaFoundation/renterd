package utils

import (
	"fmt"
	"strings"
)

func DebugQuery(query string, args []any) string {
	for _, v := range args {
		index := strings.Index(query, "?")
		if index == -1 {
			continue
		}

		var value string
		if _, ok := v.(string); ok {
			value = fmt.Sprintf("%q", v)
		} else {
			value = fmt.Sprint(v)
		}

		query = string(query[:index]) + value + string(query[index+1:])
	}
	return query
}
