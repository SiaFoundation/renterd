package object

import (
	"strings"
)

// Directories returns the directories for the given path.
func Directories(path string) (dirs []string) {
	if path == "" {
		return
	} else if path == "/" {
		dirs = append(dirs, "/")
		return
	}

	path = strings.TrimSuffix(path, "/")
	for i, r := range path {
		if r != '/' {
			continue
		}
		dirs = append(dirs, path[:i+1])
	}
	return
}
