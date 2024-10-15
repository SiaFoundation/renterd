package object

import "strings"

// Directories returns the directories for the given path, excluding the root
// directory.
func Directories(path string, explicit bool) (dirs []string) {
	if explicit {
		path = strings.TrimSuffix(path, "/")
	}
	if path == "/" {
		return nil
	}
	for i, r := range path {
		if r != '/' {
			continue
		}
		dir := path[:i+1]
		if dir == "/" {
			continue
		}
		dirs = append(dirs, dir)
	}
	return
}
