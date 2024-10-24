package object

import "strings"

// Directories returns the directories for the given path. When explicit is
// true, the returned directories do not include the path itself should it be a
// directory. The root path ('/') is always excluded.
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
