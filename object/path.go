package object

import "strings"

// Directories returns the directories for the given path, excluding the root
// directory.
func Directories(path string) (dirs []string) {
	lsc := 1
	for {
		path = strings.TrimPrefix(path, "/")
		if !strings.HasPrefix(path, "/") {
			break
		}
		lsc++
		dirs = append(dirs, strings.Repeat("/", lsc))
	}

	parts := strings.Split(path, "/")
	for i := 1; i < len(parts); i++ {
		dirs = append(dirs, strings.Repeat("/", lsc)+strings.Join(parts[:i], "/")+"/")
	}
	return
}
