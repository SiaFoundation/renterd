package object

import (
	"reflect"
	"testing"
)

func TestDirectories(t *testing.T) {
	cases := []struct {
		path     string
		explicit bool
		dirs     []string
	}{
		{"/", true, nil},
		{"/", false, nil},
		{"/foo", true, nil},
		{"/foo", false, nil},
		{"/foo/bar", true, []string{"/foo/"}},
		{"/foo/bar", false, []string{"/foo/"}},
		{"/foo/bar/", true, []string{"/foo/"}},
		{"/foo/bar/", false, []string{"/foo/", "/foo/bar/"}},
	}
	for _, c := range cases {
		if got := Directories(c.path, c.explicit); !reflect.DeepEqual(got, c.dirs) {
			t.Fatalf("unexpected dirs for path %v (explicit %t), %v != %v", c.path, c.explicit, got, c.dirs)
		}
	}

}
