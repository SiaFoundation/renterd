package object

import "testing"

func TestDirectories(t *testing.T) {
	tests := []struct {
		path string
		want []string
	}{
		{"", nil},
		{"/", []string{"/"}},
		{"/foo", []string{"/"}},
		{"/foo/bar", []string{"/", "/foo/"}},
		{"/dir/fakedir/", []string{"/", "/dir/"}},
		{"//double/", []string{"/", "//"}},
	}
	for _, tc := range tests {
		got := Directories(tc.path)
		if len(got) != len(tc.want) {
			t.Fatalf("expected %v, got %v", tc.want, got)
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Fatalf("expected %v, got %v", tc.want, got)
			}
		}
	}
}
