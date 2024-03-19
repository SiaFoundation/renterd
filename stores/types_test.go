package stores

import "testing"

func TestTypeSetting(t *testing.T) {
	s1 := setting("some setting")
	s2 := setting("v4Keypairs")

	if s1.String() != "some setting" {
		t.Fatal("unexpected string")
	} else if s2.String() != "*****" {
		t.Fatal("unexpected string")
	}
}
