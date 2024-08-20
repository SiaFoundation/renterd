package test

import (
	"testing"
)

func TestHost(t *testing.T) {
	hk := RandomHostKey()
	h := NewHost(hk, NewHostPriceTable(), NewHostSettings())

	// assert host is online
	if !h.IsOnline() {
		t.Fatal("unexpected")
	}

	// Mark last scan failed. Should still be online.
	h.Interactions.LastScanSuccess = false
	if !h.IsOnline() {
		t.Fatal("unexpected")
	}

	// Mark previous scan failed as well. Should be offline.
	h.Interactions.SecondToLastScanSuccess = false
	if h.IsOnline() {
		t.Fatal("unexpected")
	}

	// Mark last scan successful again. Should be online.
	h.Interactions.LastScanSuccess = true
	if !h.IsOnline() {
		t.Fatal("unexpected")
	}
}
