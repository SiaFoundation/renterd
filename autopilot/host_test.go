package autopilot

import (
	"testing"

	"go.sia.tech/renterd/internal/test"
)

func TestHost(t *testing.T) {
	hk := test.RandomHostKey()
	h := test.NewHost(hk, test.NewHostPriceTable(), test.NewHostSettings())

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
