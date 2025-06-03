package contractor

import (
	"testing"

	rhpv4 "go.sia.tech/core/rhp/v4"
)

func TestRegisterLostSectorsAlert(t *testing.T) {
	for _, tc := range []struct {
		dataLost   uint64
		dataStored uint64
		expected   bool
	}{
		{0, 0, false},
		{0, rhpv4.SectorSize, false},
		{rhpv4.SectorSize, 0, true},
		{rhpv4.SectorSize, 99 * rhpv4.SectorSize, true},
		{rhpv4.SectorSize, 100 * rhpv4.SectorSize, true},  // exactly 1%
		{rhpv4.SectorSize, 101 * rhpv4.SectorSize, false}, // just short of 1%
	} {
		if result := registerLostSectorsAlert(tc.dataLost, tc.dataStored); result != tc.expected {
			t.Fatalf("unexpected result for dataLost=%d, dataStored=%d: %v", tc.dataLost, tc.dataStored, result)
		}
	}
}
