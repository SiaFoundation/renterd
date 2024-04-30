package contractor

import (
	"testing"

	rhpv2 "go.sia.tech/core/rhp/v2"
)

func TestRegisterLostSectorsAlert(t *testing.T) {
	for _, tc := range []struct {
		dataLost   uint64
		dataStored uint64
		expected   bool
	}{
		{0, 0, false},
		{0, rhpv2.SectorSize, false},
		{rhpv2.SectorSize, 0, true},
		{rhpv2.SectorSize, 99 * rhpv2.SectorSize, true},
		{rhpv2.SectorSize, 100 * rhpv2.SectorSize, true},  // exactly 1%
		{rhpv2.SectorSize, 101 * rhpv2.SectorSize, false}, // just short of 1%
	} {
		if result := registerLostSectorsAlert(tc.dataLost, tc.dataStored); result != tc.expected {
			t.Fatalf("unexpected result for dataLost=%d, dataStored=%d: %v", tc.dataLost, tc.dataStored, result)
		}
	}
}
