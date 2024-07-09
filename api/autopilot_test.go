package api

import (
	"reflect"
	"testing"

	"go.sia.tech/core/types"
)

func TestSortContractsForMaintenance(t *testing.T) {
	set := "testset"
	cfg := ContractsConfig{
		Set: set,
	}

	// empty but in set
	c1 := Contract{
		ContractMetadata: ContractMetadata{
			ID:           types.FileContractID{1},
			Size:         0,
			ContractSets: []string{set},
		},
	}
	// some data and in set
	c2 := Contract{
		ContractMetadata: ContractMetadata{
			ID:           types.FileContractID{2},
			Size:         10,
			ContractSets: []string{set},
		},
	}
	// same as c2 - sort should be stable
	c3 := Contract{
		ContractMetadata: ContractMetadata{
			ID:           types.FileContractID{3},
			Size:         10,
			ContractSets: []string{set},
		},
	}
	// more data but not in set
	c4 := Contract{
		ContractMetadata: ContractMetadata{
			ID:   types.FileContractID{4},
			Size: 20,
		},
	}
	// even more data but not in set
	c5 := Contract{
		ContractMetadata: ContractMetadata{
			ID:   types.FileContractID{5},
			Size: 30,
		},
	}

	contracts := []Contract{c1, c2, c3, c4, c5}
	cfg.SortContractsForMaintenance(contracts)

	if !reflect.DeepEqual(contracts, []Contract{c2, c3, c1, c5, c4}) {
		t.Fatal("unexpected sort order")
	}
}
