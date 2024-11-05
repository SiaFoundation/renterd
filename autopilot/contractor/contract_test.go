package contractor

import (
	"reflect"
	"testing"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
)

func TestSortContractsForMaintenance(t *testing.T) {
	set := "testset"
	cfg := api.ContractsConfig{
		Set: set,
	}

	// empty but in set
	c1 := contract{
		ContractMetadata: api.ContractMetadata{
			ID:           types.FileContractID{1},
			Size:         0,
			ContractSets: []string{set},
		},
	}
	// some data and in set
	c2 := contract{
		ContractMetadata: api.ContractMetadata{
			ID:           types.FileContractID{2},
			Size:         10,
			ContractSets: []string{set},
		},
	}
	// same as c2 - sort should be stable
	c3 := contract{
		ContractMetadata: api.ContractMetadata{
			ID:           types.FileContractID{3},
			Size:         10,
			ContractSets: []string{set},
		},
	}
	// more data but not in set
	c4 := contract{
		ContractMetadata: api.ContractMetadata{
			ID:   types.FileContractID{4},
			Size: 20,
		},
	}
	// even more data but not in set
	c5 := contract{
		ContractMetadata: api.ContractMetadata{
			ID:   types.FileContractID{5},
			Size: 30,
		},
	}

	contracts := []contract{c1, c2, c3, c4, c5}
	sortContractsForMaintenance(cfg, contracts)

	if !reflect.DeepEqual(contracts, []contract{c2, c3, c1, c5, c4}) {
		t.Fatal("unexpected sort order")
	}
}
