package stores

import (
	"context"
	"testing"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/v2/api"
	isql "go.sia.tech/renterd/v2/internal/sql"
	"go.sia.tech/renterd/v2/stores/sql"
)

func TestFetchUsedContracts(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	// add host
	hk := types.PublicKey{1}
	if err := ss.addTestHost(types.PublicKey{1}); err != nil {
		t.Fatal(err)
	}

	// add 3 contracts
	for i := 1; i <= 3; i++ {
		if _, err := ss.addTestContract(types.FileContractID{byte(i)}, hk); err != nil {
			t.Fatal(err)
		}
	}

	// renew the first contract
	if err := ss.renewTestContract(types.PublicKey{1}, types.FileContractID{1}, types.FileContractID{4}, 10); err != nil {
		t.Fatal(err)
	}

	// fetch used contracts
	var ucs map[types.FileContractID]sql.UsedContract
	if err := ss.DB().Transaction(context.Background(), func(tx isql.Tx) (err error) {
		ucs, err = sql.FetchUsedContracts(context.Background(), tx, []types.FileContractID{
			{1}, // renewed
			{2}, // untouched
			{3}, // untouched
		})
		return
	}); err != nil {
		t.Fatal(err)
	} else if len(ucs) != 3 {
		t.Fatal(err)
	} else if _, ok := ucs[types.FileContractID{1}]; !ok {
		t.Fatal("unexpected result", ucs)
	} else if _, ok := ucs[types.FileContractID{2}]; !ok {
		t.Fatal("unexpected result", ucs)
	} else if _, ok := ucs[types.FileContractID{3}]; !ok {
		t.Fatal("unexpected result", ucs)
	}

	// archive the second contract
	if err := ss.ArchiveContract(context.Background(), types.FileContractID{2}, api.ContractArchivalReasonRenewed); err != nil {
		t.Fatal(err)
	}

	// fetch used contracts
	if err := ss.DB().Transaction(context.Background(), func(tx isql.Tx) (err error) {
		ucs, err = sql.FetchUsedContracts(context.Background(), tx, []types.FileContractID{
			{1}, // renewed
			{2}, // archived
			{3}, // untouched
		})
		return
	}); err != nil {
		t.Fatal(err)
	} else if len(ucs) != 2 {
		t.Fatal(err)
	} else if _, ok := ucs[types.FileContractID{1}]; !ok {
		t.Fatal("unexpected result", ucs)
	} else if _, ok := ucs[types.FileContractID{3}]; !ok {
		t.Fatal("unexpected result", ucs)
	}
}
