package stores

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/stores/sql"
)

func TestTypeCurrency(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	// prepare the table
	if _, err := ss.ExecDBSpecific(
		"CREATE TABLE currencies (id INTEGER PRIMARY KEY AUTOINCREMENT,c BLOB);", // sqlite
		"CREATE TABLE currencies (id INT AUTO_INCREMENT PRIMARY KEY, c BLOB);",   // mysql
	); err != nil {
		t.Fatal(err)
	}

	// insert currencies in random order
	if _, err := ss.DB().Exec(context.Background(), "INSERT INTO currencies (c) VALUES (?),(?),(?);", sql.BCurrency(types.MaxCurrency), sql.BCurrency(types.NewCurrency64(1)), sql.BCurrency(types.ZeroCurrency)); err != nil {
		t.Fatal(err)
	}

	// fetch currencies and assert they're sorted
	var currencies []sql.BCurrency
	rows, err := ss.DB().Query(context.Background(), "SELECT c FROM currencies ORDER BY c ASC;")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var c sql.BCurrency
		if err := rows.Scan(&c); err != nil {
			t.Fatal(err)
		}
		currencies = append(currencies, c)
	}
	if !sort.SliceIsSorted(currencies, func(i, j int) bool {
		return types.Currency(currencies[i]).Cmp(types.Currency(currencies[j])) < 0
	}) {
		t.Fatal("currencies not sorted", currencies)
	}

	// convenience variables
	c0 := currencies[0]
	c1 := currencies[1]
	cM := currencies[2]

	tests := []struct {
		a   sql.BCurrency
		b   sql.BCurrency
		cmp string
	}{
		{
			a:   c0,
			b:   c1,
			cmp: "<",
		},
		{
			a:   c1,
			b:   c0,
			cmp: ">",
		},
		{
			a:   c0,
			b:   c1,
			cmp: "!=",
		},
		{
			a:   c1,
			b:   c1,
			cmp: "=",
		},
		{
			a:   c0,
			b:   cM,
			cmp: "<",
		},
		{
			a:   cM,
			b:   c0,
			cmp: ">",
		},
		{
			a:   cM,
			b:   cM,
			cmp: "=",
		},
	}
	for i, test := range tests {
		var result bool
		if err := ss.QueryRowDBSpecific(
			fmt.Sprintf("SELECT ? %s ?", test.cmp),
			fmt.Sprintf("SELECT HEX(?) %s HEX(?)", test.cmp),
			[]any{test.a, test.b},
			[]any{test.a, test.b},
		).Scan(&result); err != nil {
			t.Fatal(err)
		} else if !result {
			t.Errorf("unexpected result in case %d/%d: expected %v %s %v to be true", i+1, len(tests), types.Currency(test.a).String(), test.cmp, types.Currency(test.b).String())
		} else if test.cmp == "<" && types.Currency(test.a).Cmp(types.Currency(test.b)) >= 0 {
			t.Fatal("invalid result")
		} else if test.cmp == ">" && types.Currency(test.a).Cmp(types.Currency(test.b)) <= 0 {
			t.Fatal("invalid result")
		} else if test.cmp == "=" && types.Currency(test.a).Cmp(types.Currency(test.b)) != 0 {
			t.Fatal("invalid result")
		}
	}
}

func TestTypeMerkleProof(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	// prepare the table
	ss.ExecDBSpecific(
		"CREATE TABLE merkle_proofs (id INTEGER PRIMARY KEY AUTOINCREMENT,merkle_proof BLOB);",
		"CREATE TABLE merkle_proofs (id INT AUTO_INCREMENT PRIMARY KEY, merkle_proof BLOB);",
	)

	// insert merkle proof
	mp1 := sql.MerkleProof{Hashes: []types.Hash256{{3}, {1}, {2}}}
	mp2 := sql.MerkleProof{Hashes: []types.Hash256{{4}}}
	if _, err := ss.DB().Exec(context.Background(), "INSERT INTO merkle_proofs (merkle_proof) VALUES (?), (?);", mp1, mp2); err != nil {
		t.Fatal(err)
	}

	// fetch first proof
	var first sql.MerkleProof
	if err := ss.DB().QueryRow(context.Background(), "SELECT merkle_proof FROM merkle_proofs").Scan(&first); err != nil {
		t.Fatal(err)
	} else if first.Hashes[0] != (types.Hash256{3}) || first.Hashes[1] != (types.Hash256{1}) || first.Hashes[2] != (types.Hash256{2}) {
		t.Fatalf("unexpected proof %+v", first)
	}

	// fetch both proofs
	var both []sql.MerkleProof
	rows, err := ss.DB().Query(context.Background(), "SELECT merkle_proof FROM merkle_proofs")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var mp sql.MerkleProof
		if err := rows.Scan(&mp); err != nil {
			t.Fatal(err)
		}
		both = append(both, mp)
	}
	if len(both) != 2 {
		t.Fatalf("unexpected number of proofs: %d", len(both))
	} else if both[1].Hashes[0] != (types.Hash256{4}) {
		t.Fatalf("unexpected proof %+v", both)
	}
}
