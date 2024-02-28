package stores

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"go.sia.tech/core/types"
)

func TestTypeCurrency(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	// prepare the table
	if isSQLite(ss.db) {
		if err := ss.db.Exec("CREATE TABLE currencies (id INTEGER PRIMARY KEY AUTOINCREMENT,c BLOB);").Error; err != nil {
			t.Fatal(err)
		}
	} else {
		if err := ss.db.Exec("CREATE TABLE currencies (id INT AUTO_INCREMENT PRIMARY KEY, c BLOB);").Error; err != nil {
			t.Fatal(err)
		}
	}

	// insert currencies in random order
	if err := ss.db.Exec("INSERT INTO currencies (c) VALUES (?),(?),(?);", bCurrency(types.MaxCurrency), bCurrency(types.NewCurrency64(1)), bCurrency(types.ZeroCurrency)).Error; err != nil {
		t.Fatal(err)
	}

	// fetch currencies and assert they're sorted
	var currencies []bCurrency
	if err := ss.db.Raw(`SELECT c FROM currencies ORDER BY c ASC`).Scan(&currencies).Error; err != nil {
		t.Fatal(err)
	} else if !sort.SliceIsSorted(currencies, func(i, j int) bool {
		return types.Currency(currencies[i]).Cmp(types.Currency(currencies[j])) < 0
	}) {
		t.Fatal("currencies not sorted", currencies)
	}

	// convenience variables
	c0 := currencies[0]
	c1 := currencies[1]
	cM := currencies[2]

	tests := []struct {
		a   bCurrency
		b   bCurrency
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
		query := fmt.Sprintf("SELECT ? %s ?", test.cmp)
		if !isSQLite(ss.db) {
			query = strings.Replace(query, "?", "HEX(?)", -1)
		}
		if err := ss.db.Raw(query, test.a, test.b).Scan(&result).Error; err != nil {
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

// TODO: can't .Take() a single merkleProof
func TestTypeMerkleProof(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	// prepare the table
	if isSQLite(ss.db) {
		if err := ss.db.Exec("CREATE TABLE merkle_proofs (id INTEGER PRIMARY KEY AUTOINCREMENT,merkle_proof BLOB);").Error; err != nil {
			t.Fatal(err)
		}
	} else {
		ss.db.Exec("DROP TABLE IF EXISTS merkle_proofs;")
		if err := ss.db.Exec("CREATE TABLE merkle_proofs (id INT AUTO_INCREMENT PRIMARY KEY, merkle_proof BLOB);").Error; err != nil {
			t.Fatal(err)
		}
	}

	// insert merkle proof
	if err := ss.db.Exec("INSERT INTO merkle_proofs (id, merkle_proof) VALUES (1,?),(2,?);", merkleProof([]types.Hash256{}), merkleProof([]types.Hash256{{2}, {1}, {3}})).Error; err != nil {
		t.Fatal(err)
	}

	// fetch invalid proof
	var proofs []merkleProof
	if err := ss.db.
		Raw(`SELECT merkle_proof FROM merkle_proofs WHERE id=1`).
		Take(&proofs).
		Error; err == nil || !strings.Contains(err.Error(), "no bytes found") {
		t.Fatalf("expected error 'no bytes found', got '%v'", err)
	}

	// fetch valid proof
	if err := ss.db.
		Raw(`SELECT merkle_proof FROM merkle_proofs WHERE id=2`).
		Take(&proofs).
		Error; err != nil {
		t.Fatalf("unexpected error '%v'", err)
	} else if proofs[0][0] != (types.Hash256{2}) || proofs[0][1] != (types.Hash256{1}) || proofs[0][2] != (types.Hash256{3}) {
		t.Fatalf("unexpected proof %+v", proofs[0])
	}
}
