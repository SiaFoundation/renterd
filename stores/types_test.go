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
	if _, err := ss.ExecDBSpecific(
		"CREATE TABLE currencies (id INTEGER PRIMARY KEY AUTOINCREMENT,c BLOB);", // sqlite
		"CREATE TABLE currencies (id INT AUTO_INCREMENT PRIMARY KEY, c BLOB);",   // mysql
	); err != nil {
		t.Fatal(err)
	}

	// insert currencies in random order
	if err := ss.gormDB.Exec("INSERT INTO currencies (c) VALUES (?),(?),(?);", bCurrency(types.MaxCurrency), bCurrency(types.NewCurrency64(1)), bCurrency(types.ZeroCurrency)).Error; err != nil {
		t.Fatal(err)
	}

	// fetch currencies and assert they're sorted
	var currencies []bCurrency
	if err := ss.gormDB.Raw(`SELECT c FROM currencies ORDER BY c ASC`).Scan(&currencies).Error; err != nil {
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
		if !isSQLite(ss.gormDB) {
			query = strings.ReplaceAll(query, "?", "HEX(?)")
		}
		if err := ss.gormDB.Raw(query, test.a, test.b).Scan(&result).Error; err != nil {
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
	if isSQLite(ss.gormDB) {
		if err := ss.gormDB.Exec("CREATE TABLE merkle_proofs (id INTEGER PRIMARY KEY AUTOINCREMENT,merkle_proof BLOB);").Error; err != nil {
			t.Fatal(err)
		}
	} else {
		ss.gormDB.Exec("DROP TABLE IF EXISTS merkle_proofs;")
		if err := ss.gormDB.Exec("CREATE TABLE merkle_proofs (id INT AUTO_INCREMENT PRIMARY KEY, merkle_proof BLOB);").Error; err != nil {
			t.Fatal(err)
		}
	}

	// insert merkle proof
	mp1 := merkleProof{proof: []types.Hash256{{3}, {1}, {2}}}
	mp2 := merkleProof{proof: []types.Hash256{{4}}}
	if err := ss.gormDB.Exec("INSERT INTO merkle_proofs (merkle_proof) VALUES (?), (?);", mp1, mp2).Error; err != nil {
		t.Fatal(err)
	}

	// fetch first proof
	var first merkleProof
	if err := ss.gormDB.
		Raw(`SELECT merkle_proof FROM merkle_proofs`).
		Take(&first).
		Error; err != nil {
		t.Fatal(err)
	} else if first.proof[0] != (types.Hash256{3}) || first.proof[1] != (types.Hash256{1}) || first.proof[2] != (types.Hash256{2}) {
		t.Fatalf("unexpected proof %+v", first)
	}

	// fetch both proofs
	var both []merkleProof
	if err := ss.gormDB.
		Raw(`SELECT merkle_proof FROM merkle_proofs`).
		Scan(&both).
		Error; err != nil {
		t.Fatal(err)
	} else if len(both) != 2 {
		t.Fatalf("unexpected number of proofs: %d", len(both))
	} else if both[1].proof[0] != (types.Hash256{4}) {
		t.Fatalf("unexpected proof %+v", both)
	}
}
