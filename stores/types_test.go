package stores

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
)

func TestTypeSetting(t *testing.T) {
	s1 := setting("some setting")
	s2 := setting("v4Keypairs")

	if s1.String() != "some setting" {
		t.Fatal("unexpected string")
	} else if s2.String() != "*****" {
		t.Fatal("unexpected string")
	}
}

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
			query = strings.ReplaceAll(query, "?", "HEX(?)")
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
	mp1 := merkleProof{proof: []types.Hash256{{3}, {1}, {2}}}
	mp2 := merkleProof{proof: []types.Hash256{{4}}}
	if err := ss.db.Exec("INSERT INTO merkle_proofs (merkle_proof) VALUES (?), (?);", mp1, mp2).Error; err != nil {
		t.Fatal(err)
	}

	// fetch first proof
	var first merkleProof
	if err := ss.db.
		Raw(`SELECT merkle_proof FROM merkle_proofs`).
		Take(&first).
		Error; err != nil {
		t.Fatal(err)
	} else if first.proof[0] != (types.Hash256{3}) || first.proof[1] != (types.Hash256{1}) || first.proof[2] != (types.Hash256{2}) {
		t.Fatalf("unexpected proof %+v", first)
	}

	// fetch both proofs
	var both []merkleProof
	if err := ss.db.
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

func TestTypeEventData(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	// prepare the table
	if isSQLite(ss.db) {
		if err := ss.db.Exec("CREATE TABLE `event_datas` (`id` integer PRIMARY KEY AUTOINCREMENT,`data` text);").Error; err != nil {
			t.Fatal(err)
		}
	} else {
		ss.db.Exec("DROP TABLE IF EXISTS event_datas;")
		if err := ss.db.Exec("CREATE TABLE `event_datas` (`id` bigint unsigned NOT NULL AUTO_INCREMENT,`data` longtext, PRIMARY KEY (`id`));").Error; err != nil {
			t.Fatal(err)
		}
	}

	// prepare event data
	sk := types.GeneratePrivateKey()
	txn := wallet.EventV1Transaction{
		SiacoinOutputs: []types.SiacoinOutput{
			{
				Value:   types.Siacoins(1),
				Address: types.StandardUnlockHash(sk.PublicKey()),
			},
		},
	}
	insert, err := toEventData(txn)
	if err != nil {
		t.Fatal(err)
	}

	// insert event data
	if err := ss.db.Exec("INSERT INTO event_datas (data) VALUES (?);", insert).Error; err != nil {
		t.Fatal(err)
	}

	// fetch event data & compare
	var data eventData
	if err := ss.db.
		Raw(`SELECT data FROM event_datas LIMIT 1`).
		Scan(&data).
		Error; err != nil {
		t.Fatal(err)
	}

	got, err := data.decodeToType(wallet.EventTypeV1Transaction)
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(got, txn) {
		t.Fatal("unexpected event data", got, txn)
	}
}
