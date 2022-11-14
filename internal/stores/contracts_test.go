package stores

import (
	"encoding/hex"
	"testing"

	"lukechampine.com/frand"
)

// TestSQLContractStore tests SQLContractStore functionality.
func TestSQLContractStore(t *testing.T) {
	dbName := hex.EncodeToString(frand.Bytes(32)) // random name for db

	conn := NewEphemeralSQLiteConnection(dbName)
	_, err := NewSQLContractStore(conn, true)
	if err != nil {
		t.Fatal(err)
	}
}
