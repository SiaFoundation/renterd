package stores

import (
	"encoding/hex"

	"go.sia.tech/siad/modules"
	"lukechampine.com/frand"
)

// newTestSQLStore creates a new SQLStore for testing.
func newTestSQLStore() (*SQLStore, string, modules.ConsensusChangeID, error) {
	dbName := hex.EncodeToString(frand.Bytes(32)) // random name for db
	conn := NewEphemeralSQLiteConnection(dbName)
	sqlStore, ccid, err := NewSQLStore(conn, true)
	if err != nil {
		return nil, "", modules.ConsensusChangeID{}, err
	}
	return sqlStore, dbName, ccid, nil
}
