package stores

import (
	"encoding/hex"
	"time"

	"go.sia.tech/siad/modules"
	"lukechampine.com/frand"
)

const testPersistInterval = time.Second

// newTestSQLStore creates a new SQLStore for testing.
func newTestSQLStore() (*SQLStore, string, modules.ConsensusChangeID, error) {
	dbName := hex.EncodeToString(frand.Bytes(32)) // random name for db
	conn := NewEphemeralSQLiteConnection(dbName)
	sqlStore, ccid, err := NewSQLStore(conn, false, true, time.Second)
	if err != nil {
		return nil, "", modules.ConsensusChangeID{}, err
	}
	return sqlStore, dbName, ccid, nil
}
