package stores

import "testing"

func TestContractSetMetrics(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()
}
