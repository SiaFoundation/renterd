package stores

import (
	"context"
	"testing"
)

// TestSQLSettingStore tests the bus.SettingStore methods on the SQLSettingStore.
func TestSQLSettingStore(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	// add a setting
	if err := ss.UpdateSetting(context.Background(), "foo", "bar"); err != nil {
		t.Fatal(err)
	}

	// assert we can query the setting by key
	if value, err := ss.Setting(context.Background(), "foo"); err != nil {
		t.Fatal(err)
	} else if value != "bar" {
		t.Fatalf("unexpected value, %s != 'bar'", value)
	}

	// assert we can update the setting
	if err := ss.UpdateSetting(context.Background(), "foo", "barbaz"); err != nil {
		t.Fatal(err)
	} else if value, err := ss.Setting(context.Background(), "foo"); err != nil {
		t.Fatal(err)
	} else if value != "barbaz" {
		t.Fatalf("unexpected value, %s != 'barbaz'", value)
	}
}
