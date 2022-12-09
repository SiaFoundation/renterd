package stores

import (
	"testing"
)

// TestSQLSettingStore tests the bus.SettingStore methods on the SQLSettingStore.
func TestSQLSettingStore(t *testing.T) {
	ss, _, _, err := newTestSQLStore()
	if err != nil {
		t.Fatal(err)
	}

	// assert there are no settings
	if keys, err := ss.Settings(); err != nil {
		t.Fatal(err)
	} else if len(keys) != 0 {
		t.Fatalf("unexpected number of settings, %v != 0", len(keys))
	}

	// add a setting
	if err := ss.UpdateSetting("foo", "bar"); err != nil {
		t.Fatal(err)
	}

	// assert it's returned
	if keys, err := ss.Settings(); err != nil {
		t.Fatal(err)
	} else if len(keys) != 1 {
		t.Fatalf("unexpected number of settings, %v != 1", len(keys))
	} else if keys[0] != "foo" {
		t.Fatalf("unexpected key, %s != 'foo'", keys[0])
	}

	// assert we can query the setting by key
	if value, err := ss.Setting("foo"); err != nil {
		t.Fatal(err)
	} else if value != "bar" {
		t.Fatalf("unexpected value, %s != 'bar'", value)
	}

	// assert we can update the setting
	if err := ss.UpdateSetting("foo", "barbaz"); err != nil {
		t.Fatal(err)
	} else if value, err := ss.Setting("foo"); err != nil {
		t.Fatal(err)
	} else if value != "barbaz" {
		t.Fatalf("unexpected value, %s != 'barbaz'", value)
	}
}
