package stores

import (
	"context"
	"errors"
	"testing"

	"go.sia.tech/renterd/api"
)

// TestSQLSettingStore tests the bus.SettingStore methods on the SQLSettingStore.
func TestSQLSettingStore(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	// assert there are no settings
	ctx := context.Background()
	if keys, err := ss.Settings(ctx); err != nil {
		t.Fatal(err)
	} else if len(keys) != 0 {
		t.Fatalf("unexpected number of settings, %v != 0", len(keys))
	}

	// add a setting
	if err := ss.UpdateSetting(ctx, "foo", "bar"); err != nil {
		t.Fatal(err)
	}

	// assert it's returned
	if keys, err := ss.Settings(ctx); err != nil {
		t.Fatal(err)
	} else if len(keys) != 1 {
		t.Fatalf("unexpected number of settings, %v != 1", len(keys))
	} else if keys[0] != "foo" {
		t.Fatalf("unexpected key, %s != 'foo'", keys[0])
	}

	// assert we can query the setting by key
	if value, err := ss.Setting(ctx, "foo"); err != nil {
		t.Fatal(err)
	} else if value != "bar" {
		t.Fatalf("unexpected value, %s != 'bar'", value)
	}

	// assert we can update the setting
	if err := ss.UpdateSetting(ctx, "foo", "barbaz"); err != nil {
		t.Fatal(err)
	} else if value, err := ss.Setting(ctx, "foo"); err != nil {
		t.Fatal(err)
	} else if value != "barbaz" {
		t.Fatalf("unexpected value, %s != 'barbaz'", value)
	}

	// delete the setting
	if err := ss.DeleteSetting(ctx, "foo"); err != nil {
		t.Fatal(err)
	} else if _, err := ss.Setting(ctx, "foo"); !errors.Is(err, api.ErrSettingNotFound) {
		t.Fatal("should fail with gorm.ErrRecordNotFound", err)
	} else if keys, err := ss.Settings(ctx); err != nil {
		t.Fatal(err)
	} else if len(keys) != 0 {
		t.Fatalf("unexpected number of settings, %v != 0", len(keys))
	}
}
