package stores

import (
	"fmt"
	"reflect"
	"strings"

	"gorm.io/gorm"
)

func detectMissingIndices(tx *gorm.DB, f func(dst interface{}, name string)) {
	for _, table := range tables {
		detectMissingIndicesOnType(tx, table, reflect.TypeOf(table), f)
	}
}

func detectMissingIndicesOnType(tx *gorm.DB, table interface{}, t reflect.Type, f func(dst interface{}, name string)) {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if field.Anonymous {
			detectMissingIndicesOnType(tx, table, field.Type, f)
			continue
		}
		if !strings.Contains(field.Tag.Get("gorm"), "index") {
			continue // no index tag
		}
		if !tx.Migrator().HasIndex(table, field.Name) {
			f(table, field.Name)
		}
	}
}

func setupJoinTables(tx *gorm.DB) error {
	jointables := []struct {
		model     interface{}
		joinTable interface{ TableName() string }
		field     string
	}{
		{
			&dbAllowlistEntry{},
			&dbHostAllowlistEntryHost{},
			"Hosts",
		},
		{
			&dbBlocklistEntry{},
			&dbHostBlocklistEntryHost{},
			"Hosts",
		},
		{
			&dbSector{},
			&dbContractSector{},
			"Contracts",
		},
		{
			&dbContractSet{},
			&dbContractSetContract{},
			"Contracts",
		},
	}
	for _, t := range jointables {
		if err := tx.SetupJoinTable(t.model, t.field, t.joinTable); err != nil {
			return fmt.Errorf("failed to setup join table '%s': %w", t.joinTable.TableName(), err)
		}
	}
	return nil
}
