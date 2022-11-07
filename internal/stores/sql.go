package stores

import (
	"fmt"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// NewEphemeralSQLiteConnection creates a connection to an in-memory SQLite DB.
// NOTE:
func NewEphemeralSQLiteConnection(name string) gorm.Dialector {
	return sqlite.Open(fmt.Sprintf("file:%s?mode=memory&cache=shared", name))
}

// NewSQLiteConnection opens a sqlite db at the given path.
func NewSQLiteConnection(path string) gorm.Dialector {
	return sqlite.Open(path)
}
