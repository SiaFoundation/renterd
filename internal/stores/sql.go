package stores

import (
	"fmt"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// NewEphemeralSQLiteConnection creates a connection to an in-memory SQLite DB.
// NOTE: Use simple names such as a random hex identifier or the filepath.Base
// of a test's name. Certain symbols will break the cfg string and cause a file
// to be created on disk.
func NewEphemeralSQLiteConnection(name string) gorm.Dialector {
	return sqlite.Open(fmt.Sprintf("file:%s?mode=memory&cache=shared", name))
}

// NewSQLiteConnection opens a sqlite db at the given path.
func NewSQLiteConnection(path string) gorm.Dialector {
	return sqlite.Open(path)
}
