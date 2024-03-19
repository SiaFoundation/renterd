package stores

import (
	"fmt"

	gormigrate "github.com/go-gormigrate/gormigrate/v2"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// initSchema is executed only on a clean database. Otherwise the individual
// migrations are executed.
func initSchema(name string, logger *zap.SugaredLogger) gormigrate.InitSchemaFunc {
	return func(tx *gorm.DB) error {
		logger.Infof("initializing '%s' schema", name)

		// init schema
		err := execSQLFile(tx, name, "schema")
		if err != nil {
			return fmt.Errorf("failed to init schema: %w", err)
		}

		logger.Info("initialization complete")
		return nil
	}
}

func performMigration(db *gorm.DB, kind, migration string, logger *zap.SugaredLogger) error {
	logger.Infof("performing %s migration '%s'", kind, migration)

	// execute migration
	err := execSQLFile(db, kind, fmt.Sprintf("migration_%s", migration))
	if err != nil {
		return fmt.Errorf("migration '%s' failed: %w", migration, err)
	}

	logger.Infof("migration '%s' complete", migration)
	return nil
}

func execSQLFile(db *gorm.DB, folder, filename string) error {
	// build path
	protocol := "mysql"
	if isSQLite(db) {
		protocol = "sqlite"
	}
	path := fmt.Sprintf("migrations/%s/%s/%s.sql", protocol, folder, filename)

	// read file
	file, err := migrations.ReadFile(path)
	if err != nil {
		return err
	}

	// execute it
	return db.Exec(string(file)).Error
}
