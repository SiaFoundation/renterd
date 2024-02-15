package stores

import (
	"fmt"

	gormigrate "github.com/go-gormigrate/gormigrate/v2"
	"go.sia.tech/renterd/api"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// initSchema is executed only on a clean database. Otherwise the individual
// migrations are executed.
func initSchema(db *gorm.DB, metrics bool, logger *zap.SugaredLogger) gormigrate.InitSchemaFunc {
	return func(tx *gorm.DB) error {
		if metrics {
			logger.Info("initializing metrics schema")
		} else {
			logger.Info("initializing schema")
		}

		// build filename
		filename := "schema"
		err := execSQLFile(tx, metrics, filename)
		if err != nil {
			return fmt.Errorf("failed to init schema: %w", err)
		}

		// add default bucket.
		if !metrics {
			if err := tx.Create(&dbBucket{
				Name: api.DefaultBucketName,
			}).Error; err != nil {
				return fmt.Errorf("failed to create default bucket: %v", err)
			}
		}

		logger.Info("initialization complete")
		return nil
	}
}

func performMigration(db *gorm.DB, name string, metrics bool, logger *zap.SugaredLogger) error {
	logger.Infof("performing migration %s", name)

	// build filename
	filename := fmt.Sprintf("migration_%s", name)

	// execute migration
	err := execSQLFile(db, metrics, filename)
	if err != nil {
		return fmt.Errorf("migration %s failed: %w", name, err)
	}

	logger.Infof("migration %s complete", name)
	return nil
}

func execSQLFile(db *gorm.DB, metrics bool, filename string) error {
	// build path
	folder := "main"
	if metrics {
		folder = "metrics"
	}
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
