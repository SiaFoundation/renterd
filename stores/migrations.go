package stores

import (
	"context"
	"fmt"

	"github.com/go-gormigrate/gormigrate/v2"
	"gorm.io/gorm"
	glogger "gorm.io/gorm/logger"
)

var (
	tables = []interface{}{
		// bus.MetadataStore tables
		&dbArchivedContract{},
		&dbContract{},
		&dbContractSet{},
		&dbObject{},
		&dbSlab{},
		&dbSector{},
		&dbSlice{},
		&dbSlabBuffer{},

		// bus.HostDB tables
		&dbAnnouncement{},
		&dbConsensusInfo{},
		&dbHost{},
		&dbInteraction{},
		&dbAllowlistEntry{},
		&dbBlocklistEntry{},

		// wallet tables
		&dbSiacoinElement{},
		&dbTransaction{},

		// bus.SettingStore tables
		&dbSetting{},

		// bus.EphemeralAccountStore tables
		&dbAccount{},

		// bus.AutopilotStore tables
		&dbAutopilot{},
	}
)

type dbHostBlocklistEntryHost struct {
	DBBlocklistEntryID uint8 `gorm:"primarykey;column:db_blocklist_entry_id"`
	DBHostID           uint8 `gorm:"primarykey;index:idx_db_host_id;column:db_host_id"`
}

func (dbHostBlocklistEntryHost) TableName() string {
	return "host_blocklist_entry_hosts"
}

// migrateShards performs the migrations necessary for removing the 'shards'
// table.
func migrateShards(ctx context.Context, db *gorm.DB, l glogger.Interface) error {
	m := db.Migrator()
	logger := l.LogMode(glogger.Info)

	// add columns
	if !m.HasColumn(&dbSlice{}, "db_slab_id") {
		logger.Info(ctx, "adding column db_slab_id to table 'slices'")
		if err := m.AddColumn(&dbSlice{}, "db_slab_id"); err != nil {
			return err
		}
		logger.Info(ctx, "done adding column db_slab_id to table 'slices'")
	}
	if !m.HasColumn(&dbSector{}, "db_slab_id") {
		logger.Info(ctx, "adding column db_slab_id to table 'sectors'")
		if err := m.AddColumn(&dbSector{}, "db_slab_id"); err != nil {
			return err
		}
		logger.Info(ctx, "done adding column db_slab_id to table 'sectors'")
	}

	// populate new columns
	var err error
	if m.HasColumn(&dbSlab{}, "db_slice_id") {
		logger.Info(ctx, "populating column 'db_slab_id' in table 'slices'")
		if isSQLite(db) {
			err = db.Exec(`UPDATE slices SET db_slab_id = (SELECT slabs.id FROM slabs WHERE slabs.db_slice_id = slices.id)`).Error
		} else {
			err = db.Exec(`UPDATE slices sli
			INNER JOIN slabs sla ON sli.id=sla.db_slice_id
			SET sli.db_slab_id=sla.id`).Error
		}
		if err != nil {
			return err
		}
		logger.Info(ctx, "done populating column 'db_slab_id' in table 'slices'")
	}
	logger.Info(ctx, "populating column 'db_slab_id' in table 'sectors'")
	if isSQLite(db) {
		err = db.Exec(`UPDATE sectors SET db_slab_id = (SELECT shards.db_slab_id FROM shards WHERE shards.db_sector_id = sectors.id)`).Error
	} else {
		err = db.Exec(`UPDATE sectors sec
			INNER JOIN shards sha ON sec.id=sha.db_sector_id
			SET sec.db_slab_id=sha.db_slab_id`).Error
	}
	if err != nil {
		return err
	}
	logger.Info(ctx, "done populating column 'db_slab_id' in table 'sectors'")

	// drop column db_slice_id from slabs
	logger.Info(ctx, "dropping constraint 'fk_slices_slab' from table 'slabs'")
	if err := m.DropConstraint(&dbSlab{}, "fk_slices_slab"); err != nil {
		return err
	}
	logger.Info(ctx, "done dropping constraint 'fk_slices_slab' from table 'slabs'")
	logger.Info(ctx, "dropping column 'db_slice_id' from table 'slabs'")
	if err := m.DropColumn(&dbSlab{}, "db_slice_id"); err != nil {
		return err
	}
	logger.Info(ctx, "done dropping column 'db_slice_id' from table 'slabs'")

	// delete any sectors that are not referenced by a slab
	logger.Info(ctx, "pruning dangling sectors")
	if err := db.Exec(`DELETE FROM sectors WHERE db_slab_id IS NULL`).Error; err != nil {
		return err
	}
	logger.Info(ctx, "done pruning dangling sectors")

	// drop table shards
	logger.Info(ctx, "dropping table 'shards'")
	if err := m.DropTable("shards"); err != nil {
		return err
	}
	logger.Info(ctx, "done dropping table 'shards'")
	return nil
}

func performMigrations(db *gorm.DB, logger glogger.Interface) error {
	migrations := []*gormigrate.Migration{
		{
			ID: "00001_gormigrate",
			Migrate: func(tx *gorm.DB) error {
				return performMigration00001_gormigrate(tx, logger)
			},
			Rollback: nil,
		},
		{
			ID: "00002_dropconstraintslabcsid",
			Migrate: func(tx *gorm.DB) error {
				return performMigration00002_dropconstraintslabcsid(tx, logger)
			},
			Rollback: nil,
		},
		{
			ID: "00003_healthcache",
			Migrate: func(tx *gorm.DB) error {
				return performMigration00003_healthcache(tx, logger)
			},
			Rollback: nil,
		},
		{
			ID: "00004_objectID_collation",
			Migrate: func(tx *gorm.DB) error {
				return performMigration00004_objectID_collation(tx, logger)
			},
			Rollback: nil,
		},
		{
			ID: "00005_contractspending",
			Migrate: func(tx *gorm.DB) error {
				return performMigration00005_contractspending(tx, logger)
			},
			Rollback: nil,
		},
	}

	// Create migrator.
	m := gormigrate.New(db, gormigrate.DefaultOptions, migrations)

	// Set init function. We only do this if the consenus info table doesn't
	// exist. Because we haven't always been using gormigrate so we want to run
	// all migrations instead of InitSchema the first time if it seems like we
	// are not starting with a clean db.
	if !db.Migrator().HasTable(&dbConsensusInfo{}) {
		m.InitSchema(initSchema)
	}

	// Perform migrations.
	if err := m.Migrate(); err != nil {
		return fmt.Errorf("failed to migrate: %v", err)
	}
	return nil
}

// initSchema is executed only on a clean database. Otherwise the individual
// migrations are executed.
func initSchema(tx *gorm.DB) error {
	err := tx.AutoMigrate(tables...)
	if err != nil {
		return err
	}
	// Change the object_id colum to use case sensitive collation.
	if !isSQLite(tx) {
		return tx.Exec("ALTER TABLE objects MODIFY COLUMN object_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;").Error
	}
	return nil
}

// performMigration00001_gormigrate performs the first migration before
// introducing gormigrate.
func performMigration00001_gormigrate(txn *gorm.DB, logger glogger.Interface) error {
	ctx := context.Background()
	m := txn.Migrator()

	// Perform pre-auto migrations
	//
	// If the consensus info table is missing the height column, drop it to
	// force a resync.
	if m.HasTable(&dbConsensusInfo{}) && !m.HasColumn(&dbConsensusInfo{}, "height") {
		if err := m.DropTable(&dbConsensusInfo{}); err != nil {
			return err
		}
	}
	// If the shards table exists, we add the db_slab_id column to slices and
	// sectors before then dropping the shards table as well as the db_slice_id
	// column from the slabs table.
	if m.HasTable("shards") {
		logger.Info(ctx, "'shards' table detected, starting migration")
		if err := migrateShards(ctx, txn, logger); err != nil {
			return fmt.Errorf("failed to migrate 'shards' table: %w", err)
		}
		logger.Info(ctx, "finished migrating 'shards' table")
	}
	fillSlabContractSetID := !m.HasColumn(&dbSlab{}, "db_contract_set_id")

	// Drop owner column from accounts table.
	if m.HasColumn(&dbAccount{}, "owner") {
		if err := m.DropColumn(&dbAccount{}, "owner"); err != nil {
			return err
		}
	}

	// Drop constraint on Slices to avoid dropping slabs and sectors.
	if m.HasConstraint(&dbSlab{}, "Slices") {
		if err := m.DropConstraint(&dbSlab{}, "Slices"); err != nil {
			return fmt.Errorf("failed to drop constraint 'Slices' from table 'slabs': %w", err)
		}
	}
	if m.HasConstraint(&dbSlab{}, "Shards") {
		if err := m.DropConstraint(&dbSlab{}, "Shards"); err != nil {
			return fmt.Errorf("failed to drop constraint 'Shards' from table 'slabs': %w", err)
		}
	}

	// Perform auto migrations.
	if err := txn.AutoMigrate(tables...); err != nil {
		return err
	}

	// Re-add both constraints.
	if !m.HasConstraint(&dbSlab{}, "Slices") {
		if err := m.CreateConstraint(&dbSlab{}, "Slices"); err != nil {
			return fmt.Errorf("failed to add constraint 'Slices' to table 'slabs': %w", err)
		}
	}
	if !m.HasConstraint(&dbSlab{}, "Shards") {
		if err := m.CreateConstraint(&dbSlab{}, "Shards"); err != nil {
			return fmt.Errorf("failed to add constraint 'Shards' to table 'slabs': %w", err)
		}
	}

	if fillSlabContractSetID {
		// Compat code for databases that don't have the db_contract_set_id. We
		// have to assign all slabs to a contract set, if we only have one
		// contract set we use that one but if we have 0 or more than 1 we
		// create a migration set.
		var sets []dbContractSet
		if err := txn.Find(&sets).Error; err != nil {
			return fmt.Errorf("failed to retrieve contract sets from the database: %w", err)
		}

		logFn := logger.Info
		var cs dbContractSet
		if len(sets) != 1 {
			cs = dbContractSet{Name: "migration-slab-contract-set-id"}
			if err := txn.FirstOrCreate(&cs).Error; err != nil {
				return fmt.Errorf("failed to create migration set: %w", err)
			}
			logFn = logger.Warn // warn to alert the user of the migration set
		} else {
			cs = sets[0]
		}

		logFn(ctx, fmt.Sprintf("slabs table is missing 'db_contract_set_id' column - adding it and associating slabs with the contract set '%s'", cs.Name))
		if err := txn.Exec("UPDATE slabs SET db_contract_set_id = ? WHERE slabs.db_contract_set_id IS NULL", cs.ID).Error; err != nil {
			return fmt.Errorf("failed to update slab contract set ID: %w", err)
		}
	}

	// Perform post-auto migrations.
	if err := m.DropTable("host_sectors"); err != nil {
		return err
	}
	if !m.HasIndex(&dbHostBlocklistEntryHost{}, "DBHostID") {
		if err := m.CreateIndex(&dbHostBlocklistEntryHost{}, "DBHostID"); err != nil {
			return err
		}
	}
	return nil
}

func performMigration00002_dropconstraintslabcsid(txn *gorm.DB, logger glogger.Interface) error {
	ctx := context.Background()
	m := txn.Migrator()

	// Disable foreign keys in SQLite to avoid issues with updating constraints.
	if isSQLite(txn) {
		fmt.Println("DISABLING constraints")
		if err := txn.Exec(`PRAGMA foreign_keys = 0`).Error; err != nil {
			return err
		}
	}

	// Drop the constraint on DBContractSet.
	if m.HasConstraint(&dbSlab{}, "DBContractSet") {
		logger.Info(ctx, "migration 00002_dropconstraintslabcsid: dropping constraint on DBContractSet")
		if err := m.DropConstraint(&dbSlab{}, "DBContractSet"); err != nil {
			return fmt.Errorf("failed to drop constraint 'DBContractSet' from table 'slabs': %w", err)
		}
	}

	// Perform auto migrations.
	if err := txn.AutoMigrate(tables...); err != nil {
		return err
	}

	// Add constraint back.
	if !m.HasConstraint(&dbSlab{}, "DBContractSet") {
		logger.Info(ctx, "migration 00002_dropconstraintslabcsid: adding constraint on DBContractSet")
		if err := m.CreateConstraint(&dbSlab{}, "DBContractSet"); err != nil {
			return fmt.Errorf("failed to add constraint 'DBContractSet' to table 'slabs': %w", err)
		}
	}

	// Enable foreign keys again.
	if isSQLite(txn) {
		if err := txn.Exec(`PRAGMA foreign_keys = 1`).Error; err != nil {
			return err
		}
		if err := txn.Exec(`PRAGMA foreign_key_check(slabs)`).Error; err != nil {
			return err
		}
	}
	return nil
}

func performMigration00003_healthcache(txn *gorm.DB, logger glogger.Interface) error {
	logger.Info(context.Background(), "performing migration 00003_healthcache")
	if !txn.Migrator().HasColumn(&dbSlab{}, "health") {
		if err := txn.Migrator().AddColumn(&dbSlab{}, "health"); err != nil {
			return err
		}
	}
	logger.Info(context.Background(), "migration 00003_healthcheck complete")
	return nil
}

func performMigration00004_objectID_collation(txn *gorm.DB, logger glogger.Interface) error {
	logger.Info(context.Background(), "performing migration 00004_objectID_collation")
	if !isSQLite(txn) {
		err := txn.Exec("ALTER TABLE objects MODIFY COLUMN object_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;").Error
		if err != nil {
			return err
		}
	}
	logger.Info(context.Background(), "migration 00004_objectID_collation complete")
	return nil
}

func performMigration00005_contractspending(txn *gorm.DB, logger glogger.Interface) error {
	logger.Info(context.Background(), "performing migration 00005_contractspending")
	if !txn.Migrator().HasColumn(&dbContract{}, "delete_spending") {
		if err := txn.Migrator().AddColumn(&dbContract{}, "delete_spending"); err != nil {
			return err
		}
	}
	if !txn.Migrator().HasColumn(&dbContract{}, "list_spending") {
		if err := txn.Migrator().AddColumn(&dbContract{}, "list_spending"); err != nil {
			return err
		}
	}
	logger.Info(context.Background(), "migration 00005_contractspending complete")
	return nil
}
