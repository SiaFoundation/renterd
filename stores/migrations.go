package stores

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/go-gormigrate/gormigrate/v2"
	"go.sia.tech/renterd/api"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

var (
	tables = []interface{}{
		// bus.MetadataStore tables
		&dbArchivedContract{},
		&dbContract{},
		&dbContractSet{},
		&dbObject{},
		&dbMultipartUpload{},
		&dbBucket{},
		&dbBufferedSlab{},
		&dbSlab{},
		&dbSector{},
		&dbSlice{},

		// bus.HostDB tables
		&dbAnnouncement{},
		&dbConsensusInfo{},
		&dbHost{},
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

		// webhooks.WebhookStore tables
		&dbWebhook{},
	}
)

// migrateShards performs the migrations necessary for removing the 'shards'
// table.
func migrateShards(ctx context.Context, db *gorm.DB, logger *zap.SugaredLogger) error {
	m := db.Migrator()

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

func performMigrations(db *gorm.DB, logger *zap.SugaredLogger) error {
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
			ID: "00005_uploadPacking",
			Migrate: func(tx *gorm.DB) error {
				return performMigration00005_uploadPacking(tx, logger)
			},
			Rollback: nil,
		},
		{
			ID: "00006_contractspending",
			Migrate: func(tx *gorm.DB) error {
				return performMigration00006_contractspending(tx, logger)
			},
			Rollback: nil,
		},
		{
			ID: "00007_contractspending",
			Migrate: func(tx *gorm.DB) error {
				return performMigration00007_archivedcontractspending(tx, logger)
			},
			Rollback: nil,
		},
		{
			ID: "00008_jointableindices",
			Migrate: func(tx *gorm.DB) error {
				return performMigration00008_jointableindices(tx, logger)
			},
			Rollback: nil,
		},
		{
			ID: "00009_dropInteractions",
			Migrate: func(tx *gorm.DB) error {
				return performMigration00009_dropInteractions(tx, logger)
			},
			Rollback: nil,
		},
		{
			ID: "00010_distinctcontractsector",
			Migrate: func(tx *gorm.DB) error {
				return performMigration00010_distinctcontractsector(tx, logger)
			},
			Rollback: nil,
		},
		{
			ID: "00011_healthValidColumn",
			Migrate: func(tx *gorm.DB) error {
				return performMigration00011_healthValidColumn(tx, logger)
			},
		},
		{
			ID: "00012_webhooks",
			Migrate: func(tx *gorm.DB) error {
				return performMigration00012_webhooks(tx, logger)
			},
		},
		{
			ID: "00013_uploadPackingOptimisations",
			Migrate: func(tx *gorm.DB) error {
				return performMigration00013_uploadPackingOptimisations(tx, logger)
			},
		},
		{
			ID: "00014_buckets",
			Migrate: func(tx *gorm.DB) error {
				return performMigration00014_buckets(tx, logger)
			},
		},
		{
			ID: "00015_multipartUploads",
			Migrate: func(tx *gorm.DB) error {
				return performMigration00015_multipartUploads(tx, logger)
			},
		},
		{
			ID: "00016_bucketPolicy",
			Migrate: func(tx *gorm.DB) error {
				return performMigration00016_bucketPolicy(tx, logger)
			},
		},
		{
			ID: "00017_mimetype",
			Migrate: func(tx *gorm.DB) error {
				return performMigration00017_mimetype(tx, logger)
			},
		},
		{
			ID: "00018_etags",
			Migrate: func(tx *gorm.DB) error {
				return performMigration00018_etags(tx, logger)
			},
		},
		{
			ID: "00019_accounts_shutdown",
			Migrate: func(tx *gorm.DB) error {
				return performMigration00019_accountsShutdown(tx, logger)
			},
		},
		{
			ID: "00020_missingIndices",
			Migrate: func(tx *gorm.DB) error {
				return performMigration00020_missingIndices(tx, logger)
			},
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
	// Setup join tables.
	err := setupJoinTables(tx)
	if err != nil {
		return fmt.Errorf("failed to setup join tables: %w", err)
	}

	// Run auto migrations.
	err = tx.AutoMigrate(tables...)
	if err != nil {
		return fmt.Errorf("failed to init schema: %w", err)
	}

	// Change the collation of columns that we need to be case sensitive.
	if !isSQLite(tx) {
		err = tx.Exec("ALTER TABLE objects MODIFY COLUMN object_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;").Error
		if err != nil {
			return fmt.Errorf("failed to change object_id collation: %w", err)
		}
		err = tx.Exec("ALTER TABLE buckets MODIFY COLUMN name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;").Error
		if err != nil {
			return fmt.Errorf("failed to change buckets_name collation: %w", err)
		}
		err = tx.Exec("ALTER TABLE multipart_uploads MODIFY COLUMN object_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;").Error
		if err != nil {
			return fmt.Errorf("failed to change object_id collation: %w", err)
		}
	}

	// Add default bucket.
	return tx.Create(&dbBucket{
		Name: api.DefaultBucketName,
	}).Error
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

func detectMissingIndices(tx *gorm.DB, f func(dst interface{}, name string)) {
	for _, table := range tables {
		detectMissingIndicesOnType(tx, table, reflect.TypeOf(table), f)
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

// performMigration00001_gormigrate performs the first migration before
// introducing gormigrate.
func performMigration00001_gormigrate(txn *gorm.DB, logger *zap.SugaredLogger) error {
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

func performMigration00002_dropconstraintslabcsid(txn *gorm.DB, logger *zap.SugaredLogger) error {
	ctx := context.Background()
	m := txn.Migrator()

	// Disable foreign keys in SQLite to avoid issues with updating constraints.
	if isSQLite(txn) {
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

func performMigration00003_healthcache(txn *gorm.DB, logger *zap.SugaredLogger) error {
	logger.Info("performing migration 00003_healthcache")
	if !txn.Migrator().HasColumn(&dbSlab{}, "health") {
		if err := txn.Migrator().AddColumn(&dbSlab{}, "health"); err != nil {
			return err
		}
	}
	logger.Info("migration 00003_healthcheck complete")
	return nil
}

func performMigration00004_objectID_collation(txn *gorm.DB, logger *zap.SugaredLogger) error {
	logger.Info("performing migration 00004_objectID_collation")
	if !isSQLite(txn) {
		err := txn.Exec("ALTER TABLE objects MODIFY COLUMN object_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;").Error
		if err != nil {
			return err
		}
	}
	logger.Info("migration 00004_objectID_collation complete")
	return nil
}

func performMigration00005_uploadPacking(txn *gorm.DB, logger *zap.SugaredLogger) error {
	logger.Info("performing migration performMigration00005_uploadPacking")
	m := txn.Migrator()

	// Disable foreign keys in SQLite to avoid issues with updating constraints.
	if isSQLite(txn) {
		if err := txn.Exec(`PRAGMA foreign_keys = 0`).Error; err != nil {
			return err
		}
	}

	if m.HasTable(&dbBufferedSlab{}) {
		// Drop buffered slabs since the schema has changed and the table was
		// unused so far.
		if err := m.DropTable(&dbBufferedSlab{}); err != nil {
			return fmt.Errorf("failed to drop table 'buffered_slabs': %w", err)
		}
	}

	// Use AutoMigrate to recreate buffered_slabs.
	if err := m.AutoMigrate(&dbBufferedSlab{}); err != nil {
		return fmt.Errorf("failed to create table 'buffered_slabs': %w", err)
	}

	// Migrate slabs.
	if isSQLite(txn) {
		if !m.HasIndex(&dbSlab{}, "MinShards") {
			if err := m.CreateIndex(&dbSlab{}, "MinShards"); err != nil {
				return fmt.Errorf("failed to create index 'MinShards' on table 'slabs': %w", err)
			}
		}
		if !m.HasIndex(&dbSlab{}, "TotalShards") {
			if err := m.CreateIndex(&dbSlab{}, "TotalShards"); err != nil {
				return fmt.Errorf("failed to create index 'TotalShards' on table 'slabs': %w", err)
			}
		}
		if !m.HasColumn(&dbSlab{}, "db_buffered_slab_id") {
			if err := m.AddColumn(&dbSlab{}, "db_buffered_slab_id"); err != nil {
				return fmt.Errorf("failed to create column 'db_buffered_slab_id' on table 'slabs': %w", err)
			}
		}
	} else if err := m.AutoMigrate(&dbSlab{}); err != nil {
		return fmt.Errorf("failed to migrate table 'slabs': %w", err)
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
	logger.Info("migration performMigration00005_uploadPacking complete")
	return nil
}

func performMigration00006_contractspending(txn *gorm.DB, logger *zap.SugaredLogger) error {
	logger.Info("performing migration 00006_contractspending")
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
	logger.Info("migration 00006_contractspending complete")
	return nil
}

func performMigration00007_archivedcontractspending(txn *gorm.DB, logger *zap.SugaredLogger) error {
	logger.Info("performing migration 00007_archivedcontractspending")
	if !txn.Migrator().HasColumn(&dbArchivedContract{}, "delete_spending") {
		if err := txn.Migrator().AddColumn(&dbArchivedContract{}, "delete_spending"); err != nil {
			return err
		}
	}
	if !txn.Migrator().HasColumn(&dbArchivedContract{}, "list_spending") {
		if err := txn.Migrator().AddColumn(&dbArchivedContract{}, "list_spending"); err != nil {
			return err
		}
	}
	logger.Info("migration 00007_archivedcontractspending complete")
	return nil
}

func performMigration00008_jointableindices(txn *gorm.DB, logger *zap.SugaredLogger) error {
	logger.Info("performing migration 00008_jointableindices")

	indices := []struct {
		joinTable interface{ TableName() string }
		column    string
	}{
		{
			&dbHostAllowlistEntryHost{},
			"DBHostID",
		},
		{
			&dbHostBlocklistEntryHost{},
			"DBHostID",
		},
		{
			&dbContractSector{},
			"DBContractID",
		},
		{
			&dbContractSetContract{},
			"DBContractID",
		},
	}

	m := txn.Migrator()
	for _, idx := range indices {
		if !m.HasIndex(idx.joinTable, idx.column) {
			if err := m.CreateIndex(idx.joinTable, idx.column); err != nil {
				return fmt.Errorf("failed to create index on column '%s' of table '%s': %w", idx.column, idx.joinTable.TableName(), err)
			}
		}
	}

	logger.Info("migration 00008_jointableindices complete")
	return nil
}

func performMigration00009_dropInteractions(txn *gorm.DB, logger *zap.SugaredLogger) error {
	logger.Info("performing migration 00009_dropInteractions")
	if !txn.Migrator().HasTable("host_interactions") {
		if err := txn.Migrator().DropTable("host_interactions"); err != nil {
			return err
		}
	}
	logger.Info("migration 00009_dropInteractions complete")
	return nil
}

func performMigration00010_distinctcontractsector(txn *gorm.DB, logger *zap.SugaredLogger) error {
	logger.Info("performing migration 00010_distinctcontractsector")

	if !txn.Migrator().HasIndex(&dbContractSector{}, "DBSectorID") {
		if err := txn.Migrator().CreateIndex(&dbContractSector{}, "DBSectorID"); err != nil {
			return fmt.Errorf("failed to create index on column 'DBSectorID' of table 'contract_sectors': %w", err)
		}
	}

	logger.Info("migration 00010_distinctcontractsector complete")
	return nil
}

func performMigration00011_healthValidColumn(txn *gorm.DB, logger *zap.SugaredLogger) error {
	logger.Info("performing migration 00011_healthValidColumn")
	if !txn.Migrator().HasColumn(&dbSlab{}, "health_valid") {
		if err := txn.Migrator().AddColumn(&dbSlab{}, "health_valid"); err != nil {
			return err
		}
	}
	logger.Info("migration 00011_healthValidColumn complete")
	return nil
}

func performMigration00012_webhooks(txn *gorm.DB, logger *zap.SugaredLogger) error {
	logger.Info("performing migration 00012_webhooks")
	if !txn.Migrator().HasTable(&dbWebhook{}) {
		if err := txn.Migrator().CreateTable(&dbWebhook{}); err != nil {
			return err
		}
	}
	logger.Info("migration 00012_webhooks complete")
	return nil
}

func performMigration00013_uploadPackingOptimisations(txn *gorm.DB, logger *zap.SugaredLogger) error {
	logger.Info("performing migration 00013_uploadPackingOptimisations")
	if txn.Migrator().HasColumn(&dbBufferedSlab{}, "lock_id") {
		if err := txn.Migrator().DropColumn(&dbBufferedSlab{}, "lock_id"); err != nil {
			return err
		}
	}
	if txn.Migrator().HasColumn(&dbBufferedSlab{}, "locked_until") {
		if err := txn.Migrator().DropColumn(&dbBufferedSlab{}, "locked_until"); err != nil {
			return err
		}
	}
	logger.Info("migration 00013_uploadPackingOptimisations complete")
	return nil
}

func performMigration00014_buckets(txn *gorm.DB, logger *zap.SugaredLogger) error {
	logger.Info("performing migration 00014_buckets")
	// Disable foreign keys in SQLite to avoid issues with updating constraints.
	if isSQLite(txn) {
		if err := txn.Exec(`PRAGMA foreign_keys = 0`).Error; err != nil {
			return err
		}
	}

	// Create buckets table
	if !txn.Migrator().HasTable(&dbBucket{}) {
		if err := txn.Migrator().CreateTable(&dbBucket{}); err != nil {
			return err
		}
		if !isSQLite(txn) {
			err := txn.Exec("ALTER TABLE buckets MODIFY COLUMN name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;").Error
			if err != nil {
				return fmt.Errorf("failed to change buckets_name collation: %w", err)
			}
		}
	}

	// Add default bucket.
	bucket := &dbBucket{
		Name: api.DefaultBucketName,
	}
	if err := txn.FirstOrCreate(&bucket).Error; err != nil {
		return err
	}

	// Add bucket id column to objects table.
	if !txn.Migrator().HasColumn(&dbObject{}, "db_bucket_id") {
		if !isSQLite(txn) {
			// MySQL
			if err := txn.Migrator().AddColumn(&dbObject{}, "db_bucket_id"); err != nil {
				return err
			}
			// Update objects to belong to default bucket
			if err := txn.Model(&dbObject{}).
				Where("db_bucket_id", 0).
				Update("db_bucket_id", bucket.ID).Error; err != nil {
				return err
			}
		} else {
			// SQLite
			if txn.Migrator().HasTable("objects_temp") {
				if err := txn.Migrator().DropTable("objects_temp"); err != nil {
					return err
				}
			}
			// Since SQLite doesn't support altering columns, we have to create
			// a new temporary objects table, copy the objects over with the
			// default bucket id and then delete the old table and rename the
			// temporary one to 'objects'.
			if err := txn.Table("objects_temp").Migrator().CreateTable(&dbObject{}); err != nil {
				return fmt.Errorf("failed to create temporary table: %w", err)
			} else if err := txn.Exec(`
			INSERT INTO objects_temp (id, created_at, db_bucket_id, object_id, key, size)
			SELECT objects.id, objects.created_at, ?, objects.object_id, objects.key, objects.size
			FROM objects
			`, bucket.ID).Error; err != nil {
				return fmt.Errorf("failed to copy objects to temporary table: %w", err)
			} else if err := txn.Migrator().DropTable("objects"); err != nil {
				return fmt.Errorf("failed to drop objects table: %w", err)
			} else if err := txn.Migrator().RenameTable("objects_temp", "objects"); err != nil {
				return fmt.Errorf("failed to rename temporary table: %w", err)
			} else if err := txn.Migrator().AutoMigrate(&dbObject{}); err != nil {
				return fmt.Errorf("failed to auto-migrate objects table: %w", err)
			}
		}
	}

	// Create missing composite index.
	if !txn.Migrator().HasIndex(&dbObject{}, "idx_object_bucket") {
		if err := txn.Migrator().CreateIndex(&dbObject{}, "idx_object_bucket"); err != nil {
			return err
		}
	}

	// Add foreign key constraint between dbObject's db_bucket_id and dbBucket's id.
	if !txn.Migrator().HasConstraint(&dbObject{}, "DBBucket") {
		if err := txn.Migrator().CreateConstraint(&dbObject{}, "DBBucket"); err != nil {
			return err
		}
	}

	// Enable foreign keys again.
	if isSQLite(txn) {
		if err := txn.Exec(`PRAGMA foreign_keys = 1`).Error; err != nil {
			return err
		}
		if err := txn.Exec(`PRAGMA foreign_key_check(objects)`).Error; err != nil {
			return err
		}
	}
	logger.Info("migration 00014_buckets complete")
	return nil
}

func performMigration00015_multipartUploads(txn *gorm.DB, logger *zap.SugaredLogger) error {
	logger.Info("performing migration 00015_multipartUploads")
	// Disable foreign keys in SQLite to avoid issues with updating constraints.
	if isSQLite(txn) {
		if err := txn.Exec(`PRAGMA foreign_keys = 0`).Error; err != nil {
			return err
		}
	}

	// Create new tables.
	if err := txn.Migrator().AutoMigrate(&dbMultipartUpload{}, &dbMultipartPart{}); err != nil {
		return err
	}

	// Add column to slices table.
	if err := txn.Migrator().AutoMigrate(&dbSlice{}); err != nil {
		return err
	}

	// Enable foreign keys again.
	if isSQLite(txn) {
		if err := txn.Exec(`PRAGMA foreign_keys = 1`).Error; err != nil {
			return err
		}
		if err := txn.Exec(`PRAGMA foreign_key_check(slices)`).Error; err != nil {
			return err
		}
	}
	logger.Info("migration 00015_multipartUploads complete")
	return nil
}

func performMigration00016_bucketPolicy(txn *gorm.DB, logger *zap.SugaredLogger) error {
	logger.Info("performing migration 00016_bucketPolicy")
	if err := txn.Migrator().AutoMigrate(&dbBucket{}); err != nil {
		return err
	}
	logger.Info("migration 00016_bucketPolicy complete")
	return nil
}

func performMigration00017_mimetype(txn *gorm.DB, logger *zap.SugaredLogger) error {
	logger.Info("performing migration 00017_mimetype")
	if !txn.Migrator().HasColumn(&dbObject{}, "MimeType") {
		if err := txn.Migrator().AddColumn(&dbObject{}, "MimeType"); err != nil {
			return err
		}
	}
	if !txn.Migrator().HasColumn(&dbMultipartUpload{}, "MimeType") {
		if err := txn.Migrator().AddColumn(&dbMultipartUpload{}, "MimeType"); err != nil {
			return err
		}
	}
	logger.Info("migration 00017_mimetype complete")
	return nil
}

func performMigration00018_etags(txn *gorm.DB, logger *zap.SugaredLogger) error {
	logger.Info("performing migration 00018_etags")
	if !txn.Migrator().HasColumn(&dbObject{}, "etag") {
		if err := txn.Migrator().AddColumn(&dbObject{}, "etag"); err != nil {
			return err
		}
	}
	logger.Info("migration 00018_etags complete")
	return nil
}

func performMigration00019_accountsShutdown(txn *gorm.DB, logger *zap.SugaredLogger) error {
	logger.Info("performing migration 00019_accounts_shutdown")
	if err := txn.Migrator().AutoMigrate(&dbAccount{}); err != nil {
		return err
	}
	if err := txn.Model(&dbAccount{}).
		Where("TRUE").
		Updates(map[string]interface{}{
			"clean_shutdown": false,
			"requires_sync":  true,
			"drift":          "0",
		}).
		Error; err != nil {
		return fmt.Errorf("failed to update accounts: %w", err)
	}
	logger.Info("migration 00019_accounts_shutdown complete")
	return nil
}

func performMigration00020_missingIndices(txn *gorm.DB, logger *zap.SugaredLogger) error {
	logger.Info("performing migration 00020_missingIndices")
	var err error
	detectMissingIndices(txn, func(dst interface{}, name string) {
		if err != nil {
			return
		}
		err = txn.Migrator().CreateIndex(dst, name)
	})
	if err != nil {
		return fmt.Errorf("failed to create missing indices: %w", err)
	}
	logger.Info("migration 00020_missingIndices complete")
	return nil
}
