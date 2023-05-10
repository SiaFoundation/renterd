package stores

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"gorm.io/gorm"
)

type dbHostBlocklistEntryHost struct {
	DBBlocklistEntryID uint8 `gorm:"primarykey;column:db_blocklist_entry_id"`
	DBHostID           uint8 `gorm:"primarykey;index:idx_db_host_id;column:db_host_id"`
}

func (dbHostBlocklistEntryHost) TableName() string {
	return "host_blocklist_entry_hosts"
}

func performMigrations(db *gorm.DB) error {
	m := db.Migrator()

	// Perform auto migrations.
	tables := []interface{}{
		// bus.MetadataStore tables
		&dbArchivedContract{},
		&dbContract{},
		&dbContractSet{},
		&dbObject{},
		&dbSector{},
		&dbShard{},
		&dbSlab{},
		&dbSlice{},

		// bus.HostDB tables
		&dbAnnouncement{},
		&dbConsensusInfo{},
		&dbHost{},
		&dbInteraction{},
		&dbAllowlistEntry{},
		&dbBlocklistEntry{},

		// bus.SettingStore tables
		&dbSetting{},

		// bus.EphemeralAccountStore tables
		&dbAccount{},
	}
	if err := db.AutoMigrate(tables...); err != nil {
		return nil
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

func (ss *SQLStore) updateHasAllowlist(err *error) {
	if *err != nil {
		return
	}

	cnt, cErr := tableCount(ss.db, &dbAllowlistEntry{})
	if cErr != nil {
		*err = cErr
		return
	}

	ss.mu.Lock()
	ss.hasAllowlist = cnt > 0
	ss.mu.Unlock()
}

func (ss *SQLStore) updateHasBlocklist(err *error) {
	if *err != nil {
		return
	}

	cnt, cErr := tableCount(ss.db, &dbBlocklistEntry{})
	if cErr != nil {
		*err = cErr
		return
	}

	ss.mu.Lock()
	ss.hasBlocklist = cnt > 0
	ss.mu.Unlock()
}

func tableCount(db *gorm.DB, model interface{}) (cnt int64, err error) {
	err = db.Model(model).Count(&cnt).Error
	return
}

// Close closes the underlying database connection of the store.
func (s *SQLStore) Close() error {
	db, err := s.db.DB()
	if err != nil {
		return err
	}

	err = db.Close()
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	return nil
}

func (s *SQLStore) retryTransaction(fc func(tx *gorm.DB) error, opts ...*sql.TxOptions) error {
	var err error
	for i := 0; i < 5; i++ {
		err = s.db.Transaction(fc, opts...)
		if err == nil {
			return nil
		}
		s.logger.Warn(context.Background(), fmt.Sprintf("transaction attempt %d/%d failed, err: %v", i+1, 5, err))
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("retryTransaction failed: %w", err)
}
