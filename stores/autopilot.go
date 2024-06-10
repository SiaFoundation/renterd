package stores

import (
	"context"
	"errors"

	"go.sia.tech/renterd/api"
	sql "go.sia.tech/renterd/stores/sql"
	"gorm.io/gorm/clause"
)

type (
	dbAutopilot struct {
		Model

		Identifier    string              `gorm:"unique;NOT NULL;"`
		Config        api.AutopilotConfig `gorm:"serializer:json"`
		CurrentPeriod uint64              `gorm:"default:0"`
	}
)

// TableName implements the gorm.Tabler interface.
func (dbAutopilot) TableName() string { return "autopilots" }

func (s *SQLStore) Autopilots(ctx context.Context) (aps []api.Autopilot, _ error) {
	err := s.bMain.Transaction(ctx, func(tx sql.DatabaseTx) (err error) {
		aps, err = tx.Autopilots(ctx)
		return
	})
	return aps, err
}

func (s *SQLStore) Autopilot(ctx context.Context, id string) (ap api.Autopilot, _ error) {
	err := s.bMain.Transaction(ctx, func(tx sql.DatabaseTx) (err error) {
		ap, err = tx.Autopilot(ctx, id)
		return
	})
	return ap, err
}

func (s *SQLStore) UpdateAutopilot(ctx context.Context, ap api.Autopilot) error {
	// validate autopilot
	if ap.ID == "" {
		return errors.New("autopilot ID cannot be empty")
	}
	if err := ap.Config.Validate(); err != nil {
		return err
	}

	// upsert
	return s.db.
		WithContext(ctx).
		Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "identifier"}},
			UpdateAll: true,
		}).Create(&dbAutopilot{
		Identifier:    ap.ID,
		Config:        ap.Config,
		CurrentPeriod: ap.CurrentPeriod,
	}).Error
}
