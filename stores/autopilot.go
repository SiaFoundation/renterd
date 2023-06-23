package stores

import (
	"context"
	"errors"

	"go.sia.tech/renterd/api"
	"gorm.io/gorm"
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

// convert converts a dbContract to a ContractMetadata.
func (c dbAutopilot) convert() api.Autopilot {
	return api.Autopilot{
		ID:            c.Identifier,
		Config:        c.Config,
		CurrentPeriod: c.CurrentPeriod,
	}
}

func (s *SQLStore) Autopilots(ctx context.Context) ([]api.Autopilot, error) {
	var entities []dbAutopilot
	err := s.db.
		Model(&dbAutopilot{}).
		Find(&entities).
		Error
	if err != nil {
		return nil, err
	}

	autopilots := make([]api.Autopilot, len(entities))
	for i, ap := range entities {
		autopilots[i] = ap.convert()
	}
	return autopilots, nil
}

func (s *SQLStore) Autopilot(ctx context.Context, id string) (api.Autopilot, error) {
	var entity dbAutopilot
	err := s.db.
		Model(&dbAutopilot{}).
		Where("identifier = ?", id).
		First(&entity).
		Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return api.Autopilot{}, api.ErrAutopilotNotFound
	} else if err != nil {
		return api.Autopilot{}, err
	}
	return entity.convert(), nil
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
	return s.db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "identifier"}},
		UpdateAll: true,
	}).Create(&dbAutopilot{
		Identifier:    ap.ID,
		Config:        ap.Config,
		CurrentPeriod: ap.CurrentPeriod,
	}).Error
}
