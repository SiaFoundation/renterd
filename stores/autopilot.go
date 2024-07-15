package stores

import (
	"context"
	"errors"

	"go.sia.tech/renterd/api"
	sql "go.sia.tech/renterd/stores/sql"
)

func (s *SQLStore) Autopilots(ctx context.Context) (aps []api.Autopilot, _ error) {
	err := s.db.Transaction(ctx, func(tx sql.DatabaseTx) (err error) {
		aps, err = tx.Autopilots(ctx)
		return
	})
	return aps, err
}

func (s *SQLStore) Autopilot(ctx context.Context, id string) (ap api.Autopilot, _ error) {
	err := s.db.Transaction(ctx, func(tx sql.DatabaseTx) (err error) {
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
	return s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		return tx.UpdateAutopilot(ctx, ap)
	})
}
