package stores

import (
	"context"

	"go.sia.tech/renterd/api"
	sql "go.sia.tech/renterd/stores/sql"
)

func (s *SQLStore) AutopilotConfig(ctx context.Context) (ap api.AutopilotConfig, _ error) {
	err := s.db.Transaction(ctx, func(tx sql.DatabaseTx) (err error) {
		ap, err = tx.AutopilotConfig(ctx)
		return
	})
	return ap, err
}

func (s *SQLStore) UpdateAutopilotConfig(ctx context.Context, cfg api.AutopilotConfig) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	return s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		return tx.UpdateAutopilotConfig(ctx, cfg)
	})
}
