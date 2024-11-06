package stores

import (
	"context"

	"go.sia.tech/renterd/api"
	sql "go.sia.tech/renterd/stores/sql"
)

func (s *SQLStore) AutopilotState(ctx context.Context) (ap api.AutopilotState, _ error) {
	err := s.db.Transaction(ctx, func(tx sql.DatabaseTx) (err error) {
		ap, err = tx.AutopilotState(ctx)
		return
	})
	return ap, err
}

func (s *SQLStore) UpdateAutopilotConfig(ctx context.Context, cfg api.AutopilotConfig) error {
	if err := cfg.Hosts.Validate(); err != nil {
		return err
	}
	return s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		return tx.UpdateAutopilotConfig(ctx, cfg)
	})
}

func (s *SQLStore) UpdateAutopilotPeriod(ctx context.Context, period uint64) error {
	return s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		return tx.UpdateAutopilotPeriod(ctx, period)
	})
}
