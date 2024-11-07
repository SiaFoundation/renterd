package stores

import (
	"context"

	"go.sia.tech/renterd/api"
	sql "go.sia.tech/renterd/stores/sql"
)

func (s *SQLStore) AutopilotConfig(ctx context.Context) (cfg api.AutopilotConfig, err error) {
	s.db.Transaction(ctx, func(tx sql.DatabaseTx) (err error) {
		cfg, err = tx.AutopilotConfig(ctx)
		return
	})
	return
}

func (s *SQLStore) AutopilotPeriod(ctx context.Context) (period uint64, err error) {
	s.db.Transaction(ctx, func(tx sql.DatabaseTx) (err error) {
		period, err = tx.AutopilotPeriod(ctx)
		return
	})
	return
}

func (s *SQLStore) InitAutopilot(ctx context.Context) error {
	return s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		return tx.InitAutopilot(ctx)
	})
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
