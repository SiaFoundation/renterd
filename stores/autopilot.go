package stores

import (
	"context"

	"go.sia.tech/renterd/api"
	sql "go.sia.tech/renterd/stores/sql"
)

func (s *SQLStore) Autopilot(ctx context.Context) (ap api.Autopilot, err error) {
	s.db.Transaction(ctx, func(tx sql.DatabaseTx) (err error) {
		ap, err = tx.Autopilot(ctx)
		return
	})
	return
}

func (s *SQLStore) InitAutopilot(ctx context.Context) error {
	return s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		return tx.InitAutopilot(ctx)
	})
}

func (s *SQLStore) EnableAutopilot(ctx context.Context, enable bool) error {
	return s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		return tx.EnableAutopilot(ctx, enable)
	})
}

func (s *SQLStore) UpdateContractsConfig(ctx context.Context, cfg api.ContractsConfig) error {
	return s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		return tx.UpdateContractsConfig(ctx, cfg)
	})
}

func (s *SQLStore) UpdateHostsConfig(ctx context.Context, cfg api.HostsConfig) error {
	return s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		return tx.UpdateHostsConfig(ctx, cfg)
	})
}

func (s *SQLStore) UpdateCurrentPeriod(ctx context.Context, period uint64) error {
	return s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		return tx.UpdateCurrentPeriod(ctx, period)
	})
}
