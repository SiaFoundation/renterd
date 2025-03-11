package stores

import (
	"context"

	"go.sia.tech/renterd/v2/api"
	sql "go.sia.tech/renterd/v2/stores/sql"
)

func (s *SQLStore) AutopilotConfig(ctx context.Context) (cfg api.AutopilotConfig, err error) {
	s.db.Transaction(ctx, func(tx sql.DatabaseTx) (err error) {
		cfg, err = tx.AutopilotConfig(ctx)
		return
	})
	return
}

func (s *SQLStore) InitAutopilotConfig(ctx context.Context) error {
	return s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		return tx.InitAutopilotConfig(ctx)
	})
}

func (s *SQLStore) UpdateAutopilotConfig(ctx context.Context, cfg api.AutopilotConfig) error {
	return s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		return tx.UpdateAutopilotConfig(ctx, cfg)
	})
}
