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

func (s *SQLStore) UpdateAutopilot(ctx context.Context, ap api.Autopilot) error {
	return s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		return tx.UpdateAutopilot(ctx, ap)
	})
}
