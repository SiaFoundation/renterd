package stores

import (
	"context"

	sql "go.sia.tech/renterd/stores/sql"
	"go.sia.tech/renterd/webhooks"
)

func (s *SQLStore) AddWebhook(ctx context.Context, wh webhooks.Webhook) error {
	return s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		return tx.AddWebhook(ctx, wh)
	})
}

func (s *SQLStore) DeleteWebhook(ctx context.Context, wh webhooks.Webhook) error {
	return s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		return tx.DeleteWebhook(ctx, wh)
	})
}

func (s *SQLStore) Webhooks(ctx context.Context) (whs []webhooks.Webhook, err error) {
	err = s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		whs, err = tx.Webhooks(ctx)
		return err
	})
	return
}
