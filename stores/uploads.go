package stores

import (
	"context"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/v2/api"
	sql "go.sia.tech/renterd/v2/stores/sql"
)

const (
	// uploadExpiry is the amount of time after which a tracked upload is
	// pruned. Workers are expected to finish their uploads so this is only
	// there to prevent leaking uploads of workers that didn't shut down
	// cleanly, which is why it's set at 24h.
	uploadExpiry = 24 * time.Hour
)

func (s *SQLStore) AddUploadingSectors(ctx context.Context, uID api.UploadID, roots []types.Hash256) error {
	return s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		return tx.AddUploadingSectors(ctx, uID, roots)
	})
}

func (s *SQLStore) FinishUpload(ctx context.Context, uID api.UploadID) error {
	return s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		if err := tx.FinishUpload(ctx, uID); err != nil {
			return err
		}
		return tx.PruneUploads(ctx, time.Now().Add(-uploadExpiry))
	})
}

func (s *SQLStore) TrackUpload(ctx context.Context, uID api.UploadID) error {
	return s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		return tx.TrackUpload(ctx, uID)
	})
}

func (s *SQLStore) UploadingSectors(ctx context.Context) (roots []types.Hash256, err error) {
	err = s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		// prune expired uploads first so the roots of uploads whose worker
		// never finished them don't stay protected forever
		if err := tx.PruneUploads(ctx, time.Now().Add(-uploadExpiry)); err != nil {
			return err
		}
		roots, err = tx.UploadingSectors(ctx)
		return err
	})
	return
}
