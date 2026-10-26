package sql

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	dsql "database/sql"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/v2/api"
	"go.sia.tech/renterd/v2/internal/sql"
)

func AddUploadingSectors(ctx context.Context, tx sql.Tx, uID api.UploadID, roots []types.Hash256) error {
	if len(roots) == 0 {
		return nil
	}

	var dbUploadID int64
	err := tx.QueryRow(ctx, "SELECT id FROM uploads WHERE upload_id = ?", uID[:]).Scan(&dbUploadID)
	if errors.Is(err, dsql.ErrNoRows) {
		return fmt.Errorf("%w; id '%v'", api.ErrUnknownUpload, uID)
	} else if err != nil {
		return fmt.Errorf("failed to fetch upload: %w", err)
	}

	args := make([]any, 0, 2*len(roots))
	for _, root := range roots {
		args = append(args, dbUploadID, Hash256(root))
	}
	_, err = tx.Exec(ctx, fmt.Sprintf("INSERT INTO uploading_sectors (db_upload_id, root) VALUES %s", strings.Repeat("(?, ?), ", len(roots)-1)+"(?, ?)"), args...)
	if err != nil {
		return fmt.Errorf("failed to insert uploading sectors: %w", err)
	}
	return nil
}

func FinishUpload(ctx context.Context, tx sql.Tx, uID api.UploadID) error {
	if _, err := tx.Exec(ctx, "DELETE FROM uploading_sectors WHERE db_upload_id IN (SELECT id FROM uploads WHERE upload_id = ?)", uID[:]); err != nil {
		return fmt.Errorf("failed to delete uploading sectors: %w", err)
	} else if _, err := tx.Exec(ctx, "DELETE FROM uploads WHERE upload_id = ?", uID[:]); err != nil {
		return fmt.Errorf("failed to delete upload: %w", err)
	}
	return nil
}

func PruneUploads(ctx context.Context, tx sql.Tx, cutoff time.Time) error {
	// NOTE: the cutoff is applied in Go since datetime comparisons behave
	// differently between the database backends and the table is small
	rows, err := tx.Query(ctx, "SELECT id, created_at FROM uploads")
	if err != nil {
		return fmt.Errorf("failed to fetch uploads: %w", err)
	}
	defer rows.Close()

	var expired []any
	for rows.Next() {
		var id int64
		var createdAt time.Time
		if err := rows.Scan(&id, &createdAt); err != nil {
			return fmt.Errorf("failed to scan upload: %w", err)
		} else if createdAt.Before(cutoff) {
			expired = append(expired, id)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	} else if len(expired) == 0 {
		return nil
	}

	placeholders := strings.Repeat("?, ", len(expired)-1) + "?"
	if _, err := tx.Exec(ctx, fmt.Sprintf("DELETE FROM uploading_sectors WHERE db_upload_id IN (%s)", placeholders), expired...); err != nil {
		return fmt.Errorf("failed to delete expired uploading sectors: %w", err)
	} else if _, err := tx.Exec(ctx, fmt.Sprintf("DELETE FROM uploads WHERE id IN (%s)", placeholders), expired...); err != nil {
		return fmt.Errorf("failed to delete expired uploads: %w", err)
	}
	return nil
}

func TrackUpload(ctx context.Context, tx sql.Tx, uID api.UploadID) error {
	var exists bool
	if err := tx.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM uploads WHERE upload_id = ?)", uID[:]).Scan(&exists); err != nil {
		return fmt.Errorf("failed to check for upload: %w", err)
	} else if exists {
		return fmt.Errorf("%w; id '%v'", api.ErrUploadAlreadyExists, uID)
	}
	if _, err := tx.Exec(ctx, "INSERT INTO uploads (created_at, upload_id) VALUES (?, ?)", time.Now(), uID[:]); err != nil {
		return fmt.Errorf("failed to insert upload: %w", err)
	}
	return nil
}

func UploadingSectors(ctx context.Context, tx sql.Tx) ([]types.Hash256, error) {
	rows, err := tx.Query(ctx, "SELECT root FROM uploading_sectors")
	if err != nil {
		return nil, fmt.Errorf("failed to fetch uploading sectors: %w", err)
	}
	defer rows.Close()

	var roots []types.Hash256
	for rows.Next() {
		var root types.Hash256
		if err := rows.Scan((*Hash256)(&root)); err != nil {
			return nil, fmt.Errorf("failed to scan uploading sector: %w", err)
		}
		roots = append(roots, root)
	}
	return roots, rows.Err()
}
