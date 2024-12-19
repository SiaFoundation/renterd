package stores

import (
	"context"

	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/stores/sql/sqlite"
)

func (s *SQLStore) Backup(ctx context.Context, dbID, destPath string) error {
	switch dbID {
	case "main":
		switch db := s.db.(type) {
		case *sqlite.MainDatabase:
			return db.Backup(ctx, dbID, destPath)
		default:
			return api.ErrBackupNotSupported
		}
	case "metrics":
		switch db := s.dbMetrics.(type) {
		case *sqlite.MetricsDatabase:
			return db.Backup(ctx, dbID, destPath)
		default:
			return api.ErrBackupNotSupported
		}
	default:
		return api.ErrInvalidDatabase
	}
}
