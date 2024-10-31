package stores

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"go.sia.tech/core/types"
	isql "go.sia.tech/renterd/internal/sql"
	"go.sia.tech/renterd/object"
	"go.sia.tech/renterd/stores/sql"
	"go.sia.tech/renterd/stores/sql/sqlite"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

// BenchmarkPrunableContractRoots benchmarks diffing the roots of a contract
// with a given set of roots to determine which roots are prunable.
//
// 15.32 MB/s | M1 Max | cd32fad7 (diff ~2TiB of contract data per second)
func BenchmarkPrunableContractRoots(b *testing.B) {
	// define parameters
	batchSize := int64(25600) // 100GiB of contract data
	contractSize := 1 << 40   // 1TiB contract
	sectorSize := 4 << 20     // 4MiB sector
	numSectors := contractSize / sectorSize

	// create database
	db, err := newTestDB(context.Background(), b.TempDir())
	if err != nil {
		b.Fatal(err)
	}

	// prepare database
	fcid := types.FileContractID{1}
	roots, err := prepareDB(db.DB(), fcid, numSectors)
	if err != nil {
		b.Fatal(err)
	}

	// prepare batch
	frand.Shuffle(len(roots), func(i, j int) {
		roots[i], roots[j] = roots[j], roots[i]
	})
	batch := roots[:batchSize]

	// start benchmark
	b.ResetTimer()
	b.SetBytes(batchSize * 32)
	for i := 0; i < b.N; i++ {
		if err := db.Transaction(context.Background(), func(tx sql.DatabaseTx) error {
			indices, err := tx.PrunableContractRoots(context.Background(), fcid, batch)
			if err != nil {
				return err
			} else if len(indices) != 0 {
				return errors.New("expected no prunable roots")
			}
			return nil
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func prepareDB(db *isql.DB, fcid types.FileContractID, n int) (roots []types.Hash256, _ error) {
	// insert contract
	hk := types.PublicKey{1}
	res, err := db.Exec(context.Background(), `
INSERT INTO contracts (fcid, host_key, start_height, v2) VALUES (?, ?, ?, ?)`, sql.PublicKey(hk), sql.FileContractID(fcid), 0, false)
	if err != nil {
		return nil, err
	}
	contractID, err := res.LastInsertId()
	if err != nil {
		return nil, err
	}

	// insert slab
	key := object.GenerateEncryptionKey(object.EncryptionKeyTypeSalted)
	res, err = db.Exec(context.Background(), `
INSERT INTO slabs (created_at, `+"`key`"+`) VALUES (?, ?)`, time.Now(), sql.EncryptionKey(key))
	if err != nil {
		return nil, err
	}
	slabID, err := res.LastInsertId()
	if err != nil {
		return nil, err
	}

	// insert sectors
	insertSectorStmt, err := db.Prepare(context.Background(), `
INSERT INTO sectors (db_slab_id, slab_index, root) VALUES (?, ?, ?) RETURNING id`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare statement to insert sector: %w", err)
	}
	defer insertSectorStmt.Close()
	var sectorIDs []int64
	for i := 0; i < n; i++ {
		var sectorID int64
		roots = append(roots, frand.Entropy256())
		err := insertSectorStmt.QueryRow(context.Background(), slabID, i, sql.Hash256(roots[i])).Scan(&sectorID)
		if err != nil {
			return nil, fmt.Errorf("failed to insert sector: %w", err)
		}
		sectorIDs = append(sectorIDs, sectorID)
	}

	// insert contract sectors
	insertLinkStmt, err := db.Prepare(context.Background(), `
INSERT INTO contract_sectors (db_contract_id, db_sector_id) VALUES (?, ?)`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare statement to insert contract sectors: %w", err)
	}
	defer insertLinkStmt.Close()
	for _, sectorID := range sectorIDs {
		if _, err := insertLinkStmt.Exec(context.Background(), contractID, sectorID); err != nil {
			return nil, fmt.Errorf("failed to insert contract sector: %w", err)
		}
	}

	// sanity check
	var cnt int
	err = db.QueryRow(context.Background(), `
SELECT COUNT(s.root)
FROM contracts c
INNER JOIN contract_sectors cs ON cs.db_contract_id = c.id
INNER JOIN sectors s ON cs.db_sector_id = s.id
WHERE c.fcid = ?`, sql.FileContractID(fcid)).Scan(&cnt)
	if cnt != n {
		return nil, fmt.Errorf("expected %v sectors, got %v", n, cnt)
	}

	return
}

func newTestDB(ctx context.Context, dir string) (*sqlite.MainDatabase, error) {
	db, err := sqlite.Open(filepath.Join(dir, "db.sqlite"))
	if err != nil {
		return nil, err
	}

	dbMain, err := sqlite.NewMainDatabase(db, zap.NewNop(), 100*time.Millisecond, 100*time.Millisecond)
	if err != nil {
		return nil, err
	}

	err = dbMain.Migrate(ctx)
	if err != nil {
		return nil, err
	}

	return dbMain, nil
}
