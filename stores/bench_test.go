package stores

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	isql "go.sia.tech/renterd/internal/sql"
	"go.sia.tech/renterd/object"
	"go.sia.tech/renterd/stores/sql"
	"go.sia.tech/renterd/stores/sql/sqlite"
	"go.uber.org/zap"

	"lukechampine.com/frand"
)

// BenchmarkArchiveContract benchmarks the performance of archiving a contract.
//
// cpu: Apple M1 Max
// BenchmarkArchiveContract-10         6087            220385 ns/op            2591 B/op         68 allocs/op
func BenchmarkArchiveContract(b *testing.B) {
	// define parameters
	contractSize := 1 << 30 // 1 GiB contract
	sectorSize := 4 << 20   // 4 MiB sector
	numSectors := contractSize / sectorSize

	// create database
	db, err := newTestDB(context.Background(), b.TempDir())
	if err != nil {
		b.Fatal(err)
	}

	// prepare host
	hk := types.PublicKey{1}
	err = insertHost(db.DB(), hk)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		fcid := generateFileContractID(i)
		if _, err := insertContract(db.DB(), hk, fcid, numSectors); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()

		if err := db.Transaction(context.Background(), func(tx sql.DatabaseTx) error {
			return tx.ArchiveContract(context.Background(), fcid, api.ContractArchivalReasonHostPruned)
		}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkObjects benchmarks the performance of various object-related
// database operations.
//
// cpu: Apple M1 Max
// BenchmarkObjects/Objects-10         	    9920	    113301 ns/op	    6758 B/op	      97 allocs/op
// BenchmarkObjects/RenameObjects-10   	   19507	     62000 ns/op	    3521 B/op	      81 allocs/op
func BenchmarkObjects(b *testing.B) {
	db, err := newTestDB(context.Background(), b.TempDir())
	if err != nil {
		b.Fatal(err)
	}

	// test parameters
	objects := int(1e2)
	bucket := "bucket"

	// prepare database
	dirs, err := insertObjects(db.DB(), bucket, objects)
	if err != nil {
		b.Fatal(err)
	}

	b.Run("Objects", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := db.Transaction(context.Background(), func(tx sql.DatabaseTx) error {
				_, err := tx.Objects(context.Background(), bucket, dirs[i%len(dirs)], "", "/", "", "", "", -1, object.EncryptionKey{})
				return err
			}); err != nil {
				b.Fatal(err)
			}
		}
	})

	// start rename benchmark
	b.Run("RenameObjects", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := db.Transaction(context.Background(), func(tx sql.DatabaseTx) error {
				err := tx.RenameObjects(context.Background(), bucket, dirs[frand.Intn(i+1)%len(dirs)], dirs[frand.Intn(i+1)%len(dirs)], true)
				if err != nil && !errors.Is(err, api.ErrObjectNotFound) {
					return err
				}
				return nil
			}); err != nil {
				b.Fatal(err)
			}
		}
	})
}

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
	hk := types.PublicKey{1}
	err = insertHost(db.DB(), hk)
	if err != nil {
		b.Fatal(err)
	}

	fcid := types.FileContractID{1}
	roots, err := insertContract(db.DB(), hk, fcid, numSectors)
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

func insertHost(db *isql.DB, hk types.PublicKey) error {
	_, err := db.Exec(context.Background(), `INSERT INTO hosts (public_key) VALUES (?)`, sql.PublicKey(hk))
	return err
}

func insertContract(db *isql.DB, hk types.PublicKey, fcid types.FileContractID, n int) (roots []types.Hash256, _ error) {
	var hostID int64
	err := db.QueryRow(context.Background(), `SELECT id FROM hosts WHERE public_key = ?`, sql.PublicKey(hk)).Scan(&hostID)
	if err != nil {
		return nil, err
	}

	// prepare usability
	var usability sql.ContractUsability
	if err := usability.LoadString(api.ContractUsabilityGood); err != nil {
		return nil, err
	}

	// insert contract
	res, err := db.Exec(context.Background(), `
INSERT INTO contracts (fcid, host_key, start_height, v2, usability) VALUES (?, ?, ?, ?, ?)`, sql.FileContractID(fcid), sql.PublicKey(hk), 0, false, usability)
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
INSERT INTO sectors (db_slab_id, slab_index, root) VALUES (?, ?, ?)`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare statement to insert sector: %w", err)
	}
	defer insertSectorStmt.Close()
	for i := 0; i < n; i++ {
		roots = append(roots, frand.Entropy256())
		_, err := insertSectorStmt.Exec(context.Background(), slabID, i, sql.Hash256(roots[i]))
		if err != nil {
			return nil, fmt.Errorf("failed to insert sector: %w", err)
		}
	}

	// query sector ids
	rows, err := db.Query(context.Background(), `SELECT id FROM sectors where db_slab_id = ?`, slabID)
	if err != nil {
		return nil, fmt.Errorf("failed to query sectors: %w", err)
	}
	defer rows.Close()

	var sectorIDs []int64
	for rows.Next() {
		var sectorID int64
		if err := rows.Scan(&sectorID); err != nil {
			return nil, fmt.Errorf("failed to scan sector id: %w", err)
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

	// insert host sectors
	insertHostSectorStmt, err := db.Prepare(context.Background(), `
INSERT INTO host_sectors (db_sector_id, db_host_id) VALUES (?, ?)`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare statement to insert host sectors: %w", err)
	}
	defer insertHostSectorStmt.Close()
	for _, sectorID := range sectorIDs {
		if _, err := insertHostSectorStmt.Exec(context.Background(), sectorID, hostID); err != nil {
			return nil, fmt.Errorf("failed to insert host sector: %w", err)
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

	return roots, nil
}

func insertObjects(db *isql.DB, bucket string, n int) (dirs []string, _ error) {
	var bucketID int64
	res, err := db.Exec(context.Background(), "INSERT INTO buckets (created_at, name) VALUES (?, ?)", time.Now(), bucket)
	if err != nil {
		return nil, err
	} else if bucketID, err = res.LastInsertId(); err != nil {
		return nil, err
	}

	stmt, err := db.Prepare(context.Background(), "INSERT INTO objects (created_at,object_id, db_bucket_id, size, mime_type, etag) VALUES (?, ?, ?, ?, '', '')")
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	var path string
	seen := make(map[string]struct{})
	for i := 0; i < n; i++ {
		for {
			path = generateRandomPath(6)
			if _, used := seen[path]; !used {
				break
			}
		}
		seen[path] = struct{}{}

		size := frand.Intn(1e3)
		if frand.Intn(10) == 0 {
			path += "/"
			size = 0
			dirs = append(dirs, path)
		}
		_, err := stmt.Exec(context.Background(), time.Now(), path, bucketID, size)
		if err != nil {
			return nil, err
		}
	}
	return dirs, nil
}

func generateFileContractID(i int) types.FileContractID {
	h := types.NewHasher()
	h.E.WriteUint64(uint64(i))
	return types.FileContractID(h.Sum())
}

func generateRandomPath(maxLevels int) string {
	numLevels := frand.Intn(maxLevels) + 1
	letters := "abcdef"

	var path []string
	for i := 0; i < numLevels; i++ {
		path = append(path, string(letters[frand.Intn(len(letters))]))
	}

	return "/" + strings.Join(path, "/")
}

func newTestDB(ctx context.Context, dir string) (*sqlite.MainDatabase, error) {
	db, err := sqlite.Open(filepath.Join(dir, "db.sqlite"))
	if err != nil {
		return nil, err
	}

	dbMain, err := sqlite.NewMainDatabase(db, zap.NewNop(), 100*time.Millisecond, 100*time.Millisecond, "")
	if err != nil {
		return nil, err
	}

	err = dbMain.Migrate(ctx)
	if err != nil {
		return nil, err
	}

	return dbMain, nil
}
