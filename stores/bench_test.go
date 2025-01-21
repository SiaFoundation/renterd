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
	"go.sia.tech/renterd/stores/sql/mysql"
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

// BenchmarkPruneHostSectors benchmarks the performance of pruning host sectors.
//
// 3.3 TB/s | M1 Max | 8363023b | SQLite
// 4.8 TB/s | M1 Max | 8363023b | MySQL
func BenchmarkPruneHostSectors(b *testing.B) {
	// define parameters
	numSectors := b.N * hostSectorPruningBatchSize
	sectorSize := 4 << 20 // 4 MiB sector

	// set the amount of bytes pruned in every iteration
	b.SetBytes(hostSectorPruningBatchSize * int64(sectorSize))

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

	// prepare contract
	fcid := types.FileContractID{1}
	_, err = insertContract(db.DB(), hk, fcid, numSectors)
	if err != nil {
		b.Fatal(err)
	}

	// archive contract
	if err := db.Transaction(context.Background(), func(tx sql.DatabaseTx) error {
		return tx.ArchiveContract(context.Background(), fcid, api.ContractArchivalReasonRenewed)
	}); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var n int64
		if err := db.Transaction(context.Background(), func(tx sql.DatabaseTx) (err error) {
			n, err = tx.PruneHostSectors(context.Background(), hostSectorPruningBatchSize)
			return
		}); err != nil {
			b.Fatal(err)
		} else if n != hostSectorPruningBatchSize {
			b.Fatalf("expected %d sectors to be pruned, got %d", hostSectorPruningBatchSize, n)
		}
	}
}

// BenchmarkPruneSlabs benchmarks pruning unreferenced slabs from a database
// containing 100 TiB worth of slabs of which 50% are prunable in batches that
// reflect our batchsize in production.
//
// 2.3 TB/s | M2 Pro | fd751630 | SQLite
// 0.8 TB/s | M2 Pro | fd751630 | MySQL
func BenchmarkPruneSlabs(b *testing.B) {
	// define parameters
	totalShardsPerSlab := 30                 // shards per slab
	slabSize := totalShardsPerSlab * 4 << 20 // 120MiB
	numSlabs := 100 << 40 / slabSize         // 100 TiB of slabs
	if numSlabs%2 != 0 {
		numSlabs++ // make number even
	}

	// prepare database
	db, err := newTestMysqlDB(context.Background())
	if err != nil {
		b.Fatal(err)
	}

	obj := object.Object{
		Key: object.GenerateEncryptionKey(object.EncryptionKeyTypeSalted),
	}
	for i := 0; i < int(numSlabs); i++ {
		slab := object.SlabSlice{
			Slab: object.Slab{
				EncryptionKey: object.GenerateEncryptionKey(object.EncryptionKeyTypeSalted),
			},
		}
		obj.Slabs = append(obj.Slabs, slab)
	}

	err = db.Transaction(context.Background(), func(tx sql.DatabaseTx) error {
		if err := tx.CreateBucket(context.Background(), testBucket, api.BucketPolicy{}); err != nil {
			b.Fatal(err)
		} else if err := tx.InsertObject(context.Background(), testBucket, "foo", obj, "", "", api.ObjectUserMetadata{}); err != nil {
			b.Fatal(err)
		}
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}

	// delete half the slices to make half the slabs prunable
	_, err = db.DB().Exec(context.Background(), "DELETE FROM slices WHERE slices.db_slab_id % 2 = 0")
	if err != nil {
		b.Fatal(err)
	}

	// sanity check slabs and slices
	var createdSlabs, createdSlices int
	if err := db.DB().QueryRow(context.Background(), "SELECT COUNT(*) FROM slabs").Scan(&createdSlabs); err != nil {
		b.Fatal(err)
	} else if err := db.DB().QueryRow(context.Background(), "SELECT COUNT(*) FROM slices").Scan(&createdSlices); err != nil {
		b.Fatal(err)
	} else if createdSlabs != numSlabs || createdSlices != numSlabs/2 {
		b.Fatalf("expected %d slabs and %d slices, got %d slabs and %d slices", numSlabs, numSlabs/2, createdSlabs, createdSlices)
	}

	// start benchmark
	batchSize := slabPruningBatchSize
	b.ResetTimer()
	b.SetBytes(int64(batchSize * slabSize))
	for i := 0; i < b.N; i++ {
		if err := db.Transaction(context.Background(), func(tx sql.DatabaseTx) error {
			pruned, err := tx.PruneSlabs(context.Background(), int64(batchSize))
			if err != nil {
				return err
			} else if pruned != int64(batchSize) {
				b.Fatal("benchmark ran out of slabs to prune, increase number of slabs in db setup")
			}
			return nil
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
// 2.5 TB/s | M1 Max | 8363023b | SQLite
// 0.5 TB/s | M1 Max | 8363023b | MySQL
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
	b.SetBytes(batchSize * int64(sectorSize))
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
INSERT INTO contracts (fcid, host_key, host_id, start_height, v2, usability) VALUES (?, ?, ?, ?, ?, ?)`, sql.FileContractID(fcid), sql.PublicKey(hk), hostID, 0, false, usability)
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
INSERT INTO host_sectors (updated_at, db_sector_id, db_host_id) VALUES (?, ?, ?)`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare statement to insert host sectors: %w", err)
	}
	defer insertHostSectorStmt.Close()
	for _, sectorID := range sectorIDs {
		if _, err := insertHostSectorStmt.Exec(context.Background(), time.Now(), sectorID, hostID); err != nil {
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

func newTestMysqlDB(ctx context.Context) (*mysql.MainDatabase, error) {
	db, err := mysql.Open("root", "test", "localhost:3306", "")
	if err != nil {
		return nil, err
	}

	dbName := fmt.Sprintf("bench_%d", time.Now().UnixNano())
	if _, err := db.Exec(fmt.Sprintf("CREATE DATABASE %s", dbName)); err != nil {
		return nil, err
	}
	if _, err := db.Exec(fmt.Sprintf("USE %s", dbName)); err != nil {
		return nil, err
	}
	dbMain, err := mysql.NewMainDatabase(db, zap.NewNop(), 100*time.Millisecond, 100*time.Millisecond, "")
	if err != nil {
		return nil, err
	}

	err = dbMain.Migrate(ctx)
	if err != nil {
		return nil, err
	}

	return dbMain, nil
}
