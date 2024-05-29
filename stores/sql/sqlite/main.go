package sqlite

import (
	"context"
	dsql "database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/sql"
	"go.sia.tech/renterd/object"
	ssql "go.sia.tech/renterd/stores/sql"
	"lukechampine.com/frand"

	"go.uber.org/zap"
)

type (
	MainDatabase struct {
		db  *sql.DB
		log *zap.SugaredLogger
	}

	MainDatabaseTx struct {
		sql.Tx
		log *zap.SugaredLogger
	}
)

// NewMainDatabase creates a new SQLite backend.
func NewMainDatabase(db *dsql.DB, log *zap.SugaredLogger, lqd, ltd time.Duration) (*MainDatabase, error) {
	store, err := sql.NewDB(db, log.Desugar(), deadlockMsgs, lqd, ltd)
	return &MainDatabase{
		db:  store,
		log: log,
	}, err
}

func (b *MainDatabase) ApplyMigration(ctx context.Context, fn func(tx sql.Tx) (bool, error)) error {
	return applyMigration(ctx, b.db, fn)
}

func (b *MainDatabase) Close() error {
	return closeDB(b.db, b.log)
}

func (b *MainDatabase) DB() *sql.DB {
	return b.db
}

func (b *MainDatabase) CreateMigrationTable(ctx context.Context) error {
	return createMigrationTable(ctx, b.db)
}

func (b *MainDatabase) MakeDirsForPath(ctx context.Context, tx sql.Tx, path string) (int64, error) {
	mtx := b.wrapTxn(tx)
	return mtx.MakeDirsForPath(ctx, path)
}

func (b *MainDatabase) Migrate(ctx context.Context) error {
	return sql.PerformMigrations(ctx, b, migrationsFs, "main", sql.MainMigrations(ctx, b, migrationsFs, b.log))
}

func (b *MainDatabase) Transaction(ctx context.Context, fn func(tx ssql.DatabaseTx) error) error {
	return b.db.Transaction(ctx, func(tx sql.Tx) error {
		return fn(b.wrapTxn(tx))
	})
}

func (b *MainDatabase) Version(ctx context.Context) (string, string, error) {
	return version(ctx, b.db)
}

func (b *MainDatabase) wrapTxn(tx sql.Tx) *MainDatabaseTx {
	return &MainDatabaseTx{tx, b.log.Named(hex.EncodeToString(frand.Bytes(16)))}
}

func (tx *MainDatabaseTx) Contracts(ctx context.Context, opts api.ContractsOpts) ([]api.ContractMetadata, error) {
	return ssql.Contracts(ctx, tx, opts)
}

func (tx *MainDatabaseTx) DeleteObject(ctx context.Context, bucket string, key string) (bool, error) {
	resp, err := tx.Exec(ctx, "DELETE FROM objects WHERE object_id = ? AND db_bucket_id = (SELECT id FROM buckets WHERE buckets.name = ?)", key, bucket)
	if err != nil {
		return false, err
	} else if n, err := resp.RowsAffected(); err != nil {
		return false, err
	} else {
		return n != 0, nil
	}
}

func (tx *MainDatabaseTx) DeleteObjects(ctx context.Context, bucket string, key string, limit int64) (bool, error) {
	resp, err := tx.Exec(ctx, `
	DELETE FROM objects
	WHERE id IN (
		SELECT id FROM objects
		WHERE object_id LIKE ? AND SUBSTR(object_id, 1, ?) = ? AND db_bucket_id = (SELECT id FROM buckets WHERE buckets.name = ?)
		LIMIT ?
	)`, key+"%", utf8.RuneCountInString(key), key, bucket, limit)
	if err != nil {
		return false, err
	} else if n, err := resp.RowsAffected(); err != nil {
		return false, err
	} else {
		return n != 0, nil
	}
}

func (tx *MainDatabaseTx) InsertObject(ctx context.Context, bucket, key, contractSet string, dirID int64, o object.Object, mimeType, eTag string, md api.ObjectUserMetadata) error {
	// get bucket id
	var bucketID int64
	err := tx.QueryRow(ctx, "SELECT id FROM buckets WHERE buckets.name = ?", bucket).Scan(&bucketID)
	if errors.Is(err, dsql.ErrNoRows) {
		return api.ErrBucketNotFound
	} else if err != nil {
		return fmt.Errorf("failed to fetch bucket id: %w", err)
	}

	// insert object
	objKey, err := o.Key.MarshalBinary()
	if err != nil {
		return fmt.Errorf("failed to marshal object key: %w", err)
	}
	var objID int64
	err = tx.QueryRow(ctx, `INSERT INTO objects (created_at, object_id, db_directory_id, db_bucket_id, key, size, mime_type, etag)
						VALUES (?, ?, ?, ?, ?, ?, ?, ?) RETURNING id`,
		time.Now(),
		key,
		dirID,
		bucketID,
		ssql.SecretKey(objKey),
		o.TotalSize(),
		mimeType,
		eTag).Scan(&objID)
	if err != nil {
		return fmt.Errorf("failed to insert object: %w", err)
	}

	// if object has no slices there is nothing to do
	slices := o.Slabs
	if len(slices) == 0 {
		return nil // nothing to do
	}

	usedContracts, err := ssql.FetchUsedContracts(ctx, tx.Tx, o.Contracts())
	if err != nil {
		return fmt.Errorf("failed to fetch used contracts: %w", err)
	}

	// get contract set id
	var contractSetID int64
	if err := tx.QueryRow(ctx, "SELECT id FROM contract_sets WHERE contract_sets.name = ?", contractSet).
		Scan(&contractSetID); err != nil {
		return fmt.Errorf("failed to fetch contract set id: %w", err)
	}

	// insert slabs
	insertSlabStmt, err := tx.Prepare(ctx, `INSERT INTO slabs (created_at, db_contract_set_id, key, min_shards, total_shards)
						VALUES (?, ?, ?, ?, ?)
						ON CONFLICT(key) DO NOTHING RETURNING id`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement to insert slab: %w", err)
	}
	defer insertSlabStmt.Close()

	querySlabIDStmt, err := tx.Prepare(ctx, "SELECT id FROM slabs WHERE key = ?")
	if err != nil {
		return fmt.Errorf("failed to prepare statement to query slab id: %w", err)
	}
	defer querySlabIDStmt.Close()

	slabIDs := make([]int64, len(slices))
	for i := range slices {
		slabKey, err := slices[i].Key.MarshalBinary()
		if err != nil {
			return fmt.Errorf("failed to marshal slab key: %w", err)
		}
		err = insertSlabStmt.QueryRow(ctx,
			time.Now(),
			contractSetID,
			ssql.SecretKey(slabKey),
			slices[i].MinShards,
			uint8(len(slices[i].Shards)),
		).Scan(&slabIDs[i])
		if errors.Is(err, dsql.ErrNoRows) {
			if err := querySlabIDStmt.QueryRow(ctx, ssql.SecretKey(slabKey)).Scan(&slabIDs[i]); err != nil {
				return fmt.Errorf("failed to fetch slab id: %w", err)
			}
		} else if err != nil {
			return fmt.Errorf("failed to insert slab: %w", err)
		}
	}

	// insert slices
	insertSliceStmt, err := tx.Prepare(ctx, `INSERT INTO slices (created_at, db_object_id, object_index, db_multipart_part_id, db_slab_id, offset, length)
								VALUES (?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement to insert slice: %w", err)
	}
	defer insertSliceStmt.Close()

	for i := range slices {
		res, err := insertSliceStmt.Exec(ctx,
			time.Now(),
			objID,
			uint(i+1),
			nil,
			slabIDs[i],
			slices[i].Offset,
			slices[i].Length,
		)
		if err != nil {
			return fmt.Errorf("failed to insert slice: %w", err)
		} else if n, err := res.RowsAffected(); err != nil {
			return fmt.Errorf("failed to get rows affected: %w", err)
		} else if n == 0 {
			return fmt.Errorf("failed to insert slice: no rows affected")
		}
	}

	// insert sectors
	var upsertSectors []upsertSector
	for i, ss := range slices {
		for j := range ss.Shards {
			upsertSectors = append(upsertSectors, upsertSector{
				slabIDs[i],
				j + 1,
				ss.Shards[j].LatestHost,
				ss.Shards[j].Root,
			})
		}
	}
	sectorIDs, err := tx.upsertSectors(ctx, upsertSectors)
	if err != nil {
		return fmt.Errorf("failed to insert sectors: %w", err)
	}

	// insert contract <-> sector links
	sectorIdx := 0
	var upsertContractSectors []upsertContractSector
	for _, ss := range slices {
		for _, shard := range ss.Shards {
			for _, fcids := range shard.Contracts {
				for _, fcid := range fcids {
					if _, ok := usedContracts[fcid]; ok {
						upsertContractSectors = append(upsertContractSectors, upsertContractSector{
							sectorIDs[sectorIdx],
							usedContracts[fcid].ID,
						})
					} else {
						tx.log.Named("InsertObject").Warn("missing contract for shard",
							"contract", fcid,
							"root", shard.Root,
							"latest_host", shard.LatestHost,
						)
					}
				}
			}
			sectorIdx++
		}
	}
	if err := tx.upsertContractSectors(ctx, upsertContractSectors); err != nil {
		return err
	}

	// update metadata
	insertMetadataStmt, err := tx.Prepare(ctx, "INSERT INTO object_user_metadata (created_at, db_object_id, key, value) VALUES (?, ?, ?, ?)")
	if err != nil {
		return fmt.Errorf("failed to prepare statement to insert object metadata: %w", err)
	}
	defer insertMetadataStmt.Close()

	for k, v := range md {
		if _, err := insertMetadataStmt.Exec(ctx, time.Now(), objID, k, v); err != nil {
			return fmt.Errorf("failed to insert object metadata: %w", err)
		}
	}
	return nil
}

func (tx *MainDatabaseTx) MakeDirsForPath(ctx context.Context, path string) (int64, error) {
	insertDirStmt, err := tx.Prepare(ctx, "INSERT INTO directories (name, db_parent_id) VALUES (?, ?) ON CONFLICT(name) DO NOTHING")
	if err != nil {
		return 0, fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer insertDirStmt.Close()

	queryDirStmt, err := tx.Prepare(ctx, "SELECT id FROM directories WHERE name = ?")
	if err != nil {
		return 0, fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer queryDirStmt.Close()

	// Create root dir.
	dirID := int64(sql.DirectoriesRootID)
	if _, err := tx.Exec(ctx, "INSERT INTO directories (id, name, db_parent_id) VALUES (?, '/', NULL) ON CONFLICT(id) DO NOTHING", dirID); err != nil {
		return 0, fmt.Errorf("failed to create root directory: %w", err)
	}

	// Create remaining directories.
	path = strings.TrimSuffix(path, "/")
	if path == "/" {
		return dirID, nil
	}
	for i := 0; i < utf8.RuneCountInString(path); i++ {
		if path[i] != '/' {
			continue
		}
		dir := path[:i+1]
		if dir == "/" {
			continue
		}
		if _, err := insertDirStmt.Exec(ctx, dir, dirID); err != nil {
			return 0, fmt.Errorf("failed to create directory %v: %w", dir, err)
		}
		var childID int64
		if err := queryDirStmt.QueryRow(ctx, dir).Scan(&childID); err != nil {
			return 0, fmt.Errorf("failed to fetch directory id %v: %w", dir, err)
		} else if childID == 0 {
			return 0, fmt.Errorf("dir we just created doesn't exist - shouldn't happen")
		}
		dirID = childID
	}
	return dirID, nil
}

func (tx *MainDatabaseTx) PruneEmptydirs(ctx context.Context) error {
	stmt, err := tx.Prepare(ctx, `
	DELETE
	FROM directories
	WHERE directories.id != 1
	AND NOT EXISTS (SELECT 1 FROM objects WHERE objects.db_directory_id = directories.id)
	AND NOT EXISTS (SELECT 1 FROM (SELECT 1 FROM directories AS d WHERE d.db_parent_id = directories.id) i)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for {
		res, err := stmt.Exec(ctx)
		if err != nil {
			return err
		} else if n, err := res.RowsAffected(); err != nil {
			return err
		} else if n == 0 {
			return nil
		}
	}
}

func (tx *MainDatabaseTx) PruneSlabs(ctx context.Context, limit int64) (int64, error) {
	res, err := tx.Exec(ctx, `
	DELETE FROM slabs
	WHERE id IN (
    SELECT id
    FROM (
        SELECT slabs.id
        FROM slabs
        WHERE NOT EXISTS (
            SELECT 1 FROM slices WHERE slices.db_slab_id = slabs.id
        )
        AND slabs.db_buffered_slab_id IS NULL
        LIMIT ?
    ) AS limited
	)`, limit)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (tx *MainDatabaseTx) RenameObject(ctx context.Context, bucket, keyOld, keyNew string, dirID int64, force bool) error {
	if force {
		// delete potentially existing object at destination
		if _, err := tx.DeleteObject(ctx, bucket, keyNew); err != nil {
			return fmt.Errorf("RenameObject: failed to delete object: %w", err)
		}
	} else {
		var exists bool
		if err := tx.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM objects WHERE object_id = ? AND db_bucket_id = (SELECT id FROM buckets WHERE buckets.name = ?))", keyNew, bucket).Scan(&exists); err != nil {
			return err
		} else if exists {
			return api.ErrObjectExists
		}
	}
	resp, err := tx.Exec(ctx, `UPDATE objects SET object_id = ?, db_directory_id = ? WHERE object_id = ? AND db_bucket_id = (SELECT id FROM buckets WHERE buckets.name = ?)`, keyNew, dirID, keyOld, bucket)
	if err != nil {
		return err
	} else if n, err := resp.RowsAffected(); err != nil {
		return err
	} else if n == 0 {
		return fmt.Errorf("%w: key %v", api.ErrObjectNotFound, keyOld)
	}
	return nil
}

func (tx *MainDatabaseTx) RenameObjects(ctx context.Context, bucket, prefixOld, prefixNew string, dirID int64, force bool) error {
	if force {
		_, err := tx.Exec(ctx, `
		DELETE
		FROM objects
		WHERE object_id IN (
			SELECT CONCAT(?, SUBSTR(object_id, ?))
			FROM objects
			WHERE object_id LIKE ?
			AND SUBSTR(object_id, 1, ?) = ?
			AND db_bucket_id = (SELECT id FROM buckets WHERE buckets.name = ?)
		)`,
			prefixNew,
			utf8.RuneCountInString(prefixOld)+1,
			prefixOld+"%",
			utf8.RuneCountInString(prefixOld), prefixOld,
			bucket)
		if err != nil {
			return err
		}
	}
	resp, err := tx.Exec(ctx, `
		UPDATE objects
		SET object_id = ? || SUBSTR(object_id, ?),
		db_directory_id = ?
		WHERE object_id LIKE ? 
		AND SUBSTR(object_id, 1, ?) = ? 
		AND db_bucket_id = (SELECT id FROM buckets WHERE buckets.name = ?)`,
		prefixNew, utf8.RuneCountInString(prefixOld)+1,
		dirID,
		prefixOld+"%",
		utf8.RuneCountInString(prefixOld), prefixOld,
		bucket)
	if err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed") {
		return api.ErrObjectExists
	} else if err != nil {
		return err
	} else if n, err := resp.RowsAffected(); err != nil {
		return err
	} else if n == 0 {
		return fmt.Errorf("%w: prefix %v", api.ErrObjectNotFound, prefixOld)
	}
	return nil
}

func (tx *MainDatabaseTx) UpdateSlab(ctx context.Context, s object.Slab, contractSet string, fcids []types.FileContractID) error {
	// find all used contracts
	usedContracts, err := ssql.FetchUsedContracts(ctx, tx, fcids)
	if err != nil {
		return fmt.Errorf("failed to fetch used contracts: %w", err)
	}

	// extract the slab key
	key, err := s.Key.MarshalBinary()
	if err != nil {
		return fmt.Errorf("failed to marshal slab key: %w", err)
	}

	// update slab
	var slabID, totalShards int64
	err = tx.QueryRow(ctx, `
		UPDATE slabs
		SET db_contract_set_id = (SELECT id FROM contract_sets WHERE name = ?),
		health_valid_until = ?,
		health = ?
		WHERE key = ?
		RETURNING id, total_shards
	`, contractSet, time.Now().Unix(), 1, ssql.SecretKey(key)).
		Scan(&slabID, &totalShards)
	if errors.Is(err, dsql.ErrNoRows) {
		return fmt.Errorf("%w: slab with key '%s' not found: %w", api.ErrSlabNotFound, string(key), err)
	} else if err != nil {
		return err
	}

	// find shards of slab
	var roots []types.Hash256
	rows, err := tx.Query(ctx, "SELECT root FROM sectors WHERE db_slab_id = ? ORDER BY sectors.slab_index ASC", slabID)
	if err != nil {
		return fmt.Errorf("failed to fetch sectors: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var root ssql.Hash256
		if err := rows.Scan(&root); err != nil {
			return fmt.Errorf("failed to scan sector id: %w", err)
		}
		roots = append(roots, types.Hash256(root))
	}
	nSectors := len(roots)

	// make sure the number of shards doesn't change.
	// NOTE: check both the slice as well as the TotalShards field to be
	// safe.
	if len(s.Shards) != int(totalShards) {
		return fmt.Errorf("%w: expected %v shards (TotalShards) but got %v", sql.ErrInvalidNumberOfShards, totalShards, len(s.Shards))
	} else if len(s.Shards) != nSectors {
		return fmt.Errorf("%w: expected %v shards (Shards) but got %v", sql.ErrInvalidNumberOfShards, nSectors, len(s.Shards))
	}

	// make sure the roots stay the same.
	for i, root := range roots {
		if root != types.Hash256(s.Shards[i].Root) {
			return fmt.Errorf("%w: shard %v has changed root from %v to %v", sql.ErrShardRootChanged, i, s.Shards[i].Root, root[:])
		}
	}

	// update sectors
	var upsertSectors []upsertSector
	for i := range s.Shards {
		upsertSectors = append(upsertSectors, upsertSector{
			slabID,
			i + 1,
			s.Shards[i].LatestHost,
			s.Shards[i].Root,
		})
	}
	sectorIDs, err := tx.upsertSectors(ctx, upsertSectors)
	if err != nil {
		return fmt.Errorf("failed to insert sectors: %w", err)
	}

	// build contract <-> sector links
	var upsertContractSectors []upsertContractSector
	for i, shard := range s.Shards {
		sectorID := sectorIDs[i]

		// ensure the associations are updated
		for _, fcids := range shard.Contracts {
			for _, fcid := range fcids {
				if _, ok := usedContracts[fcid]; ok {
					upsertContractSectors = append(upsertContractSectors, upsertContractSector{
						sectorID,
						usedContracts[fcid].ID,
					})
				} else {
					tx.log.Named("UpdateSlab").Warn("missing contract for shard",
						"contract", fcid,
						"root", shard.Root,
						"latest_host", shard.LatestHost,
					)
				}
			}
		}
	}
	if err := tx.upsertContractSectors(ctx, upsertContractSectors); err != nil {
		return err
	}

	return nil
}

type upsertContractSector struct {
	sectorID   int64
	contractID int64
}

func (tx *MainDatabaseTx) upsertContractSectors(ctx context.Context, contractSectors []upsertContractSector) error {
	if len(contractSectors) == 0 {
		return nil
	}

	// insert contract <-> sector links
	insertContractSectorStmt, err := tx.Prepare(ctx, `INSERT INTO contract_sectors (db_sector_id, db_contract_id)
											VALUES (?, ?) ON CONFLICT(db_sector_id, db_contract_id) DO NOTHING`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement to insert contract sector link: %w", err)
	}
	defer insertContractSectorStmt.Close()

	for _, cs := range contractSectors {
		_, err := insertContractSectorStmt.Exec(ctx,
			cs.sectorID,
			cs.contractID,
		)
		if err != nil {
			return fmt.Errorf("failed to insert contract sector link: %w", err)
		}
	}
	return nil
}

type upsertSector struct {
	slabID     int64
	slabIndex  int
	latestHost types.PublicKey
	root       types.Hash256
}

func (tx *MainDatabaseTx) upsertSectors(ctx context.Context, sectors []upsertSector) ([]int64, error) {
	if len(sectors) == 0 {
		return nil, nil
	}

	// insert sectors - make sure to update last_insert_id in case of a
	// duplicate key to be able to retrieve the id
	insertSectorStmt, err := tx.Prepare(ctx, `INSERT INTO sectors (created_at, db_slab_id, slab_index, latest_host, root)
								VALUES (?, ?, ?, ?, ?) ON CONFLICT(root) DO UPDATE SET latest_host = EXCLUDED.latest_host RETURNING id, db_slab_id`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare statement to insert sector: %w", err)
	}
	defer insertSectorStmt.Close()

	var sectorIDs []int64
	for _, s := range sectors {
		var sectorID, slabID int64
		err := insertSectorStmt.QueryRow(ctx,
			time.Now(),
			s.slabID,
			s.slabIndex,
			ssql.PublicKey(s.latestHost),
			s.root[:],
		).Scan(&sectorID, &slabID)
		if err != nil {
			return nil, fmt.Errorf("failed to insert sector: %w", err)
		} else if slabID != s.slabID {
			return nil, fmt.Errorf("failed to insert sector for slab %v: already exists for slab %v", s.slabID, slabID)
		}
		sectorIDs = append(sectorIDs, sectorID)
	}
	return sectorIDs, nil
}
