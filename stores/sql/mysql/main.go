package mysql

import (
	"context"
	dsql "database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/object"
	ssql "go.sia.tech/renterd/stores/sql"
	"go.sia.tech/renterd/webhooks"
	"lukechampine.com/frand"

	"go.sia.tech/renterd/internal/sql"

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

// NewMainDatabase creates a new MySQL backend.
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
	return b.db.Close()
}

func (b *MainDatabase) CreateMigrationTable(ctx context.Context) error {
	return createMigrationTable(ctx, b.db)
}

func (b *MainDatabase) DB() *sql.DB {
	return b.db
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

func (tx *MainDatabaseTx) Accounts(ctx context.Context) ([]api.Account, error) {
	return ssql.Accounts(ctx, tx)
}

func (tx *MainDatabaseTx) AddMultipartPart(ctx context.Context, bucket, path, contractSet, eTag, uploadID string, partNumber int, slices object.SlabSlices) error {
	// fetch contract set
	var csID int64
	err := tx.QueryRow(ctx, "SELECT id FROM contract_sets WHERE name = ?", contractSet).
		Scan(&csID)
	if errors.Is(err, dsql.ErrNoRows) {
		return api.ErrContractSetNotFound
	} else if err != nil {
		return fmt.Errorf("failed to fetch contract set id: %w", err)
	}

	// find multipart upload
	var muID int64
	err = tx.QueryRow(ctx, "SELECT id FROM multipart_uploads WHERE upload_id = ?", uploadID).
		Scan(&muID)
	if err != nil {
		return fmt.Errorf("failed to fetch multipart upload: %w", err)
	}

	// delete a potentially existing part
	_, err = tx.Exec(ctx, "DELETE FROM multipart_parts WHERE db_multipart_upload_id = ? AND part_number = ?",
		muID, partNumber)
	if err != nil {
		return fmt.Errorf("failed to delete existing part: %w", err)
	}

	// insert new part
	var size uint64
	for _, slice := range slices {
		size += uint64(slice.Length)
	}
	var partID int64
	res, err := tx.Exec(ctx, "INSERT INTO multipart_parts (created_at, etag, part_number, size, db_multipart_upload_id) VALUES (?, ?, ?, ?, ?)",
		time.Now(), eTag, partNumber, size, muID)
	if err != nil {
		return fmt.Errorf("failed to insert part: %w", err)
	} else if partID, err = res.LastInsertId(); err != nil {
		return fmt.Errorf("failed to fetch part id: %w", err)
	}

	// create slices
	return tx.insertSlabs(ctx, nil, &partID, contractSet, slices)
}

func (tx *MainDatabaseTx) AbortMultipartUpload(ctx context.Context, bucket, path string, uploadID string) error {
	return ssql.AbortMultipartUpload(ctx, tx, bucket, path, uploadID)
}

func (tx *MainDatabaseTx) AddWebhook(ctx context.Context, wh webhooks.Webhook) error {
	headers := "{}"
	if len(wh.Headers) > 0 {
		h, err := json.Marshal(wh.Headers)
		if err != nil {
			return fmt.Errorf("failed to marshal headers: %w", err)
		}
		headers = string(h)
	}
	_, err := tx.Exec(ctx, "INSERT INTO webhooks (created_at, module, event, url, headers) VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE headers = VALUES(headers)",
		time.Now(), wh.Module, wh.Event, wh.URL, headers)
	if err != nil {
		return fmt.Errorf("failed to insert webhook: %w", err)
	}
	return nil
}

func (tx *MainDatabaseTx) ArchiveContract(ctx context.Context, fcid types.FileContractID, reason string) error {
	return ssql.ArchiveContract(ctx, tx, fcid, reason)
}

func (tx *MainDatabaseTx) Autopilot(ctx context.Context, id string) (api.Autopilot, error) {
	return ssql.Autopilot(ctx, tx, id)
}

func (tx *MainDatabaseTx) Autopilots(ctx context.Context) ([]api.Autopilot, error) {
	return ssql.Autopilots(ctx, tx)
}

func (tx *MainDatabaseTx) Bucket(ctx context.Context, bucket string) (api.Bucket, error) {
	return ssql.Bucket(ctx, tx, bucket)
}

func (tx *MainDatabaseTx) CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, parts []api.MultipartCompletedPart, opts api.CompleteMultipartOptions) (string, error) {
	mpu, neededParts, size, eTag, err := ssql.MultipartUploadForCompletion(ctx, tx, bucket, key, uploadID, parts)
	if err != nil {
		return "", fmt.Errorf("failed to fetch multipart upload: %w", err)
	}

	// create the directory.
	dirID, err := tx.MakeDirsForPath(ctx, key)
	if err != nil {
		return "", fmt.Errorf("failed to create directory for key %s: %w", key, err)
	}

	// create the object
	objID, err := ssql.InsertObject(ctx, tx, key, dirID, mpu.BucketID, size, mpu.EC, mpu.MimeType, eTag)
	if err != nil {
		return "", fmt.Errorf("failed to insert object: %w", err)
	}

	// update slices
	updateSlicesStmt, err := tx.Prepare(ctx, `
			UPDATE slices s
			INNER JOIN multipart_parts mpp ON s.db_multipart_part_id = mpp.id
			SET s.db_object_id = ?,
				s.db_multipart_part_id = NULL,
				s.object_index = s.object_index + ?
			WHERE mpp.id = ?
	`)
	if err != nil {
		return "", fmt.Errorf("failed to prepare statement to update slices: %w", err)
	}
	defer updateSlicesStmt.Close()

	var updatedSlices int64
	for _, part := range neededParts {
		res, err := updateSlicesStmt.Exec(ctx, objID, updatedSlices, part.ID)
		if err != nil {
			return "", fmt.Errorf("failed to update slices: %w", err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return "", fmt.Errorf("failed to get rows affected: %w", err)
		}
		updatedSlices += n
	}

	// create/update metadata
	if err := ssql.InsertMetadata(ctx, tx, &objID, nil, opts.Metadata); err != nil {
		return "", fmt.Errorf("failed to insert object metadata: %w", err)
	}
	_, err = tx.Exec(ctx, "UPDATE object_user_metadata SET db_multipart_upload_id = NULL, db_object_id = ? WHERE db_multipart_upload_id = ?",
		objID, mpu.ID)
	if err != nil {
		return "", fmt.Errorf("failed to update object metadata: %w", err)
	}

	// delete the multipart upload
	if _, err := tx.Exec(ctx, "DELETE FROM multipart_uploads WHERE id = ?", mpu.ID); err != nil {
		return "", fmt.Errorf("failed to delete multipart upload: %w", err)
	}

	return eTag, nil
}

func (tx *MainDatabaseTx) Contracts(ctx context.Context, opts api.ContractsOpts) ([]api.ContractMetadata, error) {
	return ssql.Contracts(ctx, tx, opts)
}

func (tx *MainDatabaseTx) ContractSize(ctx context.Context, id types.FileContractID) (api.ContractSize, error) {
	return ssql.ContractSize(ctx, tx, id)
}

func (tx *MainDatabaseTx) CopyObject(ctx context.Context, srcBucket, dstBucket, srcKey, dstKey, mimeType string, metadata api.ObjectUserMetadata) (api.ObjectMetadata, error) {
	return ssql.CopyObject(ctx, tx, srcBucket, dstBucket, srcKey, dstKey, mimeType, metadata)
}

func (tx *MainDatabaseTx) CreateBucket(ctx context.Context, bucket string, bp api.BucketPolicy) error {
	policy, err := json.Marshal(bp)
	if err != nil {
		return err
	}
	res, err := tx.Exec(ctx, "INSERT INTO buckets (created_at, name, policy) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE id = id",
		time.Now(), bucket, policy)
	if err != nil {
		return fmt.Errorf("failed to create bucket: %w", err)
	} else if n, err := res.RowsAffected(); err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	} else if n == 0 {
		return api.ErrBucketExists
	}
	return nil
}

func (tx *MainDatabaseTx) DeleteHostSector(ctx context.Context, hk types.PublicKey, root types.Hash256) (int, error) {
	return ssql.DeleteHostSector(ctx, tx, hk, root)
}

func (tx *MainDatabaseTx) InsertBufferedSlab(ctx context.Context, fileName string, contractSetID int64, ec object.EncryptionKey, minShards, totalShards uint8) (int64, error) {
	return ssql.InsertBufferedSlab(ctx, tx, fileName, contractSetID, ec, minShards, totalShards)
}

func (tx *MainDatabaseTx) InsertMultipartUpload(ctx context.Context, bucket, key string, ec object.EncryptionKey, mimeType string, metadata api.ObjectUserMetadata) (string, error) {
	return ssql.InsertMultipartUpload(ctx, tx, bucket, key, ec, mimeType, metadata)
}

func (tx *MainDatabaseTx) DeleteWebhook(ctx context.Context, wh webhooks.Webhook) error {
	return ssql.DeleteWebhook(ctx, tx, wh)
}

func (tx *MainDatabaseTx) DeleteBucket(ctx context.Context, bucket string) error {
	return ssql.DeleteBucket(ctx, tx, bucket)
}

func (tx *MainDatabaseTx) DeleteObject(ctx context.Context, bucket string, key string) (bool, error) {
	// check if the object exists first to avoid unnecessary locking for the
	// common case
	var objID uint
	err := tx.QueryRow(ctx, "SELECT id FROM objects WHERE object_id = ? AND db_bucket_id = (SELECT id FROM buckets WHERE buckets.name = ?)", key, bucket).Scan(&objID)
	if errors.Is(err, dsql.ErrNoRows) {
		return false, nil
	} else if err != nil {
		return false, err
	}

	resp, err := tx.Exec(ctx, "DELETE FROM objects WHERE id = ?", objID)
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
	DELETE o
	FROM objects o
	JOIN (
		SELECT id
		FROM objects
		WHERE object_id LIKE ? AND db_bucket_id = (
		    SELECT id FROM buckets WHERE buckets.name = ?
		)
		LIMIT ?
	) AS limited ON o.id = limited.id`,
		key+"%", bucket, limit)
	if err != nil {
		return false, err
	} else if n, err := resp.RowsAffected(); err != nil {
		return false, err
	} else {
		return n != 0, nil
	}
}

func (tx *MainDatabaseTx) HostAllowlist(ctx context.Context) ([]types.PublicKey, error) {
	return ssql.HostAllowlist(ctx, tx)
}

func (tx *MainDatabaseTx) HostBlocklist(ctx context.Context) ([]string, error) {
	return ssql.HostBlocklist(ctx, tx)
}

func (tx *MainDatabaseTx) HostsForScanning(ctx context.Context, maxLastScan time.Time, offset, limit int) ([]api.HostAddress, error) {
	return ssql.HostsForScanning(ctx, tx, maxLastScan, offset, limit)
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
	objID, err := ssql.InsertObject(ctx, tx, key, dirID, bucketID, o.TotalSize(), objKey, mimeType, eTag)
	if err != nil {
		return fmt.Errorf("failed to insert object: %w", err)
	}

	// insert slabs
	if err := tx.insertSlabs(ctx, &objID, nil, contractSet, o.Slabs); err != nil {
		return fmt.Errorf("failed to insert slabs: %w", err)
	}

	// insert metadata
	if err := ssql.InsertMetadata(ctx, tx, &objID, nil, md); err != nil {
		return fmt.Errorf("failed to insert object metadata: %w", err)
	}
	return nil
}

func (tx *MainDatabaseTx) InvalidateSlabHealthByFCID(ctx context.Context, fcids []types.FileContractID, limit int64) (int64, error) {
	if len(fcids) == 0 {
		return 0, nil
	}
	// prepare args
	var args []any
	for _, fcid := range fcids {
		args = append(args, ssql.FileContractID(fcid))
	}
	args = append(args, time.Now().Unix())
	args = append(args, limit)
	res, err := tx.Exec(ctx, fmt.Sprintf(`
		UPDATE slabs SET health_valid_until = 0 WHERE id in (
			SELECT *
			FROM (
				SELECT slabs.id
				FROM slabs
				INNER JOIN sectors se ON se.db_slab_id = slabs.id
				INNER JOIN contract_sectors cs ON cs.db_sector_id = se.id
				INNER JOIN contracts c ON c.id = cs.db_contract_id
				WHERE c.fcid IN (%s) AND slabs.health_valid_until >= ?
				LIMIT ?
			) slab_ids
		)
	`, strings.Repeat("?, ", len(fcids)-1)+"?"), args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (tx *MainDatabaseTx) ListBuckets(ctx context.Context) ([]api.Bucket, error) {
	return ssql.ListBuckets(ctx, tx)
}

func (tx *MainDatabaseTx) MakeDirsForPath(ctx context.Context, path string) (int64, error) {
	// Create root dir.
	dirID := int64(sql.DirectoriesRootID)
	if _, err := tx.Exec(ctx, "INSERT IGNORE INTO directories (id, name, db_parent_id) VALUES (?, '/', NULL)", dirID); err != nil {
		return 0, fmt.Errorf("failed to create root directory: %w", err)
	}

	path = strings.TrimSuffix(path, "/")
	if path == "/" {
		return dirID, nil
	}

	// Create remaining directories.
	insertDirStmt, err := tx.Prepare(ctx, "INSERT INTO directories (name, db_parent_id) VALUES (?, ?) ON DUPLICATE KEY UPDATE id = last_insert_id(id)")
	if err != nil {
		return 0, fmt.Errorf("failed to prepare statement to insert dir: %w", err)
	}
	defer insertDirStmt.Close()

	for i := 0; i < utf8.RuneCountInString(path); i++ {
		if path[i] != '/' {
			continue
		}
		dir := path[:i+1]
		if dir == "/" {
			continue
		}
		if res, err := insertDirStmt.Exec(ctx, dir, dirID); err != nil {
			return 0, fmt.Errorf("failed to create directory %v: %w", dir, err)
		} else if dirID, err = res.LastInsertId(); err != nil {
			return 0, fmt.Errorf("failed to fetch directory id %v: %w", dir, err)
		}
	}
	return dirID, nil
}

func (tx *MainDatabaseTx) MultipartUpload(ctx context.Context, uploadID string) (api.MultipartUpload, error) {
	return ssql.MultipartUpload(ctx, tx, uploadID)
}

func (tx *MainDatabaseTx) MultipartUploadParts(ctx context.Context, bucket, key, uploadID string, marker int, limit int64) (api.MultipartListPartsResponse, error) {
	return ssql.MultipartUploadParts(ctx, tx, bucket, key, uploadID, marker, limit)
}

func (tx *MainDatabaseTx) MultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker string, limit int) (api.MultipartListUploadsResponse, error) {
	return ssql.MultipartUploads(ctx, tx, bucket, prefix, keyMarker, uploadIDMarker, limit)
}

func (tx *MainDatabaseTx) ObjectsStats(ctx context.Context, opts api.ObjectsStatsOpts) (api.ObjectsStatsResponse, error) {
	return ssql.ObjectsStats(ctx, tx, opts)
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

func (tx *MainDatabaseTx) RecordHostScans(ctx context.Context, scans []api.HostScan) error {
	return ssql.RecordHostScans(ctx, tx, scans)
}

func (tx *MainDatabaseTx) RecordPriceTables(ctx context.Context, priceTableUpdates []api.HostPriceTableUpdate) error {
	return ssql.RecordPriceTables(ctx, tx, priceTableUpdates)
}

func (tx *MainDatabaseTx) RemoveOfflineHosts(ctx context.Context, minRecentFailures uint64, maxDownTime time.Duration) (int64, error) {
	return ssql.RemoveOfflineHosts(ctx, tx, minRecentFailures, maxDownTime)
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
			SELECT *
			FROM (
				SELECT CONCAT(?, SUBSTR(object_id, ?))
				FROM objects
				WHERE object_id LIKE ?
				AND db_bucket_id = (SELECT id FROM buckets WHERE buckets.name = ?)
			) as i
		)`,
			prefixNew,
			utf8.RuneCountInString(prefixOld)+1,
			prefixOld+"%",
			bucket)
		if err != nil {
			return err
		}
	}
	resp, err := tx.Exec(ctx, `
		UPDATE objects
		SET object_id = CONCAT(?, SUBSTR(object_id, ?)),
		db_directory_id = ?
		WHERE object_id LIKE ?
		AND db_bucket_id = (SELECT id FROM buckets WHERE buckets.name = ?)`,
		prefixNew, utf8.RuneCountInString(prefixOld)+1,
		dirID,
		prefixOld+"%",
		bucket)
	if err != nil && strings.Contains(err.Error(), "Duplicate entry") {
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

func (tx *MainDatabaseTx) ResetLostSectors(ctx context.Context, hk types.PublicKey) error {
	return ssql.ResetLostSectors(ctx, tx, hk)
}

func (tx MainDatabaseTx) SaveAccounts(ctx context.Context, accounts []api.Account) error {
	// clean_shutdown = 1 after save
	stmt, err := tx.Prepare(ctx, `
		INSERT INTO ephemeral_accounts (created_at, account_id, clean_shutdown, host, balance, drift, requires_sync)
		VAlUES (?, ?, 1, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
		account_id = VALUES(account_id),
		clean_shutdown = 1,
		host = VALUES(host),
		balance = VALUES(balance),
		drift = VALUES(drift),
		requires_sync = VALUES(requires_sync)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, acc := range accounts {
		res, err := stmt.Exec(ctx, time.Now(), (ssql.PublicKey)(acc.ID), (ssql.PublicKey)(acc.HostKey), (*ssql.BigInt)(acc.Balance), (*ssql.BigInt)(acc.Drift), acc.RequiresSync)
		if err != nil {
			return fmt.Errorf("failed to insert account %v: %w", acc.ID, err)
		} else if n, err := res.RowsAffected(); err != nil {
			return fmt.Errorf("failed to get rows affected: %w", err)
		} else if n != 1 && n != 2 { // 1 for insert, 2 for update
			return fmt.Errorf("expected 1 row affected, got %v", n)
		}
	}
	return nil
}

func (tx *MainDatabaseTx) SearchHosts(ctx context.Context, autopilotID, filterMode, usabilityMode, addressContains string, keyIn []types.PublicKey, offset, limit int) ([]api.Host, error) {
	return ssql.SearchHosts(ctx, tx, autopilotID, filterMode, usabilityMode, addressContains, keyIn, offset, limit)
}

func (tx *MainDatabaseTx) SetUncleanShutdown(ctx context.Context) error {
	return ssql.SetUncleanShutdown(ctx, tx)
}

func (tx *MainDatabaseTx) SlabBuffers(ctx context.Context) (map[string]string, error) {
	return ssql.SlabBuffers(ctx, tx)
}

func (tx *MainDatabaseTx) UpdateAutopilot(ctx context.Context, ap api.Autopilot) error {
	res, err := tx.Exec(ctx, `
		INSERT INTO autopilots (created_at, identifier, config, current_period)
		VALUES (?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
		config = VALUES(config),
		current_period = VALUES(current_period)
	`, time.Now(), ap.ID, (*ssql.AutopilotConfig)(&ap.Config), ap.CurrentPeriod)
	if err != nil {
		return err
	} else if n, err := res.RowsAffected(); err != nil {
		return err
	} else if n != 1 && n != 2 { // 1 if inserted, 2 if updated
		return fmt.Errorf("expected 1 row affected, got %v", n)
	}
	return nil
}

func (tx *MainDatabaseTx) UpdateBucketPolicy(ctx context.Context, bucket string, bp api.BucketPolicy) error {
	return ssql.UpdateBucketPolicy(ctx, tx, bucket, bp)
}

func (tx *MainDatabaseTx) UpdateHostAllowlistEntries(ctx context.Context, add, remove []types.PublicKey, clear bool) error {
	if clear {
		if _, err := tx.Exec(ctx, "DELETE FROM host_allowlist_entries"); err != nil {
			return fmt.Errorf("failed to clear host allowlist entries: %w", err)
		}
	}

	if len(add) > 0 {
		insertStmt, err := tx.Prepare(ctx, "INSERT INTO host_allowlist_entries (entry) VALUES (?) ON DUPLICATE KEY UPDATE id = last_insert_id(id)")
		if err != nil {
			return fmt.Errorf("failed to prepare insert statement: %w", err)
		}
		defer insertStmt.Close()
		joinStmt, err := tx.Prepare(ctx, `
			INSERT IGNORE INTO host_allowlist_entry_hosts (db_allowlist_entry_id, db_host_id)
			SELECT ?, id FROM (
			SELECT id
			FROM hosts
			WHERE public_key = ?
		) AS _`)
		if err != nil {
			return fmt.Errorf("failed to prepare join statement: %w", err)
		}
		defer joinStmt.Close()

		for _, pk := range add {
			if res, err := insertStmt.Exec(ctx, ssql.PublicKey(pk)); err != nil {
				return fmt.Errorf("failed to insert host allowlist entry: %w", err)
			} else if entryID, err := res.LastInsertId(); err != nil {
				return fmt.Errorf("failed to fetch host allowlist entry id: %w", err)
			} else if _, err := joinStmt.Exec(ctx, entryID, ssql.PublicKey(pk)); err != nil {
				return fmt.Errorf("failed to join host allowlist entry: %w", err)
			}
		}
	}

	if !clear && len(remove) > 0 {
		deleteStmt, err := tx.Prepare(ctx, "DELETE FROM host_allowlist_entries WHERE entry = ?")
		if err != nil {
			return fmt.Errorf("failed to prepare delete statement: %w", err)
		}
		defer deleteStmt.Close()

		for _, pk := range remove {
			if _, err := deleteStmt.Exec(ctx, ssql.PublicKey(pk)); err != nil {
				return fmt.Errorf("failed to delete host allowlist entry: %w", err)
			}
		}
	}
	return nil
}

func (tx *MainDatabaseTx) UpdateHostBlocklistEntries(ctx context.Context, add, remove []string, clear bool) error {
	if clear {
		if _, err := tx.Exec(ctx, "DELETE FROM host_blocklist_entries"); err != nil {
			return fmt.Errorf("failed to clear host blocklist entries: %w", err)
		}
	}

	if len(add) > 0 {
		insertStmt, err := tx.Prepare(ctx, "INSERT INTO host_blocklist_entries (entry) VALUES (?) ON DUPLICATE KEY UPDATE id = last_insert_id(id)")
		if err != nil {
			return fmt.Errorf("failed to prepare insert statement: %w", err)
		}
		defer insertStmt.Close()
		joinStmt, err := tx.Prepare(ctx, `
		INSERT IGNORE INTO host_blocklist_entry_hosts (db_blocklist_entry_id, db_host_id)
		SELECT ?, id FROM (
			SELECT id
			FROM hosts
			WHERE net_address=? OR
			SUBSTRING_INDEX(net_address,':',1) = ? OR
			SUBSTRING_INDEX(net_address,':',1) LIKE ?
		) AS _
		`)
		if err != nil {
			return fmt.Errorf("failed to prepare join statement: %w", err)
		}
		defer joinStmt.Close()

		for _, entry := range add {
			if res, err := insertStmt.Exec(ctx, entry); err != nil {
				return fmt.Errorf("failed to insert host blocklist entry: %w", err)
			} else if entryID, err := res.LastInsertId(); err != nil {
				return fmt.Errorf("failed to fetch host blocklist entry id: %w", err)
			} else if _, err := joinStmt.Exec(ctx, entryID, entry, entry, fmt.Sprintf("%%.%s", entry)); err != nil {
				return fmt.Errorf("failed to join host blocklist entry: %w", err)
			}
		}
	}

	if !clear && len(remove) > 0 {
		deleteStmt, err := tx.Prepare(ctx, "DELETE FROM host_blocklist_entries WHERE entry = ?")
		if err != nil {
			return fmt.Errorf("failed to prepare delete statement: %w", err)
		}
		defer deleteStmt.Close()

		for _, entry := range remove {
			if _, err := deleteStmt.Exec(ctx, entry); err != nil {
				return fmt.Errorf("failed to delete host blocklist entry: %w", err)
			}
		}
	}
	return nil
}

func (tx *MainDatabaseTx) UpdateHostCheck(ctx context.Context, autopilot string, hk types.PublicKey, hc api.HostCheck) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO host_checks (created_at, db_autopilot_id, db_host_id, usability_blocked, usability_offline, usability_low_score,
			usability_redundant_ip, usability_gouging, usability_not_accepting_contracts, usability_not_announced, usability_not_completing_scan,
			score_age, score_collateral, score_interactions, score_storage_remaining, score_uptime, score_version, score_prices,
			gouging_contract_err, gouging_download_err, gouging_gouging_err, gouging_prune_err, gouging_upload_err)
	    VALUES (?,
			(SELECT id FROM autopilots WHERE identifier = ?),
			(SELECT id FROM hosts WHERE public_key = ?),
			?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			created_at = VALUES(created_at), db_autopilot_id = VALUES(db_autopilot_id), db_host_id = VALUES(db_host_id),
			usability_blocked = VALUES(usability_blocked), usability_offline = VALUES(usability_offline), usability_low_score = VALUES(usability_low_score),
			usability_redundant_ip = VALUES(usability_redundant_ip), usability_gouging = VALUES(usability_gouging), usability_not_accepting_contracts = VALUES(usability_not_accepting_contracts),
			usability_not_announced = VALUES(usability_not_announced), usability_not_completing_scan = VALUES(usability_not_completing_scan),
			score_age = VALUES(score_age), score_collateral = VALUES(score_collateral), score_interactions = VALUES(score_interactions),
			score_storage_remaining = VALUES(score_storage_remaining), score_uptime = VALUES(score_uptime), score_version = VALUES(score_version),
			score_prices = VALUES(score_prices), gouging_contract_err = VALUES(gouging_contract_err), gouging_download_err = VALUES(gouging_download_err),
			gouging_gouging_err = VALUES(gouging_gouging_err), gouging_prune_err = VALUES(gouging_prune_err), gouging_upload_err = VALUES(gouging_upload_err)
	`, time.Now(), autopilot, ssql.PublicKey(hk), hc.Usability.Blocked, hc.Usability.Offline, hc.Usability.LowScore,
		hc.Usability.RedundantIP, hc.Usability.Gouging, hc.Usability.NotAcceptingContracts, hc.Usability.NotAnnounced, hc.Usability.NotCompletingScan,
		hc.Score.Age, hc.Score.Collateral, hc.Score.Interactions, hc.Score.StorageRemaining, hc.Score.Uptime, hc.Score.Version, hc.Score.Prices,
		hc.Gouging.ContractErr, hc.Gouging.DownloadErr, hc.Gouging.GougingErr, hc.Gouging.PruneErr, hc.Gouging.UploadErr,
	)
	if err != nil {
		return fmt.Errorf("failed to insert host check: %w", err)
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
	res, err := tx.Exec(ctx, `
		UPDATE slabs
		SET db_contract_set_id = (SELECT id FROM contract_sets WHERE name = ?),
		health_valid_until = ?,
		health = ?
		WHERE `+"`key`"+` = ?
	`, contractSet, time.Now().Unix(), 1, ssql.SecretKey(key))
	if err != nil {
		return err
	} else if n, err := res.RowsAffected(); err != nil {
		return err
	} else if n == 0 {
		return fmt.Errorf("%w: slab with key '%s' not found: %w", api.ErrSlabNotFound, string(key), err)
	}

	// fetch slab id and total shards
	var slabID, totalShards int64
	err = tx.QueryRow(ctx, "SELECT id, total_shards FROM slabs WHERE `key` = ?", ssql.SecretKey(key)).
		Scan(&slabID, &totalShards)
	if err != nil {
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

func (tx *MainDatabaseTx) UpdateSlabHealth(ctx context.Context, limit int64, minDuration, maxDuration time.Duration) (int64, error) {
	now := time.Now()
	if err := ssql.PrepareSlabHealth(ctx, tx, limit, now); err != nil {
		return 0, fmt.Errorf("failed to compute slab health: %w", err)
	}

	res, err := tx.Exec(ctx, "UPDATE slabs sla INNER JOIN slabs_health h ON sla.id = h.id SET sla.health = h.health, health_valid_until = (FLOOR(? + RAND() * (? - ?)))",
		now.Add(minDuration).Unix(), maxDuration.Seconds(), minDuration.Seconds())
	if err != nil {
		return 0, fmt.Errorf("failed to update slab health: %w", err)
	}

	_, err = tx.Exec(ctx, `
		UPDATE objects o
		INNER JOIN (
			SELECT sli.db_object_id as id, MIN(h.health) as health
			FROM slabs_health h
			INNER JOIN slices sli ON sli.db_slab_id = h.id
			GROUP BY sli.db_object_id
		) AS object_health ON object_health.id = o.id
		SET o.health = object_health.health
		`)
	if err != nil {
		return 0, fmt.Errorf("failed to update object health: %w", err)
	}
	return res.RowsAffected()
}

func (tx *MainDatabaseTx) Webhooks(ctx context.Context) ([]webhooks.Webhook, error) {
	return ssql.Webhooks(ctx, tx)
}

func (tx *MainDatabaseTx) insertSlabs(ctx context.Context, objID, partID *int64, contractSet string, slices object.SlabSlices) error {
	if (objID == nil) == (partID == nil) {
		return errors.New("exactly one of objID and partID must be set")
	} else if len(slices) == 0 {
		return nil // nothing to do
	}

	usedContracts, err := ssql.FetchUsedContracts(ctx, tx.Tx, slices.Contracts())
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
	insertSlabStmt, err := tx.Prepare(ctx, `INSERT INTO slabs (created_at, db_contract_set_id, `+"`key`"+`, min_shards, total_shards)
						VALUES (?, ?, ?, ?, ?)
						ON DUPLICATE KEY UPDATE id = last_insert_id(id)`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement to insert slab: %w", err)
	}
	defer insertSlabStmt.Close()

	querySlabIDStmt, err := tx.Prepare(ctx, "SELECT id FROM slabs WHERE `key` = ?")
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
		res, err := insertSlabStmt.Exec(ctx,
			time.Now(),
			contractSetID,
			ssql.SecretKey(slabKey),
			slices[i].MinShards,
			uint8(len(slices[i].Shards)),
		)
		if err != nil {
			return fmt.Errorf("failed to insert slab: %w", err)
		}
		slabIDs[i], err = res.LastInsertId()
		if err != nil {
			return fmt.Errorf("failed to fetch slab id: %w", err)
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
			partID,
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
	insertContractSectorStmt, err := tx.Prepare(ctx, `INSERT IGNORE INTO contract_sectors (db_sector_id, db_contract_id)
											VALUES (?, ?)`)
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
								VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE latest_host = VALUES(latest_host), id = last_insert_id(id)`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare statement to insert sector: %w", err)
	}
	defer insertSectorStmt.Close()

	querySectorSlabIDStmt, err := tx.Prepare(ctx, "SELECT db_slab_id FROM sectors WHERE id = ?")
	if err != nil {
		return nil, fmt.Errorf("failed to prepare statement to query slab id: %w", err)
	}
	defer querySectorSlabIDStmt.Close()

	var sectorIDs []int64
	for _, s := range sectors {
		var sectorID, slabID int64
		res, err := insertSectorStmt.Exec(ctx,
			time.Now(),
			s.slabID,
			s.slabIndex,
			ssql.PublicKey(s.latestHost),
			s.root[:],
		)
		if err != nil {
			return nil, fmt.Errorf("failed to insert sector: %w", err)
		} else if sectorID, err = res.LastInsertId(); err != nil {
			return nil, fmt.Errorf("failed to fetch sector id: %w", err)
		} else if err := querySectorSlabIDStmt.QueryRow(ctx, sectorID).Scan(&slabID); err != nil {
			return nil, fmt.Errorf("failed to fetch slab id: %w", err)
		} else if slabID != s.slabID {
			return nil, fmt.Errorf("failed to insert sector for slab %v: already exists for slab %v", s.slabID, slabID)
		}
		sectorIDs = append(sectorIDs, sectorID)
	}
	return sectorIDs, nil
}
