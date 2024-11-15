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
	"go.sia.tech/coreutils/syncer"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/object"
	ssql "go.sia.tech/renterd/stores/sql"
	"go.sia.tech/renterd/webhooks"
	"lukechampine.com/frand"

	"go.sia.tech/renterd/internal/sql"

	"go.uber.org/zap"
)

const (
	batchSizeInsertSectors = 500
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
func NewMainDatabase(db *dsql.DB, log *zap.Logger, lqd, ltd time.Duration) (*MainDatabase, error) {
	log = log.Named("main")
	store, err := sql.NewDB(db, log, deadlockMsgs, lqd, ltd)
	return &MainDatabase{
		db:  store,
		log: log.Sugar(),
	}, err
}

func (b *MainDatabase) ApplyMigration(ctx context.Context, fn func(tx sql.Tx) (bool, error)) error {
	return applyMigration(ctx, b.db, fn)
}

func (b *MainDatabase) HasMigration(ctx context.Context, tx sql.Tx, id string) (bool, error) {
	return ssql.HasMigration(ctx, tx, id)
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

func (b *MainDatabase) LoadSlabBuffers(ctx context.Context) ([]ssql.LoadedSlabBuffer, []string, error) {
	return ssql.LoadSlabBuffers(ctx, b.db)
}

func (b *MainDatabase) InsertDirectories(ctx context.Context, tx sql.Tx, bucket, path string) (int64, error) {
	mtx := b.wrapTxn(tx)
	return mtx.InsertDirectoriesDeprecated(ctx, bucket, path)
}

func (b *MainDatabase) MakeDirsForPath(ctx context.Context, tx sql.Tx, path string) (int64, error) {
	mtx := b.wrapTxn(tx)
	return mtx.MakeDirsForPathDeprecated(ctx, path)
}

func (b *MainDatabase) Migrate(ctx context.Context) error {
	return sql.PerformMigrations(ctx, b, migrationsFs, "main", sql.MainMigrations(ctx, b, migrationsFs, b.log))
}

func (b *MainDatabase) Transaction(ctx context.Context, fn func(tx ssql.DatabaseTx) error) error {
	return b.db.Transaction(ctx, func(tx sql.Tx) error {
		return fn(b.wrapTxn(tx))
	})
}

func (b *MainDatabase) UpdateSetting(ctx context.Context, tx sql.Tx, key, value string) error {
	mtx := b.wrapTxn(tx)
	return mtx.UpdateSetting(ctx, key, value)
}

func (b *MainDatabase) Version(ctx context.Context) (string, string, error) {
	return version(ctx, b.db)
}

func (b *MainDatabase) wrapTxn(tx sql.Tx) *MainDatabaseTx {
	return &MainDatabaseTx{tx, b.log.Named(hex.EncodeToString(frand.Bytes(16)))}
}

func (tx *MainDatabaseTx) AbortMultipartUpload(ctx context.Context, bucket, key string, uploadID string) error {
	return ssql.AbortMultipartUpload(ctx, tx, bucket, key, uploadID)
}

func (tx *MainDatabaseTx) Accounts(ctx context.Context, owner string) ([]api.Account, error) {
	return ssql.Accounts(ctx, tx, owner)
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

func (tx *MainDatabaseTx) AddPeer(ctx context.Context, addr string) error {
	_, err := tx.Exec(ctx,
		"INSERT IGNORE INTO syncer_peers (address, first_seen, last_connect, synced_blocks, sync_duration) VALUES (?, ?, ?, ?, ?)",
		addr,
		ssql.UnixTimeMS(time.Now()),
		ssql.UnixTimeMS(time.Time{}),
		0,
		0,
	)
	return err
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

func (tx *MainDatabaseTx) AncestorContracts(ctx context.Context, fcid types.FileContractID, startHeight uint64) ([]api.ContractMetadata, error) {
	return ssql.AncestorContracts(ctx, tx, fcid, startHeight)
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

func (tx *MainDatabaseTx) BanPeer(ctx context.Context, addr string, duration time.Duration, reason string) error {
	cidr, err := ssql.NormalizePeer(addr)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx,
		"INSERT INTO syncer_bans (created_at, net_cidr, expiration, reason) VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE expiration = VALUES(expiration), reason = VALUES(reason)",
		time.Now(),
		cidr,
		ssql.UnixTimeMS(time.Now().Add(duration)),
		reason,
	)
	return err
}

func (tx *MainDatabaseTx) Bucket(ctx context.Context, bucket string) (api.Bucket, error) {
	return ssql.Bucket(ctx, tx, bucket)
}

func (tx *MainDatabaseTx) Buckets(ctx context.Context) ([]api.Bucket, error) {
	return ssql.Buckets(ctx, tx)
}

func (tx *MainDatabaseTx) CharLengthExpr() string {
	return "CHAR_LENGTH"
}

func (tx *MainDatabaseTx) CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, parts []api.MultipartCompletedPart, opts api.CompleteMultipartOptions) (string, error) {
	mpu, neededParts, size, eTag, err := ssql.MultipartUploadForCompletion(ctx, tx, bucket, key, uploadID, parts)
	if err != nil {
		return "", fmt.Errorf("failed to fetch multipart upload: %w", err)
	}

	// create the object
	objID, err := ssql.InsertObject(ctx, tx, key, mpu.BucketID, size, mpu.EC, mpu.MimeType, eTag)
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

func (tx *MainDatabaseTx) Contract(ctx context.Context, fcid types.FileContractID) (api.ContractMetadata, error) {
	return ssql.Contract(ctx, tx, fcid)
}

func (tx *MainDatabaseTx) ContractRoots(ctx context.Context, fcid types.FileContractID) ([]types.Hash256, error) {
	return ssql.ContractRoots(ctx, tx, fcid)
}

func (tx *MainDatabaseTx) Contracts(ctx context.Context, opts api.ContractsOpts) ([]api.ContractMetadata, error) {
	return ssql.Contracts(ctx, tx, opts)
}

func (tx *MainDatabaseTx) ContractSetID(ctx context.Context, contractSet string) (int64, error) {
	return ssql.ContractSetID(ctx, tx, contractSet)
}

func (tx *MainDatabaseTx) ContractSets(ctx context.Context) ([]string, error) {
	return ssql.ContractSets(ctx, tx)
}

func (tx *MainDatabaseTx) ContractSize(ctx context.Context, id types.FileContractID) (api.ContractSize, error) {
	return ssql.ContractSize(ctx, tx, id)
}

func (tx *MainDatabaseTx) ContractSizes(ctx context.Context) (map[types.FileContractID]api.ContractSize, error) {
	return ssql.ContractSizes(ctx, tx)
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

func (tx *MainDatabaseTx) DeleteSetting(ctx context.Context, key string) error {
	return ssql.DeleteSetting(ctx, tx, key)
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

func (tx *MainDatabaseTx) Hosts(ctx context.Context, opts api.HostOptions) ([]api.Host, error) {
	return ssql.Hosts(ctx, tx, opts)
}

func (tx *MainDatabaseTx) InsertBufferedSlab(ctx context.Context, fileName string, contractSetID int64, ec object.EncryptionKey, minShards, totalShards uint8) (int64, error) {
	return ssql.InsertBufferedSlab(ctx, tx, fileName, contractSetID, ec, minShards, totalShards)
}

func (tx *MainDatabaseTx) InsertMultipartUpload(ctx context.Context, bucket, key string, ec object.EncryptionKey, mimeType string, metadata api.ObjectUserMetadata) (string, error) {
	return ssql.InsertMultipartUpload(ctx, tx, bucket, key, ec, mimeType, metadata)
}

func (tx *MainDatabaseTx) InsertObject(ctx context.Context, bucket, key, contractSet string, o object.Object, mimeType, eTag string, md api.ObjectUserMetadata) error {
	// get bucket id
	var bucketID int64
	err := tx.QueryRow(ctx, "SELECT id FROM buckets WHERE buckets.name = ?", bucket).Scan(&bucketID)
	if errors.Is(err, dsql.ErrNoRows) {
		return api.ErrBucketNotFound
	} else if err != nil {
		return fmt.Errorf("failed to fetch bucket id: %w", err)
	}

	// insert object
	objID, err := ssql.InsertObject(ctx, tx, key, bucketID, o.TotalSize(), o.Key, mimeType, eTag)
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

func (tx *MainDatabaseTx) InsertDirectoriesDeprecated(ctx context.Context, bucket, path string) (int64, error) {
	// sanity check input
	if !strings.HasPrefix(path, "/") {
		return 0, errors.New("path has to have a leading slash")
	} else if bucket == "" {
		return 0, errors.New("bucket cannot be empty")
	}

	// fetch bucket
	var bucketID int64
	err := tx.QueryRow(ctx, "SELECT id FROM buckets WHERE buckets.name = ?", bucket).Scan(&bucketID)
	if errors.Is(err, dsql.ErrNoRows) {
		return 0, fmt.Errorf("bucket '%v' not found: %w", bucket, api.ErrBucketNotFound)
	} else if err != nil {
		return 0, fmt.Errorf("failed to fetch bucket id: %w", err)
	}

	// prepare statements
	insertDirStmt, err := tx.Prepare(ctx, "INSERT INTO directories (created_at, db_bucket_id, db_parent_id, name) VALUES (?, ?, ?, ?)")
	if err != nil {
		return 0, fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer insertDirStmt.Close()

	queryDirStmt, err := tx.Prepare(ctx, "SELECT id FROM directories WHERE db_bucket_id = ? AND name = ? FOR UPDATE")
	if err != nil {
		return 0, fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer queryDirStmt.Close()

	// insert directories for give path
	var dirID *int64
	for _, dir := range object.Directories(path) {
		// check if the directory exists
		var existingID int64
		if err := queryDirStmt.QueryRow(ctx, bucketID, dir).Scan(&existingID); err != nil && !errors.Is(err, dsql.ErrNoRows) {
			return 0, fmt.Errorf("failed to fetch directory id %v: %w", dir, err)
		} else if existingID > 0 {
			dirID = &existingID
			continue
		}

		// insert directory
		var insertedID int64
		if _, err := insertDirStmt.Exec(ctx, time.Now(), bucketID, dirID, dir); err != nil {
			return 0, fmt.Errorf("failed to create directory %v: %w", dir, err)
		} else if err := queryDirStmt.QueryRow(ctx, bucketID, dir).Scan(&insertedID); err != nil {
			return 0, fmt.Errorf("failed to fetch directory id %v: %w", dir, err)
		} else if insertedID == 0 {
			return 0, fmt.Errorf("dir we just created doesn't exist - shouldn't happen")
		}
		dirID = &insertedID
	}
	return *dirID, nil
}

func (tx *MainDatabaseTx) MakeDirsForPathDeprecated(ctx context.Context, path string) (int64, error) {
	// Create root dir.
	dirID := int64(1)
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

func (tx *MainDatabaseTx) MarkPackedSlabUploaded(ctx context.Context, slab api.UploadedPackedSlab) (string, error) {
	return ssql.MarkPackedSlabUploaded(ctx, tx, slab)
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

func (tx *MainDatabaseTx) Object(ctx context.Context, bucket, key string) (api.Object, error) {
	return ssql.Object(ctx, tx, bucket, key)
}

func (tx *MainDatabaseTx) Objects(ctx context.Context, bucket, prefix, substring, delim, sortBy, sortDir, marker string, limit int, slabEncryptionKey object.EncryptionKey) (api.ObjectsResponse, error) {
	return ssql.Objects(ctx, tx, bucket, prefix, substring, delim, sortBy, sortDir, marker, limit, slabEncryptionKey)
}

func (tx *MainDatabaseTx) ObjectMetadata(ctx context.Context, bucket, key string) (api.Object, error) {
	return ssql.ObjectMetadata(ctx, tx, bucket, key)
}

func (tx *MainDatabaseTx) ObjectsStats(ctx context.Context, opts api.ObjectsStatsOpts) (api.ObjectsStatsResponse, error) {
	return ssql.ObjectsStats(ctx, tx, opts)
}

func (tx *MainDatabaseTx) PeerBanned(ctx context.Context, addr string) (bool, error) {
	return ssql.PeerBanned(ctx, tx, addr)
}

func (tx *MainDatabaseTx) PeerInfo(ctx context.Context, addr string) (syncer.PeerInfo, error) {
	return ssql.PeerInfo(ctx, tx, addr)
}

func (tx *MainDatabaseTx) Peers(ctx context.Context) ([]syncer.PeerInfo, error) {
	return ssql.Peers(ctx, tx)
}

func (tx *MainDatabaseTx) ProcessChainUpdate(ctx context.Context, fn func(ssql.ChainUpdateTx) error) error {
	return fn(&chainUpdateTx{
		ctx: ctx,
		tx:  tx,
		l:   tx.log.Named("ProcessChainUpdate"),
	})
}

func (tx *MainDatabaseTx) PrunableContractRoots(ctx context.Context, fcid types.FileContractID, roots []types.Hash256) (indices []uint64, err error) {
	// build tmp table name
	tmpTable := strings.ReplaceAll(fmt.Sprintf("tmp_host_roots_%s", fcid.String()[:8]), ":", "_")

	// create temporary table
	_, err = tx.Exec(ctx, fmt.Sprintf(`
DROP TABLE IF EXISTS %s;
CREATE TEMPORARY TABLE %s (idx INT, root varbinary(32)) ENGINE=MEMORY;
CREATE INDEX %s_idx ON %s (root(32));`, tmpTable, tmpTable, tmpTable, tmpTable))
	if err != nil {
		return nil, fmt.Errorf("failed to create temporary table: %w", err)
	}

	// defer removal
	defer func() {
		if _, err := tx.Exec(ctx, fmt.Sprintf(`DROP TABLE %s;`, tmpTable)); err != nil {
			tx.log.Warnw("failed to drop temporary table", zap.Error(err))
		}
	}()

	// insert roots in batches
	for i := 0; i < len(roots); i += batchSizeInsertSectors {
		end := i + batchSizeInsertSectors
		if end > len(roots) {
			end = len(roots)
		}

		var params []interface{}
		for i, r := range roots[i:end] {
			params = append(params, uint64(i), ssql.Hash256(r))
		}

		_, err = tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (idx, root) VALUES %s`, tmpTable, strings.TrimSuffix(strings.Repeat("(?, ?), ", end-i), ", ")), params...)
		if err != nil {
			return nil, fmt.Errorf("failed to insert into roots into temporary table: %w", err)
		}
	}

	// execute query
	rows, err := tx.Query(ctx, fmt.Sprintf(`SELECT idx FROM %s tmp LEFT JOIN sectors s ON s.root = tmp.root WHERE s.root IS NULL`, tmpTable))
	if err != nil {
		return nil, fmt.Errorf("failed to fetch contract roots: %w", err)
	}
	defer rows.Close()

	// fetch indices
	for rows.Next() {
		var idx uint64
		if err := rows.Scan(&idx); err != nil {
			return nil, fmt.Errorf("failed to scan root index: %w", err)
		}
		indices = append(indices, idx)
	}
	return
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

func (tx *MainDatabaseTx) PutContract(ctx context.Context, c api.ContractMetadata) error {
	// validate metadata
	var state ssql.ContractState
	if err := state.LoadString(c.State); err != nil {
		return err
	}
	var usability ssql.ContractUsability
	if err := usability.LoadString(c.Usability); err != nil {
		return err
	}
	if c.ID == (types.FileContractID{}) {
		return errors.New("contract id is required")
	} else if c.HostKey == (types.PublicKey{}) {
		return errors.New("host key is required")
	}

	// fetch host id
	var hostID int64
	err := tx.QueryRow(ctx, `SELECT id FROM hosts WHERE public_key = ?`, ssql.PublicKey(c.HostKey)).Scan(&hostID)
	if errors.Is(err, dsql.ErrNoRows) {
		return api.ErrHostNotFound
	}

	// update contract
	_, err = tx.Exec(ctx, `
INSERT INTO contracts (
	created_at, fcid, host_id, host_key, v2,
	archival_reason, proof_height, renewed_from, renewed_to, revision_height, revision_number, size, start_height, state, usability, window_start, window_end,
	contract_price, initial_renter_funds,
	delete_spending, fund_account_spending, sector_roots_spending, upload_spending
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
	created_at = VALUES(created_at), fcid = VALUES(fcid), host_id = VALUES(host_id), host_key = VALUES(host_key), v2 = VALUES(v2),
	archival_reason = VALUES(archival_reason), proof_height = VALUES(proof_height), renewed_from = VALUES(renewed_from), renewed_to = VALUES(renewed_to), revision_height = VALUES(revision_height), revision_number = VALUES(revision_number), size = VALUES(size), start_height = VALUES(start_height), state = VALUES(state), usability = VALUES(usability), window_start = VALUES(window_start), window_end = VALUES(window_end),
	contract_price = VALUES(contract_price), initial_renter_funds = VALUES(initial_renter_funds),
	delete_spending = VALUES(delete_spending), fund_account_spending = VALUES(fund_account_spending), sector_roots_spending = VALUES(sector_roots_spending), upload_spending = VALUES(upload_spending)`,
		time.Now(), ssql.FileContractID(c.ID), hostID, ssql.PublicKey(c.HostKey), c.V2,
		ssql.NullableString(c.ArchivalReason), c.ProofHeight, ssql.FileContractID(c.RenewedFrom), ssql.FileContractID(c.RenewedTo), c.RevisionHeight, c.RevisionNumber, c.Size, c.StartHeight, state, usability, c.WindowStart, c.WindowEnd,
		ssql.Currency(c.ContractPrice), ssql.Currency(c.InitialRenterFunds),
		ssql.Currency(c.Spending.Deletions), ssql.Currency(c.Spending.FundAccount), ssql.Currency(c.Spending.SectorRoots), ssql.Currency(c.Spending.Uploads),
	)
	if err != nil {
		return fmt.Errorf("failed to update contract: %w", err)
	}
	return nil
}

func (tx *MainDatabaseTx) RecordContractSpending(ctx context.Context, fcid types.FileContractID, revisionNumber, size uint64, newSpending api.ContractSpending) error {
	return ssql.RecordContractSpending(ctx, tx, fcid, revisionNumber, size, newSpending)
}

func (tx *MainDatabaseTx) RecordHostScans(ctx context.Context, scans []api.HostScan) error {
	return ssql.RecordHostScans(ctx, tx, scans)
}

func (tx *MainDatabaseTx) RemoveContractSet(ctx context.Context, contractSet string) error {
	return ssql.RemoveContractSet(ctx, tx, contractSet)
}

func (tx *MainDatabaseTx) RemoveOfflineHosts(ctx context.Context, minRecentFailures uint64, maxDownTime time.Duration) (int64, error) {
	return ssql.RemoveOfflineHosts(ctx, tx, minRecentFailures, maxDownTime)
}

func (tx *MainDatabaseTx) RenameObject(ctx context.Context, bucket, keyOld, keyNew string, force bool) error {
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
	resp, err := tx.Exec(ctx, `UPDATE objects SET object_id = ? WHERE object_id = ? AND db_bucket_id = (SELECT id FROM buckets WHERE buckets.name = ?)`, keyNew, keyOld, bucket)
	if err != nil {
		return err
	} else if n, err := resp.RowsAffected(); err != nil {
		return err
	} else if n == 0 {
		return fmt.Errorf("%w: key %v", api.ErrObjectNotFound, keyOld)
	}
	return nil
}

func (tx *MainDatabaseTx) RenameObjects(ctx context.Context, bucket, prefixOld, prefixNew string, force bool) error {
	if force {
		// to avoid a conflict on update, we delete objects that would conflict
		// with objects being renamed, within the scope of the bucket of course
		query := `
		DELETE
		FROM objects
		WHERE
			db_bucket_id = (SELECT id FROM buckets WHERE buckets.name = ?) AND
			object_id IN (
				SELECT *
				FROM (
					SELECT CONCAT(?, SUBSTR(object_id, ?))
					FROM objects
					WHERE object_id LIKE ? AND SUBSTR(object_id, 1, ?) = ?
				) as i
			)`
		args := []any{
			bucket,
			prefixNew, utf8.RuneCountInString(prefixOld) + 1,
			prefixOld + "%", utf8.RuneCountInString(prefixOld), prefixOld,
		}
		_, err := tx.Exec(ctx, query, args...)
		if err != nil {
			return err
		}
	}

	// update objects where bucket matches, where the object_id is prefixed by
	// the old prefix (case sensitive) and it doesn't exactly match the new
	// prefix, we update the object_id at all times but only update directory_id
	// only when the object is an immediate child (no slash in suffix)
	query := `
		UPDATE objects
		SET object_id = CONCAT(?, SUBSTR(object_id, ?))
		WHERE
			db_bucket_id = (SELECT id FROM buckets WHERE buckets.name = ?) AND
			object_id LIKE ? AND SUBSTR(object_id, 1, ?) = ?`

	args := []any{
		prefixNew, utf8.RuneCountInString(prefixOld) + 1,
		bucket,
		prefixOld + "%", utf8.RuneCountInString(prefixOld), prefixOld,
	}
	resp, err := tx.Exec(ctx, query, args...)
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

func (tx *MainDatabaseTx) RenewedContract(ctx context.Context, renewedFrom types.FileContractID) (api.ContractMetadata, error) {
	return ssql.RenewedContract(ctx, tx, renewedFrom)
}

func (tx *MainDatabaseTx) ResetChainState(ctx context.Context) error {
	return ssql.ResetChainState(ctx, tx.Tx)
}

func (tx *MainDatabaseTx) ResetLostSectors(ctx context.Context, hk types.PublicKey) error {
	return ssql.ResetLostSectors(ctx, tx, hk)
}

func (tx MainDatabaseTx) SaveAccounts(ctx context.Context, accounts []api.Account) error {
	// clean_shutdown = 1 after save
	stmt, err := tx.Prepare(ctx, `
		INSERT INTO ephemeral_accounts (created_at, account_id, clean_shutdown, host, balance, drift, requires_sync, owner)
		VAlUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
		account_id = VALUES(account_id),
		clean_shutdown = VALUES(clean_shutdown),
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
		res, err := stmt.Exec(ctx, time.Now(), (ssql.PublicKey)(acc.ID), acc.CleanShutdown, (ssql.PublicKey)(acc.HostKey), (*ssql.BigInt)(acc.Balance), (*ssql.BigInt)(acc.Drift), acc.RequiresSync, acc.Owner)
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

func (tx *MainDatabaseTx) ScanObjectMetadata(s ssql.Scanner, others ...any) (md api.ObjectMetadata, err error) {
	dst := []any{&md.Key, &md.Size, &md.Health, &md.MimeType, &md.ModTime, &md.ETag, &md.Bucket}
	dst = append(dst, others...)
	if err := s.Scan(dst...); err != nil {
		return api.ObjectMetadata{}, fmt.Errorf("failed to scan object metadata: %w", err)
	}
	return md, nil
}

func (tx *MainDatabaseTx) SelectObjectMetadataExpr() string {
	return "o.object_id, o.size, o.health, o.mime_type, o.created_at, o.etag, b.name"
}

func (tx *MainDatabaseTx) Setting(ctx context.Context, key string) (string, error) {
	return ssql.Setting(ctx, tx, key)
}

func (tx *MainDatabaseTx) Slab(ctx context.Context, key object.EncryptionKey) (object.Slab, error) {
	return ssql.Slab(ctx, tx, key)
}

func (tx *MainDatabaseTx) SlabBuffers(ctx context.Context) (map[string]string, error) {
	return ssql.SlabBuffers(ctx, tx)
}

func (tx *MainDatabaseTx) Tip(ctx context.Context) (types.ChainIndex, error) {
	return ssql.Tip(ctx, tx.Tx)
}

func (tx *MainDatabaseTx) UnhealthySlabs(ctx context.Context, healthCutoff float64, set string, limit int) ([]api.UnhealthySlab, error) {
	return ssql.UnhealthySlabs(ctx, tx, healthCutoff, set, limit)
}

func (tx *MainDatabaseTx) UnspentSiacoinElements(ctx context.Context) (elements []types.SiacoinElement, err error) {
	return ssql.UnspentSiacoinElements(ctx, tx.Tx)
}

func (tx *MainDatabaseTx) UpdateAutopilot(ctx context.Context, ap api.Autopilot) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO autopilots (created_at, identifier, config, current_period)
		VALUES (?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
		config = VALUES(config),
		current_period = VALUES(current_period)
	`, time.Now(), ap.ID, (*ssql.AutopilotConfig)(&ap.Config), ap.CurrentPeriod)
	return err
}

func (tx *MainDatabaseTx) UpdateBucketPolicy(ctx context.Context, bucket string, bp api.BucketPolicy) error {
	return ssql.UpdateBucketPolicy(ctx, tx, bucket, bp)
}

func (tx *MainDatabaseTx) UpdateContract(ctx context.Context, fcid types.FileContractID, c api.ContractMetadata) error {
	return ssql.UpdateContract(ctx, tx, fcid, c)
}

func (tx *MainDatabaseTx) UpdateContractSet(ctx context.Context, name string, toAdd, toRemove []types.FileContractID) error {
	res, err := tx.Exec(ctx, "INSERT INTO contract_sets (name) VALUES (?) ON DUPLICATE KEY UPDATE id = last_insert_id(id)", name)
	if err != nil {
		return fmt.Errorf("failed to insert contract set: %w", err)
	}

	// if no changes are needed, return after creating the set
	if len(toAdd)+len(toRemove) == 0 {
		return nil
	}

	csID, err := res.LastInsertId()
	if err != nil {
		return fmt.Errorf("failed to fetch contract set id: %w", err)
	}

	prepareQuery := func(fcids []types.FileContractID) (string, []any) {
		args := []any{csID}
		query := strings.Repeat("?, ", len(fcids)-1) + "?"
		for _, fcid := range fcids {
			args = append(args, ssql.FileContractID(fcid))
		}
		return query, args
	}

	// remove unwanted contracts first
	if len(toRemove) > 0 {
		query, args := prepareQuery(toRemove)
		_, err = tx.Exec(ctx, fmt.Sprintf(`
			DELETE csc
			FROM contract_set_contracts csc
			INNER JOIN contracts c ON c.id = csc.db_contract_id
			WHERE csc.db_contract_set_id = ? AND c.fcid IN (%s)
		`, query), args...)
		if err != nil {
			return fmt.Errorf("failed to remove contracts: %w", err)
		}
	}

	// add new contracts
	if len(toAdd) > 0 {
		query, args := prepareQuery(toAdd)
		_, err = tx.Exec(ctx, fmt.Sprintf(`
			INSERT INTO contract_set_contracts (db_contract_set_id, db_contract_id)
			SELECT ?, c.id
			FROM contracts c
			WHERE c.fcid IN (%s)
			ON DUPLICATE KEY UPDATE db_contract_set_id = VALUES(db_contract_set_id)
		`, query), args...)
		if err != nil {
			return fmt.Errorf("failed to add contract set contracts: %w", err)
		}
	}
	return nil
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
			usability_redundant_ip, usability_gouging, usability_low_max_duration, usability_not_accepting_contracts, usability_not_announced, usability_not_completing_scan,
			score_age, score_collateral, score_interactions, score_storage_remaining, score_uptime, score_version, score_prices,
			gouging_contract_err, gouging_download_err, gouging_gouging_err, gouging_prune_err, gouging_upload_err)
	    VALUES (?,
			(SELECT id FROM autopilots WHERE identifier = ?),
			(SELECT id FROM hosts WHERE public_key = ?),
			?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			created_at = VALUES(created_at), db_autopilot_id = VALUES(db_autopilot_id), db_host_id = VALUES(db_host_id),
			usability_blocked = VALUES(usability_blocked), usability_offline = VALUES(usability_offline), usability_low_score = VALUES(usability_low_score),
			usability_redundant_ip = VALUES(usability_redundant_ip), usability_gouging = VALUES(usability_gouging), usability_low_max_duration = VALUES(usability_low_max_duration), usability_not_accepting_contracts = VALUES(usability_not_accepting_contracts),
			usability_not_announced = VALUES(usability_not_announced), usability_not_completing_scan = VALUES(usability_not_completing_scan),
			score_age = VALUES(score_age), score_collateral = VALUES(score_collateral), score_interactions = VALUES(score_interactions),
			score_storage_remaining = VALUES(score_storage_remaining), score_uptime = VALUES(score_uptime), score_version = VALUES(score_version),
			score_prices = VALUES(score_prices), gouging_download_err = VALUES(gouging_download_err),
			gouging_gouging_err = VALUES(gouging_gouging_err), gouging_prune_err = VALUES(gouging_prune_err), gouging_upload_err = VALUES(gouging_upload_err)
	`, time.Now(), autopilot, ssql.PublicKey(hk), hc.UsabilityBreakdown.Blocked, hc.UsabilityBreakdown.Offline, hc.UsabilityBreakdown.LowScore,
		hc.UsabilityBreakdown.RedundantIP, hc.UsabilityBreakdown.Gouging, hc.UsabilityBreakdown.LowMaxDuration, hc.UsabilityBreakdown.NotAcceptingContracts, hc.UsabilityBreakdown.NotAnnounced, hc.UsabilityBreakdown.NotCompletingScan,
		hc.ScoreBreakdown.Age, hc.ScoreBreakdown.Collateral, hc.ScoreBreakdown.Interactions, hc.ScoreBreakdown.StorageRemaining, hc.ScoreBreakdown.Uptime, hc.ScoreBreakdown.Version, hc.ScoreBreakdown.Prices,
		hc.GougingBreakdown.DownloadErr, hc.GougingBreakdown.GougingErr, hc.GougingBreakdown.PruneErr, hc.GougingBreakdown.UploadErr,
	)
	if err != nil {
		return fmt.Errorf("failed to insert host check: %w", err)
	}
	return nil
}

func (tx *MainDatabaseTx) UpdatePeerInfo(ctx context.Context, addr string, fn func(*syncer.PeerInfo)) error {
	return ssql.UpdatePeerInfo(ctx, tx, addr, fn)
}

func (tx *MainDatabaseTx) UpdateSetting(ctx context.Context, key, value string) error {
	_, err := tx.Exec(ctx, "INSERT INTO settings (created_at, `key`, value) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE value = VALUES(value)",
		time.Now(), key, value)
	if err != nil {
		return fmt.Errorf("failed to update setting '%s': %w", key, err)
	}
	return nil
}

func (tx *MainDatabaseTx) UpdateSlab(ctx context.Context, key object.EncryptionKey, sectors []api.UploadedSector) error {
	return ssql.UpdateSlab(ctx, tx, key, sectors)
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
			SELECT sli.db_object_id as id, MIN(sla.health) as health
			FROM slabs sla
			INNER JOIN slices sli ON sli.db_slab_id = sla.id
			GROUP BY sli.db_object_id
		) AS object_health ON object_health.id = o.id
		SET o.health = object_health.health
		WHERE EXISTS (
			SELECT 1
			FROM slabs_health h
			INNER JOIN slices ON slices.db_slab_id = h.id
			WHERE slices.db_object_id = o.id
		)
		`)
	if err != nil {
		return 0, fmt.Errorf("failed to update object health: %w", err)
	}
	return res.RowsAffected()
}

func (tx *MainDatabaseTx) UpsertContractSectors(ctx context.Context, contractSectors []ssql.ContractSector) error {
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
			cs.SectorID,
			cs.ContractID,
		)
		if err != nil {
			return fmt.Errorf("failed to insert contract sector link: %w", err)
		}
	}
	return nil
}

func (tx *MainDatabaseTx) UsableHosts(ctx context.Context) ([]ssql.HostInfo, error) {
	return ssql.UsableHosts(ctx, tx)
}

func (tx *MainDatabaseTx) WalletEvents(ctx context.Context, offset, limit int) ([]wallet.Event, error) {
	return ssql.WalletEvents(ctx, tx.Tx, offset, limit)
}

func (tx *MainDatabaseTx) WalletEventCount(ctx context.Context) (count uint64, err error) {
	return ssql.WalletEventCount(ctx, tx.Tx)
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
		res, err := insertSlabStmt.Exec(ctx,
			time.Now(),
			contractSetID,
			ssql.EncryptionKey(slices[i].EncryptionKey),
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
	var upsertContractSectors []ssql.ContractSector
	for _, ss := range slices {
		for _, shard := range ss.Shards {
			for _, fcids := range shard.Contracts {
				for _, fcid := range fcids {
					if _, ok := usedContracts[fcid]; ok {
						upsertContractSectors = append(upsertContractSectors, ssql.ContractSector{
							ContractID: usedContracts[fcid].ID,
							SectorID:   sectorIDs[sectorIdx],
						})
					} else {
						tx.log.Named("InsertObject").Warn("missing contract for shard",
							"contract", fcid,
							"root", shard.Root,
						)
					}
				}
			}
			sectorIdx++
		}
	}
	if err := tx.UpsertContractSectors(ctx, upsertContractSectors); err != nil {
		return err
	}
	return nil
}

type upsertSector struct {
	slabID    int64
	slabIndex int
	root      types.Hash256
}

func (tx *MainDatabaseTx) upsertSectors(ctx context.Context, sectors []upsertSector) ([]int64, error) {
	if len(sectors) == 0 {
		return nil, nil
	}

	// insert sectors - make sure to update last_insert_id in case of a
	// duplicate key to be able to retrieve the id
	insertSectorStmt, err := tx.Prepare(ctx, `INSERT INTO sectors (created_at, db_slab_id, slab_index, root)
								VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE id = last_insert_id(id)`)
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
