package sql

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"net"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	dsql "database/sql"

	rhpv4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.sia.tech/coreutils/syncer"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/renterd/v2/api"
	"go.sia.tech/renterd/v2/internal/rhp/v4"
	"go.sia.tech/renterd/v2/internal/sql"
	"go.sia.tech/renterd/v2/object"
	"lukechampine.com/frand"
)

var (
	ErrNegativeOffset  = errors.New("offset can not be negative")
	ErrSettingNotFound = errors.New("setting not found")
)

// helper types
type (
	HostInfo struct {
		api.HostInfo
		V2HS rhp.HostSettings
	}

	multipartUpload struct {
		ID       int64
		Key      string
		Bucket   string
		BucketID int64
		EC       object.EncryptionKey
		MimeType string
	}

	multipartUploadPart struct {
		ID         int64
		PartNumber int64
		Etag       string
		Size       int64
	}

	// Tx is an interface that allows for injecting custom methods into helpers
	// to avoid duplicating code.
	Tx interface {
		sql.Tx

		CharLengthExpr() string

		// ScanObjectMetadata scans the object metadata from the given scanner.
		// The columns required to scan the metadata are returned by the
		// SelectObjectMetadataExpr helper method. Additional fields can be
		// selected and scanned by passing them to the method as 'others'.
		ScanObjectMetadata(s Scanner, others ...any) (md api.ObjectMetadata, err error)
		SelectObjectMetadataExpr() string

		UpsertContractSectors(ctx context.Context, contractSectors []ContractSector) error
	}
)

func AbortMultipartUpload(ctx context.Context, tx sql.Tx, bucket, key string, uploadID string) error {
	res, err := tx.Exec(ctx, `
		DELETE
		FROM multipart_uploads
		WHERE object_id = ? AND upload_id = ? AND db_bucket_id = (
			SELECT id
			FROM buckets
			WHERE name = ?
	)`, key, uploadID, bucket)
	if err != nil {
		return fmt.Errorf("failed to delete multipart upload: %w", err)
	} else if n, err := res.RowsAffected(); err != nil {
		return fmt.Errorf("failed to fetch rows affected: %w", err)
	} else if n > 0 {
		return nil
	}

	// find out why the upload wasn't deleted
	var muKey, bucketName string
	err = tx.QueryRow(ctx, "SELECT object_id, b.name FROM multipart_uploads mu INNER JOIN buckets b ON mu.db_bucket_id = b.id WHERE upload_id = ?", uploadID).
		Scan(&muKey, &bucketName)
	if errors.Is(err, dsql.ErrNoRows) {
		return api.ErrMultipartUploadNotFound
	} else if err != nil {
		return fmt.Errorf("failed to fetch multipart upload: %w", err)
	} else if muKey != key {
		return fmt.Errorf("object id mismatch: %v != %v: %w", muKey, key, api.ErrObjectNotFound)
	} else if bucketName != bucket {
		return fmt.Errorf("bucket name mismatch: %v != %v: %w", bucketName, bucket, api.ErrBucketNotFound)
	}
	return errors.New("failed to delete multipart upload for unknown reason")
}

func Accounts(ctx context.Context, tx sql.Tx, owner string) ([]api.Account, error) {
	var whereExpr string
	var args []any
	if owner != "" {
		whereExpr = "WHERE owner = ?"
		args = append(args, owner)
	}
	rows, err := tx.Query(ctx, fmt.Sprintf("SELECT account_id, clean_shutdown, host, balance, drift, requires_sync, owner FROM ephemeral_accounts %s", whereExpr),
		args...)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch accounts: %w", err)
	}
	defer rows.Close()

	var accounts []api.Account
	for rows.Next() {
		a := api.Account{Balance: new(big.Int), Drift: new(big.Int)} // init big.Int
		if err := rows.Scan((*PublicKey)(&a.ID), &a.CleanShutdown, (*PublicKey)(&a.HostKey), (*BigInt)(a.Balance), (*BigInt)(a.Drift), &a.RequiresSync, &a.Owner); err != nil {
			return nil, fmt.Errorf("failed to scan account: %w", err)
		}
		accounts = append(accounts, a)
	}
	return accounts, nil
}

func AncestorContracts(ctx context.Context, tx sql.Tx, fcid types.FileContractID, startHeight uint64) (ancestors []api.ContractMetadata, _ error) {
	rows, err := tx.Query(ctx, `
		WITH RECURSIVE c AS
		(
			SELECT *
			FROM contracts
			WHERE renewed_to = ?
			UNION ALL
			SELECT contracts.*
			FROM c, contracts
			WHERE contracts.renewed_to = c.fcid
		)
		SELECT
			c.fcid, c.host_id, c.host_key,
			c.archival_reason, c.proof_height, c.renewed_from, c.renewed_to, c.revision_height, c.revision_number, c.size, c.start_height, c.state, c.usability, c.window_start, c.window_end,
			c.contract_price, c.initial_renter_funds,
			c.delete_spending, c.fund_account_spending, c.sector_roots_spending, c.upload_spending
		FROM contracts AS c
		WHERE start_height >= ? AND archival_reason IS NOT NULL
		ORDER BY start_height DESC
	`, FileContractID(fcid), startHeight)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch ancestor contracts: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var r ContractRow
		if err := r.Scan(rows); err != nil {
			return nil, fmt.Errorf("failed to scan ancestor: %w", err)
		}
		ancestors = append(ancestors, r.ContractMetadata())
	}

	return
}

func ArchiveContract(ctx context.Context, tx sql.Tx, fcid types.FileContractID, reason string) error {
	// validate reason
	if reason == "" {
		return fmt.Errorf("archival reason cannot be empty")
	}

	// archive contract
	_, err := tx.Exec(ctx, "UPDATE contracts SET host_id = NULL, archival_reason = ?, usability = ? WHERE fcid = ?", reason, contractUsabilityBad, FileContractID(fcid))
	if err != nil {
		return fmt.Errorf("failed to archive contract: %w", err)
	}

	// delete its sectors
	_, err = tx.Exec(ctx, "DELETE FROM contract_sectors WHERE db_contract_id IN (SELECT id FROM contracts WHERE fcid = ?)", FileContractID(fcid))
	if err != nil {
		return fmt.Errorf("failed to delete contract_sectors: %w", err)
	}

	return nil
}

func AutopilotConfig(ctx context.Context, tx sql.Tx) (cfg api.AutopilotConfig, err error) {
	err = tx.QueryRow(ctx, `
SELECT
	enabled,
	contracts_amount,
	contracts_period,
	contracts_renew_window,
	contracts_download,
	contracts_upload,
	contracts_storage,
	contracts_prune,
	hosts_max_downtime_hours,
	hosts_min_protocol_version,
	hosts_max_consecutive_scan_failures
FROM autopilot_config
WHERE id = ?`, sql.AutopilotID).Scan(
		&cfg.Enabled,
		&cfg.Contracts.Amount,
		&cfg.Contracts.Period,
		&cfg.Contracts.RenewWindow,
		&cfg.Contracts.Download,
		&cfg.Contracts.Upload,
		&cfg.Contracts.Storage,
		&cfg.Contracts.Prune,
		&cfg.Hosts.MaxDowntimeHours,
		&cfg.Hosts.MinProtocolVersion,
		&cfg.Hosts.MaxConsecutiveScanFailures,
	)
	return
}

func Bucket(ctx context.Context, tx sql.Tx, bucket string) (api.Bucket, error) {
	b, err := scanBucket(tx.QueryRow(ctx, "SELECT created_at, name, COALESCE(policy, '{}') FROM buckets WHERE name = ?", bucket))
	if err != nil {
		return api.Bucket{}, fmt.Errorf("failed to fetch bucket: %w", err)
	}
	return b, nil
}

func Buckets(ctx context.Context, tx sql.Tx) ([]api.Bucket, error) {
	rows, err := tx.Query(ctx, "SELECT created_at, name, COALESCE(policy, '{}') FROM buckets")
	if err != nil {
		return nil, fmt.Errorf("failed to fetch buckets: %w", err)
	}
	defer rows.Close()

	var buckets []api.Bucket
	for rows.Next() {
		bucket, err := scanBucket(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan bucket: %w", err)
		}
		buckets = append(buckets, bucket)
	}
	return buckets, nil
}

func Contract(ctx context.Context, tx sql.Tx, fcid types.FileContractID) (api.ContractMetadata, error) {
	contracts, err := QueryContracts(ctx, tx, []string{"c.fcid = ?", "c.archival_reason IS NULL"}, []any{FileContractID(fcid)})
	if err != nil {
		return api.ContractMetadata{}, fmt.Errorf("failed to fetch contract: %w", err)
	} else if len(contracts) == 0 {
		return api.ContractMetadata{}, api.ErrContractNotFound
	}
	return contracts[0], nil
}

func ContractRoots(ctx context.Context, tx sql.Tx, fcid types.FileContractID) ([]types.Hash256, error) {
	rows, err := tx.Query(ctx, `
		SELECT s.root
		FROM contract_sectors cs
		INNER JOIN sectors s ON s.id = cs.db_sector_id
		INNER JOIN contracts c ON c.id = cs.db_contract_id
		WHERE c.fcid = ?
	`, FileContractID(fcid))
	if err != nil {
		return nil, fmt.Errorf("failed to fetch contract roots: %w", err)
	}
	defer rows.Close()

	var roots []types.Hash256
	for rows.Next() {
		var root types.Hash256
		if err := rows.Scan((*Hash256)(&root)); err != nil {
			return nil, fmt.Errorf("failed to scan root: %w", err)
		}
		roots = append(roots, root)
	}
	return roots, nil
}

func Contracts(ctx context.Context, tx sql.Tx, opts api.ContractsOpts) ([]api.ContractMetadata, error) {
	var whereExprs []string
	var whereArgs []any
	if opts.FilterMode != "" {
		// validate filter mode
		switch opts.FilterMode {
		case api.ContractFilterModeActive:
			whereExprs = append(whereExprs, "c.archival_reason IS NULL")
		case api.ContractFilterModeArchived:
			whereExprs = append(whereExprs, "c.archival_reason IS NOT NULL")
		case api.ContractFilterModeGood:
			whereExprs = append(whereExprs, "c.archival_reason IS NULL AND c.usability = ?")
			whereArgs = append(whereArgs, contractUsabilityGood)
		case api.ContractFilterModeAll:
		default:
			return nil, fmt.Errorf("invalid filter mode: %v", opts.FilterMode)
		}
	} else {
		// default to active contracts
		whereExprs = append(whereExprs, "c.archival_reason IS NULL")
	}

	return QueryContracts(ctx, tx, whereExprs, whereArgs)
}

func ContractSize(ctx context.Context, tx sql.Tx, id types.FileContractID) (api.ContractSize, error) {
	var contractID, size uint64
	if err := tx.QueryRow(ctx, "SELECT id, size FROM contracts WHERE fcid = ? AND archival_reason IS NULL", FileContractID(id)).
		Scan(&contractID, &size); errors.Is(err, dsql.ErrNoRows) {
		return api.ContractSize{}, api.ErrContractNotFound
	} else if err != nil {
		return api.ContractSize{}, err
	}

	var nSectors uint64
	if err := tx.QueryRow(ctx, "SELECT COUNT(*) FROM contract_sectors WHERE db_contract_id = ?", contractID).
		Scan(&nSectors); err != nil {
		return api.ContractSize{}, err
	}
	sectorsSize := nSectors * rhpv4.SectorSize

	var prunable uint64
	if size > sectorsSize {
		prunable = size - sectorsSize
	}
	return api.ContractSize{
		Size:     size,
		Prunable: prunable,
	}, nil
}

func ContractSizes(ctx context.Context, tx sql.Tx) (map[types.FileContractID]api.ContractSize, error) {
	// the following query consists of two parts:
	// 1. fetch all contracts that have no sectors and consider their size as
	//    prunable
	// 2. fetch all contracts that have sectors and calculate the prunable size
	//    based on the number of sectors
	rows, err := tx.Query(ctx, `
		SELECT c.fcid, c.size, c.size
		FROM contracts c
		WHERE archival_reason IS NULL AND NOT EXISTS (
			SELECT 1
			FROM contract_sectors cs
			WHERE cs.db_contract_id = c.id
		)
		UNION ALL
		SELECT fcid, size, CASE WHEN contract_size > sector_size THEN contract_size - sector_size ELSE 0 END
		FROM (
			SELECT c.fcid, c.size, MAX(c.size) as contract_size, COUNT(*) * ? as sector_size
			FROM contracts c
			INNER JOIN contract_sectors cs ON cs.db_contract_id = c.id
			WHERE archival_reason IS NULL
			GROUP BY c.fcid
		) i
	`, rhpv4.SectorSize)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch contract sizes: %w", err)
	}
	defer rows.Close()

	sizes := make(map[types.FileContractID]api.ContractSize)
	for rows.Next() {
		var fcid types.FileContractID
		var cs api.ContractSize
		if err := rows.Scan((*FileContractID)(&fcid), &cs.Size, &cs.Prunable); err != nil {
			return nil, fmt.Errorf("failed to scan contract size: %w", err)
		}
		sizes[fcid] = cs
	}
	return sizes, nil
}

func CopyObject(ctx context.Context, tx sql.Tx, srcBucket, dstBucket, srcKey, dstKey, mimeType string, metadata api.ObjectUserMetadata) (api.ObjectMetadata, error) {
	// stmt to fetch bucket id
	bucketIDStmt, err := tx.Prepare(ctx, "SELECT id FROM buckets WHERE name = ?")
	if err != nil {
		return api.ObjectMetadata{}, fmt.Errorf("failed to prepare statement to fetch bucket id: %w", err)
	}
	defer bucketIDStmt.Close()

	// fetch source bucket
	var srcBID int64
	err = bucketIDStmt.QueryRow(ctx, srcBucket).Scan(&srcBID)
	if errors.Is(err, dsql.ErrNoRows) {
		return api.ObjectMetadata{}, fmt.Errorf("%w: source bucket", api.ErrBucketNotFound)
	} else if err != nil {
		return api.ObjectMetadata{}, fmt.Errorf("failed to fetch src bucket id: %w", err)
	}

	// fetch src object id
	var srcObjID int64
	err = tx.QueryRow(ctx, "SELECT id FROM objects WHERE db_bucket_id = ? AND object_id = ?", srcBID, srcKey).
		Scan(&srcObjID)
	if errors.Is(err, dsql.ErrNoRows) {
		return api.ObjectMetadata{}, api.ErrObjectNotFound
	} else if err != nil {
		return api.ObjectMetadata{}, fmt.Errorf("failed to fetch object id: %w", err)
	}

	// helper to fetch metadata
	fetchMetadata := func(objID int64) (om api.ObjectMetadata, err error) {
		err = tx.QueryRow(ctx, "SELECT etag, health, created_at, object_id, size, mime_type FROM objects WHERE id = ?", objID).
			Scan(&om.ETag, &om.Health, (*time.Time)(&om.ModTime), &om.Key, &om.Size, &om.MimeType)
		if err != nil {
			return api.ObjectMetadata{}, fmt.Errorf("failed to fetch new object: %w", err)
		}
		return om, nil
	}

	if srcBucket == dstBucket && srcKey == dstKey {
		// No copying is happening. We just update the metadata on the src
		// object.
		if _, err := tx.Exec(ctx, "UPDATE objects SET mime_type = ? WHERE id = ?", mimeType, srcObjID); err != nil {
			return api.ObjectMetadata{}, fmt.Errorf("failed to update mime type: %w", err)
		} else if err := UpdateMetadata(ctx, tx, srcObjID, metadata); err != nil {
			return api.ObjectMetadata{}, fmt.Errorf("failed to update metadata: %w", err)
		}
		return fetchMetadata(srcObjID)
	}

	// fetch destination bucket
	var dstBID int64
	err = bucketIDStmt.QueryRow(ctx, dstBucket).Scan(&dstBID)
	if errors.Is(err, dsql.ErrNoRows) {
		return api.ObjectMetadata{}, fmt.Errorf("%w: destination bucket", api.ErrBucketNotFound)
	} else if err != nil {
		return api.ObjectMetadata{}, fmt.Errorf("failed to fetch dest bucket id: %w", err)
	}

	// copy object
	res, err := tx.Exec(ctx, `INSERT INTO objects (created_at, object_id, db_bucket_id,`+"`key`"+`, size, mime_type, etag)
						SELECT ?, ?, ?, `+"`key`"+`, size, ?, etag
						FROM objects
						WHERE id = ?`, time.Now(), dstKey, dstBID, mimeType, srcObjID)
	if err != nil {
		return api.ObjectMetadata{}, fmt.Errorf("failed to insert object: %w", err)
	}
	dstObjID, err := res.LastInsertId()
	if err != nil {
		return api.ObjectMetadata{}, fmt.Errorf("failed to fetch object id: %w", err)
	}

	// copy slices
	_, err = tx.Exec(ctx, `INSERT INTO slices (created_at, db_object_id, object_index, db_slab_id, offset, length)
				SELECT ?, ?, object_index, db_slab_id, offset, length
				FROM slices
				WHERE db_object_id = ?`, time.Now(), dstObjID, srcObjID)
	if err != nil {
		return api.ObjectMetadata{}, fmt.Errorf("failed to copy slices: %w", err)
	}

	// create metadata
	if err := InsertMetadata(ctx, tx, &dstObjID, nil, metadata); err != nil {
		return api.ObjectMetadata{}, fmt.Errorf("failed to insert metadata: %w", err)
	}

	// fetch copied object
	return fetchMetadata(dstObjID)
}

func DeleteBucket(ctx context.Context, tx sql.Tx, bucket string) error {
	var id int64
	err := tx.QueryRow(ctx, "SELECT id FROM buckets WHERE name = ?", bucket).Scan(&id)
	if errors.Is(err, dsql.ErrNoRows) {
		return api.ErrBucketNotFound
	} else if err != nil {
		return fmt.Errorf("failed to fetch bucket id: %w", err)
	}
	var empty bool
	err = tx.QueryRow(ctx, "SELECT NOT EXISTS(SELECT 1 FROM objects WHERE db_bucket_id = ?)", id).Scan(&empty)
	if err != nil {
		return fmt.Errorf("failed to check if bucket is empty: %w", err)
	} else if !empty {
		return api.ErrBucketNotEmpty
	}
	_, err = tx.Exec(ctx, "DELETE FROM buckets WHERE id = ?", id)
	if err != nil {
		return fmt.Errorf("failed to delete bucket: %w", err)
	}
	return nil
}

func DeleteHostSector(ctx context.Context, tx sql.Tx, hk types.PublicKey, root types.Hash256) (int, error) {
	// fetch sector id
	var sectorID int64
	if err := tx.QueryRow(ctx, "SELECT s.id FROM sectors s WHERE s.root = ?", Hash256(root)).
		Scan(&sectorID); errors.Is(err, dsql.ErrNoRows) {
		return 0, nil
	} else if err != nil {
		return 0, fmt.Errorf("failed to fetch sector id: %w", err)
	}

	// remove potential links between the host's contracts and the sector
	res, err := tx.Exec(ctx, `
		DELETE FROM contract_sectors
		WHERE db_sector_id = ? AND db_contract_id IN (
			SELECT c.id
			FROM contracts c
			WHERE c.host_key = ?
		)
	`, sectorID, PublicKey(hk))
	if err != nil {
		return 0, fmt.Errorf("failed to delete contract sectors: %w", err)
	}
	deletedSectors, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to check number of deleted contract sectors: %w", err)
	} else if deletedSectors == 0 {
		return 0, nil // nothing to do
	}

	// invalidate the health of related slabs
	_, err = tx.Exec(ctx, `
		UPDATE slabs
		SET health_valid_until = 0
		WHERE id IN (
			SELECT db_slab_id
			FROM sectors
			WHERE root = ?
		)
	`, Hash256(root))
	if err != nil {
		return 0, fmt.Errorf("failed to invalidate slab health: %w", err)
	}

	// increment host's lost sector count
	_, err = tx.Exec(ctx, `
		UPDATE hosts
		SET lost_sectors = lost_sectors + ?
		WHERE public_key = ?
	`, deletedSectors, PublicKey(hk))
	if err != nil {
		return 0, fmt.Errorf("failed to update lost sectors: %w", err)
	}

	// remove sector from host_sectors
	_, err = tx.Exec(ctx, `
		DELETE FROM host_sectors
		WHERE db_sector_id = ? AND db_host_id IN (
			SELECT h.id
			FROM hosts h
			WHERE h.public_key = ?
		)
	`, sectorID, PublicKey(hk))
	if err != nil {
		return 0, fmt.Errorf("failed to delete host sector: %w", err)
	}

	return int(deletedSectors), nil
}

func DeleteMetadata(ctx context.Context, tx sql.Tx, objID int64) error {
	_, err := tx.Exec(ctx, "DELETE FROM object_user_metadata WHERE db_object_id = ?", objID)
	return err
}

func DeleteSetting(ctx context.Context, tx sql.Tx, key string) error {
	if _, err := tx.Exec(ctx, "DELETE FROM settings WHERE `key` = ?", key); err != nil {
		return fmt.Errorf("failed to delete setting '%s': %w", key, err)
	}
	return nil
}

func FetchUsedContracts(ctx context.Context, tx sql.Tx, fcids []types.FileContractID) (map[types.FileContractID]UsedContract, error) {
	if len(fcids) == 0 {
		return make(map[types.FileContractID]UsedContract), nil
	}

	// build args
	var args []interface{}
	lookup := make(map[FileContractID]struct{}, len(fcids))
	for _, fcid := range fcids {
		args = append(args, FileContractID(fcid))
		lookup[FileContractID(fcid)] = struct{}{}
	}
	args = append(args, args...)

	// build placeholder
	placeholder := strings.Repeat("?, ", len(fcids)-1) + "?"

	// fetch used contracts
	rows, err := tx.Query(ctx, fmt.Sprintf(`
SELECT id, fcid, host_id, renewed_from
FROM contracts
WHERE (fcid IN (%s) OR renewed_from IN (%s)) AND contracts.archival_reason IS NULL`, placeholder, placeholder), args...)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch used contracts: %w", err)
	}
	defer rows.Close()

	// build map of used contracts
	usedContracts := make(map[types.FileContractID]UsedContract, len(fcids))
	for rows.Next() {
		var c UsedContract
		var rf FileContractID
		if err := rows.Scan(&c.ID, &c.FCID, &c.HostID, &rf); err != nil {
			return nil, fmt.Errorf("failed to scan used contract: %w", err)
		}

		// swap the fcid for renewals
		if _, ok := lookup[c.FCID]; !ok {
			c.FCID = rf
		}
		usedContracts[types.FileContractID(c.FCID)] = c
	}
	return usedContracts, nil
}

func HasMigration(ctx context.Context, tx sql.Tx, id string) (applied bool, err error) {
	err = tx.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM migrations WHERE id = ?)", id).Scan(&applied)
	return
}

func HostAllowlist(ctx context.Context, tx sql.Tx) ([]types.PublicKey, error) {
	rows, err := tx.Query(ctx, "SELECT entry FROM host_allowlist_entries")
	if err != nil {
		return nil, fmt.Errorf("failed to fetch host allowlist: %w", err)
	}
	defer rows.Close()

	var allowlist []types.PublicKey
	for rows.Next() {
		var pk PublicKey
		if err := rows.Scan(&pk); err != nil {
			return nil, fmt.Errorf("failed to scan public key: %w", err)
		}
		allowlist = append(allowlist, types.PublicKey(pk))
	}
	return allowlist, nil
}

func HostBlocklist(ctx context.Context, tx sql.Tx) ([]string, error) {
	rows, err := tx.Query(ctx, "SELECT entry FROM host_blocklist_entries")
	if err != nil {
		return nil, fmt.Errorf("failed to fetch host blocklist: %w", err)
	}
	defer rows.Close()

	var blocklist []string
	for rows.Next() {
		var entry string
		if err := rows.Scan(&entry); err != nil {
			return nil, fmt.Errorf("failed to scan blocklist entry: %w", err)
		}
		blocklist = append(blocklist, entry)
	}
	return blocklist, nil
}

func Hosts(ctx context.Context, tx sql.Tx, opts api.HostOptions) ([]api.Host, error) {
	if opts.Offset < 0 {
		return nil, ErrNegativeOffset
	}

	var hasAllowlist, hasBlocklist bool
	if err := tx.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM host_allowlist_entries)").Scan(&hasAllowlist); err != nil {
		return nil, fmt.Errorf("failed to check for allowlist: %w", err)
	} else if err := tx.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM host_blocklist_entries)").Scan(&hasBlocklist); err != nil {
		return nil, fmt.Errorf("failed to check for blocklist: %w", err)
	}

	// validate filterMode
	switch opts.FilterMode {
	case api.HostFilterModeAllowed:
	case api.HostFilterModeBlocked:
	case api.HostFilterModeAll:
	default:
		return nil, fmt.Errorf("invalid filter mode: %v", opts.FilterMode)
	}

	var whereExprs []string
	var args []any

	// filter allowlist/blocklist
	switch opts.FilterMode {
	case api.HostFilterModeAllowed:
		if hasAllowlist {
			whereExprs = append(whereExprs, "EXISTS (SELECT 1 FROM host_allowlist_entry_hosts hbeh WHERE hbeh.db_host_id = h.id)")
		}
		if hasBlocklist {
			whereExprs = append(whereExprs, "NOT EXISTS (SELECT 1 FROM host_blocklist_entry_hosts hbeh WHERE hbeh.db_host_id = h.id)")
		}
	case api.HostFilterModeBlocked:
		if hasAllowlist {
			whereExprs = append(whereExprs, "NOT EXISTS (SELECT 1 FROM host_allowlist_entry_hosts hbeh WHERE hbeh.db_host_id = h.id)")
		}
		if hasBlocklist {
			whereExprs = append(whereExprs, "EXISTS (SELECT 1 FROM host_blocklist_entry_hosts hbeh WHERE hbeh.db_host_id = h.id)")
		}
		if !hasAllowlist && !hasBlocklist {
			// if neither an allowlist nor a blocklist exist, all hosts are
			// allowed which means we return none
			return []api.Host{}, nil
		}
	}

	// filter address
	if opts.AddressContains != "" {
		whereExprs = append(whereExprs, "(h.net_address LIKE ? OR (SELECT EXISTS (SELECT 1 FROM host_addresses ha WHERE ha.db_host_id = h.id AND ha.net_address LIKE ?)))")
		args = append(args, "%"+opts.AddressContains+"%", "%"+opts.AddressContains+"%")
	}

	// filter public key
	if len(opts.KeyIn) > 0 {
		pubKeys := make([]any, len(opts.KeyIn))
		for i, pk := range opts.KeyIn {
			pubKeys[i] = PublicKey(pk)
		}
		placeholders := strings.Repeat("?, ", len(opts.KeyIn)-1) + "?"
		whereExprs = append(whereExprs, fmt.Sprintf("h.public_key IN (%s)", placeholders))
		args = append(args, pubKeys...)
	}

	// filter usability
	if opts.UsabilityMode != api.UsabilityFilterModeAll {
		switch opts.UsabilityMode {
		case api.UsabilityFilterModeUsable:
			whereExprs = append(whereExprs, "EXISTS (SELECT 1 FROM hosts h2 INNER JOIN host_checks hc ON hc.db_host_id = h2.id AND h2.id = h.id WHERE (hc.usability_blocked = 0 AND hc.usability_offline = 0 AND hc.usability_low_score = 0 AND hc.usability_redundant_ip = 0 AND hc.usability_gouging = 0 AND hc.usability_low_max_duration = 0 AND hc.usability_not_accepting_contracts = 0 AND hc.usability_not_announced = 0 AND hc.usability_not_completing_scan = 0))")
		case api.UsabilityFilterModeUnusable:
			whereExprs = append(whereExprs, "EXISTS (SELECT 1 FROM hosts h2 INNER JOIN host_checks hc ON hc.db_host_id = h2.id AND h2.id = h.id WHERE (hc.usability_blocked = 1 OR hc.usability_offline = 1 OR hc.usability_low_score = 1 OR hc.usability_redundant_ip = 1 OR hc.usability_gouging = 1 OR hc.usability_low_max_duration = 1 OR hc.usability_not_accepting_contracts = 1 OR hc.usability_not_announced = 1 OR hc.usability_not_completing_scan = 1))")
		}
	}

	// filter max last scan
	if !opts.MaxLastScan.IsZero() {
		whereExprs = append(whereExprs, "last_scan < ?")
		args = append(args, UnixTimeMS(opts.MaxLastScan))
	}

	// offset + limit
	if opts.Limit == -1 {
		opts.Limit = math.MaxInt64
	}
	offsetLimitStr := fmt.Sprintf("LIMIT %d OFFSET %d", opts.Limit, opts.Offset)

	// fetch stored data for each host
	rows, err := tx.Query(ctx, "SELECT host_key, SUM(size) FROM contracts WHERE archival_reason IS NULL GROUP BY host_key")
	if err != nil {
		return nil, fmt.Errorf("failed to fetch stored data: %w", err)
	}
	defer rows.Close()

	storedDataMap := make(map[types.PublicKey]uint64)
	for rows.Next() {
		var hostKey PublicKey
		var storedData uint64
		if err := rows.Scan(&hostKey, &storedData); err != nil {
			return nil, fmt.Errorf("failed to scan stored data: %w", err)
		}
		storedDataMap[types.PublicKey(hostKey)] = storedData
	}

	// query hosts
	var blockedExprs []string
	if hasAllowlist {
		blockedExprs = append(blockedExprs, "NOT EXISTS (SELECT 1 FROM host_allowlist_entry_hosts hbeh WHERE hbeh.db_host_id = h.id)")
	}
	if hasBlocklist {
		blockedExprs = append(blockedExprs, "EXISTS (SELECT 1 FROM host_blocklist_entry_hosts hbeh WHERE hbeh.db_host_id = h.id)")
	}

	var orderByExpr string
	var blockedExpr string
	if len(blockedExprs) > 0 {
		blockedExpr = strings.Join(blockedExprs, " OR ")
	} else {
		blockedExpr = "FALSE"
	}
	var whereExpr string
	if len(whereExprs) > 0 {
		whereExpr = "WHERE " + strings.Join(whereExprs, " AND ")
	}

	rows, err = tx.Query(ctx, fmt.Sprintf(`
SELECT
	h.id,
	h.created_at,
	h.last_announcement,
	h.public_key,

	h.v2_settings,

	h.total_scans,
	h.last_scan,
	h.last_scan_success,
	h.second_to_last_scan_success,
	h.uptime,
	h.downtime,
	h.successful_interactions,
	h.failed_interactions,
	COALESCE(h.lost_sectors, 0),
	h.scanned,

	%s,

	COALESCE(hc.usability_blocked, 0),
	COALESCE(hc.usability_offline, 0),
	COALESCE(hc.usability_low_score, 0),
	COALESCE(hc.usability_redundant_ip, 0),
	COALESCE(hc.usability_gouging, 0),
	COALESCE(hc.usability_low_max_duration, 0),
	COALESCE(hc.usability_not_accepting_contracts, 0),
	COALESCE(hc.usability_not_announced, 0),
	COALESCE(hc.usability_not_completing_scan, 0),

	COALESCE(hc.score_age,0),
	COALESCE(hc.score_collateral,0),
	COALESCE(hc.score_interactions,0),
	COALESCE(hc.score_storage_remaining,0),
	COALESCE(hc.score_uptime,0),
	COALESCE(hc.score_version,0),
	COALESCE(hc.score_prices,0),

	COALESCE(hc.gouging_download_err, ""),
	COALESCE(hc.gouging_gouging_err, ""),
	COALESCE(hc.gouging_prune_err, ""),
	COALESCE(hc.gouging_upload_err, "")
FROM hosts h
LEFT JOIN host_checks hc ON hc.db_host_id = h.id
%s
%s
%s`, blockedExpr, whereExpr, orderByExpr, offsetLimitStr), args...)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch hosts: %w", err)
	}
	defer rows.Close()

	var hosts []api.Host
	var hostIDs []int64
	for rows.Next() {
		var h api.Host
		var hostID int64
		err := rows.Scan(&hostID, &h.KnownSince, &h.LastAnnouncement, (*PublicKey)(&h.PublicKey),
			(*HostSettings)(&h.V2Settings), &h.Interactions.TotalScans, (*UnixTimeMS)(&h.Interactions.LastScan), &h.Interactions.LastScanSuccess,
			&h.Interactions.SecondToLastScanSuccess, (*DurationMS)(&h.Interactions.Uptime), (*DurationMS)(&h.Interactions.Downtime),
			&h.Interactions.SuccessfulInteractions, &h.Interactions.FailedInteractions, &h.Interactions.LostSectors,
			&h.Scanned, &h.Blocked, &h.Checks.UsabilityBreakdown.Blocked, &h.Checks.UsabilityBreakdown.Offline, &h.Checks.UsabilityBreakdown.LowScore, &h.Checks.UsabilityBreakdown.RedundantIP,
			&h.Checks.UsabilityBreakdown.Gouging, &h.Checks.UsabilityBreakdown.LowMaxDuration, &h.Checks.UsabilityBreakdown.NotAcceptingContracts, &h.Checks.UsabilityBreakdown.NotAnnounced, &h.Checks.UsabilityBreakdown.NotCompletingScan,
			&h.Checks.ScoreBreakdown.Age, &h.Checks.ScoreBreakdown.Collateral, &h.Checks.ScoreBreakdown.Interactions, &h.Checks.ScoreBreakdown.StorageRemaining, &h.Checks.ScoreBreakdown.Uptime,
			&h.Checks.ScoreBreakdown.Version, &h.Checks.ScoreBreakdown.Prices, &h.Checks.GougingBreakdown.DownloadErr, &h.Checks.GougingBreakdown.GougingErr,
			&h.Checks.GougingBreakdown.PruneErr, &h.Checks.GougingBreakdown.UploadErr)
		if err != nil {
			return nil, fmt.Errorf("failed to scan host: %w", err)
		}

		h.StoredData = storedDataMap[h.PublicKey]
		hosts = append(hosts, h)
		hostIDs = append(hostIDs, hostID)
	}

	// fill in v2 addresses
	err = fillInV2Addresses(ctx, tx, hostIDs, func(i int, addrs []string) {
		hosts[i].V2SiamuxAddresses = addrs
		i++
	})
	if err != nil {
		return nil, err
	}
	return hosts, nil
}

func InsertBufferedSlab(ctx context.Context, tx sql.Tx, fileName string, ec object.EncryptionKey, minShards, totalShards uint8) (int64, error) {
	// insert buffered slab
	res, err := tx.Exec(ctx, `INSERT INTO buffered_slabs (created_at, filename) VALUES (?, ?)`,
		time.Now(), fileName)
	if err != nil {
		return 0, fmt.Errorf("failed to insert buffered slab: %w", err)
	}
	bufferedSlabID, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("failed to fetch buffered slab id: %w", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO slabs (created_at, db_buffered_slab_id, `+"`key`"+`, min_shards, total_shards)
			VALUES (?, ?, ?, ?, ?)`,
		time.Now(), bufferedSlabID, EncryptionKey(ec), minShards, totalShards)
	if err != nil {
		return 0, fmt.Errorf("failed to insert slab: %w", err)
	}
	return bufferedSlabID, nil
}

func InsertMetadata(ctx context.Context, tx sql.Tx, objID, muID *int64, md api.ObjectUserMetadata) error {
	if len(md) == 0 {
		return nil
	} else if (objID == nil) == (muID == nil) {
		return errors.New("either objID or muID must be set")
	}
	insertMetadataStmt, err := tx.Prepare(ctx, "INSERT INTO object_user_metadata (created_at, db_object_id, db_multipart_upload_id, `key`, value) VALUES (?, ?, ?, ?, ?)")
	if err != nil {
		return fmt.Errorf("failed to prepare statement to insert object metadata: %w", err)
	}
	defer insertMetadataStmt.Close()

	for k, v := range md {
		if _, err := insertMetadataStmt.Exec(ctx, time.Now(), objID, muID, k, v); err != nil {
			return fmt.Errorf("failed to insert object metadata: %w", err)
		}
	}
	return nil
}

func InsertMultipartUpload(ctx context.Context, tx sql.Tx, bucket, key string, ec object.EncryptionKey, mimeType string, metadata api.ObjectUserMetadata) (string, error) {
	// fetch bucket id
	var bucketID int64
	err := tx.QueryRow(ctx, "SELECT id FROM buckets WHERE buckets.name = ?", bucket).
		Scan(&bucketID)
	if errors.Is(err, dsql.ErrNoRows) {
		return "", fmt.Errorf("bucket %v not found: %w", bucket, api.ErrBucketNotFound)
	} else if err != nil {
		return "", fmt.Errorf("failed to fetch bucket id: %w", err)
	}

	// insert multipart upload
	uploadIDEntropy := frand.Entropy256()
	uploadID := hex.EncodeToString(uploadIDEntropy[:])
	var muID int64
	res, err := tx.Exec(ctx, `
		INSERT INTO multipart_uploads (created_at, `+"`key`"+`, upload_id, object_id, db_bucket_id, mime_type)
		VALUES (?, ?, ?, ?, ?, ?)
	`, time.Now(), EncryptionKey(ec), uploadID, key, bucketID, mimeType)
	if err != nil {
		return "", fmt.Errorf("failed to create multipart upload: %w", err)
	} else if muID, err = res.LastInsertId(); err != nil {
		return "", fmt.Errorf("failed to fetch multipart upload id: %w", err)
	}

	// insert metadata
	if err := InsertMetadata(ctx, tx, nil, &muID, metadata); err != nil {
		return "", fmt.Errorf("failed to insert multipart metadata: %w", err)
	}
	return uploadID, nil
}

func InsertObject(ctx context.Context, tx sql.Tx, key string, bucketID, size int64, ec object.EncryptionKey, mimeType, eTag string) (int64, error) {
	res, err := tx.Exec(ctx, `INSERT INTO objects (created_at, object_id, db_bucket_id, `+"`key`"+`, size, mime_type, etag)
						VALUES (?, ?, ?, ?, ?, ?, ?)`,
		time.Now(),
		key,
		bucketID,
		EncryptionKey(ec),
		size,
		mimeType,
		eTag)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func LoadSlabBuffers(ctx context.Context, tx sql.Tx) (bufferedSlabs []LoadedSlabBuffer, orphanedBuffers []string, err error) {
	// collect all buffers
	rows, err := tx.Query(ctx, `
			SELECT bs.id, bs.filename, sla.key, sla.min_shards, sla.total_shards
			FROM buffered_slabs bs
			INNER JOIN slabs sla ON sla.db_buffered_slab_id = bs.id
		`)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var bs LoadedSlabBuffer
		if err := rows.Scan(&bs.ID, &bs.Filename, (*EncryptionKey)(&bs.Key), &bs.MinShards, &bs.TotalShards); err != nil {
			return nil, nil, fmt.Errorf("failed to scan buffered slab: %w", err)
		}
		bufferedSlabs = append(bufferedSlabs, bs)
	}

	// fill in sizes
	for i := range bufferedSlabs {
		err = tx.QueryRow(ctx, `
				SELECT COALESCE(MAX(offset+length), 0)
				FROM slabs sla
				INNER JOIN slices sli ON sla.id = sli.db_slab_id
				WHERE sla.db_buffered_slab_id = ?
		`, bufferedSlabs[i].ID).Scan(&bufferedSlabs[i].Size)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to fetch buffered slab size: %w", err)
		}
	}

	// find orphaned buffers and delete them
	rows, err = tx.Query(ctx, `
			SELECT bs.id, bs.filename
			FROM buffered_slabs bs
			LEFT JOIN slabs ON slabs.db_buffered_slab_id = bs.id
			WHERE slabs.id IS NULL
		`)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to fetch orphaned buffers: %w", err)
	}
	var toDelete []int64
	for rows.Next() {
		var id int64
		var filename string
		if err := rows.Scan(&id, &filename); err != nil {
			return nil, nil, fmt.Errorf("failed to scan orphaned buffer: %w", err)
		}
		orphanedBuffers = append(orphanedBuffers, filename)
		toDelete = append(toDelete, id)
	}
	for _, id := range toDelete {
		if _, err := tx.Exec(ctx, "DELETE FROM buffered_slabs WHERE id = ?", id); err != nil {
			return nil, nil, fmt.Errorf("failed to delete orphaned buffer: %w", err)
		}
	}
	return bufferedSlabs, orphanedBuffers, nil
}

func UpdateMetadata(ctx context.Context, tx sql.Tx, objID int64, md api.ObjectUserMetadata) error {
	if err := DeleteMetadata(ctx, tx, objID); err != nil {
		return err
	} else if err := InsertMetadata(ctx, tx, &objID, nil, md); err != nil {
		return err
	}
	return nil
}

func PrepareSlabHealth(ctx context.Context, tx sql.Tx, limit int64, now time.Time) error {
	_, err := tx.Exec(ctx, "DROP TABLE IF EXISTS slabs_health")
	if err != nil {
		return fmt.Errorf("failed to drop temporary table: %w", err)
	}

	_, err = tx.Exec(ctx, `
CREATE TEMPORARY TABLE slabs_health AS
	SELECT
		id,
		CASE WHEN no_redundancy THEN CASE WHEN unique_hosts < min_shards THEN -1 ELSE 1 END ELSE (unique_hosts - min_shards) / (total_shards - min_shards) END as health
	FROM (
		SELECT
			slabs.id as id,
			CAST(COUNT(DISTINCT(c.host_key)) AS FLOAT) as unique_hosts,
			CAST(slabs.min_shards AS FLOAT) as min_shards,
			CAST(slabs.total_shards AS FLOAT) as total_shards,
			slabs.min_shards = slabs.total_shards as no_redundancy
		FROM slabs
		INNER JOIN sectors s ON s.db_slab_id = slabs.id
		LEFT JOIN contract_sectors se ON s.id = se.db_sector_id
		LEFT JOIN contracts c ON se.db_contract_id = c.id AND c.usability = ?
		WHERE slabs.health_valid_until <= ?
		GROUP BY slabs.id
		LIMIT ?
	) as t`, contractUsabilityGood, now.Unix(), limit)
	if err != nil {
		return fmt.Errorf("failed to create temporary table: %w", err)
	}
	if _, err := tx.Exec(ctx, "CREATE INDEX slabs_health_id ON slabs_health (id)"); err != nil {
		return fmt.Errorf("failed to create index on temporary table: %w", err)
	}
	return err
}

func whereObjectMarker(marker, sortBy, sortDir string, queryMarker func(dst any, marker, col string) error) (whereExprs []string, whereArgs []any, _ error) {
	if marker == "" {
		return nil, nil, nil
	} else if sortBy == "" || sortDir == "" {
		return nil, nil, fmt.Errorf("sortBy and sortDir must be set")
	}

	desc := strings.ToLower(sortDir) == api.SortDirDesc
	switch strings.ToLower(sortBy) {
	case api.ObjectSortByName:
		if desc {
			whereExprs = append(whereExprs, "o.object_id < ?")
		} else {
			whereExprs = append(whereExprs, "o.object_id > ?")
		}
		whereArgs = append(whereArgs, marker)
	case api.ObjectSortByHealth:
		var markerHealth float64
		if err := queryMarker(&markerHealth, marker, "health"); err != nil {
			return nil, nil, fmt.Errorf("failed to fetch health marker: %w", err)
		} else if desc {
			whereExprs = append(whereExprs, "((o.health <= ? AND o.object_id >?) OR o.health < ?)")
			whereArgs = append(whereArgs, markerHealth, marker, markerHealth)
		} else {
			whereExprs = append(whereExprs, "(o.health > ? OR (o.health >= ? AND object_id > ?))")
			whereArgs = append(whereArgs, markerHealth, markerHealth, marker)
		}
	case api.ObjectSortBySize:
		var markerSize int64
		if err := queryMarker(&markerSize, marker, "size"); err != nil {
			return nil, nil, fmt.Errorf("failed to fetch health marker: %w", err)
		} else if desc {
			whereExprs = append(whereExprs, "((o.size <= ? AND o.object_id >?) OR o.size < ?)")
			whereArgs = append(whereArgs, markerSize, marker, markerSize)
		} else {
			whereExprs = append(whereExprs, "(o.size > ? OR (o.size >= ? AND object_id > ?))")
			whereArgs = append(whereArgs, markerSize, markerSize, marker)
		}
	default:
		return nil, nil, fmt.Errorf("invalid marker: %v", marker)
	}
	return whereExprs, whereArgs, nil
}

func orderByObject(sortBy, sortDir string) (orderByExprs []string, _ error) {
	if sortBy == "" || sortDir == "" {
		return nil, fmt.Errorf("sortBy and sortDir must be set")
	}

	dir2SQL := map[string]string{
		api.SortDirAsc:  "ASC",
		api.SortDirDesc: "DESC",
	}
	if _, ok := dir2SQL[strings.ToLower(sortDir)]; !ok {
		return nil, fmt.Errorf("invalid sortDir: %v", sortDir)
	}
	switch strings.ToLower(sortBy) {
	case "", api.ObjectSortByName:
		orderByExprs = append(orderByExprs, "o.object_id "+dir2SQL[strings.ToLower(sortDir)])
	case api.ObjectSortByHealth:
		orderByExprs = append(orderByExprs, "o.health "+dir2SQL[strings.ToLower(sortDir)])
	case api.ObjectSortBySize:
		orderByExprs = append(orderByExprs, "o.size "+dir2SQL[strings.ToLower(sortDir)])
	default:
		return nil, fmt.Errorf("invalid sortBy: %v", sortBy)
	}

	// always sort by object_id as well if we aren't explicitly
	if sortBy != api.ObjectSortByName {
		orderByExprs = append(orderByExprs, "o.object_id ASC")
	}
	return orderByExprs, nil
}

func MultipartUpload(ctx context.Context, tx sql.Tx, uploadID string) (api.MultipartUpload, error) {
	resp, err := scanMultipartUpload(tx.QueryRow(ctx, "SELECT b.name, mu.key, mu.object_id, mu.upload_id, mu.created_at FROM multipart_uploads mu INNER JOIN buckets b ON b.id = mu.db_bucket_id WHERE mu.upload_id = ?", uploadID))
	if err != nil {
		return api.MultipartUpload{}, fmt.Errorf("failed to fetch multipart upload: %w", err)
	}
	return resp, nil
}

func MultipartUploadParts(ctx context.Context, tx sql.Tx, bucket, key, uploadID string, marker int, limit int64) (api.MultipartListPartsResponse, error) {
	limitExpr := ""
	limitUsed := limit > 0
	if limitUsed {
		limitExpr = fmt.Sprintf("LIMIT %d", limit+1)
	}

	rows, err := tx.Query(ctx, fmt.Sprintf(`
		SELECT mp.part_number, mp.created_at, mp.etag, mp.size
		FROM multipart_parts mp
		INNER JOIN multipart_uploads mus ON mus.id = mp.db_multipart_upload_id
		INNER JOIN buckets b ON b.id = mus.db_bucket_id
		WHERE mus.object_id = ? AND b.name = ? AND mus.upload_id = ? AND part_number > ?
		ORDER BY part_number ASC
		%s
	`, limitExpr), key, bucket, uploadID, marker)
	if err != nil {
		return api.MultipartListPartsResponse{}, fmt.Errorf("failed to fetch multipart parts: %w", err)
	}
	defer rows.Close()

	var parts []api.MultipartListPartItem
	for rows.Next() {
		var part api.MultipartListPartItem
		if err := rows.Scan(&part.PartNumber, (*time.Time)(&part.LastModified), &part.ETag, &part.Size); err != nil {
			return api.MultipartListPartsResponse{}, fmt.Errorf("failed to scan part: %w", err)
		}
		parts = append(parts, part)
	}

	// check if there are more parts beyond 'limit'.
	var hasMore bool
	var nextMarker int
	if limitUsed && len(parts) > int(limit) {
		hasMore = true
		parts = parts[:len(parts)-1]
		nextMarker = parts[len(parts)-1].PartNumber
	}

	return api.MultipartListPartsResponse{
		HasMore:    hasMore,
		NextMarker: nextMarker,
		Parts:      parts,
	}, nil
}

func MultipartUploads(ctx context.Context, tx sql.Tx, bucket, prefix, keyMarker, uploadIDMarker string, limit int) (api.MultipartListUploadsResponse, error) {
	// both markers must be used together
	if (keyMarker == "" && uploadIDMarker != "") || (keyMarker != "" && uploadIDMarker == "") {
		return api.MultipartListUploadsResponse{}, errors.New("both keyMarker and uploadIDMarker must be set or neither")
	}

	// prepare 'limit' expression
	limitExpr := ""
	limitUsed := limit > 0
	if limitUsed {
		limitExpr = fmt.Sprintf("LIMIT %d", limit+1)
	}

	// prepare 'where' expression
	var whereExprs []string
	var args []any
	if keyMarker != "" {
		whereExprs = append(whereExprs, "object_id > ? OR (object_id = ? AND upload_id > ?)")
		args = append(args, keyMarker, keyMarker, uploadIDMarker)
	}
	if prefix != "" {
		whereExprs = append(whereExprs, "SUBSTR(object_id, 1, ?) = ?")
		args = append(args, utf8.RuneCountInString(prefix), prefix)
	}
	if bucket != "" {
		whereExprs = append(whereExprs, "b.name = ?")
		args = append(args, bucket)
	}
	whereExpr := ""
	if len(whereExprs) > 0 {
		whereExpr = "WHERE " + strings.Join(whereExprs, " AND ")
	}

	// fetch multipart uploads
	var uploads []api.MultipartUpload
	rows, err := tx.Query(ctx, fmt.Sprintf("SELECT b.name, mu.key, mu.object_id, mu.upload_id, mu.created_at FROM multipart_uploads mu INNER JOIN buckets b ON b.id = mu.db_bucket_id %s ORDER BY object_id ASC, upload_id ASC %s",
		whereExpr, limitExpr), args...)
	if err != nil {
		return api.MultipartListUploadsResponse{}, fmt.Errorf("failed to fetch multipart uploads: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		upload, err := scanMultipartUpload(rows)
		if err != nil {
			return api.MultipartListUploadsResponse{}, fmt.Errorf("failed to scan multipart upload: %w", err)
		}
		uploads = append(uploads, upload)
	}

	// check if there are more uploads beyond 'limit'.
	var hasMore bool
	var nextPathMarker, nextUploadIDMarker string
	if limitUsed && len(uploads) > int(limit) {
		hasMore = true
		uploads = uploads[:len(uploads)-1]
		nextPathMarker = uploads[len(uploads)-1].Key
		nextUploadIDMarker = uploads[len(uploads)-1].UploadID
	}

	return api.MultipartListUploadsResponse{
		HasMore:            hasMore,
		NextPathMarker:     nextPathMarker,
		NextUploadIDMarker: nextUploadIDMarker,
		Uploads:            uploads,
	}, nil
}

func MultipartUploadForCompletion(ctx context.Context, tx sql.Tx, bucket, key, uploadID string, parts []api.MultipartCompletedPart) (multipartUpload, []multipartUploadPart, int64, string, error) {
	// fetch upload
	var ec []byte
	var mpu multipartUpload
	err := tx.QueryRow(ctx, `
		SELECT mu.id, mu.object_id, mu.mime_type, mu.key, b.name, b.id
		FROM multipart_uploads mu INNER JOIN buckets b ON b.id = mu.db_bucket_id
		WHERE mu.upload_id = ?`, uploadID).
		Scan(&mpu.ID, &mpu.Key, &mpu.MimeType, &ec, &mpu.Bucket, &mpu.BucketID)
	if err != nil {
		return multipartUpload{}, nil, 0, "", fmt.Errorf("failed to fetch upload: %w", err)
	} else if mpu.Key != key {
		return multipartUpload{}, nil, 0, "", fmt.Errorf("object id mismatch: %v != %v: %w", mpu.Key, key, api.ErrObjectNotFound)
	} else if mpu.Bucket != bucket {
		return multipartUpload{}, nil, 0, "", fmt.Errorf("bucket name mismatch: %v != %v: %w", mpu.Bucket, bucket, api.ErrBucketNotFound)
	} else if err := mpu.EC.UnmarshalBinary(ec); err != nil {
		return multipartUpload{}, nil, 0, "", fmt.Errorf("failed to unmarshal encryption key: %w", err)
	}

	// find relevant parts
	rows, err := tx.Query(ctx, "SELECT id, part_number, etag, size FROM multipart_parts WHERE db_multipart_upload_id = ? ORDER BY part_number ASC", mpu.ID)
	if err != nil {
		return multipartUpload{}, nil, 0, "", fmt.Errorf("failed to fetch parts: %w", err)
	}
	defer rows.Close()

	var storedParts []multipartUploadPart
	for rows.Next() {
		var p multipartUploadPart
		if err := rows.Scan(&p.ID, &p.PartNumber, &p.Etag, &p.Size); err != nil {
			return multipartUpload{}, nil, 0, "", fmt.Errorf("failed to scan part: %w", err)
		}
		storedParts = append(storedParts, p)
	}

	var neededParts []multipartUploadPart
	var size int64
	h := types.NewHasher()
	j := 0
	for _, part := range storedParts {
		for {
			if j >= len(storedParts) {
				// ran out of parts in the database
				return multipartUpload{}, nil, 0, "", api.ErrPartNotFound
			} else if storedParts[j].PartNumber > part.PartNumber {
				// missing part
				return multipartUpload{}, nil, 0, "", api.ErrPartNotFound
			} else if storedParts[j].PartNumber == part.PartNumber && storedParts[j].Etag == strings.Trim(part.Etag, "\"") {
				// found a match
				neededParts = append(neededParts, storedParts[j])
				size += storedParts[j].Size
				j++

				// update hasher
				if _, err = h.E.Write([]byte(part.Etag)); err != nil {
					return multipartUpload{}, nil, 0, "", fmt.Errorf("failed to hash etag: %w", err)
				}
				break
			} else {
				// try next
				j++
			}
		}
	}

	// compute ETag.
	sum := h.Sum()
	eTag := hex.EncodeToString(sum[:])
	return mpu, neededParts, size, eTag, nil
}

func NormalizePeer(peer string) (string, error) {
	host, _, err := net.SplitHostPort(peer)
	if err != nil {
		host = peer
	}
	if strings.IndexByte(host, '/') != -1 {
		_, subnet, err := net.ParseCIDR(host)
		if err != nil {
			return "", fmt.Errorf("failed to parse CIDR: %w", err)
		}
		return subnet.String(), nil
	}

	ip := net.ParseIP(host)
	if ip == nil {
		return "", errors.New("invalid IP address")
	}

	var maskLen int
	if ip.To4() != nil {
		maskLen = 32
	} else {
		maskLen = 128
	}

	_, normalized, err := net.ParseCIDR(fmt.Sprintf("%s/%d", ip.String(), maskLen))
	if err != nil {
		panic("failed to parse CIDR")
	}
	return normalized.String(), nil
}

func Objects(ctx context.Context, tx Tx, bucket, prefix, substring, delim, sortBy, sortDir, marker string, limit int, slabEncryptionKey object.EncryptionKey) (resp api.ObjectsResponse, err error) {
	switch delim {
	case "":
		resp, err = listObjectsNoDelim(ctx, tx, bucket, prefix, substring, sortBy, sortDir, marker, limit, slabEncryptionKey)
	case "/":
		resp, err = listObjectsSlashDelim(ctx, tx, bucket, prefix, sortBy, sortDir, marker, limit, slabEncryptionKey)
	default:
		err = fmt.Errorf("unsupported delimiter: '%s'", delim)
	}
	return
}

func ObjectMetadata(ctx context.Context, tx Tx, bucket, key string) (api.Object, error) {
	// fetch object id
	var objID int64
	if err := tx.QueryRow(ctx, `
		SELECT o.id
		FROM objects o
		INNER JOIN buckets b ON b.id = o.db_bucket_id
		WHERE o.object_id = ? AND b.name = ?
	`, key, bucket).Scan(&objID); errors.Is(err, dsql.ErrNoRows) {
		return api.Object{}, api.ErrObjectNotFound
	} else if err != nil {
		return api.Object{}, fmt.Errorf("failed to fetch object id: %w", err)
	}

	// fetch metadata
	om, err := tx.ScanObjectMetadata(tx.QueryRow(ctx, fmt.Sprintf(`
		SELECT %s
		FROM objects o
		INNER JOIN buckets b ON b.id = o.db_bucket_id
		WHERE o.id = ?
	`, tx.SelectObjectMetadataExpr()), objID))
	if err != nil {
		return api.Object{}, fmt.Errorf("failed to fetch object metadata: %w", err)
	}

	// fetch user metadata
	rows, err := tx.Query(ctx, `
		SELECT oum.key, oum.value
		FROM object_user_metadata oum
		WHERE oum.db_object_id = ?
		ORDER BY oum.id ASC
	`, objID)
	if err != nil {
		return api.Object{}, fmt.Errorf("failed to fetch user metadata: %w", err)
	}
	defer rows.Close()

	// build object
	metadata := make(api.ObjectUserMetadata)
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			return api.Object{}, fmt.Errorf("failed to scan user metadata: %w", err)
		}
		metadata[key] = value
	}

	return api.Object{
		Metadata:       metadata,
		ObjectMetadata: om,
		Object:         nil, // only return metadata
	}, nil
}

func ObjectsStats(ctx context.Context, tx sql.Tx, opts api.ObjectsStatsOpts) (api.ObjectsStatsResponse, error) {
	var args []any
	var bucketExpr string
	var bucketID int64
	if opts.Bucket != "" {
		err := tx.QueryRow(ctx, "SELECT id FROM buckets WHERE name = ?", opts.Bucket).
			Scan(&bucketID)
		if errors.Is(err, dsql.ErrNoRows) {
			return api.ObjectsStatsResponse{}, api.ErrBucketNotFound
		} else if err != nil {
			return api.ObjectsStatsResponse{}, fmt.Errorf("failed to fetch bucket id: %w", err)
		}
		bucketExpr = "WHERE db_bucket_id = ?"
		args = append(args, bucketID)
	}

	// objects stats
	var numObjects, totalObjectsSize uint64
	var minHealth float64
	err := tx.QueryRow(ctx, "SELECT COUNT(*), COALESCE(MIN(health), 1), COALESCE(SUM(size), 0) FROM objects "+bucketExpr, args...).
		Scan(&numObjects, &minHealth, &totalObjectsSize)
	if err != nil {
		return api.ObjectsStatsResponse{}, fmt.Errorf("failed to fetch objects stats: %w", err)
	}

	// multipart upload stats
	var unfinishedObjects uint64
	err = tx.QueryRow(ctx, "SELECT COUNT(*) FROM multipart_uploads "+bucketExpr, args...).
		Scan(&unfinishedObjects)
	if err != nil {
		return api.ObjectsStatsResponse{}, fmt.Errorf("failed to fetch multipart upload stats: %w", err)
	}

	// multipart upload part stats
	var totalUnfinishedObjectsSize uint64
	err = tx.QueryRow(ctx, "SELECT COALESCE(SUM(size), 0) FROM multipart_parts mp INNER JOIN multipart_uploads mu ON mp.db_multipart_upload_id = mu.id "+bucketExpr, args...).
		Scan(&totalUnfinishedObjectsSize)
	if err != nil {
		return api.ObjectsStatsResponse{}, fmt.Errorf("failed to fetch multipart upload part stats: %w", err)
	}

	// total sectors
	var whereExpr string
	var whereArgs []any
	if opts.Bucket != "" {
		whereExpr = `
			AND EXISTS (
				SELECT 1 FROM slices sli
				INNER JOIN objects o ON o.id = sli.db_object_id AND o.db_bucket_id = ?
				WHERE sli.db_slab_id = sla.id
			)
		`
		whereArgs = append(whereArgs, bucketID)
	}
	var totalSectors uint64
	err = tx.QueryRow(ctx, "SELECT COALESCE(SUM(total_shards), 0) FROM slabs sla WHERE db_buffered_slab_id IS NULL "+whereExpr, whereArgs...).
		Scan(&totalSectors)
	if err != nil {
		return api.ObjectsStatsResponse{}, fmt.Errorf("failed to fetch total sector stats: %w", err)
	}

	var totalUploaded uint64
	err = tx.QueryRow(ctx, "SELECT COALESCE(SUM(size), 0) FROM contracts WHERE archival_reason IS NULL").
		Scan(&totalUploaded)
	if err != nil {
		return api.ObjectsStatsResponse{}, fmt.Errorf("failed to fetch contract stats: %w", err)
	}

	return api.ObjectsStatsResponse{
		MinHealth:                  minHealth,
		NumObjects:                 numObjects,
		NumUnfinishedObjects:       unfinishedObjects,
		TotalUnfinishedObjectsSize: totalUnfinishedObjectsSize,
		TotalObjectsSize:           totalObjectsSize,
		TotalSectorsSize:           totalSectors * rhpv4.SectorSize,
		TotalUploadedSize:          totalUploaded,
	}, nil
}

func PeerBanned(ctx context.Context, tx sql.Tx, addr string) (bool, error) {
	// normalize the address to a CIDR
	netCIDR, err := NormalizePeer(addr)
	if err != nil {
		return false, err
	}

	// parse the subnet
	_, subnet, err := net.ParseCIDR(netCIDR)
	if err != nil {
		return false, err
	}

	// check all subnets from the given subnet to the max subnet length
	var maxMaskLen int
	if subnet.IP.To4() != nil {
		maxMaskLen = 32
	} else {
		maxMaskLen = 128
	}

	checkSubnets := make([]any, 0, maxMaskLen)
	for i := maxMaskLen; i > 0; i-- {
		_, subnet, err := net.ParseCIDR(subnet.IP.String() + "/" + strconv.Itoa(i))
		if err != nil {
			return false, err
		}
		checkSubnets = append(checkSubnets, subnet.String())
	}

	var expiration time.Time
	err = tx.QueryRow(ctx, fmt.Sprintf(`SELECT expiration FROM syncer_bans WHERE net_cidr IN (%s) ORDER BY expiration DESC LIMIT 1`, strings.Repeat("?, ", len(checkSubnets)-1)+"?"), checkSubnets...).
		Scan((*UnixTimeMS)(&expiration))
	if errors.Is(err, dsql.ErrNoRows) {
		return false, nil
	} else if err != nil {
		return false, err
	}

	return time.Now().Before(expiration), nil
}

func PeerInfo(ctx context.Context, tx sql.Tx, addr string) (syncer.PeerInfo, error) {
	var peer syncer.PeerInfo
	err := tx.QueryRow(ctx, "SELECT address, first_seen, last_connect, synced_blocks, sync_duration FROM syncer_peers WHERE address = ?", addr).
		Scan(&peer.Address, (*UnixTimeMS)(&peer.FirstSeen), (*UnixTimeMS)(&peer.LastConnect), (*Unsigned64)(&peer.SyncedBlocks), &peer.SyncDuration)
	if errors.Is(err, dsql.ErrNoRows) {
		return syncer.PeerInfo{}, syncer.ErrPeerNotFound
	} else if err != nil {
		return syncer.PeerInfo{}, fmt.Errorf("failed to fetch peer: %w", err)
	}
	return peer, nil
}

func Peers(ctx context.Context, tx sql.Tx) ([]syncer.PeerInfo, error) {
	rows, err := tx.Query(ctx, "SELECT address, first_seen, last_connect, synced_blocks, sync_duration FROM syncer_peers")
	if err != nil {
		return nil, fmt.Errorf("failed to fetch peers: %w", err)
	}
	defer rows.Close()

	var peers []syncer.PeerInfo
	for rows.Next() {
		var peer syncer.PeerInfo
		if err := rows.Scan(&peer.Address, (*UnixTimeMS)(&peer.FirstSeen), (*UnixTimeMS)(&peer.LastConnect), (*Unsigned64)(&peer.SyncedBlocks), &peer.SyncDuration); err != nil {
			return nil, fmt.Errorf("failed to scan peer: %w", err)
		}
		peers = append(peers, peer)
	}
	return peers, nil
}

func PruneSlabs(ctx context.Context, tx sql.Tx, limit int64) (int64, error) {
	res, err := tx.Exec(ctx, `
	DELETE FROM slabs
	WHERE id IN (
		SELECT id FROM (
			SELECT s.id
			FROM slabs s
			LEFT JOIN slices sl ON sl.db_slab_id = s.id
			WHERE s.db_buffered_slab_id IS NULL AND sl.db_slab_id IS NULL
			LIMIT ?
		) AS limited
	)`, limit)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func RecordHostScans(ctx context.Context, tx sql.Tx, scans []api.HostScan) error {
	if len(scans) == 0 {
		return nil
	}
	// NOTE: The order of the assignments in the UPDATE statement is important
	// for MySQL compatibility. e.g. second_to_last_scan_success must be set
	// before last_scan_success.
	stmt, err := tx.Prepare(ctx, `
		UPDATE hosts SET
		scanned = scanned OR ?,
		total_scans = total_scans + 1,
		second_to_last_scan_success = last_scan_success,
		last_scan_success = ?,
		recent_downtime = CASE WHEN ? AND last_scan > 0 AND last_scan < ? THEN recent_downtime + ? - last_scan ELSE CASE WHEN ? THEN 0 ELSE recent_downtime END END,
		recent_scan_failures = CASE WHEN ? THEN 0 ELSE recent_scan_failures + 1 END,
		downtime = CASE WHEN ? AND last_scan > 0 AND last_scan < ? THEN downtime + ? - last_scan ELSE downtime END,
		uptime = CASE WHEN ? AND last_scan > 0 AND last_scan < ? THEN uptime + ? - last_scan ELSE uptime END,
		last_scan = ?,
		v2_settings = CASE WHEN ? THEN ? ELSE v2_settings END,
		successful_interactions = CASE WHEN ? THEN successful_interactions + 1 ELSE successful_interactions END,
		failed_interactions = CASE WHEN ? THEN failed_interactions + 1 ELSE failed_interactions END
		WHERE public_key = ?
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement to update host with scan: %w", err)
	}
	defer stmt.Close()

	for _, scan := range scans {
		scanTime := scan.Timestamp.UnixMilli()
		_, err = stmt.Exec(ctx,
			scan.Success,                                    // scanned
			scan.Success,                                    // last_scan_success
			!scan.Success, scanTime, scanTime, scan.Success, // recent_downtime
			scan.Success,                      // recent_scan_failures
			!scan.Success, scanTime, scanTime, // downtime
			scan.Success, scanTime, scanTime, // uptime
			scanTime,                                    // last_scan
			scan.Success, HostSettings(scan.V2Settings), // settings
			scan.Success,  // successful_interactions
			!scan.Success, // failed_interactions
			PublicKey(scan.HostKey),
		)
		if err != nil {
			return fmt.Errorf("failed to update host with scan: %w", err)
		}
	}
	return nil
}

func RemoveOfflineHosts(ctx context.Context, tx sql.Tx, minRecentFailures uint64, maxDownTime time.Duration) (int64, error) {
	// fetch contracts belonging to offline hosts
	rows, err := tx.Query(ctx, `
SELECT fcid
FROM contracts c
INNER JOIN hosts h ON h.public_key = c.host_key
WHERE h.recent_downtime >= ? AND h.recent_scan_failures >= ?`, DurationMS(maxDownTime), minRecentFailures)
	if err != nil {
		return 0, fmt.Errorf("failed to fetch contracts: %w", err)
	}
	defer rows.Close()

	var fcids []types.FileContractID
	for rows.Next() {
		var fcid FileContractID
		if err := rows.Scan(&fcid); err != nil {
			return 0, fmt.Errorf("failed to scan contract: %w", err)
		}
		fcids = append(fcids, types.FileContractID(fcid))
	}

	// archive those contracts
	for _, fcid := range fcids {
		if err := ArchiveContract(ctx, tx, fcid, api.ContractArchivalReasonHostPruned); err != nil {
			return 0, fmt.Errorf("failed to archive contract %v: %w", fcid, err)
		}
	}

	// delete hosts
	res, err := tx.Exec(ctx, `DELETE FROM hosts WHERE recent_downtime >= ? AND recent_scan_failures >= ?`, DurationMS(maxDownTime), minRecentFailures)
	if err != nil {
		return 0, fmt.Errorf("failed to delete hosts: %w", err)
	}
	return res.RowsAffected()
}

func QueryContracts(ctx context.Context, tx sql.Tx, whereExprs []string, whereArgs []any) ([]api.ContractMetadata, error) {
	var whereExpr string
	if len(whereExprs) > 0 {
		whereExpr = "WHERE " + strings.Join(whereExprs, " AND ")
	}

	rows, err := tx.Query(ctx, fmt.Sprintf(`
SELECT
	c.fcid, c.host_id, c.host_key,
	c.archival_reason, c.proof_height, c.renewed_from, c.renewed_to, c.revision_height, c.revision_number, c.size, c.start_height, c.state, c.usability, c.window_start, c.window_end,
	c.contract_price, c.initial_renter_funds,
	c.delete_spending, c.fund_account_spending, c.sector_roots_spending, c.upload_spending
FROM contracts AS c
%s
ORDER BY c.id ASC`, whereExpr), whereArgs...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var scannedRows []ContractRow
	for rows.Next() {
		var r ContractRow
		if err := r.Scan(rows); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		scannedRows = append(scannedRows, r)
	}

	if len(scannedRows) == 0 {
		return nil, nil
	}

	// merge 'Host', 'Name' and 'Contract' into dbContracts
	var contracts []api.ContractMetadata
	current, scannedRows := scannedRows[0].ContractMetadata(), scannedRows[1:]
	for {
		if len(scannedRows) == 0 {
			contracts = append(contracts, current)
			break
		} else if current.ID != types.FileContractID(scannedRows[0].FCID) {
			contracts = append(contracts, current)
		}
		current, scannedRows = scannedRows[0].ContractMetadata(), scannedRows[1:]
	}
	return contracts, nil
}

func RenewedContract(ctx context.Context, tx sql.Tx, renewedFrom types.FileContractID) (api.ContractMetadata, error) {
	contracts, err := QueryContracts(ctx, tx, []string{"c.renewed_from = ?"}, []any{FileContractID(renewedFrom)})
	if err != nil {
		return api.ContractMetadata{}, fmt.Errorf("failed to query renewed contract: %w", err)
	} else if len(contracts) == 0 {
		return api.ContractMetadata{}, api.ErrContractNotFound
	}
	return contracts[0], nil
}

func ResetChainState(ctx context.Context, tx sql.Tx) error {
	if _, err := tx.Exec(ctx, "DELETE FROM consensus_infos"); err != nil {
		return err
	} else if _, err := tx.Exec(ctx, "DELETE FROM wallet_events"); err != nil {
		return err
	} else if _, err := tx.Exec(ctx, "DELETE FROM wallet_outputs"); err != nil {
		return err
	} else if _, err := tx.Exec(ctx, "DELETE FROM contract_elements"); err != nil {
		return err
	}
	return nil
}

func ResetLostSectors(ctx context.Context, tx sql.Tx, hk types.PublicKey) error {
	_, err := tx.Exec(ctx, "UPDATE hosts SET lost_sectors = 0 WHERE public_key = ?", PublicKey(hk))
	if err != nil {
		return fmt.Errorf("failed to reset lost sectors for host %v: %w", hk, err)
	}
	return nil
}

func Setting(ctx context.Context, tx sql.Tx, key string) (string, error) {
	var value string
	err := tx.QueryRow(ctx, "SELECT value FROM settings WHERE `key` = ?", key).Scan((*BusSetting)(&value))
	if errors.Is(err, dsql.ErrNoRows) {
		return "", ErrSettingNotFound
	} else if err != nil {
		return "", fmt.Errorf("failed to fetch setting '%s': %w", key, err)
	}
	return value, nil
}

func Slab(ctx context.Context, tx sql.Tx, key object.EncryptionKey) (object.Slab, error) {
	// fetch slab
	var slabID int64
	slab := object.Slab{EncryptionKey: key}
	err := tx.QueryRow(ctx, `
		SELECT id, health, min_shards
		FROM slabs sla
		WHERE sla.key = ?
	`, EncryptionKey(key)).Scan(&slabID, &slab.Health, &slab.MinShards)
	if errors.Is(err, dsql.ErrNoRows) {
		return object.Slab{}, api.ErrSlabNotFound
	} else if err != nil {
		return object.Slab{}, fmt.Errorf("failed to fetch slab: %w", err)
	}

	// fetch sectors
	rows, err := tx.Query(ctx, `
		SELECT id, root
		FROM sectors s
		WHERE s.db_slab_id = ?
		ORDER BY s.slab_index
	`, slabID)
	if err != nil {
		return object.Slab{}, fmt.Errorf("failed to fetch sectors: %w", err)
	}
	defer rows.Close()

	var sectorIDs []int64
	for rows.Next() {
		var sectorID int64
		var sector object.Sector
		if err := rows.Scan(&sectorID, (*Hash256)(&sector.Root)); err != nil {
			return object.Slab{}, fmt.Errorf("failed to scan sector: %w", err)
		}
		slab.Shards = append(slab.Shards, sector)
		sectorIDs = append(sectorIDs, sectorID)
	}

	// fetch contracts for each sector
	stmt, err := tx.Prepare(ctx, `
		SELECT h.public_key, c.fcid
		FROM contract_sectors cs
		INNER JOIN contracts c ON c.id = cs.db_contract_id
		INNER JOIN hosts h ON h.public_key = c.host_key
		WHERE cs.db_sector_id = ?
		ORDER BY c.id
	`)
	if err != nil {
		return object.Slab{}, fmt.Errorf("failed to prepare statement to fetch contracts: %w", err)
	}
	defer stmt.Close()

	// fetch hosts for each sector
	hostStmt, err := tx.Prepare(ctx, `
		SELECT h.public_key
		FROM host_sectors hs
		INNER JOIN hosts h ON h.id = hs.db_host_id
		WHERE hs.db_sector_id = ?
	`)
	if err != nil {
		return object.Slab{}, fmt.Errorf("failed to prepare statement to fetch hosts: %w", err)
	}
	defer hostStmt.Close()

	for i, sectorID := range sectorIDs {
		slab.Shards[i].Contracts = make(map[types.PublicKey][]types.FileContractID)

		// hosts
		rows, err = hostStmt.Query(ctx, sectorID)
		if err != nil {
			return object.Slab{}, fmt.Errorf("failed to fetch hosts: %w", err)
		}
		if err := func() error {
			defer rows.Close()

			for rows.Next() {
				var pk types.PublicKey
				if err := rows.Scan((*PublicKey)(&pk)); err != nil {
					return fmt.Errorf("failed to scan host: %w", err)
				}
				if _, exists := slab.Shards[i].Contracts[pk]; !exists {
					slab.Shards[i].Contracts[pk] = []types.FileContractID{}
				}
			}
			return nil
		}(); err != nil {
			return object.Slab{}, err
		}

		// contracts
		rows, err := stmt.Query(ctx, sectorID)
		if err != nil {
			return object.Slab{}, fmt.Errorf("failed to fetch contracts: %w", err)
		}
		if err := func() error {
			defer rows.Close()

			for rows.Next() {
				var pk types.PublicKey
				var fcid types.FileContractID
				if err := rows.Scan((*PublicKey)(&pk), (*FileContractID)(&fcid)); err != nil {
					return fmt.Errorf("failed to scan contract: %w", err)
				}
				if _, exists := slab.Shards[i].Contracts[pk]; !exists {
					slab.Shards[i].Contracts[pk] = []types.FileContractID{}
				}
				if fcid != (types.FileContractID{}) {
					slab.Shards[i].Contracts[pk] = append(slab.Shards[i].Contracts[pk], fcid)
				}
			}
			return nil
		}(); err != nil {
			return object.Slab{}, err
		}
	}
	return slab, nil
}

func Tip(ctx context.Context, tx sql.Tx) (types.ChainIndex, error) {
	var id Hash256
	var height uint64
	if err := tx.QueryRow(ctx, "SELECT height, block_id FROM consensus_infos WHERE id = ?", sql.ConsensusInfoID).
		Scan(&height, &id); errors.Is(err, dsql.ErrNoRows) {
		// init
		_, err = tx.Exec(ctx, "INSERT INTO consensus_infos (created_at, id, height, block_id) VALUES (?, ?, ?, ?)", time.Now(), sql.ConsensusInfoID, 0, Hash256{})
		if err != nil {
			return types.ChainIndex{}, fmt.Errorf("failed to init consensus_infos: %w", err)
		}
		return types.ChainIndex{}, nil
	} else if err != nil {
		return types.ChainIndex{}, fmt.Errorf("failed to fetch chain index: %w", err)
	}
	return types.ChainIndex{
		ID:     types.BlockID(id),
		Height: height,
	}, nil
}

func SlabsForMigration(ctx context.Context, tx sql.Tx, healthCutoff float64, limit int) ([]api.UnhealthySlab, error) {
	rows, err := tx.Query(ctx, `
		SELECT sla.key, sla.health
		FROM slabs sla
		WHERE sla.health <= ? AND sla.health_valid_until > ? AND sla.db_buffered_slab_id IS NULL
		ORDER BY sla.health ASC
		LIMIT ?
	`, healthCutoff, time.Now().Unix(), limit)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch unhealthy slabs: %w", err)
	}
	defer rows.Close()

	var slabs []api.UnhealthySlab
	for rows.Next() {
		var slab api.UnhealthySlab
		if err := rows.Scan((*EncryptionKey)(&slab.EncryptionKey), &slab.Health); err != nil {
			return nil, fmt.Errorf("failed to scan unhealthy slab: %w", err)
		}
		slabs = append(slabs, slab)
	}
	return slabs, nil
}

func UpdateBucketPolicy(ctx context.Context, tx sql.Tx, bucket string, bp api.BucketPolicy) error {
	policy, err := json.Marshal(bp)
	if err != nil {
		return err
	}
	res, err := tx.Exec(ctx, "UPDATE buckets SET policy = ? WHERE name = ?", policy, bucket)
	if err != nil {
		return fmt.Errorf("failed to update bucket policy: %w", err)
	} else if n, err := res.RowsAffected(); err != nil {
		return fmt.Errorf("failed to check rows affected: %w", err)
	} else if n == 0 {
		return api.ErrBucketNotFound
	}
	return nil
}

func UpdateContract(ctx context.Context, tx sql.Tx, fcid types.FileContractID, c api.ContractMetadata) error {
	// validate metadata
	var state ContractState
	if err := state.LoadString(c.State); err != nil {
		return err
	}
	var usability ContractUsability
	if err := usability.LoadString(c.Usability); err != nil {
		return err
	}
	if c.ID == (types.FileContractID{}) {
		return errors.New("contract id is required")
	} else if c.HostKey == (types.PublicKey{}) {
		return errors.New("host key is required")
	}

	// update contract
	_, err := tx.Exec(ctx, `
UPDATE contracts SET
	created_at = ?, fcid = ?,
	proof_height = ?, renewed_from = ?, revision_height = ?, revision_number = ?, size = ?, start_height = ?, state = ?, usability = ?, window_start = ?, window_end = ?,
	contract_price = ?, initial_renter_funds = ?,
	delete_spending = ?, fund_account_spending = ?, sector_roots_spending = ?, upload_spending = ?
WHERE fcid = ?`,
		time.Now(), FileContractID(c.ID),
		0, FileContractID(c.RenewedFrom), 0, fmt.Sprint(c.RevisionNumber), c.Size, c.StartHeight, state, usability, c.WindowStart, c.WindowEnd,
		Currency(c.ContractPrice), Currency(c.InitialRenterFunds),
		ZeroCurrency, ZeroCurrency, ZeroCurrency, ZeroCurrency,
		FileContractID(c.RenewedFrom),
	)
	if err != nil {
		return fmt.Errorf("failed to update contract: %w", err)
	}
	return nil
}

func UpdateContractUsability(ctx context.Context, tx sql.Tx, fcid types.FileContractID, usability string) error {
	var u ContractUsability
	if err := u.LoadString(usability); err != nil {
		return err
	}

	_, err := tx.Exec(ctx, `UPDATE contracts SET usability = ? WHERE fcid = ?`, u, FileContractID(fcid))
	if errors.Is(err, dsql.ErrNoRows) {
		return api.ErrContractNotFound
	}
	return err
}

func UpdateAutopilotConfig(ctx context.Context, tx sql.Tx, cfg api.AutopilotConfig) error {
	_, err := tx.Exec(ctx, `
UPDATE autopilot_config
SET enabled = ?,
	contracts_amount = ?,
	contracts_period = ?,
	contracts_renew_window = ?,
	contracts_download = ?,
	contracts_upload = ?,
	contracts_storage = ?,
	contracts_prune = ?,
	hosts_max_downtime_hours = ?,
	hosts_min_protocol_version = ?,
	hosts_max_consecutive_scan_failures = ?
WHERE id = ?`,
		cfg.Enabled,
		cfg.Contracts.Amount,
		cfg.Contracts.Period,
		cfg.Contracts.RenewWindow,
		cfg.Contracts.Download,
		cfg.Contracts.Upload,
		cfg.Contracts.Storage,
		cfg.Contracts.Prune,
		cfg.Hosts.MaxDowntimeHours,
		cfg.Hosts.MinProtocolVersion,
		cfg.Hosts.MaxConsecutiveScanFailures,
		sql.AutopilotID)
	return err
}

func UpdatePeerInfo(ctx context.Context, tx sql.Tx, addr string, fn func(*syncer.PeerInfo)) error {
	info, err := PeerInfo(ctx, tx, addr)
	if err != nil {
		return err
	}
	fn(&info)

	res, err := tx.Exec(ctx, "UPDATE syncer_peers SET last_connect = ?, synced_blocks = ?, sync_duration = ? WHERE address = ?",
		UnixTimeMS(info.LastConnect),
		Unsigned64(info.SyncedBlocks),
		info.SyncDuration,
		addr,
	)
	if err != nil {
		return fmt.Errorf("failed to update peer info: %w", err)
	} else if n, err := res.RowsAffected(); err != nil {
		return fmt.Errorf("failed to check rows affected: %w", err)
	} else if n == 0 {
		return syncer.ErrPeerNotFound
	}

	return nil
}

func UpdateSlab(ctx context.Context, tx Tx, key object.EncryptionKey, updated []api.UploadedSector) error {
	// update slab
	res, err := tx.Exec(ctx, `
		UPDATE slabs
		SET health_valid_until = ?, health = ?
		WHERE `+"`key`"+` = ?
	`, time.Now().Unix(), 1, EncryptionKey(key))
	if err != nil {
		return err
	} else if n, err := res.RowsAffected(); err != nil {
		return err
	} else if n == 0 {
		return api.ErrSlabNotFound
	}

	// fetch sectors
	rows, err := tx.Query(ctx, "SELECT s.id, s.root FROM sectors s INNER JOIN slabs sl ON s.db_slab_id = sl.id WHERE sl.key = ? ORDER BY s.slab_index ASC", EncryptionKey(key))
	if err != nil {
		return fmt.Errorf("failed to fetch sectors: %w", err)
	}
	defer rows.Close()

	sectors := make(map[types.Hash256]int64)
	for rows.Next() {
		var id int64
		var root Hash256
		if err := rows.Scan(&id, &root); err != nil {
			return fmt.Errorf("failed to scan sector id: %w", err)
		}
		sectors[types.Hash256(root)] = id
	}

	// check sectors
	var fcids []types.FileContractID
	for _, s := range updated {
		if _, ok := sectors[s.Root]; !ok {
			return api.ErrUnknownSector
		}
		fcids = append(fcids, s.ContractID)
	}

	// fetch contracts
	contracts, err := FetchUsedContracts(ctx, tx, fcids)
	if err != nil {
		return err
	}

	// build contract <-> sector links
	var upsert []ContractSector
	for _, sector := range updated {
		contract, ok := contracts[sector.ContractID]
		if !ok {
			return api.ErrContractNotFound
		}
		sectorID, ok := sectors[sector.Root]
		if !ok {
			panic("sector not found") // developer error (already asserted)
		}
		upsert = append(upsert, ContractSector{
			HostID:     contract.HostID,
			ContractID: contract.ID,
			SectorID:   sectorID,
		})
	}
	return tx.UpsertContractSectors(ctx, upsert)
}

func UnspentSiacoinElements(ctx context.Context, tx sql.Tx) (ci types.ChainIndex, elements []types.SiacoinElement, err error) {
	rows, err := tx.Query(ctx, "SELECT output_id, leaf_index, merkle_proof, address, value, maturity_height FROM wallet_outputs")
	if err != nil {
		return types.ChainIndex{}, nil, fmt.Errorf("failed to fetch wallet events: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		element, err := scanSiacoinElement(rows)
		if err != nil {
			return types.ChainIndex{}, nil, fmt.Errorf("failed to scan wallet event: %w", err)
		}
		elements = append(elements, element)
	}
	ci, err = Tip(ctx, tx)
	if err != nil {
		return types.ChainIndex{}, nil, fmt.Errorf("failed to fetch chain tip: %w", err)
	}
	return
}

func UsableHosts(ctx context.Context, tx sql.Tx) ([]HostInfo, error) {
	// only include allowed hosts
	var whereExprs []string
	var hasAllowlist bool
	if err := tx.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM host_allowlist_entries)").Scan(&hasAllowlist); err != nil {
		return nil, fmt.Errorf("failed to check for allowlist: %w", err)
	} else if hasAllowlist {
		whereExprs = append(whereExprs, "EXISTS (SELECT 1 FROM host_allowlist_entry_hosts hbeh WHERE hbeh.db_host_id = h.id)")
	}

	// exclude blocked hosts
	var hasBlocklist bool
	if err := tx.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM host_blocklist_entries)").Scan(&hasBlocklist); err != nil {
		return nil, fmt.Errorf("failed to check for blocklist: %w", err)
	} else if hasBlocklist {
		whereExprs = append(whereExprs, "NOT EXISTS (SELECT 1 FROM host_blocklist_entry_hosts hbeh WHERE hbeh.db_host_id = h.id)")
	}

	// only include usable hosts
	whereExprs = append(whereExprs, `
EXISTS (
	SELECT 1
	FROM hosts h2
	INNER JOIN host_checks hc ON hc.db_host_id = h2.id AND h2.id = h.id
	WHERE
		hc.usability_blocked = 0 AND
		hc.usability_offline = 0 AND
		hc.usability_low_score = 0 AND
		hc.usability_redundant_ip = 0 AND
		hc.usability_gouging = 0 AND
		hc.usability_low_max_duration = 0 AND
		hc.usability_not_accepting_contracts = 0 AND
		hc.usability_not_announced = 0 AND
		hc.usability_not_completing_scan = 0
)`)

	// query hosts
	rows, err := tx.Query(ctx, fmt.Sprintf(`
	SELECT
	h.id,
	h.public_key,
	COALESCE(h.net_address, ""),
	COALESCE(h.settings->>'$.siamuxport', "") AS siamux_port,
	h.v2_settings
	FROM hosts h
	INNER JOIN contracts c on c.host_id = h.id and c.archival_reason IS NULL AND c.usability = ?
	INNER JOIN host_checks hc on hc.db_host_id = h.id
	WHERE %s
	GROUP by h.id`, strings.Join(whereExprs, " AND ")), contractUsabilityGood)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch hosts: %w", err)
	}
	defer rows.Close()

	var hosts []HostInfo
	var hostIDs []int64
	for rows.Next() {
		var hostID int64
		var hk PublicKey
		var addr, port string
		var v2Hs HostSettings
		err := rows.Scan(&hostID, &hk, &addr, &port, &v2Hs)
		if err != nil {
			return nil, fmt.Errorf("failed to scan host: %w", err)
		}

		hosts = append(hosts, HostInfo{
			api.HostInfo{
				PublicKey: types.PublicKey(hk),
			},
			rhp.HostSettings(v2Hs),
		})
		hostIDs = append(hostIDs, hostID)
	}

	// fill in v2 addresses
	err = fillInV2Addresses(ctx, tx, hostIDs, func(i int, addrs []string) {
		hosts[i].V2SiamuxAddresses = addrs
		i++
	})
	if err != nil {
		return nil, err
	}

	return hosts, nil
}

func WalletEvents(ctx context.Context, tx sql.Tx, offset, limit int) (events []wallet.Event, _ error) {
	if limit == 0 || limit == -1 {
		limit = math.MaxInt64
	}

	rows, err := tx.Query(ctx, "SELECT event_id, block_id, height, inflow, outflow, type, data, maturity_height, timestamp FROM wallet_events ORDER BY timestamp DESC LIMIT ? OFFSET ?", limit, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch wallet events: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		event, err := scanWalletEvent(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan wallet event: %w", err)
		}
		events = append(events, event)
	}
	return
}

func WalletEventCount(ctx context.Context, tx sql.Tx) (count uint64, err error) {
	var n int64
	err = tx.QueryRow(ctx, "SELECT COUNT(*) FROM wallet_events").Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("failed to count wallet events: %w", err)
	}
	return uint64(n), nil
}

func WalletLockedOutputs(ctx context.Context, tx sql.Tx, threshold time.Time) ([]types.SiacoinOutputID, error) {
	rows, err := tx.Query(ctx, `SELECT output_id FROM wallet_locked_outputs WHERE unlock_timestamp > ?`, UnixTimeMS(threshold))
	if err != nil {
		return nil, fmt.Errorf("failed to query locked outputs: %w", err)
	}
	defer rows.Close()

	var locked []types.SiacoinOutputID
	for rows.Next() {
		var id types.SiacoinOutputID
		if err := rows.Scan((*Hash256)(&id)); err != nil {
			return nil, fmt.Errorf("failed to scan locked output: %w", err)
		}
		locked = append(locked, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate locked outputs: %w", err)
	}

	return locked, nil
}

func WalletReleaseOutputs(ctx context.Context, tx sql.Tx, scois []types.SiacoinOutputID) error {
	if len(scois) == 0 {
		return nil
	}

	var args []any
	for _, id := range scois {
		args = append(args, Hash256(id))
	}
	args = append(args, UnixTimeMS(time.Now()))

	_, err := tx.Exec(ctx, fmt.Sprintf(`DELETE FROM wallet_locked_outputs WHERE output_id IN (%s) OR unlock_timestamp < ?`, strings.Repeat("?, ", len(scois)-1)+"?"), args...)
	return err
}

func scanBucket(s Scanner) (api.Bucket, error) {
	var createdAt time.Time
	var name, policy string
	err := s.Scan(&createdAt, &name, &policy)
	if errors.Is(err, dsql.ErrNoRows) {
		return api.Bucket{}, api.ErrBucketNotFound
	} else if err != nil {
		return api.Bucket{}, err
	}
	var bp api.BucketPolicy
	if err := json.Unmarshal([]byte(policy), &bp); err != nil {
		return api.Bucket{}, err
	}
	return api.Bucket{
		CreatedAt: api.TimeRFC3339(createdAt),
		Name:      name,
		Policy:    bp,
	}, nil
}

func scanMultipartUpload(s Scanner) (resp api.MultipartUpload, _ error) {
	err := s.Scan(&resp.Bucket, (*EncryptionKey)(&resp.EncryptionKey), &resp.Key, &resp.UploadID, &resp.CreatedAt)
	if errors.Is(err, dsql.ErrNoRows) {
		return api.MultipartUpload{}, api.ErrMultipartUploadNotFound
	} else if err != nil {
		return api.MultipartUpload{}, fmt.Errorf("failed to fetch multipart upload: %w", err)
	}
	return
}

func scanWalletEvent(s Scanner) (wallet.Event, error) {
	var blockID, eventID Hash256
	var height, maturityHeight uint64
	var inflow, outflow Currency
	var edata []byte
	var etype string
	var ts UnixTimeMS
	if err := s.Scan(
		&eventID,
		&blockID,
		&height,
		&inflow,
		&outflow,
		&etype,
		&edata,
		&maturityHeight,
		&ts,
	); err != nil {
		return wallet.Event{}, err
	}

	data, err := UnmarshalEventData(edata, etype)
	if err != nil {
		return wallet.Event{}, err
	}
	return wallet.Event{
		ID: types.Hash256(eventID),
		Index: types.ChainIndex{
			ID:     types.BlockID(blockID),
			Height: height,
		},
		Type:           etype,
		Data:           data,
		MaturityHeight: maturityHeight,
		Timestamp:      time.Time(ts),
	}, nil
}

func scanSiacoinElement(s Scanner) (el types.SiacoinElement, err error) {
	var id Hash256
	var leafIndex, maturityHeight uint64
	var merkleProof MerkleProof
	var address Hash256
	var value Currency
	err = s.Scan(&id, &leafIndex, &merkleProof, &address, &value, &maturityHeight)
	if err != nil {
		return types.SiacoinElement{}, err
	}
	return types.SiacoinElement{
		ID: types.SiacoinOutputID(id),
		StateElement: types.StateElement{
			LeafIndex:   leafIndex,
			MerkleProof: merkleProof.Hashes,
		},
		SiacoinOutput: types.SiacoinOutput{
			Address: types.Address(address),
			Value:   types.Currency(value),
		},
		MaturityHeight: maturityHeight,
	}, nil
}

func scanFileContractStateElement(s Scanner) (se FileContractStateElement, err error) {
	var proof MerkleProof
	err = s.Scan(&se.ID, &se.LeafIndex, &proof)
	se.MerkleProof = proof.Hashes
	return
}

func scanSiacoinStateElement(s Scanner) (se SiacoinStateElement, err error) {
	var proof MerkleProof
	err = s.Scan(&se.ID, &se.LeafIndex, &proof)
	se.MerkleProof = proof.Hashes
	return
}

func MarkPackedSlabUploaded(ctx context.Context, tx Tx, slab api.UploadedPackedSlab) (string, error) {
	// fetch relevant slab info
	var slabID, bufferedSlabID int64
	var bufferFileName string
	if err := tx.QueryRow(ctx, `
		SELECT sla.id, bs.id, bs.filename
		FROM slabs sla
		INNER JOIN buffered_slabs bs ON bs.id = sla.db_buffered_slab_id
		WHERE sla.db_buffered_slab_id = ?
	`, slab.BufferID).
		Scan(&slabID, &bufferedSlabID, &bufferFileName); err != nil {
		return "", fmt.Errorf("failed to fetch slab id: %w", err)
	}

	// set 'db_buffered_slab_id' to NULL
	if _, err := tx.Exec(ctx, "UPDATE slabs SET db_buffered_slab_id = NULL WHERE id = ?", slabID); err != nil {
		return "", fmt.Errorf("failed to update slab: %w", err)
	}

	// delete buffer slab
	if _, err := tx.Exec(ctx, "DELETE FROM buffered_slabs WHERE id = ?", bufferedSlabID); err != nil {
		return "", fmt.Errorf("failed to delete buffered slab: %w", err)
	}

	// fetch used contracts
	contracts, err := FetchUsedContracts(ctx, tx, slab.Contracts())
	if err != nil {
		return "", fmt.Errorf("failed to fetch used contracts: %w", err)
	}

	// stmt to add sector
	sectorStmt, err := tx.Prepare(ctx, "INSERT INTO sectors (db_slab_id, slab_index, root) VALUES (?, ?, ?)")
	if err != nil {
		return "", fmt.Errorf("failed to prepare statement to insert sectors: %w", err)
	}
	defer sectorStmt.Close()

	// stmt to insert contract_sector
	contractSectorStmt, err := tx.Prepare(ctx, "INSERT INTO contract_sectors (db_contract_id, db_sector_id) VALUES (?, ?)")
	if err != nil {
		return "", fmt.Errorf("failed to prepare statement to insert contract sectors: %w", err)
	}
	defer contractSectorStmt.Close()

	// stmt to insert host_sector
	hostSectorStmt, err := tx.Prepare(ctx, "INSERT INTO host_sectors (updated_at, db_host_id, db_sector_id) VALUES (?, ?, ?)")
	if err != nil {
		return "", fmt.Errorf("failed to prepare statement to insert host sectors: %w", err)
	}
	defer hostSectorStmt.Close()

	// insert shards
	for i, sector := range slab.Shards {
		// insert shard
		res, err := sectorStmt.Exec(ctx, slabID, i+1, slab.Shards[i].Root[:])
		if err != nil {
			return "", fmt.Errorf("failed to insert sector: %w", err)
		}
		sectorID, err := res.LastInsertId()
		if err != nil {
			return "", fmt.Errorf("failed to get sector id: %w", err)
		}

		uc, ok := contracts[sector.ContractID]
		if !ok {
			continue
		}

		// insert contract sector links
		if _, err := contractSectorStmt.Exec(ctx, uc.ID, sectorID); err != nil {
			return "", fmt.Errorf("failed to insert contract sector: %w", err)
		}

		// insert host sector link
		if _, err := hostSectorStmt.Exec(ctx, time.Now(), uc.HostID, sectorID); err != nil {
			return "", fmt.Errorf("failed to insert host sector: %w", err)
		}
	}
	return bufferFileName, nil
}

func RecordContractSpending(ctx context.Context, tx Tx, fcid types.FileContractID, revisionNumber, size uint64, newSpending api.ContractSpending) error {
	var updateKeys []string
	var updateValues []interface{}

	if !newSpending.Uploads.IsZero() {
		updateKeys = append(updateKeys, "upload_spending = ?")
		updateValues = append(updateValues, Currency(newSpending.Uploads))
	}
	if !newSpending.FundAccount.IsZero() {
		updateKeys = append(updateKeys, "fund_account_spending = ?")
		updateValues = append(updateValues, Currency(newSpending.FundAccount))
	}
	if !newSpending.Deletions.IsZero() {
		updateKeys = append(updateKeys, "delete_spending = ?")
		updateValues = append(updateValues, Currency(newSpending.Deletions))
	}
	if !newSpending.SectorRoots.IsZero() {
		updateKeys = append(updateKeys, "sector_roots_spending = ?")
		updateValues = append(updateValues, Currency(newSpending.SectorRoots))
	}
	updateKeys = append(updateKeys, "revision_number = ?", "size = ?")
	updateValues = append(updateValues, revisionNumber, size)

	updateValues = append(updateValues, FileContractID(fcid))
	_, err := tx.Exec(ctx, fmt.Sprintf(`
    UPDATE contracts
    SET %s
    WHERE fcid = ?
  `, strings.Join(updateKeys, ",")), updateValues...)
	if err != nil {
		return fmt.Errorf("failed to record contract spending: %w", err)
	}
	return nil
}

func Object(ctx context.Context, tx Tx, bucket, key string) (api.Object, error) {
	/// fetch object metadata
	row := tx.QueryRow(ctx, fmt.Sprintf(`
		SELECT %s, o.id, o.key
		FROM objects o
		INNER JOIN buckets b ON o.db_bucket_id = b.id
		WHERE o.object_id = ? AND b.name = ?
	`,
		tx.SelectObjectMetadataExpr()), key, bucket)
	var objID int64
	var ec object.EncryptionKey
	om, err := tx.ScanObjectMetadata(row, &objID, (*EncryptionKey)(&ec))
	if errors.Is(err, dsql.ErrNoRows) {
		return api.Object{}, api.ErrObjectNotFound
	} else if err != nil {
		return api.Object{}, err
	}

	// fetch user metadata
	rows, err := tx.Query(ctx, `
		SELECT oum.key, oum.value
		FROM object_user_metadata oum
		WHERE oum.db_object_id = ?
	`, objID)
	if err != nil {
		return api.Object{}, fmt.Errorf("failed to fetch user metadata: %w", err)
	}
	defer rows.Close()

	oum := make(api.ObjectUserMetadata)
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			return api.Object{}, fmt.Errorf("failed to scan user metadata: %w", err)
		}
		oum[key] = value
	}

	// fetch slab slices
	rows, err = tx.Query(ctx, `
		SELECT sla.health, sla.key, sla.min_shards, sli.offset, sli.length
		FROM slices sli
		INNER JOIN slabs sla ON sli.db_slab_id = sla.id
		WHERE sli.db_object_id = ?
		ORDER BY sli.object_index ASC
	`, objID)
	if err != nil {
		return api.Object{}, fmt.Errorf("failed to fetch slabs: %w", err)
	}
	defer rows.Close()

	slabSlices := object.SlabSlices{}
	for rows.Next() {
		var ss object.SlabSlice
		if err := rows.Scan(&ss.Health, (*EncryptionKey)(&ss.EncryptionKey), &ss.MinShards, &ss.Offset, &ss.Length); err != nil {
			return api.Object{}, fmt.Errorf("failed to scan slab slice: %w", err)
		}
		slabSlices = append(slabSlices, ss)
	}

	// fill in the shards
	for i := range slabSlices {
		slabSlices[i].Slab, err = Slab(ctx, tx, slabSlices[i].EncryptionKey)
		if err != nil {
			return api.Object{}, fmt.Errorf("failed to fetch slab: %w", err)
		}
	}

	return api.Object{
		Metadata:       oum,
		ObjectMetadata: om,
		Object: &object.Object{
			Key:   ec,
			Slabs: slabSlices,
		},
	}, nil
}

func fillInV2Addresses(ctx context.Context, tx sql.Tx, hostIDs []int64, assignFn func(int, []string)) error {
	// fill in v2 addresses
	netAddrsStmt, err := tx.Prepare(ctx, "SELECT ha.net_address, ha.protocol FROM host_addresses ha INNER JOIN hosts h ON ha.db_host_id = h.id WHERE h.id = ?")
	if err != nil {
		return fmt.Errorf("failed to prepare stmt for fetching host addresses: %w", err)
	}
	defer netAddrsStmt.Close()

	fetchAddrs := func(hostID int64) ([]chain.NetAddress, error) {
		rows, err := netAddrsStmt.Query(ctx, hostID)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var addrs []chain.NetAddress
		for rows.Next() {
			var addr chain.NetAddress
			if err := rows.Scan(&addr.Address, (*ChainProtocol)(&addr.Protocol)); err != nil {
				return nil, err
			}
			addrs = append(addrs, addr)
		}
		return addrs, nil
	}

	for i, hostID := range hostIDs {
		netAddrs, err := fetchAddrs(hostID)
		if err != nil {
			return fmt.Errorf("failed to fetch net addresses for host %d: %w", hostIDs[i], err)
		}
		var addrs []string
		for _, addr := range netAddrs {
			if addr.Protocol == siamux.Protocol {
				addrs = append(addrs, addr.Address)
			}
		}
		assignFn(i, addrs)
	}
	return nil
}

func listObjectsNoDelim(ctx context.Context, tx Tx, bucket, prefix, substring, sortBy, sortDir, marker string, limit int, slabEncryptionKey object.EncryptionKey) (api.ObjectsResponse, error) {
	// fetch one more to see if there are more entries
	if limit <= -1 {
		limit = math.MaxInt
	} else if limit != math.MaxInt {
		limit++
	}

	// establish sane defaults for sorting
	if sortBy == "" {
		sortBy = api.ObjectSortByName
	}
	if sortDir == "" {
		sortDir = api.SortDirAsc
	}

	var whereExprs []string
	var whereArgs []any

	// apply bucket
	if bucket != "" {
		whereExprs = append(whereExprs, "b.name = ?")
		whereArgs = append(whereArgs, bucket)
	}

	// apply prefix
	if prefix != "" {
		whereExprs = append(whereExprs, "o.object_id LIKE ? AND SUBSTR(o.object_id, 1, ?) = ?")
		whereArgs = append(whereArgs, prefix+"%", utf8.RuneCountInString(prefix), prefix)
	}

	// apply substring
	if substring != "" {
		whereExprs = append(whereExprs, "INSTR(o.object_id, ?) > 0")
		whereArgs = append(whereArgs, substring)
	}

	// apply sorting
	orderByExprs, err := orderByObject(sortBy, sortDir)
	if err != nil {
		return api.ObjectsResponse{}, fmt.Errorf("failed to apply sorting: %w", err)
	}

	// apply marker
	markerExprs, markerArgs, err := whereObjectMarker(marker, sortBy, sortDir, func(dst any, marker, col string) error {
		markerExprs := []string{"o.object_id = ?"}
		markerArgs := []any{marker}

		if bucket != "" {
			markerExprs = append(markerExprs, "b.name = ?")
			markerArgs = append(markerArgs, bucket)
		}

		err := tx.QueryRow(ctx, fmt.Sprintf(`
			SELECT o.%s
			FROM objects o
			INNER JOIN buckets b ON o.db_bucket_id = b.id
			WHERE %s
		`, col, strings.Join(markerExprs, " AND ")), markerArgs...).Scan(dst)
		if errors.Is(err, dsql.ErrNoRows) {
			return api.ErrMarkerNotFound
		} else {
			return err
		}
	})
	if err != nil {
		return api.ObjectsResponse{}, fmt.Errorf("failed to get marker exprs: %w", err)
	}
	whereExprs = append(whereExprs, markerExprs...)
	whereArgs = append(whereArgs, markerArgs...)

	// apply slab key
	if slabEncryptionKey != (object.EncryptionKey{}) {
		whereExprs = append(whereExprs, "EXISTS(SELECT 1 FROM objects o2 INNER JOIN slices sli ON sli.db_object_id = o2.id INNER JOIN slabs sla ON sla.id = sli.db_slab_id WHERE o2.id = o.id AND sla.key = ?)")
		whereArgs = append(whereArgs, EncryptionKey(slabEncryptionKey))
	}

	// apply limit
	whereArgs = append(whereArgs, limit)

	// build where expression
	var whereExpr string
	if len(whereExprs) > 0 {
		whereExpr = fmt.Sprintf("WHERE %s", strings.Join(whereExprs, " AND "))
	}

	query := fmt.Sprintf(`
	SELECT %s
	FROM objects o
	INNER JOIN buckets b ON b.id = o.db_bucket_id
	%s
	ORDER BY %s
	LIMIT ?
`,
		tx.SelectObjectMetadataExpr(),
		whereExpr,
		strings.Join(orderByExprs, ", "))
	// run query
	rows, err := tx.Query(ctx, query, whereArgs...)
	if err != nil {
		return api.ObjectsResponse{}, fmt.Errorf("failed to fetch objects: %w", err)
	}
	defer rows.Close()

	var objects []api.ObjectMetadata
	for rows.Next() {
		om, err := tx.ScanObjectMetadata(rows)
		if err != nil {
			return api.ObjectsResponse{}, fmt.Errorf("failed to scan object metadata: %w", err)
		}
		objects = append(objects, om)
	}

	var hasMore bool
	var nextMarker string
	if len(objects) == limit {
		objects = objects[:len(objects)-1]
		if len(objects) > 0 {
			hasMore = true
			nextMarker = objects[len(objects)-1].Key
		}
	}

	return api.ObjectsResponse{
		HasMore:    hasMore,
		NextMarker: nextMarker,
		Objects:    objects,
	}, nil
}

func listObjectsSlashDelim(ctx context.Context, tx Tx, bucket, prefix, sortBy, sortDir, marker string, limit int, slabEncryptionKey object.EncryptionKey) (api.ObjectsResponse, error) {
	// split prefix into path and object prefix
	path := "/" // root of bucket
	if idx := strings.LastIndex(prefix, "/"); idx != -1 {
		path = prefix[:idx+1]
		prefix = prefix[idx+1:]
	}
	if !strings.HasSuffix(path, "/") {
		panic("path must end with /")
	}

	// fetch one more to see if there are more entries
	if limit <= -1 {
		limit = math.MaxInt
	} else if limit != math.MaxInt {
		limit++
	}

	// establish sane defaults for sorting
	if sortBy == "" {
		sortBy = api.ObjectSortByName
	}
	if sortDir == "" {
		sortDir = api.SortDirAsc
	}

	// add object query args
	var args []any
	if bucket != "" {
		args = append(args, bucket)
	}
	args = append(args,
		path+"%", utf8.RuneCountInString(path), path, // case-sensitive object_id LIKE
		path,                           // exclude exact path
		utf8.RuneCountInString(path)+1, // exclude dirs
	)

	var slabKeyObjExpr string
	if slabEncryptionKey != (object.EncryptionKey{}) {
		slabKeyObjExpr = "AND EXISTS(SELECT 1 FROM objects o2 INNER JOIN slices sli ON sli.db_object_id = o2.id INNER JOIN slabs sla ON sla.id = sli.db_slab_id WHERE o2.id = o.id AND sla.key = ?)"
		args = append(args, EncryptionKey(slabEncryptionKey))
	}

	// add directory query args
	args = append(args,
		utf8.RuneCountInString(path), utf8.RuneCountInString(path)+1,
	)
	if bucket != "" {
		args = append(args, bucket)
	}
	args = append(args,
		path+"%", utf8.RuneCountInString(path), path, // case-sensitive object_id LIKE
		utf8.RuneCountInString(path), utf8.RuneCountInString(path)+1, path,
		utf8.RuneCountInString(path), utf8.RuneCountInString(path)+1,
	)
	var slabKeyDirExpr string
	if slabEncryptionKey != (object.EncryptionKey{}) {
		slabKeyDirExpr = "AND 1=0" // no directories when filtering by slab key
	}

	// apply marker
	var whereExprs []string
	markerExprs, markerArgs, err := whereObjectMarker(marker, sortBy, sortDir, func(dst any, marker, col string) error {
		var groupFn string
		switch col {
		case "size":
			groupFn = "SUM"
		case "health":
			groupFn = "MIN"
		default:
			return fmt.Errorf("unknown column: %v", col)
		}

		markerExprsObj := []string{"o.object_id = ?"}
		markerArgsObj := []any{marker}
		if bucket != "" {
			markerExprsObj = append(markerExprsObj, "b.name = ?")
			markerArgsObj = append(markerArgsObj, bucket)
		}

		markerExprsDir := []string{"SUBSTR(o.object_id, 1, ?) = ?"}
		markerArgsDir := []any{utf8.RuneCountInString(marker), marker}
		if bucket != "" {
			markerExprsDir = append(markerExprsDir, "b.name = ?")
			markerArgsDir = append(markerArgsDir, bucket)
		}

		err := tx.QueryRow(ctx, fmt.Sprintf(`
			SELECT o.%s
			FROM objects o
			INNER JOIN buckets b ON o.db_bucket_id = b.id
			WHERE %s
			UNION ALL
			SELECT %s(o.%s)
			FROM objects o
			INNER JOIN buckets b ON o.db_bucket_id = b.id
			WHERE %s
			GROUP BY o.db_bucket_id
		`, col, strings.Join(markerExprsObj, " AND "), groupFn, col, strings.Join(markerExprsDir, " AND ")), append(markerArgsObj, markerArgsDir...)...).Scan(dst)
		if errors.Is(err, dsql.ErrNoRows) {
			return api.ErrMarkerNotFound
		} else {
			return err
		}
	})
	if err != nil {
		return api.ObjectsResponse{}, fmt.Errorf("failed to query marker: %w", err)
	} else if len(markerExprs) > 0 {
		whereExprs = append(whereExprs, markerExprs...)
	}
	args = append(args, markerArgs...)

	// apply bucket
	bucketObjExpr := "1 = 1 AND"
	bucketDirExpr := "1 = 1 AND"
	if bucket != "" {
		bucketObjExpr = "b.name = ? AND"
		bucketDirExpr = "b.name = ? AND"
	}

	// apply prefix
	if prefix != "" {
		whereExprs = append(whereExprs, "SUBSTR(object_id, 1, ?) = ?")
		args = append(args,
			utf8.RuneCountInString(path+prefix), path+prefix,
		)
	}

	// apply sorting
	orderByExprs, err := orderByObject(sortBy, sortDir)
	if err != nil {
		return api.ObjectsResponse{}, fmt.Errorf("failed to apply sorting: %w", err)
	}

	// apply offset and limit
	args = append(args, limit)

	// build where expression
	var whereExpr string
	if len(whereExprs) > 0 {
		whereExpr = fmt.Sprintf("WHERE %s", strings.Join(whereExprs, " AND "))
	}

	// objectsQuery consists of 2 parts
	// 1. fetch all objects in requested directory
	// 2. fetch all sub-directories
	query := fmt.Sprintf(`
	SELECT %s
	FROM (
		SELECT o.db_bucket_id, o.object_id, o.size, o.health, o.mime_type, o.created_at, o.etag
		FROM objects o
		INNER JOIN buckets b ON b.id = o.db_bucket_id
		WHERE
			%s
			o.object_id LIKE ? AND SUBSTR(o.object_id, 1, ?) = ? AND
			o.object_id != ? AND
			INSTR(SUBSTR(o.object_id, ?), "/") = 0
			AND SUBSTR(o.object_id, -1, 1) != "/"
			%s

		UNION ALL

		SELECT MIN(o.db_bucket_id), MIN(SUBSTR(o.object_id, 1, ?+INSTR(SUBSTR(o.object_id, ?), "/"))) as object_id, SUM(o.size) as size, MIN(o.health), '' as mime_type, MAX(o.created_at), '' as etag
		FROM objects o
		INNER JOIN buckets b ON b.id = o.db_bucket_id
		WHERE
			%s
			o.object_id LIKE ? AND SUBSTR(o.object_id, 1, ?) = ? AND
			SUBSTR(o.object_id, 1, ?+INSTR(SUBSTR(o.object_id, ?), "/")) != ?
			%s
		GROUP BY SUBSTR(o.object_id, 1, ?+INSTR(SUBSTR(o.object_id, ?), "/"))
	) AS o
	INNER JOIN buckets b ON b.id = o.db_bucket_id
	%s
	ORDER BY %s
	LIMIT ?
`,
		tx.SelectObjectMetadataExpr(),
		bucketObjExpr,
		slabKeyObjExpr,
		bucketDirExpr,
		slabKeyDirExpr,
		whereExpr,
		strings.Join(orderByExprs, ", "),
	)

	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return api.ObjectsResponse{}, fmt.Errorf("failed to fetch objects: %w", err)
	}
	defer rows.Close()

	var objects []api.ObjectMetadata
	for rows.Next() {
		om, err := tx.ScanObjectMetadata(rows)
		if err != nil {
			return api.ObjectsResponse{}, fmt.Errorf("failed to scan object metadata: %w", err)
		}
		objects = append(objects, om)
	}

	// trim last element if we have more
	var hasMore bool
	var nextMarker string
	if len(objects) == limit {
		objects = objects[:len(objects)-1]
		if len(objects) > 0 {
			hasMore = true
			nextMarker = objects[len(objects)-1].Key
		}
	}

	return api.ObjectsResponse{
		HasMore:    hasMore,
		NextMarker: nextMarker,
		Objects:    objects,
	}, nil
}
