package sql

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strings"
	"time"
	"unicode/utf8"

	dsql "database/sql"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/sql"
	"go.sia.tech/renterd/object"
	"go.sia.tech/renterd/webhooks"
	"go.sia.tech/siad/modules"
	"lukechampine.com/frand"
)

const consensuInfoID = 1

var ErrNegativeOffset = errors.New("offset can not be negative")

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

func Accounts(ctx context.Context, tx sql.Tx) ([]api.Account, error) {
	rows, err := tx.Query(ctx, "SELECT account_id, clean_shutdown, host, balance, drift, requires_sync FROM ephemeral_accounts")
	if err != nil {
		return nil, fmt.Errorf("failed to fetch accounts: %w", err)
	}
	defer rows.Close()

	var accounts []api.Account
	for rows.Next() {
		a := api.Account{Balance: new(big.Int), Drift: new(big.Int)} // init big.Int
		if err := rows.Scan((*PublicKey)(&a.ID), &a.CleanShutdown, (*PublicKey)(&a.HostKey), (*BigInt)(a.Balance), (*BigInt)(a.Drift), &a.RequiresSync); err != nil {
			return nil, fmt.Errorf("failed to scan account: %w", err)
		}
		accounts = append(accounts, a)
	}
	return accounts, nil
}

func AncestorContracts(ctx context.Context, tx sql.Tx, fcid types.FileContractID, startHeight uint64) ([]api.ArchivedContract, error) {
	rows, err := tx.Query(ctx, `
		WITH RECURSIVE ancestors AS 
		(
			SELECT *
			FROM archived_contracts
			WHERE renewed_to = ?
			UNION ALL
			SELECT archived_contracts.*
			FROM ancestors, archived_contracts
			WHERE archived_contracts.renewed_to = ancestors.fcid
		)
		SELECT fcid, host, renewed_to, upload_spending, download_spending, fund_account_spending, delete_spending,
		proof_height, revision_height, revision_number, size, start_height, state, window_start, window_end
		FROM ancestors
		WHERE start_height >= ?
	`, FileContractID(fcid), startHeight)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch ancestor contracts: %w", err)
	}
	defer rows.Close()

	var contracts []api.ArchivedContract
	for rows.Next() {
		var c api.ArchivedContract
		var state ContractState
		err := rows.Scan((*FileContractID)(&c.ID), (*PublicKey)(&c.HostKey), (*FileContractID)(&c.RenewedTo),
			(*Currency)(&c.Spending.Uploads), (*Currency)(&c.Spending.Downloads), (*Currency)(&c.Spending.FundAccount),
			(*Currency)(&c.Spending.Deletions), &c.ProofHeight,
			&c.RevisionHeight, &c.RevisionNumber, &c.Size, &c.StartHeight, &state, &c.WindowStart,
			&c.WindowEnd)
		if err != nil {
			return nil, fmt.Errorf("failed to scan contract: %w", err)
		}
		c.State = state.String()
		contracts = append(contracts, c)
	}
	return contracts, nil
}

func ArchiveContract(ctx context.Context, tx sql.Tx, fcid types.FileContractID, reason string) error {
	if err := copyContractToArchive(ctx, tx, fcid, nil, reason); err != nil {
		return fmt.Errorf("failed to copy contract to archived_contracts: %w", err)
	}
	res, err := tx.Exec(ctx, "DELETE FROM contracts WHERE fcid = ?", FileContractID(fcid))
	if err != nil {
		return fmt.Errorf("failed to delete contract from contracts: %w", err)
	} else if n, err := res.RowsAffected(); err != nil {
		return fmt.Errorf("failed to fetch rows affected: %w", err)
	} else if n == 0 {
		return fmt.Errorf("expected to delete 1 row, deleted %d", n)
	}
	return nil
}

func Autopilot(ctx context.Context, tx sql.Tx, id string) (api.Autopilot, error) {
	row := tx.QueryRow(ctx, "SELECT identifier, config, current_period FROM autopilots WHERE identifier = ?", id)
	ap, err := scanAutopilot(row)
	if errors.Is(err, dsql.ErrNoRows) {
		return api.Autopilot{}, api.ErrAutopilotNotFound
	} else if err != nil {
		return api.Autopilot{}, fmt.Errorf("failed to fetch autopilot: %w", err)
	}
	return ap, nil
}

func Autopilots(ctx context.Context, tx sql.Tx) ([]api.Autopilot, error) {
	rows, err := tx.Query(ctx, "SELECT identifier, config, current_period FROM autopilots")
	if err != nil {
		return nil, fmt.Errorf("failed to fetch autopilots: %w", err)
	}
	defer rows.Close()

	var autopilots []api.Autopilot
	for rows.Next() {
		ap, err := scanAutopilot(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan autopilot: %w", err)
		}
		autopilots = append(autopilots, ap)
	}
	return autopilots, nil
}

func Bucket(ctx context.Context, tx sql.Tx, bucket string) (api.Bucket, error) {
	b, err := scanBucket(tx.QueryRow(ctx, "SELECT created_at, name, COALESCE(policy, '{}') FROM buckets WHERE name = ?", bucket))
	if err != nil {
		return api.Bucket{}, fmt.Errorf("failed to fetch bucket: %w", err)
	}
	return b, nil
}

func Contract(ctx context.Context, tx sql.Tx, fcid types.FileContractID) (api.ContractMetadata, error) {
	contracts, err := QueryContracts(ctx, tx, []string{"c.fcid = ?"}, []any{FileContractID(fcid)})
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
	if opts.ContractSet != "" {
		var contractSetID int64
		err := tx.QueryRow(ctx, "SELECT id FROM contract_sets WHERE contract_sets.name = ?", opts.ContractSet).
			Scan(&contractSetID)
		if errors.Is(err, dsql.ErrNoRows) {
			return nil, api.ErrContractSetNotFound
		}
		whereExprs = append(whereExprs, "cs.id = ?")
		whereArgs = append(whereArgs, contractSetID)
	}
	return QueryContracts(ctx, tx, whereExprs, whereArgs)
}

func ContractSets(ctx context.Context, tx sql.Tx) ([]string, error) {
	rows, err := tx.Query(ctx, "SELECT name FROM contract_sets")
	if err != nil {
		return nil, fmt.Errorf("failed to fetch contract sets: %w", err)
	}
	defer rows.Close()

	var sets []string
	for rows.Next() {
		var cs string
		if err := rows.Scan(&cs); err != nil {
			return nil, fmt.Errorf("failed to scan contract set: %w", err)
		}
		sets = append(sets, cs)
	}
	return sets, nil
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
		WHERE NOT EXISTS (
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
			GROUP BY c.fcid
		) i
	`, rhpv2.SectorSize)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch contract sizes: %w", err)
	}

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
			Scan(&om.ETag, &om.Health, (*time.Time)(&om.ModTime), &om.Name, &om.Size, &om.MimeType)
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
	res, err := tx.Exec(ctx, `INSERT INTO objects (created_at, object_id, db_directory_id, db_bucket_id,`+"`key`"+`, size, mime_type, etag)
						SELECT ?, ?, db_directory_id, ?, `+"`key`"+`, size, ?, etag
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

func DeleteHostSector(ctx context.Context, tx sql.Tx, hk types.PublicKey, root types.Hash256) (int, error) {
	// update the latest_host field of the sector
	_, err := tx.Exec(ctx, `
		UPDATE sectors
		SET latest_host = COALESCE((
			SELECT * FROM (
				SELECT h.public_key
				FROM hosts h
				INNER JOIN contracts c ON c.host_id = h.id
				INNER JOIN contract_sectors cs ON cs.db_contract_id = c.id
				INNER JOIN sectors s ON s.id = cs.db_sector_id
				WHERE s.root = ? AND h.public_key != ?
				LIMIT 1
			) AS _
		), ?)
		WHERE root = ? AND latest_host = ?
	`, Hash256(root), PublicKey(hk), PublicKey{}, Hash256(root), PublicKey(hk))
	if err != nil {
		return 0, fmt.Errorf("failed to update sector: %w", err)
	}

	// remove potential links between the host's contracts and the sector
	res, err := tx.Exec(ctx, `
		DELETE FROM contract_sectors
		WHERE db_sector_id = (
			SELECT s.id
			FROM sectors s
			WHERE root = ?
		) AND db_contract_id IN (
			SELECT c.id
			FROM contracts c
			INNER JOIN hosts h ON h.id = c.host_id
			WHERE h.public_key = ?
		)
	`, Hash256(root), PublicKey(hk))
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
	return int(deletedSectors), nil
}

func DeleteSettings(ctx context.Context, tx sql.Tx, key string) error {
	if _, err := tx.Exec(ctx, "DELETE FROM settings WHERE `key` = ?", key); err != nil {
		return fmt.Errorf("failed to delete setting '%s': %w", key, err)
	}
	return nil
}

func DeleteWebhook(ctx context.Context, tx sql.Tx, wh webhooks.Webhook) error {
	res, err := tx.Exec(ctx, "DELETE FROM webhooks WHERE module = ? AND event = ? AND url = ?", wh.Module, wh.Event, wh.URL)
	if err != nil {
		return fmt.Errorf("failed to delete webhook: %w", err)
	} else if n, err := res.RowsAffected(); err != nil {
		return fmt.Errorf("failed to check rows affected: %w", err)
	} else if n == 0 {
		return webhooks.ErrWebhookNotFound
	}
	return nil
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

func HostsForScanning(ctx context.Context, tx sql.Tx, maxLastScan time.Time, offset, limit int) ([]api.HostAddress, error) {
	if offset < 0 {
		return nil, ErrNegativeOffset
	} else if limit == -1 {
		limit = math.MaxInt64
	}

	rows, err := tx.Query(ctx, "SELECT public_key, net_address FROM hosts WHERE last_scan < ? LIMIT ? OFFSET ?",
		maxLastScan.UnixNano(), limit, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch hosts for scanning: %w", err)
	}
	defer rows.Close()

	var hosts []api.HostAddress
	for rows.Next() {
		var ha api.HostAddress
		if err := rows.Scan((*PublicKey)(&ha.PublicKey), &ha.NetAddress); err != nil {
			return nil, fmt.Errorf("failed to scan host row: %w", err)
		}
		hosts = append(hosts, ha)
	}
	return hosts, nil
}

func InsertBufferedSlab(ctx context.Context, tx sql.Tx, fileName string, contractSetID int64, ec object.EncryptionKey, minShards, totalShards uint8) (int64, error) {
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

	key, err := ec.MarshalBinary()
	if err != nil {
		return 0, err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO slabs (created_at, db_contract_set_id, db_buffered_slab_id, `+"`key`"+`, min_shards, total_shards)
			VALUES (?, ?, ?, ?, ?, ?)`,
		time.Now(), contractSetID, bufferedSlabID, SecretKey(key), minShards, totalShards)
	if err != nil {
		return 0, fmt.Errorf("failed to insert slab: %w", err)
	}
	return bufferedSlabID, nil
}

func RenewContract(ctx context.Context, tx sql.Tx, rev rhpv2.ContractRevision, contractPrice, totalCost types.Currency, startHeight uint64, renewedFrom types.FileContractID, state string) (api.ContractMetadata, error) {
	var contractState ContractState
	if err := contractState.LoadString(state); err != nil {
		return api.ContractMetadata{}, fmt.Errorf("failed to load contract state: %w", err)
	}
	// create copy of contract in archived_contracts
	if err := copyContractToArchive(ctx, tx, renewedFrom, &rev.Revision.ParentID, api.ContractArchivalReasonRenewed); err != nil {
		return api.ContractMetadata{}, fmt.Errorf("failed to copy contract to archived_contracts: %w", err)
	}
	// update existing contract
	_, err := tx.Exec(ctx, `
		UPDATE contracts SET
			created_at = ?,
			fcid = ?,
			renewed_from = ?,
			contract_price = ?,
			state = ?,
			total_cost = ?,
			proof_height = ?,
			revision_height = ?,
			revision_number = ?,
			size = ?,
			start_height = ?,
			window_start = ?,
			window_end = ?,
			upload_spending = ?,
			download_spending = ?,
			fund_account_spending = ?,
			delete_spending = ?,
			list_spending = ?
		WHERE fcid = ?
	`,
		time.Now(),
		FileContractID(rev.ID()),
		FileContractID(renewedFrom),
		Currency(contractPrice),
		contractState,
		Currency(totalCost),
		0,
		0,
		fmt.Sprint(rev.Revision.RevisionNumber),
		rev.Revision.Filesize,
		startHeight, rev.Revision.WindowStart, rev.Revision.WindowEnd,
		ZeroCurrency, ZeroCurrency, ZeroCurrency, ZeroCurrency, ZeroCurrency,
		FileContractID(renewedFrom))
	if err != nil {
		return api.ContractMetadata{}, fmt.Errorf("failed to update contract: %w", err)
	}
	return Contract(ctx, tx, rev.ID())
}

func InsertContract(ctx context.Context, tx sql.Tx, rev rhpv2.ContractRevision, contractPrice, totalCost types.Currency, startHeight uint64, renewedFrom types.FileContractID, state string) (api.ContractMetadata, error) {
	var contractState ContractState
	if err := contractState.LoadString(state); err != nil {
		return api.ContractMetadata{}, fmt.Errorf("failed to load contract state: %w", err)
	}
	var hostID int64
	if err := tx.QueryRow(ctx, "SELECT id FROM hosts WHERE public_key = ?",
		PublicKey(rev.HostKey())).Scan(&hostID); err != nil {
		return api.ContractMetadata{}, api.ErrHostNotFound
	}

	res, err := tx.Exec(ctx, `
		INSERT INTO contracts (created_at, host_id, fcid, renewed_from, contract_price, state, total_cost, proof_height,
		revision_height, revision_number, size, start_height, window_start, window_end, upload_spending, download_spending, 
		fund_account_spending, delete_spending, list_spending)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, time.Now(), hostID, FileContractID(rev.ID()), FileContractID(renewedFrom), Currency(contractPrice),
		contractState, Currency(totalCost), 0, 0, "0", rev.Revision.Filesize, startHeight, rev.Revision.WindowStart, rev.Revision.WindowEnd,
		ZeroCurrency, ZeroCurrency, ZeroCurrency, ZeroCurrency, ZeroCurrency)
	if err != nil {
		return api.ContractMetadata{}, fmt.Errorf("failed to insert contract: %w", err)
	}
	cid, err := res.LastInsertId()
	if err != nil {
		return api.ContractMetadata{}, fmt.Errorf("failed to fetch contract id: %w", err)
	}

	contracts, err := QueryContracts(ctx, tx, []string{"c.id = ?"}, []any{cid})
	if err != nil {
		return api.ContractMetadata{}, fmt.Errorf("failed to fetch contract: %w", err)
	} else if len(contracts) == 0 {
		return api.ContractMetadata{}, api.ErrContractNotFound
	}
	return contracts[0], nil
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

	// marshal key
	ecBytes, err := ec.MarshalBinary()
	if err != nil {
		return "", err
	}

	// insert multipart upload
	uploadIDEntropy := frand.Entropy256()
	uploadID := hex.EncodeToString(uploadIDEntropy[:])
	var muID int64
	res, err := tx.Exec(ctx, `
		INSERT INTO multipart_uploads (created_at, `+"`key`"+`, upload_id, object_id, db_bucket_id, mime_type)
		VALUES (?, ?, ?, ?, ?, ?)
	`, time.Now(), SecretKey(ecBytes), uploadID, key, bucketID, mimeType)
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

func InsertObject(ctx context.Context, tx sql.Tx, key string, dirID, bucketID, size int64, ec []byte, mimeType, eTag string) (int64, error) {
	res, err := tx.Exec(ctx, `INSERT INTO objects (created_at, object_id, db_directory_id, db_bucket_id, `+"`key`"+`, size, mime_type, etag)
						VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		time.Now(),
		key,
		dirID,
		bucketID,
		SecretKey(ec),
		size,
		mimeType,
		eTag)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func LoadSlabBuffers(ctx context.Context, db *sql.DB) (bufferedSlabs []LoadedSlabBuffer, orphanedBuffers []string, err error) {
	err = db.Transaction(ctx, func(tx sql.Tx) error {
		// collect all buffers
		rows, err := db.Query(ctx, `
			SELECT bs.id, bs.filename, sla.db_contract_set_id, sla.key, sla.min_shards, sla.total_shards
			FROM buffered_slabs bs
			INNER JOIN slabs sla ON sla.db_buffered_slab_id = bs.id
		`)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var bs LoadedSlabBuffer
			var sk SecretKey
			if err := rows.Scan(&bs.ID, &bs.Filename, &bs.ContractSetID, &sk, &bs.MinShards, &bs.TotalShards); err != nil {
				return fmt.Errorf("failed to scan buffered slab: %w", err)
			} else if err := bs.Key.UnmarshalBinary(sk[:]); err != nil {
				return fmt.Errorf("failed to unmarshal secret key: %w", err)
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
				return fmt.Errorf("failed to fetch buffered slab size: %w", err)
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
			return fmt.Errorf("failed to fetch orphaned buffers: %w", err)
		}
		var toDelete []int64
		for rows.Next() {
			var id int64
			var filename string
			if err := rows.Scan(&id, &filename); err != nil {
				return fmt.Errorf("failed to scan orphaned buffer: %w", err)
			}
			orphanedBuffers = append(orphanedBuffers, filename)
			toDelete = append(toDelete, id)
		}
		for _, id := range toDelete {
			if _, err := tx.Exec(ctx, "DELETE FROM buffered_slabs WHERE id = ?", id); err != nil {
				return fmt.Errorf("failed to delete orphaned buffer: %w", err)
			}
		}
		return nil
	})
	return
}

func UpdateMetadata(ctx context.Context, tx sql.Tx, objID int64, md api.ObjectUserMetadata) error {
	if err := DeleteMetadata(ctx, tx, objID); err != nil {
		return err
	} else if err := InsertMetadata(ctx, tx, &objID, nil, md); err != nil {
		return err
	}
	return nil
}

func DeleteMetadata(ctx context.Context, tx sql.Tx, objID int64) error {
	_, err := tx.Exec(ctx, "DELETE FROM object_user_metadata WHERE db_object_id = ?", objID)
	return err
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

func ContractSize(ctx context.Context, tx sql.Tx, id types.FileContractID) (api.ContractSize, error) {
	var contractID, size uint64
	if err := tx.QueryRow(ctx, "SELECT id, size FROM contracts WHERE fcid = ?", FileContractID(id)).
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
	sectorsSize := nSectors * rhpv2.SectorSize

	var prunable uint64
	if size > sectorsSize {
		prunable = size - sectorsSize
	}
	return api.ContractSize{
		Size:     size,
		Prunable: prunable,
	}, nil
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

func FetchUsedContracts(ctx context.Context, tx sql.Tx, fcids []types.FileContractID) (map[types.FileContractID]UsedContract, error) {
	if len(fcids) == 0 {
		return make(map[types.FileContractID]UsedContract), nil
	}

	// flatten map to get all used contract ids
	usedFCIDs := make([]FileContractID, 0, len(fcids))
	for _, fcid := range fcids {
		usedFCIDs = append(usedFCIDs, FileContractID(fcid))
	}

	placeholders := make([]string, len(usedFCIDs))
	for i := range usedFCIDs {
		placeholders[i] = "?"
	}
	placeholdersStr := strings.Join(placeholders, ", ")

	args := make([]interface{}, len(usedFCIDs)*2)
	for i := range args {
		args[i] = usedFCIDs[i%len(usedFCIDs)]
	}

	// fetch all contracts, take into account renewals
	rows, err := tx.Query(ctx, fmt.Sprintf(`SELECT id, fcid, renewed_from
				   FROM contracts
				   WHERE contracts.fcid IN (%s) OR renewed_from IN (%s)
				   `, placeholdersStr, placeholdersStr), args...)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch used contracts: %w", err)
	}
	defer rows.Close()

	var contracts []UsedContract
	for rows.Next() {
		var c UsedContract
		if err := rows.Scan(&c.ID, &c.FCID, &c.RenewedFrom); err != nil {
			return nil, fmt.Errorf("failed to scan used contract: %w", err)
		}
		contracts = append(contracts, c)
	}

	fcidMap := make(map[types.FileContractID]struct{}, len(fcids))
	for _, fcid := range fcids {
		fcidMap[fcid] = struct{}{}
	}

	// build map of used contracts
	usedContracts := make(map[types.FileContractID]UsedContract, len(contracts))
	for _, c := range contracts {
		if _, used := fcidMap[types.FileContractID(c.FCID)]; used {
			usedContracts[types.FileContractID(c.FCID)] = c
		}
		if _, used := fcidMap[types.FileContractID(c.RenewedFrom)]; used {
			usedContracts[types.FileContractID(c.RenewedFrom)] = c
		}
	}
	return usedContracts, nil
}

func PrepareSlabHealth(ctx context.Context, tx sql.Tx, limit int64, now time.Time) error {
	_, err := tx.Exec(ctx, "DROP TABLE IF EXISTS slabs_health")
	if err != nil {
		return fmt.Errorf("failed to drop temporary table: %w", err)
	}
	_, err = tx.Exec(ctx, `
		CREATE TEMPORARY TABLE slabs_health AS
			SELECT slabs.id as id, CASE WHEN (slabs.min_shards = slabs.total_shards)
			THEN
				CASE WHEN (COUNT(DISTINCT(CASE WHEN cs.name IS NULL THEN NULL ELSE c.host_id END)) < slabs.min_shards)
				THEN -1
				ELSE 1
				END
			ELSE (CAST(COUNT(DISTINCT(CASE WHEN cs.name IS NULL THEN NULL ELSE c.host_id END)) AS FLOAT) - CAST(slabs.min_shards AS FLOAT)) / Cast(slabs.total_shards - slabs.min_shards AS FLOAT)
			END as health
			FROM slabs
			INNER JOIN sectors s ON s.db_slab_id = slabs.id
			LEFT JOIN contract_sectors se ON s.id = se.db_sector_id
			LEFT JOIN contracts c ON se.db_contract_id = c.id
			LEFT JOIN contract_set_contracts csc ON csc.db_contract_id = c.id AND csc.db_contract_set_id = slabs.db_contract_set_id
			LEFT JOIN contract_sets cs ON cs.id = csc.db_contract_set_id
			WHERE slabs.health_valid_until <= ?
			GROUP BY slabs.id
			LIMIT ?
	`, now.Unix(), limit)
	if err != nil {
		return fmt.Errorf("failed to create temporary table: %w", err)
	}
	if _, err := tx.Exec(ctx, "CREATE INDEX slabs_health_id ON slabs_health (id)"); err != nil {
		return fmt.Errorf("failed to create index on temporary table: %w", err)
	}
	return err
}

func ListBuckets(ctx context.Context, tx sql.Tx) ([]api.Bucket, error) {
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
		nextPathMarker = uploads[len(uploads)-1].Path
		nextUploadIDMarker = uploads[len(uploads)-1].UploadID
	}

	return api.MultipartListUploadsResponse{
		HasMore:            hasMore,
		NextPathMarker:     nextPathMarker,
		NextUploadIDMarker: nextUploadIDMarker,
		Uploads:            uploads,
	}, nil
}

type multipartUpload struct {
	ID       int64
	Key      string
	Bucket   string
	BucketID int64
	EC       []byte
	MimeType string
}

type multipartUploadPart struct {
	ID         int64
	PartNumber int64
	Etag       string
	Size       int64
}

func MultipartUploadForCompletion(ctx context.Context, tx sql.Tx, bucket, key, uploadID string, parts []api.MultipartCompletedPart) (multipartUpload, []multipartUploadPart, int64, string, error) {
	// fetch upload
	var mpu multipartUpload
	err := tx.QueryRow(ctx, `
		SELECT mu.id, mu.object_id, mu.mime_type, mu.key, b.name, b.id
		FROM multipart_uploads mu INNER JOIN buckets b ON b.id = mu.db_bucket_id
		WHERE mu.upload_id = ?`, uploadID).
		Scan(&mpu.ID, &mpu.Key, &mpu.MimeType, &mpu.EC, &mpu.Bucket, &mpu.BucketID)
	if err != nil {
		return multipartUpload{}, nil, 0, "", fmt.Errorf("failed to fetch upload: %w", err)
	} else if mpu.Key != key {
		return multipartUpload{}, nil, 0, "", fmt.Errorf("object id mismatch: %v != %v: %w", mpu.Key, key, api.ErrObjectNotFound)
	} else if mpu.Bucket != bucket {
		return multipartUpload{}, nil, 0, "", fmt.Errorf("bucket name mismatch: %v != %v: %w", mpu.Bucket, bucket, api.ErrBucketNotFound)
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
	err = tx.QueryRow(ctx, "SELECT COALESCE(SUM(size), 0) FROM contracts").
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
		TotalSectorsSize:           totalSectors * rhpv2.SectorSize,
		TotalUploadedSize:          totalUploaded,
	}, nil
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
		settings = CASE WHEN ? THEN ? ELSE settings END,
		price_table = CASE WHEN ? AND (price_table_expiry IS NULL OR ? > price_table_expiry) THEN ? ELSE price_table END,
		price_table_expiry = CASE WHEN ? AND (price_table_expiry IS NULL OR ? > price_table_expiry) THEN ? ELSE price_table_expiry END,
		successful_interactions = CASE WHEN ? THEN successful_interactions + 1 ELSE successful_interactions END,
		failed_interactions = CASE WHEN ? THEN failed_interactions + 1 ELSE failed_interactions END,
		subnets = CASE WHEN ? THEN ? ELSE subnets END
		WHERE public_key = ?
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement to update host with scan: %w", err)
	}
	defer stmt.Close()

	now := time.Now()
	for _, scan := range scans {
		scanTime := scan.Timestamp.UnixNano()
		_, err = stmt.Exec(ctx,
			scan.Success,                                    // scanned
			scan.Success,                                    // last_scan_success
			!scan.Success, scanTime, scanTime, scan.Success, // recent_downtime
			scan.Success,                      // recent_scan_failures
			!scan.Success, scanTime, scanTime, // downtime
			scan.Success, scanTime, scanTime, // uptime
			scanTime,                                  // last_scan
			scan.Success, HostSettings(scan.Settings), // settings
			scan.Success, now, PriceTable(scan.PriceTable), // price_table
			scan.Success, now, now, // price_table_expiry
			scan.Success,  // successful_interactions
			!scan.Success, // failed_interactions
			len(scan.Subnets) > 0, strings.Join(scan.Subnets, ","),
			PublicKey(scan.HostKey),
		)
		if err != nil {
			return fmt.Errorf("failed to update host with scan: %w", err)
		}
	}
	return nil
}

func RecordPriceTables(ctx context.Context, tx sql.Tx, priceTableUpdates []api.HostPriceTableUpdate) error {
	if len(priceTableUpdates) == 0 {
		return nil
	}

	stmt, err := tx.Prepare(ctx, `
		UPDATE hosts SET
		recent_downtime = CASE WHEN ? THEN recent_downtime = 0 ELSE recent_downtime END,
		recent_scan_failures = CASE WHEN ? THEN recent_scan_failures = 0 ELSE recent_scan_failures END,
		price_table = CASE WHEN ? THEN ? ELSE price_table END,
		price_table_expiry =  CASE WHEN ? THEN ? ELSE price_table_expiry END,
		successful_interactions =  CASE WHEN ? THEN successful_interactions + 1 ELSE successful_interactions END,
		failed_interactions = CASE WHEN ? THEN failed_interactions + 1 ELSE failed_interactions END
		WHERE public_key = ?
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement to update host with price table: %w", err)
	}
	defer stmt.Close()

	for _, ptu := range priceTableUpdates {
		_, err := stmt.Exec(ctx,
			ptu.Success,                                            // recent_downtime
			ptu.Success,                                            // recent_scan_failures
			ptu.Success, PriceTable(ptu.PriceTable.HostPriceTable), // price_table
			ptu.Success, ptu.PriceTable.Expiry, // price_table_expiry
			ptu.Success,  // successful_interactions
			!ptu.Success, // failed_interactions
			PublicKey(ptu.HostKey),
		)
		if err != nil {
			return fmt.Errorf("failed to update host with price table: %w", err)
		}
	}
	return nil
}

func RemoveContractSet(ctx context.Context, tx sql.Tx, contractSet string) error {
	_, err := tx.Exec(ctx, "DELETE FROM contract_sets WHERE name = ?", contractSet)
	if err != nil {
		return fmt.Errorf("failed to delete contract set: %w", err)
	}
	return nil
}

func RemoveOfflineHosts(ctx context.Context, tx sql.Tx, minRecentFailures uint64, maxDownTime time.Duration) (int64, error) {
	// fetch contracts
	rows, err := tx.Query(ctx, `
		SELECT fcid
		FROM contracts
		INNER JOIN hosts h ON h.id = contracts.host_id
		WHERE recent_downtime >= ? AND recent_scan_failures >= ?
	`, maxDownTime, minRecentFailures)
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

	// archive contracts
	for _, fcid := range fcids {
		if err := ArchiveContract(ctx, tx, fcid, api.ContractArchivalReasonHostPruned); err != nil {
			return 0, fmt.Errorf("failed to archive contract %v: %w", fcid, err)
		}
	}

	// delete hosts
	res, err := tx.Exec(ctx, "DELETE FROM hosts WHERE recent_downtime >= ? AND recent_scan_failures >= ?",
		maxDownTime, minRecentFailures)
	if err != nil {
		return 0, fmt.Errorf("failed to delete hosts: %w", err)
	}
	return res.RowsAffected()
}

func InitConsensusInfo(ctx context.Context, tx sql.Tx) (types.ChainIndex, modules.ConsensusChangeID, error) {
	// try fetch existing
	var ccid modules.ConsensusChangeID
	var ci types.ChainIndex
	err := tx.QueryRow(ctx, "SELECT cc_id, height, block_id FROM consensus_infos WHERE id = ?", consensuInfoID).
		Scan((*CCID)(&ccid), &ci.Height, (*Hash256)(&ci.ID))
	if err != nil && !errors.Is(err, dsql.ErrNoRows) {
		return types.ChainIndex{}, modules.ConsensusChangeID{}, fmt.Errorf("failed to fetch consensus info: %w", err)
	} else if err == nil {
		return ci, ccid, nil
	}
	// otherwise init
	ci = types.ChainIndex{}
	if _, err := tx.Exec(ctx, "INSERT INTO consensus_infos (id, created_at, cc_id, height, block_id) VALUES (?, ?, ?, ?, ?)",
		consensuInfoID, time.Now(), (CCID)(modules.ConsensusChangeBeginning), ci.Height, (Hash256)(ci.ID)); err != nil {
		return types.ChainIndex{}, modules.ConsensusChangeID{}, fmt.Errorf("failed to init consensus infos: %w", err)
	}
	return types.ChainIndex{}, modules.ConsensusChangeBeginning, nil
}

func QueryContracts(ctx context.Context, tx sql.Tx, whereExprs []string, whereArgs []any) ([]api.ContractMetadata, error) {
	var whereExpr string
	if len(whereExprs) > 0 {
		whereExpr = "WHERE " + strings.Join(whereExprs, " AND ")
	}
	rows, err := tx.Query(ctx, fmt.Sprintf(`
			SELECT c.fcid, c.renewed_from, c.contract_price, c.state, c.total_cost, c.proof_height,
			c.revision_height, c.revision_number, c.size, c.start_height, c.window_start, c.window_end,
			c.upload_spending, c.download_spending, c.fund_account_spending, c.delete_spending, c.list_spending,
			COALESCE(cs.name, ""), h.net_address, h.public_key, h.settings->>'$.siamuxport' AS siamux_port
			FROM contracts AS c
			INNER JOIN hosts h ON h.id = c.host_id
			LEFT JOIN contract_set_contracts csc ON csc.db_contract_id = c.id
			LEFT JOIN contract_sets cs ON cs.id = csc.db_contract_set_id
			%s
			ORDER BY c.id ASC`, whereExpr),
		whereArgs...,
	)
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
		} else if scannedRows[0].ContractSet != "" {
			current.ContractSets = append(current.ContractSets, scannedRows[0].ContractSet)
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

func ResetConsensusSubscription(ctx context.Context, tx sql.Tx) (ci types.ChainIndex, err error) {
	if _, err := tx.Exec(ctx, "DELETE FROM consensus_infos"); err != nil {
		return types.ChainIndex{}, fmt.Errorf("failed to delete consensus infos: %w", err)
	} else if _, err := tx.Exec(ctx, "DELETE FROM siacoin_elements"); err != nil {
		return types.ChainIndex{}, fmt.Errorf("failed to delete siacoin elements: %w", err)
	} else if _, err := tx.Exec(ctx, "DELETE FROM transactions"); err != nil {
		return types.ChainIndex{}, fmt.Errorf("failed to delete transactions: %w", err)
	} else if ci, _, err = InitConsensusInfo(ctx, tx); err != nil {
		return types.ChainIndex{}, fmt.Errorf("failed to initialize consensus info: %w", err)
	}
	return ci, nil
}

func ResetLostSectors(ctx context.Context, tx sql.Tx, hk types.PublicKey) error {
	_, err := tx.Exec(ctx, "UPDATE hosts SET lost_sectors = 0 WHERE public_key = ?", PublicKey(hk))
	if err != nil {
		return fmt.Errorf("failed to reset lost sectors for host %v: %w", hk, err)
	}
	return nil
}

func SearchHosts(ctx context.Context, tx sql.Tx, autopilot, filterMode, usabilityMode, addressContains string, keyIn []types.PublicKey, offset, limit int) ([]api.Host, error) {
	if offset < 0 {
		return nil, ErrNegativeOffset
	}

	var hasAllowlist, hasBlocklist bool
	if err := tx.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM host_allowlist_entries)").Scan(&hasAllowlist); err != nil {
		return nil, fmt.Errorf("failed to check for allowlist: %w", err)
	} else if err := tx.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM host_blocklist_entries)").Scan(&hasBlocklist); err != nil {
		return nil, fmt.Errorf("failed to check for blocklist: %w", err)
	}

	// validate filterMode
	switch filterMode {
	case api.HostFilterModeAllowed:
	case api.HostFilterModeBlocked:
	case api.HostFilterModeAll:
	default:
		return nil, fmt.Errorf("invalid filter mode: %v", filterMode)
	}

	var whereExprs []string
	var args []any

	// fetch autopilot id
	var autopilotID int64
	if autopilot != "" {
		if err := tx.QueryRow(ctx, "SELECT id FROM autopilots WHERE identifier = ?", autopilot).
			Scan(&autopilotID); errors.Is(err, dsql.ErrNoRows) {
			return nil, api.ErrAutopilotNotFound
		} else if err != nil {
			return nil, fmt.Errorf("failed to fetch autopilot id: %w", err)
		}
	}

	// filter allowlist/blocklist
	switch filterMode {
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
	if addressContains != "" {
		whereExprs = append(whereExprs, "h.net_address LIKE ?")
		args = append(args, "%"+addressContains+"%")
	}

	// filter public key
	if len(keyIn) > 0 {
		pubKeys := make([]any, len(keyIn))
		for i, pk := range keyIn {
			pubKeys[i] = PublicKey(pk)
		}
		placeholders := strings.Repeat("?, ", len(keyIn)-1) + "?"
		whereExprs = append(whereExprs, fmt.Sprintf("h.public_key IN (%s)", placeholders))
		args = append(args, pubKeys...)
	}

	// filter usability
	whereApExpr := ""
	if autopilot != "" {
		whereApExpr = "AND hc.db_autopilot_id = ?"
	}
	switch usabilityMode {
	case api.UsabilityFilterModeUsable:
		whereExprs = append(whereExprs, fmt.Sprintf("EXISTS (SELECT 1 FROM hosts h2 INNER JOIN host_checks hc ON hc.db_host_id = h2.id AND h2.id = h.id WHERE (hc.usability_blocked = 0 AND hc.usability_offline = 0 AND hc.usability_low_score = 0 AND hc.usability_redundant_ip = 0 AND hc.usability_gouging = 0 AND hc.usability_not_accepting_contracts = 0 AND hc.usability_not_announced = 0 AND hc.usability_not_completing_scan = 0) %s)", whereApExpr))
		args = append(args, autopilotID)
	case api.UsabilityFilterModeUnusable:
		whereExprs = append(whereExprs, fmt.Sprintf("EXISTS (SELECT 1 FROM hosts h2 INNER JOIN host_checks hc ON hc.db_host_id = h2.id AND h2.id = h.id WHERE (hc.usability_blocked = 1 OR hc.usability_offline = 1 OR hc.usability_low_score = 1 OR hc.usability_redundant_ip = 1 OR hc.usability_gouging = 1 OR hc.usability_not_accepting_contracts = 1 OR hc.usability_not_announced = 1 OR hc.usability_not_completing_scan = 1) %s)", whereApExpr))
		args = append(args, autopilotID)
	}

	// offset + limit
	if limit == -1 {
		limit = math.MaxInt64
	}
	offsetLimitStr := fmt.Sprintf("LIMIT %d OFFSET %d", limit, offset)

	// fetch stored data for each host
	rows, err := tx.Query(ctx, "SELECT host_id, SUM(size) FROM contracts GROUP BY host_id")
	if err != nil {
		return nil, fmt.Errorf("failed to fetch stored data: %w", err)
	}
	defer rows.Close()

	storedDataMap := make(map[int64]uint64)
	for rows.Next() {
		var hostID int64
		var storedData uint64
		if err := rows.Scan(&hostID, &storedData); err != nil {
			return nil, fmt.Errorf("failed to scan stored data: %w", err)
		}
		storedDataMap[hostID] = storedData
	}

	// query hosts
	var blockedExprs []string
	if hasAllowlist {
		blockedExprs = append(blockedExprs, "NOT EXISTS (SELECT 1 FROM host_allowlist_entry_hosts hbeh WHERE hbeh.db_host_id = h.id)")
	}
	if hasBlocklist {
		blockedExprs = append(blockedExprs, "EXISTS (SELECT 1 FROM host_blocklist_entry_hosts hbeh WHERE hbeh.db_host_id = h.id)")
	}
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
		SELECT h.id, h.created_at, h.last_announcement, h.public_key, h.net_address, h.price_table, h.price_table_expiry,
			h.settings, h.total_scans, h.last_scan, h.last_scan_success, h.second_to_last_scan_success,
			h.uptime, h.downtime, h.successful_interactions, h.failed_interactions, COALESCE(h.lost_sectors, 0),
			h.scanned, h.subnets, %s
		FROM hosts h
		%s
		%s
	`, blockedExpr, whereExpr, offsetLimitStr), args...)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch hosts: %w", err)
	}
	defer rows.Close()

	var hosts []api.Host
	for rows.Next() {
		var h api.Host
		var hostID int64
		var pte dsql.NullTime
		var subnets string
		err := rows.Scan(&hostID, &h.KnownSince, &h.LastAnnouncement, (*PublicKey)(&h.PublicKey),
			&h.NetAddress, (*PriceTable)(&h.PriceTable.HostPriceTable), &pte,
			(*HostSettings)(&h.Settings), &h.Interactions.TotalScans, (*UnixTimeNS)(&h.Interactions.LastScan), &h.Interactions.LastScanSuccess,
			&h.Interactions.SecondToLastScanSuccess, &h.Interactions.Uptime, &h.Interactions.Downtime,
			&h.Interactions.SuccessfulInteractions, &h.Interactions.FailedInteractions, &h.Interactions.LostSectors,
			&h.Scanned, &subnets, &h.Blocked,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan host: %w", err)
		}

		if subnets != "" {
			h.Subnets = strings.Split(subnets, ",")
		}
		h.PriceTable.Expiry = pte.Time
		h.StoredData = storedDataMap[hostID]
		hosts = append(hosts, h)
	}

	// query host checks
	var apExpr string
	if autopilot != "" {
		apExpr = "WHERE ap.identifier = ?"
		args = append(args, autopilot)
	}
	rows, err = tx.Query(ctx, fmt.Sprintf(`
		SELECT h.public_key, ap.identifier, hc.usability_blocked, hc.usability_offline, hc.usability_low_score, hc.usability_redundant_ip,
			hc.usability_gouging, usability_not_accepting_contracts, hc.usability_not_announced, hc.usability_not_completing_scan,
			hc.score_age, hc.score_collateral, hc.score_interactions, hc.score_storage_remaining, hc.score_uptime,
			hc.score_version, hc.score_prices, hc.gouging_contract_err, hc.gouging_download_err, hc.gouging_gouging_err,
			hc.gouging_prune_err, hc.gouging_upload_err
		FROM (
			SELECT h.id, h.public_key
			FROM hosts h
			%s
			%s
		) AS h
		INNER JOIN host_checks hc ON hc.db_host_id = h.id
		INNER JOIN autopilots ap ON hc.db_autopilot_id = ap.id
		%s
	`, whereExpr, offsetLimitStr, apExpr), args...)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch host checks: %w", err)
	}
	defer rows.Close()

	hostChecks := make(map[types.PublicKey]map[string]api.HostCheck)
	for rows.Next() {
		var ap string
		var pk PublicKey
		var hc api.HostCheck
		err := rows.Scan(&pk, &ap, &hc.Usability.Blocked, &hc.Usability.Offline, &hc.Usability.LowScore, &hc.Usability.RedundantIP,
			&hc.Usability.Gouging, &hc.Usability.NotAcceptingContracts, &hc.Usability.NotAnnounced, &hc.Usability.NotCompletingScan,
			&hc.Score.Age, &hc.Score.Collateral, &hc.Score.Interactions, &hc.Score.StorageRemaining, &hc.Score.Uptime,
			&hc.Score.Version, &hc.Score.Prices, &hc.Gouging.ContractErr, &hc.Gouging.DownloadErr, &hc.Gouging.GougingErr,
			&hc.Gouging.PruneErr, &hc.Gouging.UploadErr)
		if err != nil {
			return nil, fmt.Errorf("failed to scan host: %w", err)
		}
		if _, ok := hostChecks[types.PublicKey(pk)]; !ok {
			hostChecks[types.PublicKey(pk)] = make(map[string]api.HostCheck)
		}
		hostChecks[types.PublicKey(pk)][ap] = hc
	}

	// fill in hosts
	for i := range hosts {
		hosts[i].Checks = hostChecks[hosts[i].PublicKey]
	}
	return hosts, nil
}

func Setting(ctx context.Context, tx sql.Tx, key string) (string, error) {
	var value string
	err := tx.QueryRow(ctx, "SELECT value FROM settings WHERE `key` = ?", key).Scan((*BusSetting)(&value))
	if errors.Is(err, dsql.ErrNoRows) {
		return "", api.ErrSettingNotFound
	} else if err != nil {
		return "", fmt.Errorf("failed to fetch setting '%s': %w", key, err)
	}
	return value, nil
}

func Settings(ctx context.Context, tx sql.Tx) ([]string, error) {
	rows, err := tx.Query(ctx, "SELECT `key` FROM settings")
	if err != nil {
		return nil, fmt.Errorf("failed to query settings: %w", err)
	}
	var settings []string
	for rows.Next() {
		var setting string
		if err := rows.Scan(&setting); err != nil {
			return nil, fmt.Errorf("failed to scan setting key")
		}
		settings = append(settings, setting)
	}
	return settings, nil
}

func SetUncleanShutdown(ctx context.Context, tx sql.Tx) error {
	_, err := tx.Exec(ctx, "UPDATE ephemeral_accounts SET clean_shutdown = 0, requires_sync = 1")
	if err != nil {
		return fmt.Errorf("failed to set unclean shutdown: %w", err)
	}
	return err
}

func SlabBuffers(ctx context.Context, tx sql.Tx) (map[string]string, error) {
	rows, err := tx.Query(ctx, `
		SELECT buffered_slabs.filename, cs.name
		FROM buffered_slabs
		INNER JOIN slabs sla ON sla.db_buffered_slab_id = buffered_slabs.id
		INNER JOIN contract_sets cs ON cs.id = sla.db_contract_set_id
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch contract sets")
	}
	defer rows.Close()

	fileNameToContractSet := make(map[string]string)
	for rows.Next() {
		var fileName string
		var contractSetName string
		if err := rows.Scan(&fileName, &contractSetName); err != nil {
			return nil, fmt.Errorf("failed to scan contract set: %w", err)
		}
		fileNameToContractSet[fileName] = contractSetName
	}
	return fileNameToContractSet, nil
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

func Webhooks(ctx context.Context, tx sql.Tx) ([]webhooks.Webhook, error) {
	rows, err := tx.Query(ctx, "SELECT module, event, url, headers FROM webhooks")
	if err != nil {
		return nil, fmt.Errorf("failed to fetch webhooks: %w", err)
	}
	defer rows.Close()

	var whs []webhooks.Webhook
	for rows.Next() {
		var webhook webhooks.Webhook
		var headers string
		if err := rows.Scan(&webhook.Module, &webhook.Event, &webhook.URL, &headers); err != nil {
			return nil, fmt.Errorf("failed to scan webhook: %w", err)
		} else if err := json.Unmarshal([]byte(headers), &webhook.Headers); err != nil {
			return nil, fmt.Errorf("failed to unmarshal headers: %w", err)
		}
		whs = append(whs, webhook)
	}
	return whs, nil
}

func scanAutopilot(s scanner) (api.Autopilot, error) {
	var a api.Autopilot
	if err := s.Scan(&a.ID, (*AutopilotConfig)(&a.Config), &a.CurrentPeriod); err != nil {
		return api.Autopilot{}, err
	}
	return a, nil
}

func scanBucket(s scanner) (api.Bucket, error) {
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

func scanMultipartUpload(s scanner) (resp api.MultipartUpload, _ error) {
	var key SecretKey
	err := s.Scan(&resp.Bucket, &key, &resp.Path, &resp.UploadID, &resp.CreatedAt)
	if errors.Is(err, dsql.ErrNoRows) {
		return api.MultipartUpload{}, api.ErrMultipartUploadNotFound
	} else if err != nil {
		return api.MultipartUpload{}, fmt.Errorf("failed to fetch multipart upload: %w", err)
	} else if err := resp.Key.UnmarshalBinary(key); err != nil {
		return api.MultipartUpload{}, fmt.Errorf("failed to unmarshal encryption key: %w", err)
	}
	return
}

func scanObjectMetadata(s scanner) (api.ObjectMetadata, error) {
	var md api.ObjectMetadata
	if err := s.Scan(&md.Name, &md.Size, &md.Health, &md.MimeType, &md.ModTime, &md.ETag); err != nil {
		return api.ObjectMetadata{}, fmt.Errorf("failed to scan object metadata: %w", err)
	}
	return md, nil
}

func ListObjects(ctx context.Context, tx sql.Tx, bucket, prefix, sortBy, sortDir, marker string, limit int) (api.ObjectsListResponse, error) {
	// fetch one more to see if there are more entries
	if limit <= -1 {
		limit = math.MaxInt
	} else {
		limit++
	}

	// establish sane defaults for sorting
	if sortBy == "" {
		sortBy = api.ObjectSortByName
	}
	if sortDir == "" {
		sortDir = api.ObjectSortDirAsc
	}

	// filter by bucket
	whereExprs := []string{"o.db_bucket_id = (SELECT id FROM buckets b WHERE b.name = ?)"}
	whereArgs := []any{bucket}

	// apply prefix
	if prefix != "" {
		whereExprs = append(whereExprs, "o.object_id LIKE ? AND SUBSTR(o.object_id, 1, ?) = ?")
		whereArgs = append(whereArgs, prefix+"%", utf8.RuneCountInString(prefix), prefix)
	}

	// apply sorting
	dir2SQL := map[string]string{
		api.ObjectSortDirAsc:  "ASC",
		api.ObjectSortDirDesc: "DESC",
	}
	if _, ok := dir2SQL[strings.ToLower(sortDir)]; !ok {
		return api.ObjectsListResponse{}, fmt.Errorf("invalid sortDir: %v", sortDir)
	}
	var orderByExprs []string
	switch strings.ToLower(sortBy) {
	case "", api.ObjectSortByName:
		orderByExprs = append(orderByExprs, "o.object_id "+dir2SQL[strings.ToLower(sortDir)])
	case api.ObjectSortByHealth:
		orderByExprs = append(orderByExprs, "o.health "+dir2SQL[strings.ToLower(sortDir)])
	case api.ObjectSortBySize:
		orderByExprs = append(orderByExprs, "o.size "+dir2SQL[strings.ToLower(sortDir)])
	default:
		return api.ObjectsListResponse{}, fmt.Errorf("invalid sortBy: %v", sortBy)
	}

	// always sort by object_id as well if we aren't explicitly
	if sortBy != api.ObjectSortByName {
		orderByExprs = append(orderByExprs, "o.object_id ASC")
	}

	// apply marker
	queryMarker := func(dst any, marker, col string) error {
		err := tx.QueryRow(ctx, fmt.Sprintf(`
			SELECT o.%s
			FROM objects o
			INNER JOIN buckets b ON o.db_bucket_id = b.id
			WHERE b.name = ? AND o.object_id = ?
		`, col), bucket, marker).Scan(dst)
		if errors.Is(err, dsql.ErrNoRows) {
			return api.ErrMarkerNotFound
		} else {
			return err
		}
	}
	desc := strings.ToLower(sortDir) == api.ObjectSortDirDesc
	if marker != "" {
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
				return api.ObjectsListResponse{}, fmt.Errorf("failed to fetch health marker: %w", err)
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
				return api.ObjectsListResponse{}, fmt.Errorf("failed to fetch health marker: %w", err)
			} else if desc {
				whereExprs = append(whereExprs, "((o.size <= ? AND o.object_id >?) OR o.size < ?)")
				whereArgs = append(whereArgs, markerSize, marker, markerSize)
			} else {
				whereExprs = append(whereExprs, "(o.size > ? OR (o.size >= ? AND object_id > ?))")
				whereArgs = append(whereArgs, markerSize, markerSize, marker)
			}
		default:
			return api.ObjectsListResponse{}, fmt.Errorf("invalid marker: %v", marker)
		}
	}

	// apply limit
	whereArgs = append(whereArgs, limit)

	// run query
	rows, err := tx.Query(ctx, fmt.Sprintf(`
		SELECT o.object_id, o.size, o.health, o.mime_type, o.created_at, o.etag
		FROM objects o
		WHERE %s
		ORDER BY %s
		LIMIT ?
	`,
		strings.Join(whereExprs, " AND "),
		strings.Join(orderByExprs, ", ")),
		whereArgs...)
	if err != nil {
		return api.ObjectsListResponse{}, fmt.Errorf("failed to fetch objects: %w", err)
	}
	defer rows.Close()

	var objects []api.ObjectMetadata
	for rows.Next() {
		om, err := scanObjectMetadata(rows)
		if err != nil {
			return api.ObjectsListResponse{}, fmt.Errorf("failed to scan object metadata: %w", err)
		}
		objects = append(objects, om)
	}

	var hasMore bool
	var nextMarker string
	if len(objects) == limit {
		objects = objects[:len(objects)-1]
		if len(objects) > 0 {
			hasMore = true
			nextMarker = objects[len(objects)-1].Name
		}
	}

	return api.ObjectsListResponse{
		HasMore:    hasMore,
		NextMarker: nextMarker,
		Objects:    objects,
	}, nil
}

func copyContractToArchive(ctx context.Context, tx sql.Tx, fcid types.FileContractID, renewedTo *types.FileContractID, reason string) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO archived_contracts (created_at, fcid, renewed_from, contract_price, state, total_cost,
			proof_height, revision_height, revision_number, size, start_height, window_start, window_end,
			upload_spending, download_spending, fund_account_spending, delete_spending, list_spending, renewed_to,
			host, reason)
		SELECT ?, fcid, renewed_from, contract_price, state, total_cost, proof_height, revision_height, revision_number,
			size, start_height, window_start, window_end, upload_spending, download_spending, fund_account_spending,
			delete_spending, list_spending, ?, h.public_key, ?
		FROM contracts c
		INNER JOIN hosts h ON h.id = c.host_id
		WHERE fcid = ?
	`, time.Now(), (*FileContractID)(renewedTo), reason, FileContractID(fcid))
	return err
}
