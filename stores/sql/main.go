package sql

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	dsql "database/sql"

	rhpv2 "go.sia.tech/core/rhp/v2"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/sql"
)

var ErrNegativeOffset = errors.New("offset can not be negative")

func Bucket(ctx context.Context, tx sql.Tx, bucket string) (api.Bucket, error) {
	b, err := scanBucket(tx.QueryRow(ctx, "SELECT created_at, name, COALESCE(policy, '{}') FROM buckets WHERE name = ?", bucket))
	if errors.Is(err, dsql.ErrNoRows) {
		return api.Bucket{}, api.ErrBucketNotFound
	} else if err != nil {
		return api.Bucket{}, fmt.Errorf("failed to fetch bucket: %w", err)
	}
	return b, nil
}

func Contracts(ctx context.Context, tx sql.Tx, opts api.ContractsOpts) ([]api.ContractMetadata, error) {
	var rows *sql.LoggedRows
	var err error
	if opts.ContractSet != "" {
		// if we filter by contract set, we fetch the set first to check if it
		// exists and then fetch the contracts. Knowing that the contracts are
		// part of at least one set allows us to use INNER JOINs.
		var contractSetID int64
		err = tx.QueryRow(ctx, "SELECT id FROM contract_sets WHERE contract_sets.name = ?", opts.ContractSet).
			Scan(&contractSetID)
		if errors.Is(err, dsql.ErrNoRows) {
			return nil, api.ErrContractSetNotFound
		}
		rows, err = tx.Query(ctx, `
			SELECT c.fcid, c.renewed_from, c.contract_price, c.state, c.total_cost, c.proof_height,
			c.revision_height, c.revision_number, c.size, c.start_height, c.window_start, c.window_end,
			c.upload_spending, c.download_spending, c.fund_account_spending, c.delete_spending, c.list_spending,
			cs.name, h.net_address, h.public_key, h.settings->>'$.siamuxport' AS siamux_port
			FROM (
				SELECT contracts.*
				FROM contracts
				INNER JOIN contract_set_contracts csc ON csc.db_contract_id = contracts.id
				INNER JOIN contract_sets cs ON cs.id = csc.db_contract_set_id
				WHERE cs.id = ?
			) AS c
			INNER JOIN hosts h ON h.id = c.host_id
			INNER JOIN contract_set_contracts csc ON csc.db_contract_id = c.id
			INNER JOIN contract_sets cs ON cs.id = csc.db_contract_set_id
			ORDER BY c.id ASC`, contractSetID)
	} else {
		// if we don't filter, we need to left join here to ensure we don't miss
		// contracts that are not part of any set
		rows, err = tx.Query(ctx, `
			SELECT c.fcid, c.renewed_from, c.contract_price, c.state, c.total_cost, c.proof_height,
			c.revision_height, c.revision_number, c.size, c.start_height, c.window_start, c.window_end,
			c.upload_spending, c.download_spending, c.fund_account_spending, c.delete_spending, c.list_spending,
			COALESCE(cs.name, ""), h.net_address, h.public_key, h.settings->>'$.siamuxport' AS siamux_port
			FROM contracts AS c
			INNER JOIN hosts h ON h.id = c.host_id
			LEFT JOIN contract_set_contracts csc ON csc.db_contract_id = c.id
			LEFT JOIN contract_sets cs ON cs.id = csc.db_contract_set_id
			ORDER BY c.id ASC`)
	}
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
	if err := InsertMetadata(ctx, tx, dstObjID, metadata); err != nil {
		return api.ObjectMetadata{}, fmt.Errorf("failed to insert metadata: %w", err)
	}

	// fetch copied object
	return fetchMetadata(dstObjID)
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

func UpdateMetadata(ctx context.Context, tx sql.Tx, objID int64, md api.ObjectUserMetadata) error {
	if err := DeleteMetadata(ctx, tx, objID); err != nil {
		return err
	} else if err := InsertMetadata(ctx, tx, objID, md); err != nil {
		return err
	}
	return nil
}

func DeleteMetadata(ctx context.Context, tx sql.Tx, objID int64) error {
	_, err := tx.Exec(ctx, "DELETE FROM object_user_metadata WHERE db_object_id = ?", objID)
	return err
}

func InsertMetadata(ctx context.Context, tx sql.Tx, objID int64, md api.ObjectUserMetadata) error {
	if len(md) == 0 {
		return nil
	}
	insertMetadataStmt, err := tx.Prepare(ctx, "INSERT INTO object_user_metadata (created_at, db_object_id, `key`, value) VALUES (?, ?, ?, ?)")
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
	err := tx.QueryRow(ctx, "SELECT mu.id, mu.object_id, mu.mime_type, mu.key, b.name, b.id FROM multipart_uploads mu INNER JOIN buckets b ON b.id = mu.db_bucket_id").
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

func SearchHosts(ctx context.Context, tx sql.Tx, autopilotID, filterMode, usabilityMode, addressContains string, keyIn []types.PublicKey, offset, limit int, hasAllowlist, hasBlocklist bool) ([]api.Host, error) {
	if offset < 0 {
		return nil, ErrNegativeOffset
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

	// filter autopilot
	if autopilotID != "" {
		whereExprs = append(whereExprs, "EXISTS (SELECT 1 FROM hosts h2 INNER JOIN host_checks hc ON hc.db_host_id = h2.id AND h.id = h2.id INNER JOIN autopilots ap ON hc.db_autopilot_id = ap.id WHERE ap.identifier = ?)")
		args = append(args, autopilotID)
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
	switch usabilityMode {
	case api.UsabilityFilterModeUsable:
		whereExprs = append(whereExprs, "EXISTS (SELECT 1 FROM hosts h2 INNER JOIN host_checks hc ON hc.db_host_id = h2.id AND h2.id = h.id WHERE hc.usability_blocked = 0 AND hc.usability_offline = 0 AND hc.usability_low_score = 0 AND hc.usability_redundant_ip = 0 AND hc.usability_gouging = 0 AND hc.usability_not_accepting_contracts = 0 AND hc.usability_not_announced = 0 AND hc.usability_not_completing_scan = 0)")
	case api.UsabilityFilterModeUnusable:
		whereExprs = append(whereExprs, "EXISTS (SELECT 1 FROM hosts h2 INNER JOIN host_checks hc ON hc.db_host_id = h2.id AND h2.id = h.id WHERE hc.usability_blocked = 1 OR hc.usability_offline = 1 OR hc.usability_low_score = 1 OR hc.usability_redundant_ip = 1 OR hc.usability_gouging = 1 OR hc.usability_not_accepting_contracts = 1 OR hc.usability_not_announced = 1 OR hc.usability_not_completing_scan = 1)")
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
			h.scanned, %s
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
		err := rows.Scan(&hostID, &h.KnownSince, &h.LastAnnouncement, (*PublicKey)(&h.PublicKey),
			&h.NetAddress, (*PriceTable)(&h.PriceTable.HostPriceTable), &pte,
			(*Settings)(&h.Settings), &h.Interactions.TotalScans, (*UnixTimeNS)(&h.Interactions.LastScan), &h.Interactions.LastScanSuccess,
			&h.Interactions.SecondToLastScanSuccess, &h.Interactions.Uptime, &h.Interactions.Downtime,
			&h.Interactions.SuccessfulInteractions, &h.Interactions.FailedInteractions, &h.Interactions.LostSectors,
			&h.Scanned, &h.Blocked,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan host: %w", err)
		}

		h.PriceTable.Expiry = pte.Time
		h.StoredData = storedDataMap[hostID]
		hosts = append(hosts, h)
	}

	// query host checks
	var apExpr string
	if autopilotID != "" {
		apExpr = "WHERE ap.identifier = ?"
		args = append(args, autopilotID)
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

func scanBucket(s scanner) (api.Bucket, error) {
	var createdAt time.Time
	var name, policy string
	err := s.Scan(&createdAt, &name, &policy)
	if err != nil {
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
