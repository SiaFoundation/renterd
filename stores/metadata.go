package stores

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/v2/alerts"
	"go.sia.tech/renterd/v2/api"
	"go.sia.tech/renterd/v2/object"
	sql "go.sia.tech/renterd/v2/stores/sql"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

const (
	// batchDurationThreshold is the upper bound for the duration of a batch
	// operation on the database. As long as we are below the threshold, we
	// increase the batch size.
	batchDurationThreshold = time.Second

	// refreshHealthBatchSize is the number of slabs for which we update the
	// health per db transaction. 10000 equals roughtly 1.2TiB of slabs at a
	// 10/30 erasure coding and takes <1s to execute on an SSD in SQLite.
	refreshHealthBatchSize = 10000

	// slabPruningBatchSize is the number of slabs per batch when we prune
	// slabs. We limit this to 100 slabs which is 3000 sectors at default
	// redundancy.
	slabPruningBatchSize = 100

	// hostSectorPruningBatchSize is the number of host sectors per batch when
	// we prune host sectors.
	hostSectorPruningBatchSize = 10000

	refreshHealthMinHealthValidity = 12 * time.Hour
	refreshHealthMaxHealthValidity = 72 * time.Hour
)

var (
	pruneHostSectorsAlertID = frand.Entropy256()
	pruneSlabsAlertID       = frand.Entropy256()
)

var objectDeleteBatchSizes = []int64{10, 50, 100, 200, 500, 1000, 5000, 10000, 50000, 100000}

func (s *SQLStore) Bucket(ctx context.Context, bucket string) (b api.Bucket, err error) {
	err = s.db.Transaction(ctx, func(tx sql.DatabaseTx) (err error) {
		b, err = tx.Bucket(ctx, bucket)
		return
	})
	return
}

func (s *SQLStore) Buckets(ctx context.Context) (buckets []api.Bucket, err error) {
	err = s.db.Transaction(ctx, func(tx sql.DatabaseTx) (err error) {
		buckets, err = tx.Buckets(ctx)
		return
	})
	return
}

func (s *SQLStore) CreateBucket(ctx context.Context, bucket string, policy api.BucketPolicy) error {
	return s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		return tx.CreateBucket(ctx, bucket, policy)
	})
}

func (s *SQLStore) UpdateBucketPolicy(ctx context.Context, bucket string, policy api.BucketPolicy) error {
	return s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		return tx.UpdateBucketPolicy(ctx, bucket, policy)
	})
}

func (s *SQLStore) DeleteBucket(ctx context.Context, bucket string) error {
	return s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		return tx.DeleteBucket(ctx, bucket)
	})
}

// ObjectsStats returns some info related to the objects stored in the store. To
// reduce locking and make sure all results are consistent, everything is done
// within a single transaction.
func (s *SQLStore) ObjectsStats(ctx context.Context, opts api.ObjectsStatsOpts) (resp api.ObjectsStatsResponse, _ error) {
	err := s.db.Transaction(ctx, func(tx sql.DatabaseTx) (err error) {
		resp, err = tx.ObjectsStats(ctx, opts)
		return
	})
	return resp, err
}

func (s *SQLStore) SlabBuffers(ctx context.Context) ([]api.SlabBuffer, error) {
	return s.slabBufferMgr.SlabBuffers(), nil
}

func (s *SQLStore) AddRenewal(ctx context.Context, c api.ContractMetadata) error {
	return s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		// fetch renewed contract
		renewed, err := tx.Contract(ctx, c.RenewedFrom)
		if err != nil {
			return err
		}

		// insert renewal by updating the renewed contract
		err = tx.UpdateContract(ctx, c.RenewedFrom, c)
		if err != nil {
			return err
		}

		// reinsert renewed contract
		renewed.ArchivalReason = api.ContractArchivalReasonRenewed
		renewed.RenewedTo = c.ID
		renewed.Usability = api.ContractUsabilityBad
		return tx.PutContract(ctx, renewed)
	})
}

func (s *SQLStore) AncestorContracts(ctx context.Context, id types.FileContractID, startHeight uint64) (ancestors []api.ContractMetadata, err error) {
	err = s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		ancestors, err = tx.AncestorContracts(ctx, id, startHeight)
		return err
	})
	return
}

func (s *SQLStore) ArchiveContract(ctx context.Context, id types.FileContractID, reason string) error {
	return s.ArchiveContracts(ctx, map[types.FileContractID]string{id: reason})
}

func (s *SQLStore) ArchiveContracts(ctx context.Context, toArchive map[types.FileContractID]string) error {
	defer s.triggerHostSectorPruning()

	// archive contracts one-by-one to avoid overwhelming the database due to
	// the cascade deletion of contract-sectors.
	var errs []string
	for fcid, reason := range toArchive {
		// invalidate health of related sectors before archiving the contract
		// NOTE: even if this is not done in the same transaction it won't have any
		// lasting negative effects.
		if err := s.invalidateSlabHealthByFCID(ctx, []types.FileContractID{fcid}); err != nil {
			return fmt.Errorf("ArchiveContracts: failed to invalidate slab health: %w", err)
		}

		// archive the contract but don't interrupt the process if one contract
		// fails
		if err := s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
			return tx.ArchiveContract(ctx, fcid, reason)
		}); err != nil {
			errs = append(errs, fmt.Sprintf("%v: %v", fcid, err))
			continue
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("ArchiveContracts: failed to archive at least one contract: %v", strings.Join(errs, "; "))
	}
	return nil
}

func (s *SQLStore) ArchiveAllContracts(ctx context.Context, reason string) error {
	contracts, err := s.Contracts(ctx, api.ContractsOpts{})
	if err != nil {
		return fmt.Errorf("failed to fetch contracts: %w", err)
	}
	toArchive := make(map[types.FileContractID]string)
	for _, c := range contracts {
		toArchive[c.ID] = reason
	}
	return s.ArchiveContracts(ctx, toArchive)
}

func (s *SQLStore) Contract(ctx context.Context, id types.FileContractID) (cm api.ContractMetadata, err error) {
	err = s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		cm, err = tx.Contract(ctx, id)
		return err
	})
	return
}

func (s *SQLStore) Contracts(ctx context.Context, opts api.ContractsOpts) ([]api.ContractMetadata, error) {
	var contracts []api.ContractMetadata
	err := s.db.Transaction(ctx, func(tx sql.DatabaseTx) (err error) {
		contracts, err = tx.Contracts(ctx, opts)
		return
	})
	return contracts, err
}

func (s *SQLStore) ContractRoots(ctx context.Context, id types.FileContractID) (roots []types.Hash256, err error) {
	err = s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		roots, err = tx.ContractRoots(ctx, id)
		return err
	})
	return
}

func (s *SQLStore) ContractSizes(ctx context.Context) (sizes map[types.FileContractID]api.ContractSize, err error) {
	err = s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		sizes, err = tx.ContractSizes(ctx)
		return err
	})
	return
}

func (s *SQLStore) ContractSize(ctx context.Context, id types.FileContractID) (cs api.ContractSize, err error) {
	err = s.db.Transaction(ctx, func(tx sql.DatabaseTx) (err error) {
		cs, err = tx.ContractSize(ctx, id)
		return
	})
	return cs, err
}

func (s *SQLStore) PutContract(ctx context.Context, c api.ContractMetadata) error {
	return s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		return tx.PutContract(ctx, c)
	})
}

func (s *SQLStore) UpdateContractUsability(ctx context.Context, fcid types.FileContractID, usability string) error {
	// update usability
	if err := s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		return tx.UpdateContractUsability(ctx, fcid, usability)
	}); err != nil {
		return fmt.Errorf("failed to update contract usability: %w", err)
	}

	// invalidate health
	if err := s.invalidateSlabHealthByFCID(ctx, []types.FileContractID{fcid}); err != nil {
		return fmt.Errorf("failed to invalidate slab health: %w", err)
	}

	return nil
}

func (s *SQLStore) RenewedContract(ctx context.Context, renewedFrom types.FileContractID) (cm api.ContractMetadata, err error) {
	err = s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		cm, err = tx.RenewedContract(ctx, renewedFrom)
		return err
	})
	return
}

func (s *SQLStore) Object(ctx context.Context, bucket, key string) (obj api.Object, err error) {
	err = s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		obj, err = tx.Object(ctx, bucket, key)
		return err
	})
	return
}

func (s *SQLStore) RecordContractSpending(ctx context.Context, records []api.ContractSpendingRecord) error {
	if len(records) == 0 {
		return nil // nothing to do
	}

	squashedRecords := make(map[types.FileContractID]api.ContractSpending)
	latestValues := make(map[types.FileContractID]struct {
		revision          uint64
		size              uint64
		missedHostPayout  types.Currency
		validRenterPayout types.Currency
	})
	for _, r := range records {
		squashedRecords[r.ContractID] = squashedRecords[r.ContractID].Add(r.ContractSpending)
		v := latestValues[r.ContractID]
		if r.RevisionNumber > latestValues[r.ContractID].revision {
			v.revision = r.RevisionNumber
			v.size = r.Size
			v.missedHostPayout = r.MissedHostPayout
			v.validRenterPayout = r.ValidRenterPayout
			latestValues[r.ContractID] = v
		}
	}
	metrics := make([]api.ContractMetric, 0, len(squashedRecords))
	for fcid, newSpending := range squashedRecords {
		var metric api.ContractMetric
		err := s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
			contract, err := tx.Contract(ctx, fcid)
			if errors.Is(err, api.ErrContractNotFound) {
			} else if err != nil {
				return fmt.Errorf("failed to fetch contract: %w", err)
			}

			remainingCollateral := types.ZeroCurrency
			if mhp := latestValues[fcid].missedHostPayout; types.Currency(contract.ContractPrice).Cmp(mhp) <= 0 {
				remainingCollateral = mhp.Sub(types.Currency(contract.ContractPrice))
			}
			metric = api.ContractMetric{
				Timestamp:           api.TimeNow(),
				ContractID:          fcid,
				HostKey:             contract.HostKey,
				RemainingCollateral: remainingCollateral,
				RemainingFunds:      latestValues[fcid].validRenterPayout,
				RevisionNumber:      latestValues[fcid].revision,
				UploadSpending:      contract.Spending.Uploads.Add(newSpending.Uploads),
				FundAccountSpending: contract.Spending.FundAccount.Add(newSpending.FundAccount),
				DeleteSpending:      contract.Spending.Deletions.Add(newSpending.Deletions),
				SectorRootsSpending: contract.Spending.SectorRoots.Add(newSpending.SectorRoots),
			}

			var updates api.ContractSpending
			if !newSpending.Uploads.IsZero() {
				updates.Uploads = metric.UploadSpending
			}
			if !newSpending.FundAccount.IsZero() {
				updates.FundAccount = metric.FundAccountSpending
			}
			if !newSpending.Deletions.IsZero() {
				updates.Deletions = metric.DeleteSpending
			}
			if !newSpending.SectorRoots.IsZero() {
				updates.SectorRoots = metric.SectorRootsSpending
			}
			return tx.RecordContractSpending(ctx, fcid, latestValues[fcid].revision, latestValues[fcid].size, updates)
		})
		if err != nil {
			return err
		}
		metrics = append(metrics, metric)
	}
	if len(metrics) > 0 {
		if err := s.RecordContractMetric(ctx, metrics...); err != nil {
			s.logger.Errorw("failed to record contract metrics", zap.Error(err))
		}
	}
	return nil
}

func (s *SQLStore) RenameObject(ctx context.Context, bucket, keyOld, keyNew string, force bool) error {
	return s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		err := tx.RenameObject(ctx, bucket, keyOld, keyNew, force)
		if err != nil {
			return err
		}
		s.triggerSlabPruning()
		return nil
	})
}

func (s *SQLStore) RenameObjects(ctx context.Context, bucket, prefixOld, prefixNew string, force bool) error {
	return s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		if err := tx.RenameObjects(ctx, bucket, prefixOld, prefixNew, force); err != nil {
			return err
		}
		s.triggerSlabPruning()
		return nil
	})
}

func (s *SQLStore) FetchPartialSlab(ctx context.Context, ec object.EncryptionKey, offset, length uint32) ([]byte, error) {
	return s.slabBufferMgr.FetchPartialSlab(ctx, ec, offset, length)
}

func (s *SQLStore) AddPartialSlab(ctx context.Context, data []byte, minShards, totalShards uint8) ([]object.SlabSlice, int64, error) {
	return s.slabBufferMgr.AddPartialSlab(ctx, data, minShards, totalShards)
}

func (s *SQLStore) CopyObject(ctx context.Context, srcBucket, dstBucket, srcPath, dstPath, mimeType string, metadata api.ObjectUserMetadata) (om api.ObjectMetadata, err error) {
	err = s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		if srcBucket != dstBucket || srcPath != dstPath {
			_, err = tx.DeleteObject(ctx, dstBucket, dstPath)
			if err != nil {
				return fmt.Errorf("CopyObject: failed to delete object: %w", err)
			}
		}
		om, err = tx.CopyObject(ctx, srcBucket, dstBucket, srcPath, dstPath, mimeType, metadata)
		return err
	})
	return
}

func (s *SQLStore) DeleteHostSector(ctx context.Context, hk types.PublicKey, root types.Hash256) (deletedSectors int, err error) {
	err = s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		deletedSectors, err = tx.DeleteHostSector(ctx, hk, root)
		return err
	})
	return
}

func (s *SQLStore) UpdateObject(ctx context.Context, bucket, key, eTag, mimeType string, metadata api.ObjectUserMetadata, o object.Object) error {
	// Sanity check input.
	for _, s := range o.Slabs {
		for i, shard := range s.Shards {
			// Verify that all hosts have a contract.
			if len(shard.Contracts) == 0 {
				return fmt.Errorf("missing hosts for slab %d", i)
			}
		}
	}

	// UpdateObject is ACID.
	var prune bool
	err := s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		// Try to delete. We want to get rid of the object and its slices if it
		// exists.
		//
		// NOTE: the object's created_at is currently used as its ModTime, if we
		// ever stop recreating the object but update it instead we need to take
		// this into account
		//
		// NOTE: the metadata is not deleted because this delete will cascade,
		// if we stop recreating the object we have to make sure to delete the
		// object's metadata before trying to recreate it
		var err error
		prune, err = tx.DeleteObject(ctx, bucket, key)
		if err != nil {
			return fmt.Errorf("UpdateObject: failed to delete object: %w", err)
		}

		// Insert a new object.
		err = tx.InsertObject(ctx, bucket, key, o, mimeType, eTag, metadata)
		if err != nil {
			return fmt.Errorf("failed to insert object: %w", err)
		}
		return nil
	})
	if err != nil {
		return err
	} else if prune {
		// trigger pruning if we deleted an object
		s.triggerSlabPruning()
	}
	return nil
}

func (s *SQLStore) RemoveObject(ctx context.Context, bucket, key string) error {
	var prune bool
	err := s.db.Transaction(ctx, func(tx sql.DatabaseTx) (err error) {
		prune, err = tx.DeleteObject(ctx, bucket, key)
		return
	})
	if err != nil {
		return fmt.Errorf("RemoveObject: failed to delete object: %w", err)
	} else if !prune {
		return fmt.Errorf("%w: key: %s", api.ErrObjectNotFound, key)
	}
	s.triggerSlabPruning()
	return nil
}

func (s *SQLStore) RemoveObjects(ctx context.Context, bucket, prefix string) error {
	var prune bool
	batchSizeIdx := 0
	for {
		start := time.Now()
		var done bool
		var deleted bool
		var duration time.Duration
		if err := s.db.Transaction(ctx, func(tx sql.DatabaseTx) (err error) {
			deleted, err = tx.DeleteObjects(ctx, bucket, prefix, objectDeleteBatchSizes[batchSizeIdx])
			return
		}); err != nil {
			return fmt.Errorf("failed to delete objects: %w", err)
		}
		if deleted {
			prune = true
		}
		done = !deleted
		if done {
			break // nothing more to delete
		}
		duration = time.Since(start)

		// increase the batch size if deletion was faster than the threshold
		if duration < batchDurationThreshold && batchSizeIdx < len(objectDeleteBatchSizes)-1 {
			batchSizeIdx++
		}
	}
	if !prune {
		return fmt.Errorf("%w: prefix: %s", api.ErrObjectNotFound, prefix)
	}
	s.triggerSlabPruning()
	return nil
}

func (s *SQLStore) Slab(ctx context.Context, key object.EncryptionKey) (slab object.Slab, err error) {
	err = s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		slab, err = tx.Slab(ctx, key)
		return err
	})
	return
}

func (s *SQLStore) UpdateSlab(ctx context.Context, key object.EncryptionKey, sectors []api.UploadedSector) error {
	return s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		return tx.UpdateSlab(ctx, key, sectors)
	})
}

func (s *SQLStore) RefreshHealth(ctx context.Context) error {
	for {
		// update slabs
		var rowsAffected int64
		err := s.db.Transaction(ctx, func(tx sql.DatabaseTx) (err error) {
			rowsAffected, err = tx.UpdateSlabHealth(ctx, refreshHealthBatchSize, refreshHealthMinHealthValidity, refreshHealthMaxHealthValidity)
			return
		})
		if err != nil {
			return fmt.Errorf("failed to update slab health: %w", err)
		}
		// check if done
		if rowsAffected < refreshHealthBatchSize {
			return nil // done
		}
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-time.After(time.Second):
		}
	}
}

// SlabsForMigration returns up to 'limit' slabs that do not reach full
// redundancy. These slabs need to be migrated to good contracts so they are
// restored to full health.
func (s *SQLStore) SlabsForMigration(ctx context.Context, healthCutoff float64, limit int) (slabs []api.UnhealthySlab, err error) {
	if limit <= -1 {
		limit = math.MaxInt
	}
	err = s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		slabs, err = tx.SlabsForMigration(ctx, healthCutoff, limit)
		return err
	})
	return
}

// ObjectMetadata returns an object's metadata
func (s *SQLStore) ObjectMetadata(ctx context.Context, bucket, key string) (obj api.Object, err error) {
	err = s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		obj, err = tx.ObjectMetadata(ctx, bucket, key)
		return err
	})
	return
}

// PackedSlabsForUpload returns up to 'limit' packed slabs that are ready for
// uploading. They are locked for 'lockingDuration' time before being handed out
// again.
func (s *SQLStore) PackedSlabsForUpload(ctx context.Context, lockingDuration time.Duration, minShards, totalShards uint8, limit int) ([]api.PackedSlab, error) {
	return s.slabBufferMgr.SlabsForUpload(ctx, lockingDuration, minShards, totalShards, limit)
}

func (s *SQLStore) PrunableContractRoots(ctx context.Context, fcid types.FileContractID, roots []types.Hash256) (indices []uint64, err error) {
	err = s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		indices, err = tx.PrunableContractRoots(ctx, fcid, roots)
		return err
	})
	return
}

// MarkPackedSlabsUploaded marks the given slabs as uploaded and deletes them
// from the buffer.
func (s *SQLStore) MarkPackedSlabsUploaded(ctx context.Context, slabs []api.UploadedPackedSlab) error {
	// sanity check input
	for i, ss := range slabs {
		for _, shard := range ss.Shards {
			if shard.ContractID == (types.FileContractID{}) {
				return fmt.Errorf("slab %d is invalid, ContractID can not be empty", i)
			} else if shard.Root == (types.Hash256{}) {
				return fmt.Errorf("slab %d is invalid, Root can not be empty", i)
			}
		}
	}
	var fileNames []string
	err := s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		fileNames = make([]string, len(slabs))
		for i, slab := range slabs {
			fileName, err := tx.MarkPackedSlabUploaded(ctx, slab)
			if err != nil {
				return err
			}
			fileNames[i] = fileName
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("marking slabs as uploaded in the db failed: %w", err)
	}

	// Delete buffer from disk.
	s.slabBufferMgr.RemoveBuffers(fileNames...)
	return nil
}

func (s *SQLStore) pruneHostSectorLoop() {
	for {
		select {
		case <-s.hostSectorPruneSigChan:
		case <-s.shutdownCtx.Done():
			return
		}

		// prune host sectors
		pruneSuccess := true
		for {
			var deleted int64
			err := s.db.Transaction(s.shutdownCtx, func(dt sql.DatabaseTx) error {
				var err error
				deleted, err = dt.PruneHostSectors(s.shutdownCtx, hostSectorPruningBatchSize)
				return err
			})
			if err != nil {
				s.logger.Errorw("hest sector pruning failed", zap.Error(err))
				s.alerts.RegisterAlert(s.shutdownCtx, alerts.Alert{
					ID:        pruneHostSectorsAlertID,
					Severity:  alerts.SeverityWarning,
					Message:   "Failed to prune host sectors",
					Timestamp: time.Now(),
					Data: map[string]interface{}{
						"error": err.Error(),
						"hint":  "This might happen when your database is under a lot of load due to deleting objects rapidly. This alert will disappear the next time host sectors are pruned successfully.",
					},
				})
				pruneSuccess = false
			} else {
				s.alerts.DismissAlerts(s.shutdownCtx, pruneHostSectorsAlertID)
			}

			if deleted < hostSectorPruningBatchSize {
				break // done
			}
		}

		// mark the last prune time where host sectors were pruned
		if pruneSuccess {
			s.mu.Lock()
			s.lastPrunedHostSectorsAt = time.Now()
			s.mu.Unlock()
			s.logger.Debug("host sectors pruned successfully")
		}
	}
}

func (s *SQLStore) pruneSlabsLoop() {
	for {
		select {
		case <-s.slabPruneSigChan:
		case <-s.shutdownCtx.Done():
			return
		}

		// prune slabs
		pruneSuccess := true
		for {
			var deleted int64
			err := s.db.Transaction(s.shutdownCtx, func(dt sql.DatabaseTx) error {
				var err error
				deleted, err = dt.PruneSlabs(s.shutdownCtx, slabPruningBatchSize)
				return err
			})
			if err != nil {
				s.logger.Errorw("slab pruning failed", zap.Error(err))
				s.alerts.RegisterAlert(s.shutdownCtx, alerts.Alert{
					ID:        pruneSlabsAlertID,
					Severity:  alerts.SeverityWarning,
					Message:   "Failed to prune slabs",
					Timestamp: time.Now(),
					Data: map[string]interface{}{
						"error": err.Error(),
						"hint":  "This might happen when your database is under a lot of load due to deleting objects rapidly. This alert will disappear the next time slabs are pruned successfully.",
					},
				})
				pruneSuccess = false
			} else {
				s.alerts.DismissAlerts(s.shutdownCtx, pruneSlabsAlertID)
			}

			if deleted < slabPruningBatchSize {
				break // done
			}
		}

		// mark the last prune time where both slabs and dirs were pruned
		if pruneSuccess {
			s.mu.Lock()
			s.lastPrunedSlabsAt = time.Now()
			s.mu.Unlock()
		}
	}
}

func (s *SQLStore) triggerHostSectorPruning() {
	select {
	case s.hostSectorPruneSigChan <- struct{}{}:
	default:
	}
}

func (s *SQLStore) triggerSlabPruning() {
	select {
	case s.slabPruneSigChan <- struct{}{}:
	default:
	}
}

func (s *SQLStore) invalidateSlabHealthByFCID(ctx context.Context, fcids []types.FileContractID) error {
	for {
		var affected int64
		err := s.db.Transaction(ctx, func(tx sql.DatabaseTx) (err error) {
			affected, err = tx.InvalidateSlabHealthByFCID(ctx, fcids, refreshHealthBatchSize)
			return
		})
		if err != nil {
			return fmt.Errorf("failed to invalidate slab health: %w", err)
		} else if affected < refreshHealthBatchSize {
			return nil // done
		}
		time.Sleep(time.Second)
	}
}

func (s *SQLStore) Objects(ctx context.Context, bucket, prefix, substring, delim, sortBy, sortDir, marker string, limit int, slabEncryptionKey object.EncryptionKey) (resp api.ObjectsResponse, err error) {
	err = s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		resp, err = tx.Objects(ctx, bucket, prefix, substring, delim, sortBy, sortDir, marker, limit, slabEncryptionKey)
		return err
	})
	return
}
