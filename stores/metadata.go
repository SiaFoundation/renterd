package stores

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/alerts"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/object"
	sql "go.sia.tech/renterd/stores/sql"
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

	refreshHealthMinHealthValidity = 12 * time.Hour
	refreshHealthMaxHealthValidity = 72 * time.Hour
)

var (
	pruneSlabsAlertID = frand.Entropy256()
	pruneDirsAlertID  = frand.Entropy256()
)

var objectDeleteBatchSizes = []int64{10, 50, 100, 200, 500, 1000, 5000, 10000, 50000, 100000}

func (s *SQLStore) Bucket(ctx context.Context, bucket string) (b api.Bucket, err error) {
	err = s.db.Transaction(ctx, func(tx sql.DatabaseTx) (err error) {
		b, err = tx.Bucket(ctx, bucket)
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

func (s *SQLStore) ListBuckets(ctx context.Context) (buckets []api.Bucket, err error) {
	err = s.db.Transaction(ctx, func(tx sql.DatabaseTx) (err error) {
		buckets, err = tx.ListBuckets(ctx)
		return
	})
	return
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
	var err error
	var fileNameToContractSet map[string]string
	err = s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		fileNameToContractSet, err = tx.SlabBuffers(ctx)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch slab buffers: %w", err)
	}

	// Fetch in-memory buffer info and fill in contract set name.
	buffers := s.slabBufferMgr.SlabBuffers()
	for i := range buffers {
		buffers[i].ContractSet = fileNameToContractSet[buffers[i].Filename]
	}
	return buffers, nil
}

func (s *SQLStore) AddContract(ctx context.Context, c rhpv2.ContractRevision, contractPrice, totalCost types.Currency, startHeight uint64, state string) (_ api.ContractMetadata, err error) {
	var contract api.ContractMetadata
	err = s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		contract, err = tx.InsertContract(ctx, c, contractPrice, totalCost, startHeight, types.FileContractID{}, state)
		return err
	})
	if err != nil {
		return api.ContractMetadata{}, fmt.Errorf("failed to add contract: %w", err)
	}

	return contract, nil
}

func (s *SQLStore) Contracts(ctx context.Context, opts api.ContractsOpts) ([]api.ContractMetadata, error) {
	var contracts []api.ContractMetadata
	err := s.db.Transaction(ctx, func(tx sql.DatabaseTx) (err error) {
		contracts, err = tx.Contracts(ctx, opts)
		return
	})
	return contracts, err
}

// AddRenewedContract adds a new contract which was created as the result of a renewal to the store.
// The old contract specified as 'renewedFrom' will be deleted from the active
// contracts and moved to the archive. Both new and old contract will be linked
// to each other through the RenewedFrom and RenewedTo fields respectively.
func (s *SQLStore) AddRenewedContract(ctx context.Context, c rhpv2.ContractRevision, contractPrice, totalCost types.Currency, startHeight uint64, renewedFrom types.FileContractID, state string) (renewed api.ContractMetadata, err error) {
	err = s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		renewed, err = tx.RenewContract(ctx, c, contractPrice, totalCost, startHeight, renewedFrom, state)
		return err
	})
	if err != nil {
		return api.ContractMetadata{}, fmt.Errorf("failed to add renewed contract: %w", err)
	}
	return
}

func (s *SQLStore) AncestorContracts(ctx context.Context, id types.FileContractID, startHeight uint64) (ancestors []api.ArchivedContract, err error) {
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

func (s *SQLStore) ContractRoots(ctx context.Context, id types.FileContractID) (roots []types.Hash256, err error) {
	err = s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		roots, err = tx.ContractRoots(ctx, id)
		return err
	})
	return
}

func (s *SQLStore) ContractSets(ctx context.Context) (sets []string, err error) {
	err = s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		sets, err = tx.ContractSets(ctx)
		return err
	})
	return sets, err
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

func (s *SQLStore) SetContractSet(ctx context.Context, name string, contractIds []types.FileContractID) error {
	wanted := make(map[types.FileContractID]struct{})
	for _, fcid := range contractIds {
		wanted[types.FileContractID(fcid)] = struct{}{}
	}

	var diff []types.FileContractID
	var nContractsAfter int
	err := s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		// build diff
		prevContracts, err := tx.Contracts(ctx, api.ContractsOpts{ContractSet: name})
		if err != nil && !errors.Is(err, api.ErrContractSetNotFound) {
			return fmt.Errorf("failed to fetch contracts: %w", err)
		}
		diff = nil // reset
		for _, c := range prevContracts {
			if _, exists := wanted[c.ID]; !exists {
				diff = append(diff, c.ID) // removal
			} else {
				delete(wanted, c.ID)
			}
		}
		for fcid := range wanted {
			diff = append(diff, fcid) // addition
		}
		// update contract set
		if err := tx.SetContractSet(ctx, name, contractIds); err != nil {
			return fmt.Errorf("failed to set contract set: %w", err)
		}
		// fetch contracts after update
		afterContracts, err := tx.Contracts(ctx, api.ContractsOpts{ContractSet: name})
		if err != nil {
			return fmt.Errorf("failed to fetch contracts after update: %w", err)
		}
		nContractsAfter = len(afterContracts)
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to set contract set: %w", err)
	}

	// Invalidate slab health.
	err = s.invalidateSlabHealthByFCID(ctx, diff)
	if err != nil {
		return fmt.Errorf("failed to invalidate slab health: %w", err)
	}

	// Record the update.
	err = s.RecordContractSetMetric(ctx, api.ContractSetMetric{
		Name:      name,
		Contracts: nContractsAfter,
		Timestamp: api.TimeNow(),
	})
	if err != nil {
		return fmt.Errorf("failed to record contract set metric: %w", err)
	}
	return nil
}

func (s *SQLStore) RemoveContractSet(ctx context.Context, name string) error {
	return s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		return tx.RemoveContractSet(ctx, name)
	})
}

func (s *SQLStore) RenewedContract(ctx context.Context, renewedFrom types.FileContractID) (cm api.ContractMetadata, err error) {
	err = s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		cm, err = tx.RenewedContract(ctx, renewedFrom)
		return err
	})
	return
}

func (s *SQLStore) SearchObjects(ctx context.Context, bucket, substring string, offset, limit int) (objects []api.ObjectMetadata, err error) {
	err = s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		objects, err = tx.SearchObjects(ctx, bucket, substring, offset, limit)
		return err
	})
	return
}

func (s *SQLStore) ObjectEntries(ctx context.Context, bucket, path, prefix, sortBy, sortDir, marker string, offset, limit int) (metadata []api.ObjectMetadata, hasMore bool, err error) {
	err = s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		metadata, hasMore, err = tx.ObjectEntries(ctx, bucket, path, prefix, sortBy, sortDir, marker, offset, limit)
		return err
	})
	return
}

func (s *SQLStore) Object(ctx context.Context, bucket, path string) (obj api.Object, err error) {
	err = s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		obj, err = tx.Object(ctx, bucket, path)
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
			m := api.ContractMetric{
				Timestamp:           api.TimeNow(),
				ContractID:          fcid,
				HostKey:             contract.HostKey,
				RemainingCollateral: remainingCollateral,
				RemainingFunds:      latestValues[fcid].validRenterPayout,
				RevisionNumber:      latestValues[fcid].revision,
				UploadSpending:      contract.Spending.Uploads.Add(newSpending.Uploads),
				DownloadSpending:    contract.Spending.Downloads.Add(newSpending.Downloads),
				FundAccountSpending: contract.Spending.FundAccount.Add(newSpending.FundAccount),
				DeleteSpending:      contract.Spending.Deletions.Add(newSpending.Deletions),
				ListSpending:        contract.Spending.SectorRoots.Add(newSpending.SectorRoots),
			}
			metrics = append(metrics, m)

			var updates api.ContractSpending
			if !newSpending.Uploads.IsZero() {
				updates.Uploads = m.UploadSpending
			}
			if !newSpending.Downloads.IsZero() {
				updates.Downloads = m.DownloadSpending
			}
			if !newSpending.FundAccount.IsZero() {
				updates.FundAccount = m.FundAccountSpending
			}
			if !newSpending.Deletions.IsZero() {
				updates.Deletions = m.DeleteSpending
			}
			if !newSpending.SectorRoots.IsZero() {
				updates.SectorRoots = m.ListSpending
			}
			return tx.RecordContractSpending(ctx, fcid, latestValues[fcid].revision, latestValues[fcid].size, updates)
		})
		if err != nil {
			return err
		}
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
		// create new dir
		dirID, err := tx.MakeDirsForPath(ctx, keyNew)
		if err != nil {
			return err
		}
		// update object
		err = tx.RenameObject(ctx, bucket, keyOld, keyNew, dirID, force)
		if err != nil {
			return err
		}
		// delete old dir if empty
		s.triggerSlabPruning()
		return nil
	})
}

func (s *SQLStore) RenameObjects(ctx context.Context, bucket, prefixOld, prefixNew string, force bool) error {
	return s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		// create new dir
		dirID, err := tx.MakeDirsForPath(ctx, prefixNew)
		if err != nil {
			return fmt.Errorf("RenameObjects: failed to create new directory: %w", err)
		} else if err := tx.RenameObjects(ctx, bucket, prefixOld, prefixNew, dirID, force); err != nil {
			return err
		}
		// prune old dirs
		s.triggerSlabPruning()
		return nil
	})
}

func (s *SQLStore) FetchPartialSlab(ctx context.Context, ec object.EncryptionKey, offset, length uint32) ([]byte, error) {
	return s.slabBufferMgr.FetchPartialSlab(ctx, ec, offset, length)
}

func (s *SQLStore) AddPartialSlab(ctx context.Context, data []byte, minShards, totalShards uint8, contractSet string) ([]object.SlabSlice, int64, error) {
	return s.slabBufferMgr.AddPartialSlab(ctx, data, minShards, totalShards, contractSet)
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

func (s *SQLStore) UpdateObject(ctx context.Context, bucket, path, contractSet, eTag, mimeType string, metadata api.ObjectUserMetadata, o object.Object) error {
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
		prune, err = tx.DeleteObject(ctx, bucket, path)
		if err != nil {
			return fmt.Errorf("UpdateObject: failed to delete object: %w", err)
		}

		// create the dir
		dirID, err := tx.MakeDirsForPath(ctx, path)
		if err != nil {
			return fmt.Errorf("failed to create directories for path '%s': %w", path, err)
		}

		// Insert a new object.
		err = tx.InsertObject(ctx, bucket, path, contractSet, dirID, o, mimeType, eTag, metadata)
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

func (s *SQLStore) RemoveObject(ctx context.Context, bucket, path string) error {
	var prune bool
	err := s.db.Transaction(ctx, func(tx sql.DatabaseTx) (err error) {
		prune, err = tx.DeleteObject(ctx, bucket, path)
		return
	})
	if err != nil {
		return fmt.Errorf("RemoveObject: failed to delete object: %w", err)
	} else if !prune {
		return fmt.Errorf("%w: key: %s", api.ErrObjectNotFound, path)
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
		var duration time.Duration
		if err := s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
			deleted, err := tx.DeleteObjects(ctx, bucket, prefix, objectDeleteBatchSizes[batchSizeIdx])
			if err != nil {
				return err
			}
			prune = prune || deleted
			done = !deleted
			return nil
		}); err != nil {
			return fmt.Errorf("failed to delete objects: %w", err)
		} else if done {
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

func (s *SQLStore) UpdateSlab(ctx context.Context, slab object.Slab, contractSet string) error {
	// sanity check the shards don't contain an empty root
	for _, shard := range slab.Shards {
		if shard.Root == (types.Hash256{}) {
			return errors.New("shard root can never be the empty root")
		}
	}
	// Sanity check input.
	for i, shard := range slab.Shards {
		// Verify that all hosts have a contract.
		if len(shard.Contracts) == 0 {
			return fmt.Errorf("missing hosts for slab %d", i)
		}
	}

	// Update slab.
	return s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		return tx.UpdateSlab(ctx, slab, contractSet, slab.Contracts())
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

// UnhealthySlabs returns up to 'limit' slabs that do not reach full redundancy
// in the given contract set. These slabs need to be migrated to good contracts
// so they are restored to full health.
func (s *SQLStore) UnhealthySlabs(ctx context.Context, healthCutoff float64, set string, limit int) (slabs []api.UnhealthySlab, err error) {
	if limit <= -1 {
		limit = math.MaxInt
	}
	err = s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		slabs, err = tx.UnhealthySlabs(ctx, healthCutoff, set, limit)
		return err
	})
	return
}

// ObjectMetadata returns an object's metadata
func (s *SQLStore) ObjectMetadata(ctx context.Context, bucket, path string) (obj api.Object, err error) {
	err = s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		obj, err = tx.ObjectMetadata(ctx, bucket, path)
		return err
	})
	return
}

// PackedSlabsForUpload returns up to 'limit' packed slabs that are ready for
// uploading. They are locked for 'lockingDuration' time before being handed out
// again.
func (s *SQLStore) PackedSlabsForUpload(ctx context.Context, lockingDuration time.Duration, minShards, totalShards uint8, set string, limit int) ([]api.PackedSlab, error) {
	return s.slabBufferMgr.SlabsForUpload(ctx, lockingDuration, minShards, totalShards, set, limit)
}

func (s *SQLStore) ObjectsBySlabKey(ctx context.Context, bucket string, slabKey object.EncryptionKey) (metadata []api.ObjectMetadata, err error) {
	err = s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		metadata, err = tx.ObjectsBySlabKey(ctx, bucket, slabKey)
		return err
	})
	return
}

func (s *SQLStore) PrunableContractRoots(ctx context.Context, fcid types.FileContractID, roots []types.Hash256) (indices []uint64, err error) {
	err = s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		indices, err = tx.PrunableContractRoots(ctx, fcid, roots, s.logger.Named("debugpj"))
		return err
	})
	return
}

// MarkPackedSlabsUploaded marks the given slabs as uploaded and deletes them
// from the buffer.
func (s *SQLStore) MarkPackedSlabsUploaded(ctx context.Context, slabs []api.UploadedPackedSlab) error {
	// Sanity check input.
	for i, ss := range slabs {
		for _, shard := range ss.Shards {
			// Verify that all hosts have a contract.
			if len(shard.Contracts) == 0 {
				return fmt.Errorf("missing hosts for slab %d", i)
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

		// prune dirs
		err := s.db.Transaction(s.shutdownCtx, func(dt sql.DatabaseTx) error {
			return dt.PruneEmptydirs(s.shutdownCtx)
		})
		if err != nil {
			s.logger.Errorw("dir pruning failed", zap.Error(err))
			s.alerts.RegisterAlert(s.shutdownCtx, alerts.Alert{
				ID:        pruneDirsAlertID,
				Severity:  alerts.SeverityWarning,
				Message:   "Failed to prune dirs",
				Timestamp: time.Now(),
				Data: map[string]interface{}{
					"error": err.Error(),
					"hint":  "This might happen when your database is under a lot of load due to deleting objects rapidly. This alert will disappear the next time slabs are pruned successfully.",
				},
			})
			pruneSuccess = false
		} else {
			s.alerts.DismissAlerts(s.shutdownCtx, pruneDirsAlertID)
		}

		// mark the last prune time where both slabs and dirs were pruned
		if pruneSuccess {
			s.mu.Lock()
			s.lastPrunedAt = time.Now()
			s.mu.Unlock()
		}
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

// TODO: we can use ObjectEntries instead of ListObject if we want to use '/' as
// a delimiter for now (see backend.go) but it would be interesting to have
// arbitrary 'delim' support in ListObjects.
func (s *SQLStore) ListObjects(ctx context.Context, bucket, prefix, sortBy, sortDir, marker string, limit int) (resp api.ObjectsListResponse, err error) {
	err = s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		resp, err = tx.ListObjects(ctx, bucket, prefix, sortBy, sortDir, marker, limit)
		return err
	})
	return
}
