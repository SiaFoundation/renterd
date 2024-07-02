package stores

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
	"unicode/utf8"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/alerts"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/object"
	sql "go.sia.tech/renterd/stores/sql"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
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

	// sectorInsertionBatchSize is the number of sectors per batch when we
	// upsert sectors.
	sectorInsertionBatchSize = 500

	// slabPruningBatchSize is the number of slabs per batch when we prune
	// slabs. We limit this to 100 slabs which is 3000 sectors at default
	// redundancy.
	slabPruningBatchSize = 100

	refreshHealthMinHealthValidity = 12 * time.Hour
	refreshHealthMaxHealthValidity = 72 * time.Hour
)

const (
	contractStateInvalid contractState = iota
	contractStatePending
	contractStateActive
	contractStateComplete
	contractStateFailed
)

var (
	pruneSlabsAlertID = frand.Entropy256()
	pruneDirsAlertID  = frand.Entropy256()
)

var (
	objectDeleteBatchSizes = []int64{10, 50, 100, 200, 500, 1000, 5000, 10000, 50000, 100000}
)

type (
	contractState uint8

	dbArchivedContract struct {
		Model

		ContractCommon
		RenewedTo fileContractID `gorm:"index;size:32"`

		Host   publicKey `gorm:"index;NOT NULL;size:32"`
		Reason string
	}

	dbContract struct {
		Model

		ContractCommon

		HostID uint `gorm:"index"`
		Host   dbHost

		ContractSets []dbContractSet `gorm:"many2many:contract_set_contracts;constraint:OnDelete:CASCADE"`
	}

	ContractCommon struct {
		FCID        fileContractID `gorm:"unique;index;NOT NULL;column:fcid;size:32"`
		RenewedFrom fileContractID `gorm:"index;size:32"`

		ContractPrice  currency
		State          contractState `gorm:"index;NOT NULL;default:0"`
		TotalCost      currency
		ProofHeight    uint64 `gorm:"index;default:0"`
		RevisionHeight uint64 `gorm:"index;default:0"`
		RevisionNumber string `gorm:"NOT NULL;default:'0'"` // string since db can't store math.MaxUint64
		Size           uint64
		StartHeight    uint64 `gorm:"index;NOT NULL"`
		WindowStart    uint64 `gorm:"index;NOT NULL;default:0"`
		WindowEnd      uint64 `gorm:"index;NOT NULL;default:0"`

		// spending fields
		UploadSpending      currency
		DownloadSpending    currency
		FundAccountSpending currency
		DeleteSpending      currency
		ListSpending        currency
	}

	dbContractSet struct {
		Model

		Name      string       `gorm:"unique;index;"`
		Contracts []dbContract `gorm:"many2many:contract_set_contracts;constraint:OnDelete:CASCADE"`
	}

	dbDirectory struct {
		Model

		Name       string
		DBParentID uint
	}

	dbObject struct {
		Model

		DBDirectoryID uint

		DBBucketID uint `gorm:"index;uniqueIndex:idx_object_bucket;NOT NULL"`
		DBBucket   dbBucket
		ObjectID   string `gorm:"index;uniqueIndex:idx_object_bucket"`

		Key      secretKey
		Slabs    []dbSlice              // no CASCADE, slices are deleted via trigger
		Metadata []dbObjectUserMetadata `gorm:"constraint:OnDelete:CASCADE"` // CASCADE to delete metadata too
		Health   float64                `gorm:"index;default:1.0; NOT NULL"`
		Size     int64

		MimeType string `json:"index"`
		Etag     string `gorm:"index"`
	}

	dbObjectUserMetadata struct {
		Model

		DBObjectID          *uint  `gorm:"index:uniqueIndex:idx_object_user_metadata_key"`
		DBMultipartUploadID *uint  `gorm:"index:uniqueIndex:idx_object_user_metadata_key"`
		Key                 string `gorm:"index:uniqueIndex:idx_object_user_metadata_key"`
		Value               string
	}

	dbBucket struct {
		Model

		Policy api.BucketPolicy `gorm:"serializer:json"`
		Name   string           `gorm:"unique;index;NOT NULL"`
	}

	dbSlice struct {
		Model
		DBObjectID        *uint `gorm:"index"`
		ObjectIndex       uint  `gorm:"index:idx_slices_object_index"`
		DBMultipartPartID *uint `gorm:"index"`

		// Slice related fields.
		DBSlabID uint `gorm:"index"`
		Offset   uint32
		Length   uint32
	}

	dbSlab struct {
		Model
		DBContractSetID  uint `gorm:"index"`
		DBContractSet    dbContractSet
		DBBufferedSlabID uint `gorm:"index;default: NULL"`

		Health           float64   `gorm:"index;default:1.0; NOT NULL"`
		HealthValidUntil int64     `gorm:"index;default:0; NOT NULL"` // unix timestamp
		Key              secretKey `gorm:"unique;NOT NULL;size:32"`   // json string
		MinShards        uint8     `gorm:"index"`
		TotalShards      uint8     `gorm:"index"`

		Slices []dbSlice
		Shards []dbSector `gorm:"constraint:OnDelete:CASCADE"` // CASCADE to delete shards too
	}

	dbSector struct {
		Model

		DBSlabID  uint `gorm:"index:idx_sectors_db_slab_id;uniqueIndex:idx_sectors_slab_id_slab_index;NOT NULL"`
		SlabIndex int  `gorm:"index:idx_sectors_slab_index;uniqueIndex:idx_sectors_slab_id_slab_index;NOT NULL"`

		LatestHost publicKey `gorm:"NOT NULL"`
		Root       []byte    `gorm:"index;unique;NOT NULL;size:32"`

		Contracts []dbContract `gorm:"many2many:contract_sectors;constraint:OnDelete:CASCADE"`
	}

	// dbContractSector is a join table between dbContract and dbSector.
	dbContractSector struct {
		DBSectorID   uint `gorm:"primaryKey;index"`
		DBContractID uint `gorm:"primaryKey;index"`
	}

	// rawObject is used for hydration and is made up of one or many raw sectors.
	rawObject []rawObjectSector

	// rawObjectRow contains all necessary information to reconstruct the object.
	rawObjectSector struct {
		// object
		ObjectID       uint
		ObjectIndex    uint64
		ObjectKey      []byte
		ObjectName     string
		ObjectSize     int64
		ObjectModTime  time.Time
		ObjectMimeType string
		ObjectHealth   float64
		ObjectETag     string

		// slice
		SliceOffset uint32
		SliceLength uint32

		// slab
		SlabBuffered  bool
		SlabID        uint
		SlabHealth    float64
		SlabKey       []byte
		SlabMinShards uint8

		// sector
		SectorIndex uint
		SectorRoot  []byte
		LatestHost  publicKey

		// contract
		FCID    fileContractID
		HostKey publicKey
	}

	// rawObjectMetadata is used for hydrating object metadata.
	rawObjectMetadata struct {
		ETag       string
		Health     float64
		MimeType   string
		ModTime    datetime
		ObjectName string
		Size       int64
	}
)

func (s *contractState) LoadString(state string) error {
	switch strings.ToLower(state) {
	case api.ContractStateInvalid:
		*s = contractStateInvalid
	case api.ContractStatePending:
		*s = contractStatePending
	case api.ContractStateActive:
		*s = contractStateActive
	case api.ContractStateComplete:
		*s = contractStateComplete
	case api.ContractStateFailed:
		*s = contractStateFailed
	default:
		*s = contractStateInvalid
	}
	return nil
}

func (s contractState) String() string {
	switch s {
	case contractStateInvalid:
		return api.ContractStateInvalid
	case contractStatePending:
		return api.ContractStatePending
	case contractStateActive:
		return api.ContractStateActive
	case contractStateComplete:
		return api.ContractStateComplete
	case contractStateFailed:
		return api.ContractStateFailed
	default:
		return api.ContractStateUnknown
	}
}

func (s dbSlab) HealthValid() bool {
	return time.Now().Before(time.Unix(s.HealthValidUntil, 0))
}

// TableName implements the gorm.Tabler interface.
func (dbArchivedContract) TableName() string { return "archived_contracts" }

// TableName implements the gorm.Tabler interface.
func (dbBucket) TableName() string { return "buckets" }

// TableName implements the gorm.Tabler interface.
func (dbContract) TableName() string { return "contracts" }

// TableName implements the gorm.Tabler interface.
func (dbContractSector) TableName() string { return "contract_sectors" }

// TableName implements the gorm.Tabler interface.
func (dbContractSet) TableName() string { return "contract_sets" }

// TableName implements the gorm.Tabler interface.
func (dbDirectory) TableName() string { return "directories" }

// TableName implements the gorm.Tabler interface.
func (dbObject) TableName() string { return "objects" }

// TableName implements the gorm.Tabler interface.
func (dbObjectUserMetadata) TableName() string { return "object_user_metadata" }

// TableName implements the gorm.Tabler interface.
func (dbSector) TableName() string { return "sectors" }

// TableName implements the gorm.Tabler interface.
func (dbSlab) TableName() string { return "slabs" }

// TableName implements the gorm.Tabler interface.
func (dbSlice) TableName() string { return "slices" }

// convert converts a dbContract to a ContractMetadata.
func (c dbContract) convert() api.ContractMetadata {
	var revisionNumber uint64
	_, _ = fmt.Sscan(c.RevisionNumber, &revisionNumber)
	var contractSets []string
	for _, cs := range c.ContractSets {
		contractSets = append(contractSets, cs.Name)
	}
	return api.ContractMetadata{
		ContractPrice: types.Currency(c.ContractPrice),
		ID:            types.FileContractID(c.FCID),
		HostIP:        c.Host.NetAddress,
		HostKey:       types.PublicKey(c.Host.PublicKey),
		SiamuxAddr:    rhpv2.HostSettings(c.Host.Settings).SiamuxAddr(),

		RenewedFrom: types.FileContractID(c.RenewedFrom),
		TotalCost:   types.Currency(c.TotalCost),
		Spending: api.ContractSpending{
			Uploads:     types.Currency(c.UploadSpending),
			Downloads:   types.Currency(c.DownloadSpending),
			FundAccount: types.Currency(c.FundAccountSpending),
			Deletions:   types.Currency(c.DeleteSpending),
			SectorRoots: types.Currency(c.ListSpending),
		},
		ProofHeight:    c.ProofHeight,
		RevisionHeight: c.RevisionHeight,
		RevisionNumber: revisionNumber,
		ContractSets:   contractSets,
		Size:           c.Size,
		StartHeight:    c.StartHeight,
		State:          c.State.String(),
		WindowStart:    c.WindowStart,
		WindowEnd:      c.WindowEnd,
	}
}

// convert turns a dbObject into a object.Slab.
func (s dbSlab) convert() (slab object.Slab, err error) {
	// unmarshal key
	err = slab.Key.UnmarshalBinary(s.Key)
	if err != nil {
		return
	}

	// set health
	slab.Health = s.Health

	// set shards
	slab.MinShards = s.MinShards
	slab.Shards = make([]object.Sector, len(s.Shards))

	// hydrate shards
	for i, shard := range s.Shards {
		slab.Shards[i].LatestHost = types.PublicKey(shard.LatestHost)
		slab.Shards[i].Root = *(*types.Hash256)(shard.Root)
		for _, c := range shard.Contracts {
			if slab.Shards[i].Contracts == nil {
				slab.Shards[i].Contracts = make(map[types.PublicKey][]types.FileContractID)
			}
			slab.Shards[i].Contracts[types.PublicKey(c.Host.PublicKey)] = append(slab.Shards[i].Contracts[types.PublicKey(c.Host.PublicKey)], types.FileContractID(c.FCID))
		}
	}

	return
}

func (raw rawObjectMetadata) convert() api.ObjectMetadata {
	return newObjectMetadata(
		raw.ObjectName,
		raw.ETag,
		raw.MimeType,
		raw.Health,
		time.Time(raw.ModTime),
		raw.Size,
	)
}

func (raw rawObject) toSlabSlice() (slice object.SlabSlice, _ error) {
	if len(raw) == 0 {
		return object.SlabSlice{}, errors.New("no sectors found")
	} else if raw[0].SlabBuffered && len(raw) != 1 {
		return object.SlabSlice{}, errors.New("buffered slab with multiple sectors")
	}

	// unmarshal key
	if err := slice.Slab.Key.UnmarshalBinary(raw[0].SlabKey); err != nil {
		return object.SlabSlice{}, err
	}

	// handle partial slab
	if raw[0].SlabBuffered {
		slice.Offset = raw[0].SliceOffset
		slice.Length = raw[0].SliceLength
		slice.Slab.MinShards = raw[0].SlabMinShards
		slice.Slab.Health = raw[0].SlabHealth
		return
	}

	// hydrate all sectors
	slabID := raw[0].SlabID
	sectors := make([]object.Sector, 0, len(raw))
	secIdx := uint(0)
	for _, sector := range raw {
		if sector.SlabID != slabID {
			return object.SlabSlice{}, errors.New("sectors from different slabs") // developer error
		}
		latestHost := types.PublicKey(sector.LatestHost)
		fcid := types.FileContractID(sector.FCID)

		// next sector
		if sector.SectorIndex != secIdx {
			sectors = append(sectors, object.Sector{
				Contracts:  make(map[types.PublicKey][]types.FileContractID),
				LatestHost: latestHost,
				Root:       *(*types.Hash256)(sector.SectorRoot),
			})
			secIdx = sector.SectorIndex
		}

		// add host+contract to sector
		if fcid != (types.FileContractID{}) {
			sectors[len(sectors)-1].Contracts[types.PublicKey(sector.HostKey)] = append(sectors[len(sectors)-1].Contracts[types.PublicKey(sector.HostKey)], fcid)
		}
	}

	// hydrate all fields
	slice.Slab.Health = raw[0].SlabHealth
	slice.Slab.Shards = sectors
	slice.Slab.MinShards = raw[0].SlabMinShards
	slice.Offset = raw[0].SliceOffset
	slice.Length = raw[0].SliceLength
	return slice, nil
}

func (s *SQLStore) Bucket(ctx context.Context, bucket string) (b api.Bucket, err error) {
	err = s.bMain.Transaction(ctx, func(tx sql.DatabaseTx) (err error) {
		b, err = tx.Bucket(ctx, bucket)
		return
	})
	return
}

func (s *SQLStore) CreateBucket(ctx context.Context, bucket string, policy api.BucketPolicy) error {
	return s.bMain.Transaction(ctx, func(tx sql.DatabaseTx) error {
		return tx.CreateBucket(ctx, bucket, policy)
	})
}

func (s *SQLStore) UpdateBucketPolicy(ctx context.Context, bucket string, policy api.BucketPolicy) error {
	return s.bMain.Transaction(ctx, func(tx sql.DatabaseTx) error {
		return tx.UpdateBucketPolicy(ctx, bucket, policy)
	})
}

func (s *SQLStore) DeleteBucket(ctx context.Context, bucket string) error {
	return s.bMain.Transaction(ctx, func(tx sql.DatabaseTx) error {
		return tx.DeleteBucket(ctx, bucket)
	})
}

func (s *SQLStore) ListBuckets(ctx context.Context) (buckets []api.Bucket, err error) {
	err = s.bMain.Transaction(ctx, func(tx sql.DatabaseTx) (err error) {
		buckets, err = tx.ListBuckets(ctx)
		return
	})
	return
}

// ObjectsStats returns some info related to the objects stored in the store. To
// reduce locking and make sure all results are consistent, everything is done
// within a single transaction.
func (s *SQLStore) ObjectsStats(ctx context.Context, opts api.ObjectsStatsOpts) (resp api.ObjectsStatsResponse, _ error) {
	err := s.bMain.Transaction(ctx, func(tx sql.DatabaseTx) (err error) {
		resp, err = tx.ObjectsStats(ctx, opts)
		return
	})
	return resp, err
}

func (s *SQLStore) SlabBuffers(ctx context.Context) ([]api.SlabBuffer, error) {
	var err error
	var fileNameToContractSet map[string]string
	err = s.bMain.Transaction(ctx, func(tx sql.DatabaseTx) error {
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
	err = s.bMain.Transaction(ctx, func(tx sql.DatabaseTx) error {
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
	err := s.bMain.Transaction(ctx, func(tx sql.DatabaseTx) (err error) {
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
	err = s.bMain.Transaction(ctx, func(tx sql.DatabaseTx) error {
		renewed, err = tx.RenewContract(ctx, c, contractPrice, totalCost, startHeight, renewedFrom, state)
		return err
	})
	if err != nil {
		return api.ContractMetadata{}, fmt.Errorf("failed to add renewed contract: %w", err)
	}
	return
}

func (s *SQLStore) AncestorContracts(ctx context.Context, id types.FileContractID, startHeight uint64) (ancestors []api.ArchivedContract, err error) {
	err = s.bMain.Transaction(ctx, func(tx sql.DatabaseTx) error {
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
		if err := s.bMain.Transaction(ctx, func(tx sql.DatabaseTx) error {
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
	err = s.bMain.Transaction(ctx, func(tx sql.DatabaseTx) error {
		cm, err = tx.Contract(ctx, id)
		return err
	})
	return
}

func (s *SQLStore) ContractRoots(ctx context.Context, id types.FileContractID) (roots []types.Hash256, err error) {
	err = s.bMain.Transaction(ctx, func(tx sql.DatabaseTx) error {
		roots, err = tx.ContractRoots(ctx, id)
		return err
	})
	return
}

func (s *SQLStore) ContractSets(ctx context.Context) (sets []string, err error) {
	err = s.bMain.Transaction(ctx, func(tx sql.DatabaseTx) error {
		sets, err = tx.ContractSets(ctx)
		return err
	})
	return sets, err
}

func (s *SQLStore) ContractSizes(ctx context.Context) (sizes map[types.FileContractID]api.ContractSize, err error) {
	err = s.bMain.Transaction(ctx, func(tx sql.DatabaseTx) error {
		sizes, err = tx.ContractSizes(ctx)
		return err
	})
	return
}

func (s *SQLStore) ContractSize(ctx context.Context, id types.FileContractID) (cs api.ContractSize, err error) {
	err = s.bMain.Transaction(ctx, func(tx sql.DatabaseTx) (err error) {
		cs, err = tx.ContractSize(ctx, id)
		return
	})
	return cs, err
}

func (s *SQLStore) SetContractSet(ctx context.Context, name string, contractIds []types.FileContractID) error {
	var wantedIds []fileContractID
	wanted := make(map[fileContractID]struct{})
	for _, fcid := range contractIds {
		wantedIds = append(wantedIds, fileContractID(fcid))
		wanted[fileContractID(fcid)] = struct{}{}
	}

	var diff []types.FileContractID
	var nContractsAfter int
	err := s.retryTransaction(ctx, func(tx *gorm.DB) error {
		// fetch contract set
		var cs dbContractSet
		err := tx.
			Where(dbContractSet{Name: name}).
			Preload("Contracts").
			FirstOrCreate(&cs).
			Error
		if err != nil {
			return err
		}

		// fetch contracts
		var dbContracts []dbContract
		err = tx.
			Model(&dbContract{}).
			Where("fcid IN (?)", wantedIds).
			Find(&dbContracts).
			Error
		if err != nil {
			return err
		}
		nContractsAfter = len(dbContracts)

		// add removals to the diff
		for _, contract := range cs.Contracts {
			if _, ok := wanted[contract.FCID]; !ok {
				diff = append(diff, types.FileContractID(contract.FCID))
			}
			delete(wanted, contract.FCID)
		}

		// add additions to the diff
		for fcid := range wanted {
			diff = append(diff, types.FileContractID(fcid))
		}

		// update the association
		if err := tx.Model(&cs).Association("Contracts").Replace(&dbContracts); err != nil {
			return err
		}

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
	return s.bMain.Transaction(ctx, func(tx sql.DatabaseTx) error {
		return tx.RemoveContractSet(ctx, name)
	})
}

func (s *SQLStore) RenewedContract(ctx context.Context, renewedFrom types.FileContractID) (cm api.ContractMetadata, err error) {
	err = s.bMain.Transaction(ctx, func(tx sql.DatabaseTx) error {
		cm, err = tx.RenewedContract(ctx, renewedFrom)
		return err
	})
	return
}

func (s *SQLStore) SearchObjects(ctx context.Context, bucket, substring string, offset, limit int) ([]api.ObjectMetadata, error) {
	// fetch one more to see if there are more entries
	if limit <= -1 {
		limit = math.MaxInt
	}

	var objects []api.ObjectMetadata
	err := s.db.
		WithContext(ctx).
		Select("o.object_id as Name, o.size as Size, o.health as Health, o.mime_type as MimeType, o.etag as ETag, o.created_at as ModTime").
		Model(&dbObject{}).
		Table("objects o").
		Joins("INNER JOIN buckets b ON o.db_bucket_id = b.id").
		Where("INSTR(o.object_id, ?) > 0 AND b.name = ?", substring, bucket).
		Order("o.object_id ASC").
		Offset(offset).
		Limit(limit).
		Scan(&objects).Error
	if err != nil {
		return nil, err
	}

	return objects, nil
}

func (s *SQLStore) ObjectEntries(ctx context.Context, bucket, path, prefix, sortBy, sortDir, marker string, offset, limit int) (metadata []api.ObjectMetadata, hasMore bool, err error) {
	// sanity check we are passing a directory
	if !strings.HasSuffix(path, "/") {
		panic("path must end in /")
	}

	// convenience variables
	usingMarker := marker != ""
	usingOffset := offset > 0

	// sanity check we are passing sane paging parameters
	if usingMarker && usingOffset {
		return nil, false, errors.New("fetching entries using a marker and an offset is not supported at the same time")
	}

	// sanity check we are passing sane sorting parameters
	if err := validateSort(sortBy, sortDir); err != nil {
		return nil, false, err
	}

	// sanitize sorting parameters
	if sortBy == "" {
		sortBy = api.ObjectSortByName
	}
	if sortDir == "" {
		sortDir = api.ObjectSortDirAsc
	} else {
		sortDir = strings.ToLower(sortDir)
	}

	// ensure marker is '/' prefixed
	if usingMarker && !strings.HasPrefix(marker, "/") {
		marker = fmt.Sprintf("/%s", marker)
	}

	// ensure limit is out of play
	if limit <= -1 {
		limit = math.MaxInt
	}

	// fetch one more to see if there are more entries
	if limit != math.MaxInt {
		limit += 1
	}

	// ensure offset is out of play
	if usingMarker {
		offset = 0
	}

	// fetch id of directory to query
	dirID, err := s.dirID(s.db, path)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return []api.ObjectMetadata{}, false, nil
	} else if err != nil {
		return nil, false, err
	}

	// fetch bucket id
	var dBucket dbBucket
	if err := s.db.Select("id").
		Where("name", bucket).
		Take(&dBucket).Error; err != nil {
		return nil, false, fmt.Errorf("failed to fetch bucket id: %w", err)
	}

	// build prefix expression
	prefixExpr := "TRUE"
	if prefix != "" {
		prefixExpr = "SUBSTR(o.object_id, 1, ?) = ?"
	}

	lengthFn := "CHAR_LENGTH"
	if isSQLite(s.db) {
		lengthFn = "LENGTH"
	}

	// objectsQuery consists of 2 parts
	// 1. fetch all objects in requested directory
	// 2. fetch all sub-directories
	objectsQuery := fmt.Sprintf(`
SELECT o.etag as ETag, o.created_at as ModTime, o.object_id as ObjectName, o.size as Size, o.health as Health, o.mime_type as MimeType
FROM objects o
WHERE o.object_id != ? AND o.db_directory_id = ? AND o.db_bucket_id = (SELECT id FROM buckets b WHERE b.name = ?) AND %s
UNION ALL
SELECT '' as ETag, MAX(o.created_at) as ModTime, d.name as ObjectName, SUM(o.size) as Size, MIN(o.health) as Health, '' as MimeType
FROM objects o
INNER JOIN directories d ON SUBSTR(o.object_id, 1, %s(d.name)) = d.name AND %s
WHERE o.db_bucket_id = (SELECT id FROM buckets b WHERE b.name = ?)
AND o.object_id LIKE ?
AND SUBSTR(o.object_id, 1, ?) = ?
AND d.db_parent_id = ?
GROUP BY d.id
`, prefixExpr,
		lengthFn,
		prefixExpr)

	// build query params
	var objectsQueryParams []interface{}
	if prefix != "" {
		objectsQueryParams = []interface{}{
			path,          // o.object_id != ?
			dirID, bucket, // o.db_directory_id = ? AND b.name = ?
			utf8.RuneCountInString(path + prefix), path + prefix,
			utf8.RuneCountInString(path + prefix), path + prefix,
			bucket,                             // b.name = ?
			path + "%",                         // o.object_id LIKE ?
			utf8.RuneCountInString(path), path, // SUBSTR(o.object_id, 1, ?) = ?
			dirID, // d.db_parent_id = ?
		}
	} else {
		objectsQueryParams = []interface{}{
			path,          // o.object_id != ?
			dirID, bucket, // o.db_directory_id = ? AND b.name = ?
			bucket,
			path + "%",                         // o.object_id LIKE ?
			utf8.RuneCountInString(path), path, // SUBSTR(o.object_id, 1, ?) = ?
			dirID, // d.db_parent_id = ?
		}
	}

	// build marker expr
	markerExpr := "1 = 1"
	var markerParams []interface{}
	if usingMarker {
		switch sortBy {
		case api.ObjectSortByHealth:
			var markerHealth float64
			if err = s.db.
				WithContext(ctx).
				Raw(fmt.Sprintf(`SELECT Health FROM (SELECT * FROM (%s) h WHERE ObjectName >= ? ORDER BY ObjectName LIMIT 1) as n`, objectsQuery), append(objectsQueryParams, marker)...).
				Scan(&markerHealth).
				Error; err != nil {
				return
			}

			if sortDir == api.ObjectSortDirAsc {
				markerExpr = "(Health > ? OR (Health = ? AND ObjectName > ?))"
				markerParams = []interface{}{markerHealth, markerHealth, marker}
			} else {
				markerExpr = "(Health = ? AND ObjectName > ?) OR Health < ?"
				markerParams = []interface{}{markerHealth, marker, markerHealth}
			}
		case api.ObjectSortBySize:
			var markerSize float64
			if err = s.db.
				WithContext(ctx).
				Raw(fmt.Sprintf(`SELECT Size FROM (SELECT * FROM (%s) s WHERE ObjectName >= ? ORDER BY ObjectName LIMIT 1) as n`, objectsQuery), append(objectsQueryParams, marker)...).
				Scan(&markerSize).
				Error; err != nil {
				return
			}

			if sortDir == api.ObjectSortDirAsc {
				markerExpr = "(Size > ? OR (Size = ? AND ObjectName > ?))"
				markerParams = []interface{}{markerSize, markerSize, marker}
			} else {
				markerExpr = "(Size = ? AND ObjectName > ?) OR Size < ?"
				markerParams = []interface{}{markerSize, marker, markerSize}
			}
		case api.ObjectSortByName:
			if sortDir == api.ObjectSortDirAsc {
				markerExpr = "ObjectName > ?"
			} else {
				markerExpr = "ObjectName < ?"
			}
			markerParams = []interface{}{marker}
		default:
			panic("unhandled sortBy") // developer error
		}
	}

	// build order clause
	if sortBy == api.ObjectSortByName {
		sortBy = "ObjectName"
	}
	orderByClause := fmt.Sprintf("%s %s", sortBy, sortDir)
	if sortBy != "ObjectName" {
		orderByClause += ", ObjectName"
	}

	var rows []rawObjectMetadata
	query := fmt.Sprintf(`SELECT * FROM (%s ORDER BY %s) AS n WHERE %s LIMIT ? OFFSET ?`,
		objectsQuery,
		orderByClause,
		markerExpr,
	)
	parameters := append(append(objectsQueryParams, markerParams...), limit, offset)

	if err = s.db.
		WithContext(ctx).
		Raw(query, parameters...).
		Scan(&rows).
		Error; err != nil {
		return
	}

	// trim last element if we have more
	if len(rows) == limit {
		hasMore = true
		rows = rows[:len(rows)-1]
	}

	// convert rows into metadata
	for _, row := range rows {
		metadata = append(metadata, row.convert())
	}
	return
}

func (s *SQLStore) Object(ctx context.Context, bucket, path string) (obj api.Object, err error) {
	err = s.retryTransaction(ctx, func(tx *gorm.DB) error {
		obj, err = s.object(tx, bucket, path)
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
		err := s.retryTransaction(ctx, func(tx *gorm.DB) error {
			var contract dbContract
			err := tx.Model(&dbContract{}).
				Where("fcid", fileContractID(fcid)).
				Joins("Host").
				Take(&contract).Error
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil // contract not found, continue with next one
			} else if err != nil {
				return err
			}

			remainingCollateral := types.ZeroCurrency
			if mhp := latestValues[fcid].missedHostPayout; types.Currency(contract.ContractPrice).Cmp(mhp) <= 0 {
				remainingCollateral = mhp.Sub(types.Currency(contract.ContractPrice))
			}
			m := api.ContractMetric{
				Timestamp:           api.TimeNow(),
				ContractID:          fcid,
				HostKey:             types.PublicKey(contract.Host.PublicKey),
				RemainingCollateral: remainingCollateral,
				RemainingFunds:      latestValues[fcid].validRenterPayout,
				RevisionNumber:      latestValues[fcid].revision,
				UploadSpending:      types.Currency(contract.UploadSpending).Add(newSpending.Uploads),
				DownloadSpending:    types.Currency(contract.DownloadSpending).Add(newSpending.Downloads),
				FundAccountSpending: types.Currency(contract.FundAccountSpending).Add(newSpending.FundAccount),
				DeleteSpending:      types.Currency(contract.DeleteSpending).Add(newSpending.Deletions),
				ListSpending:        types.Currency(contract.ListSpending).Add(newSpending.SectorRoots),
			}
			metrics = append(metrics, m)

			updates := make(map[string]interface{})
			if !newSpending.Uploads.IsZero() {
				updates["upload_spending"] = currency(m.UploadSpending)
			}
			if !newSpending.Downloads.IsZero() {
				updates["download_spending"] = currency(m.DownloadSpending)
			}
			if !newSpending.FundAccount.IsZero() {
				updates["fund_account_spending"] = currency(m.FundAccountSpending)
			}
			if !newSpending.Deletions.IsZero() {
				updates["delete_spending"] = currency(m.DeleteSpending)
			}
			if !newSpending.SectorRoots.IsZero() {
				updates["list_spending"] = currency(m.ListSpending)
			}
			updates["revision_number"] = latestValues[fcid].revision
			updates["size"] = latestValues[fcid].size
			err = tx.Model(&contract).Updates(updates).Error
			return err
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

func fetchUsedContracts(tx *gorm.DB, usedContractsByHost map[types.PublicKey]map[types.FileContractID]struct{}) (map[types.FileContractID]dbContract, error) {
	// flatten map to get all used contract ids
	fcids := make([]fileContractID, 0, len(usedContractsByHost))
	for _, hostFCIDs := range usedContractsByHost {
		for fcid := range hostFCIDs {
			fcids = append(fcids, fileContractID(fcid))
		}
	}

	// fetch all contracts, take into account renewals
	var contracts []dbContract
	err := tx.Model(&dbContract{}).
		Joins("Host").
		Where("fcid IN (?) OR renewed_from IN (?)", fcids, fcids).
		Find(&contracts).Error
	if err != nil {
		return nil, err
	}

	// build map of used contracts
	usedContracts := make(map[types.FileContractID]dbContract, len(contracts))
	for _, c := range contracts {
		if _, used := usedContractsByHost[types.PublicKey(c.Host.PublicKey)][types.FileContractID(c.FCID)]; used {
			usedContracts[types.FileContractID(c.FCID)] = c
		}
		if _, used := usedContractsByHost[types.PublicKey(c.Host.PublicKey)][types.FileContractID(c.RenewedFrom)]; used {
			usedContracts[types.FileContractID(c.RenewedFrom)] = c
		}
	}
	return usedContracts, nil
}

func (s *SQLStore) RenameObject(ctx context.Context, bucket, keyOld, keyNew string, force bool) error {
	return s.bMain.Transaction(ctx, func(tx sql.DatabaseTx) error {
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
	return s.bMain.Transaction(ctx, func(tx sql.DatabaseTx) error {
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
	err = s.bMain.Transaction(ctx, func(tx sql.DatabaseTx) error {
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
	err = s.bMain.Transaction(ctx, func(tx sql.DatabaseTx) error {
		deletedSectors, err = tx.DeleteHostSector(ctx, hk, root)
		return err
	})
	return
}

func (s *SQLStore) dirID(tx *gorm.DB, dirPath string) (uint, error) {
	if !strings.HasPrefix(dirPath, "/") {
		return 0, fmt.Errorf("path must start with /")
	} else if !strings.HasSuffix(dirPath, "/") {
		return 0, fmt.Errorf("path must end with /")
	}

	if dirPath == "/" {
		return 1, nil // root dir returned
	}

	var dir dbDirectory
	if err := tx.Where("name", dirPath).
		Select("id").
		Take(&dir).
		Error; err != nil {
		return 0, fmt.Errorf("failed to fetch directory: %w", err)
	}
	return dir.ID, nil
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
	err := s.bMain.Transaction(ctx, func(tx sql.DatabaseTx) error {
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
	err := s.bMain.Transaction(ctx, func(tx sql.DatabaseTx) (err error) {
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
		if err := s.bMain.Transaction(ctx, func(tx sql.DatabaseTx) error {
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

func (s *SQLStore) Slab(ctx context.Context, key object.EncryptionKey) (object.Slab, error) {
	k, err := key.MarshalBinary()
	if err != nil {
		return object.Slab{}, err
	}
	var slab dbSlab
	tx := s.db.Where(&dbSlab{Key: k}).
		Preload("Shards.Contracts.Host").
		Take(&slab)
	if errors.Is(tx.Error, gorm.ErrRecordNotFound) {
		return object.Slab{}, api.ErrSlabNotFound
	}
	return slab.convert()
}

func (ss *SQLStore) UpdateSlab(ctx context.Context, s object.Slab, contractSet string) error {
	// sanity check the shards don't contain an empty root
	for _, s := range s.Shards {
		if s.Root == (types.Hash256{}) {
			return errors.New("shard root can never be the empty root")
		}
	}
	// Sanity check input.
	for i, shard := range s.Shards {
		// Verify that all hosts have a contract.
		if len(shard.Contracts) == 0 {
			return fmt.Errorf("missing hosts for slab %d", i)
		}
	}

	// Update slab.
	return ss.bMain.Transaction(ctx, func(tx sql.DatabaseTx) error {
		return tx.UpdateSlab(ctx, s, contractSet, s.Contracts())
	})
}

func (s *SQLStore) RefreshHealth(ctx context.Context) error {
	for {
		// update slabs
		var rowsAffected int64
		err := s.bMain.Transaction(ctx, func(tx sql.DatabaseTx) (err error) {
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
func (s *SQLStore) UnhealthySlabs(ctx context.Context, healthCutoff float64, set string, limit int) ([]api.UnhealthySlab, error) {
	if limit <= -1 {
		limit = math.MaxInt
	}

	var rows []struct {
		Key    []byte
		Health float64
	}

	if err := s.retryTransaction(ctx, func(tx *gorm.DB) error {
		return tx.Select("slabs.key, slabs.health").
			Joins("INNER JOIN contract_sets cs ON slabs.db_contract_set_id = cs.id").
			Model(&dbSlab{}).
			Where("health <= ? AND cs.name = ?", healthCutoff, set).
			Order("health ASC").
			Limit(limit).
			Find(&rows).
			Error
	}); err != nil {
		return nil, err
	}

	slabs := make([]api.UnhealthySlab, len(rows))
	for i, row := range rows {
		var key object.EncryptionKey
		if err := key.UnmarshalBinary(row.Key); err != nil {
			return nil, err
		}
		slabs[i] = api.UnhealthySlab{
			Key:    key,
			Health: row.Health,
		}
	}
	return slabs, nil
}

// object retrieves an object from the store.
func (s *SQLStore) object(tx *gorm.DB, bucket, path string) (api.Object, error) {
	// fetch raw object data
	raw, err := s.objectRaw(tx, bucket, path)
	if errors.Is(err, gorm.ErrRecordNotFound) || (err == nil && len(raw) == 0) {
		return api.Object{}, api.ErrObjectNotFound
	} else if err != nil {
		return api.Object{}, err
	}

	// hydrate raw object data
	return s.objectHydrate(tx, bucket, path, raw)
}

// objectHydrate hydrates a raw object and returns an api.Object.
func (s *SQLStore) objectHydrate(tx *gorm.DB, bucket, path string, obj rawObject) (api.Object, error) {
	// parse object key
	var key object.EncryptionKey
	if err := key.UnmarshalBinary(obj[0].ObjectKey); err != nil {
		return api.Object{}, err
	}

	// filter out slabs without slab ID and buffered slabs - this is expected
	// for an empty object or objects that end with a partial slab.
	var filtered rawObject
	minHealth := math.MaxFloat64
	for _, sector := range obj {
		if sector.SlabID != 0 {
			filtered = append(filtered, sector)
			if sector.SlabHealth < minHealth {
				minHealth = sector.SlabHealth
			}
		}
	}

	// hydrate all slabs
	slabs := make([]object.SlabSlice, 0, len(filtered))
	if len(filtered) > 0 {
		var start int
		// create a helper function to add a slab and update the state
		addSlab := func(end int) error {
			if slab, err := filtered[start:end].toSlabSlice(); err != nil {
				return err
			} else {
				slabs = append(slabs, slab)
				start = end
			}
			return nil
		}

		curr := filtered[0]
		for j, sector := range filtered {
			if sector.ObjectIndex == 0 {
				return api.Object{}, api.ErrObjectCorrupted
			} else if sector.SectorIndex == 0 && !sector.SlabBuffered {
				return api.Object{}, api.ErrObjectCorrupted
			}
			if sector.ObjectIndex != curr.ObjectIndex {
				if err := addSlab(j); err != nil {
					return api.Object{}, err
				}
				curr = sector
			}
		}
		if err := addSlab(len(filtered)); err != nil {
			return api.Object{}, err
		}
	}

	// fetch object metadata
	metadata, err := s.objectMetadata(tx, bucket, path)
	if err != nil {
		return api.Object{}, err
	}

	// return object
	return api.Object{
		Metadata: metadata,
		ObjectMetadata: newObjectMetadata(
			obj[0].ObjectName,
			obj[0].ObjectETag,
			obj[0].ObjectMimeType,
			obj[0].ObjectHealth,
			obj[0].ObjectModTime,
			obj[0].ObjectSize,
		),
		Object: &object.Object{
			Key:   key,
			Slabs: slabs,
		},
	}, nil
}

// ObjectMetadata returns an object's metadata
func (s *SQLStore) ObjectMetadata(ctx context.Context, bucket, path string) (api.Object, error) {
	var resp api.Object
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var obj dbObject
		err := tx.Model(&dbObject{}).
			Joins("INNER JOIN buckets b ON objects.db_bucket_id = b.id").
			Where("b.name", bucket).
			Where("object_id", path).
			Take(&obj).
			Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return api.ErrObjectNotFound
		} else if err != nil {
			return err
		}
		oum, err := s.objectMetadata(tx, bucket, path)
		if err != nil {
			return err
		}
		resp = api.Object{
			ObjectMetadata: newObjectMetadata(
				obj.ObjectID,
				obj.Etag,
				obj.MimeType,
				obj.Health,
				obj.CreatedAt,
				obj.Size,
			),
			Metadata: oum,
		}
		return nil
	})
	return resp, err
}

func (s *SQLStore) objectMetadata(tx *gorm.DB, bucket, path string) (api.ObjectUserMetadata, error) {
	var rows []dbObjectUserMetadata
	err := tx.
		Model(&dbObjectUserMetadata{}).
		Table("object_user_metadata oum").
		Joins("INNER JOIN objects o ON oum.db_object_id = o.id").
		Joins("INNER JOIN buckets b ON o.db_bucket_id = b.id").
		Where("o.object_id = ? AND b.name = ?", path, bucket).
		Find(&rows).
		Error
	if err != nil {
		return nil, err
	}
	metadata := make(api.ObjectUserMetadata)
	for _, row := range rows {
		metadata[row.Key] = row.Value
	}
	return metadata, nil
}

func newObjectMetadata(name, etag, mimeType string, health float64, modTime time.Time, size int64) api.ObjectMetadata {
	return api.ObjectMetadata{
		ETag:     etag,
		Health:   health,
		ModTime:  api.TimeRFC3339(modTime.UTC()),
		Name:     name,
		Size:     size,
		MimeType: mimeType,
	}
}

func (s *SQLStore) objectRaw(txn *gorm.DB, bucket string, path string) (rows rawObject, err error) {
	// NOTE: we LEFT JOIN here because empty objects are valid and need to be
	// included in the result set, when we convert the rawObject before
	// returning it we'll check for SlabID and/or SectorID being 0 and act
	// accordingly
	err = txn.
		Select("o.id as ObjectID, o.health as ObjectHealth, sli.object_index as ObjectIndex, o.key as ObjectKey, o.object_id as ObjectName, o.size as ObjectSize, o.mime_type as ObjectMimeType, o.created_at as ObjectModTime, o.etag as ObjectETag, sli.object_index, sli.offset as SliceOffset, sli.length as SliceLength, sla.id as SlabID, sla.health as SlabHealth, sla.key as SlabKey, sla.min_shards as SlabMinShards, bs.id IS NOT NULL AS SlabBuffered, sec.slab_index as SectorIndex, sec.root as SectorRoot, sec.latest_host as LatestHost, c.fcid as FCID, h.public_key as HostKey").
		Model(&dbObject{}).
		Table("objects o").
		Joins("INNER JOIN buckets b ON o.db_bucket_id = b.id").
		Joins("LEFT JOIN slices sli ON o.id = sli.`db_object_id`").
		Joins("LEFT JOIN slabs sla ON sli.db_slab_id = sla.`id`").
		Joins("LEFT JOIN sectors sec ON sla.id = sec.`db_slab_id`").
		Joins("LEFT JOIN contract_sectors cs ON sec.id = cs.`db_sector_id`").
		Joins("LEFT JOIN contracts c ON cs.`db_contract_id` = c.`id`").
		Joins("LEFT JOIN hosts h ON c.host_id = h.id").
		Joins("LEFT JOIN buffered_slabs bs ON sla.db_buffered_slab_id = bs.`id`").
		Where("o.object_id = ? AND b.name = ?", path, bucket).
		Order("sli.object_index ASC").
		Order("sec.slab_index ASC").
		Scan(&rows).
		Error
	return
}

// PackedSlabsForUpload returns up to 'limit' packed slabs that are ready for
// uploading. They are locked for 'lockingDuration' time before being handed out
// again.
func (s *SQLStore) PackedSlabsForUpload(ctx context.Context, lockingDuration time.Duration, minShards, totalShards uint8, set string, limit int) ([]api.PackedSlab, error) {
	return s.slabBufferMgr.SlabsForUpload(ctx, lockingDuration, minShards, totalShards, set, limit)
}

func (s *SQLStore) ObjectsBySlabKey(ctx context.Context, bucket string, slabKey object.EncryptionKey) (metadata []api.ObjectMetadata, err error) {
	var rows []rawObjectMetadata
	key, err := slabKey.MarshalBinary()
	if err != nil {
		return nil, err
	}

	err = s.retryTransaction(ctx, func(tx *gorm.DB) error {
		return tx.Raw(`
SELECT DISTINCT obj.object_id as ObjectName, obj.size as Size, obj.mime_type as MimeType, sla.health as Health
FROM slabs sla
INNER JOIN slices sli ON sli.db_slab_id = sla.id
INNER JOIN objects obj ON sli.db_object_id = obj.id
INNER JOIN buckets b ON obj.db_bucket_id = b.id AND b.name = ?
WHERE sla.key = ?
	`, bucket, key).
			Scan(&rows).
			Error
	})
	if err != nil {
		return nil, err
	}

	// convert rows
	for _, row := range rows {
		metadata = append(metadata, row.convert())
	}
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
	var fileName string
	err := s.retryTransaction(ctx, func(tx *gorm.DB) error {
		for _, slab := range slabs {
			var err error
			fileName, err = s.markPackedSlabUploaded(tx, slab)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("marking slabs as uploaded in the db failed: %w", err)
	}

	// Delete buffer from disk.
	s.slabBufferMgr.RemoveBuffers(fileName)
	return nil
}

func (s *SQLStore) markPackedSlabUploaded(tx *gorm.DB, slab api.UploadedPackedSlab) (string, error) {
	// collect all used contracts
	usedContracts := slab.Contracts()
	contracts, err := fetchUsedContracts(tx, usedContracts)
	if err != nil {
		return "", err
	}

	// find the slab
	var sla dbSlab
	if err := tx.Where("db_buffered_slab_id", slab.BufferID).
		Take(&sla).Error; err != nil {
		return "", err
	}

	// update the slab
	if err := tx.Model(&dbSlab{}).
		Where("id", sla.ID).
		Updates(map[string]interface{}{
			"db_buffered_slab_id": nil,
		}).Error; err != nil {
		return "", fmt.Errorf("failed to set buffered slab NULL: %w", err)
	}

	// delete buffer
	var fileName string
	if err := tx.Raw("SELECT filename FROM buffered_slabs WHERE id = ?", slab.BufferID).
		Scan(&fileName).Error; err != nil {
		return "", err
	}
	if err := tx.Exec("DELETE FROM buffered_slabs WHERE id = ?", slab.BufferID).Error; err != nil {
		return "", err
	}

	// add the shards to the slab
	var shards []dbSector
	for i := range slab.Shards {
		sector := dbSector{
			DBSlabID:   sla.ID,
			SlabIndex:  i + 1,
			LatestHost: publicKey(slab.Shards[i].LatestHost),
			Root:       slab.Shards[i].Root[:],
		}
		for _, fcids := range slab.Shards[i].Contracts {
			for _, fcid := range fcids {
				if c, ok := contracts[fcid]; ok {
					sector.Contracts = append(sector.Contracts, c)
				}
			}
		}
		shards = append(shards, sector)
	}
	if err := tx.Create(&shards).Error; err != nil {
		return "", fmt.Errorf("failed to create shards: %w", err)
	}
	return fileName, nil
}

// contract retrieves a contract from the store.
func contract(tx *gorm.DB, id fileContractID) (contract dbContract, err error) {
	err = tx.
		Where(&dbContract{ContractCommon: ContractCommon{FCID: id}}).
		Joins("Host").
		Take(&contract).
		Error

	if errors.Is(err, gorm.ErrRecordNotFound) {
		err = api.ErrContractNotFound
	}
	return
}

// contractsForHost retrieves all contracts for the given host
func contractsForHost(tx *gorm.DB, host dbHost) (contracts []dbContract, err error) {
	err = tx.
		Where(&dbContract{HostID: host.ID}).
		Joins("Host").
		Find(&contracts).
		Error
	return
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
			err := s.bMain.Transaction(s.shutdownCtx, func(dt sql.DatabaseTx) error {
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
		err := s.bMain.Transaction(s.shutdownCtx, func(dt sql.DatabaseTx) error {
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
		err := s.bMain.Transaction(ctx, func(tx sql.DatabaseTx) (err error) {
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
	err = s.bMain.Transaction(ctx, func(tx sql.DatabaseTx) error {
		resp, err = tx.ListObjects(ctx, bucket, prefix, sortBy, sortDir, marker, limit)
		return err
	})
	return
}

func validateSort(sortBy, sortDir string) error {
	allowed := func(s string, allowed ...string) bool {
		for _, a := range allowed {
			if strings.EqualFold(s, a) {
				return true
			}
		}
		return false
	}

	if !allowed(sortDir, "", api.ObjectSortDirAsc, api.ObjectSortDirDesc) {
		return fmt.Errorf("invalid dir '%v', allowed values are '%v' and '%v'; %w", sortDir, api.ObjectSortDirAsc, api.ObjectSortDirDesc, api.ErrInvalidObjectSortParameters)
	}

	if !allowed(sortBy, "", api.ObjectSortByHealth, api.ObjectSortByName, api.ObjectSortBySize) {
		return fmt.Errorf("invalid sort by '%v', allowed values are '%v', '%v' and '%v'; %w", sortBy, api.ObjectSortByHealth, api.ObjectSortByName, api.ObjectSortBySize, api.ErrInvalidObjectSortParameters)
	}
	return nil
}

// upsertSectors creates a sector or updates it if it exists already. The
// resulting ID is set on the input sector.
func upsertSectors(tx *gorm.DB, sectors []dbSector) ([]uint, error) {
	if len(sectors) == 0 {
		return nil, nil // nothing to do
	}
	err := tx.
		Clauses(clause.OnConflict{
			UpdateAll: true,
			Columns:   []clause.Column{{Name: "root"}},
		}).
		CreateInBatches(&sectors, sectorInsertionBatchSize).
		Error
	if err != nil {
		return nil, err
	}

	sectorIDs := make([]uint, len(sectors))
	for i := range sectors {
		var id uint
		if err := tx.Model(dbSector{}).
			Where("root", sectors[i].Root).
			Select("id").Take(&id).Error; err != nil {
			return nil, err
		}
		sectorIDs[i] = id
	}
	return sectorIDs, nil
}
