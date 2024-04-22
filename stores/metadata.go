package stores

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/alerts"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/object"
	"go.sia.tech/siad/modules"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"lukechampine.com/frand"
)

var (
	pruneSlabsAlertID = frand.Entropy256()
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

	refreshHealthMinHealthValidity = 12 * time.Hour
	refreshHealthMaxHealthValidity = 72 * time.Hour
)

var (
	errInvalidNumberOfShards = errors.New("slab has invalid number of shards")
	errShardRootChanged      = errors.New("shard root changed")

	objectDeleteBatchSizes = []int64{10, 50, 100, 200, 500, 1000, 5000, 10000, 50000, 100000}
)

const (
	contractStateInvalid contractState = iota
	contractStatePending
	contractStateActive
	contractStateComplete
	contractStateFailed
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

	dbObject struct {
		Model

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

	dbBufferedSlab struct {
		Model

		DBSlab dbSlab

		Filename string
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
		ETag     string
		Health   float64
		MimeType string
		ModTime  datetime
		Name     string
		Size     int64
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
func (dbObject) TableName() string { return "objects" }

// TableName implements the gorm.Tabler interface.
func (dbObjectUserMetadata) TableName() string { return "object_user_metadata" }

// TableName implements the gorm.Tabler interface.
func (dbSector) TableName() string { return "sectors" }

// TableName implements the gorm.Tabler interface.
func (dbSlab) TableName() string { return "slabs" }

// TableName implements the gorm.Tabler interface.
func (dbBufferedSlab) TableName() string { return "buffered_slabs" }

// TableName implements the gorm.Tabler interface.
func (dbSlice) TableName() string { return "slices" }

// convert converts a dbContract to an ArchivedContract.
func (c dbArchivedContract) convert() api.ArchivedContract {
	var revisionNumber uint64
	_, _ = fmt.Sscan(c.RevisionNumber, &revisionNumber)
	return api.ArchivedContract{
		ID:        types.FileContractID(c.FCID),
		HostKey:   types.PublicKey(c.Host),
		RenewedTo: types.FileContractID(c.RenewedTo),

		ProofHeight:    c.ProofHeight,
		RevisionHeight: c.RevisionHeight,
		RevisionNumber: revisionNumber,
		Size:           c.Size,
		StartHeight:    c.StartHeight,
		State:          c.State.String(),
		WindowStart:    c.WindowStart,
		WindowEnd:      c.WindowEnd,

		Spending: api.ContractSpending{
			Uploads:     types.Currency(c.UploadSpending),
			Downloads:   types.Currency(c.DownloadSpending),
			FundAccount: types.Currency(c.FundAccountSpending),
			Deletions:   types.Currency(c.DeleteSpending),
			SectorRoots: types.Currency(c.ListSpending),
		},
	}
}

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
		raw.Name,
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

func (s *SQLStore) Bucket(ctx context.Context, bucket string) (api.Bucket, error) {
	var b dbBucket
	err := s.db.
		WithContext(ctx).
		Model(&dbBucket{}).
		Where("name = ?", bucket).
		Take(&b).
		Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return api.Bucket{}, api.ErrBucketNotFound
	} else if err != nil {
		return api.Bucket{}, err
	}
	return api.Bucket{
		CreatedAt: api.TimeRFC3339(b.CreatedAt.UTC()),
		Name:      b.Name,
		Policy:    b.Policy,
	}, nil
}

func (s *SQLStore) CreateBucket(ctx context.Context, bucket string, policy api.BucketPolicy) error {
	// Create bucket.
	return s.retryTransaction(ctx, func(tx *gorm.DB) error {
		res := tx.Clauses(clause.OnConflict{
			DoNothing: true,
		}).
			Create(&dbBucket{
				Name:   bucket,
				Policy: policy,
			})
		if res.Error != nil {
			return res.Error
		} else if res.RowsAffected == 0 {
			return api.ErrBucketExists
		}
		return nil
	})
}

func (s *SQLStore) UpdateBucketPolicy(ctx context.Context, bucket string, policy api.BucketPolicy) error {
	b, err := json.Marshal(policy)
	if err != nil {
		return err
	}
	return s.retryTransaction(ctx, func(tx *gorm.DB) error {
		return tx.
			Model(&dbBucket{}).
			Where("name", bucket).
			Updates(map[string]interface{}{
				"policy": string(b),
			},
			).
			Error
	})
}

func (s *SQLStore) DeleteBucket(ctx context.Context, bucket string) error {
	// Delete bucket.
	return s.retryTransaction(ctx, func(tx *gorm.DB) error {
		var b dbBucket
		if err := tx.Take(&b, "name = ?", bucket).Error; errors.Is(err, gorm.ErrRecordNotFound) {
			return api.ErrBucketNotFound
		} else if err != nil {
			return err
		}
		var count int64
		if err := tx.Model(&dbObject{}).Where("db_bucket_id = ?", b.ID).
			Limit(1).
			Count(&count).Error; err != nil {
			return err
		}
		if count > 0 {
			return api.ErrBucketNotEmpty
		}
		res := tx.Delete(&b)
		if res.Error != nil {
			return res.Error
		}
		return nil
	})
}

func (s *SQLStore) ListBuckets(ctx context.Context) ([]api.Bucket, error) {
	var buckets []dbBucket
	err := s.db.
		WithContext(ctx).
		Model(&dbBucket{}).
		Find(&buckets).
		Error
	if err != nil {
		return nil, err
	}

	resp := make([]api.Bucket, len(buckets))
	for i, b := range buckets {
		resp[i] = api.Bucket{
			CreatedAt: api.TimeRFC3339(b.CreatedAt.UTC()),
			Name:      b.Name,
			Policy:    b.Policy,
		}
	}
	return resp, nil
}

// ObjectsStats returns some info related to the objects stored in the store. To
// reduce locking and make sure all results are consistent, everything is done
// within a single transaction.
func (s *SQLStore) ObjectsStats(ctx context.Context, opts api.ObjectsStatsOpts) (api.ObjectsStatsResponse, error) {
	db := s.db.WithContext(ctx)

	// fetch bucket id if a bucket was specified
	var bucketID uint
	if opts.Bucket != "" {
		err := db.Model(&dbBucket{}).Select("id").Where("name = ?", opts.Bucket).Take(&bucketID).Error
		if err != nil {
			return api.ObjectsStatsResponse{}, err
		}
	}

	// number of objects
	var objInfo struct {
		NumObjects       uint64
		MinHealth        float64
		TotalObjectsSize uint64
	}
	objInfoQuery := db.
		Model(&dbObject{}).
		Select("COUNT(*) AS NumObjects, COALESCE(MIN(health), 1) as MinHealth, SUM(size) AS TotalObjectsSize")
	if opts.Bucket != "" {
		objInfoQuery = objInfoQuery.Where("db_bucket_id", bucketID)
	}
	err := objInfoQuery.Scan(&objInfo).Error
	if err != nil {
		return api.ObjectsStatsResponse{}, err
	}

	// number of unfinished objects
	var unfinishedObjects uint64
	unfinishedObjectsQuery := db.
		Model(&dbMultipartUpload{}).
		Select("COUNT(*)")
	if opts.Bucket != "" {
		unfinishedObjectsQuery = unfinishedObjectsQuery.Where("db_bucket_id", bucketID)
	}
	err = unfinishedObjectsQuery.Scan(&unfinishedObjects).Error
	if err != nil {
		return api.ObjectsStatsResponse{}, err
	}

	// size of unfinished objects
	var totalUnfinishedObjectsSize uint64
	totalUnfinishedObjectsSizeQuery := db.
		Model(&dbMultipartPart{}).
		Joins("INNER JOIN multipart_uploads mu ON multipart_parts.db_multipart_upload_id = mu.id").
		Select("COALESCE(SUM(size), 0)")
	if opts.Bucket != "" {
		totalUnfinishedObjectsSizeQuery = totalUnfinishedObjectsSizeQuery.Where("db_bucket_id", bucketID)
	}
	err = totalUnfinishedObjectsSizeQuery.Scan(&totalUnfinishedObjectsSize).Error
	if err != nil {
		return api.ObjectsStatsResponse{}, err
	}

	var totalSectors int64
	totalSectorsQuery := db.
		Table("slabs sla").
		Select("COALESCE(SUM(total_shards), 0)").
		Where("db_buffered_slab_id IS NULL")

	if opts.Bucket != "" {
		totalSectorsQuery = totalSectorsQuery.Where(`
			EXISTS (
				SELECT 1 FROM slices sli
				INNER JOIN objects o ON o.id = sli.db_object_id AND o.db_bucket_id = ?
				WHERE sli.db_slab_id = sla.id
			)
		`, bucketID)
	}
	err = totalSectorsQuery.Scan(&totalSectors).Error
	if err != nil {
		return api.ObjectsStatsResponse{}, err
	}

	var totalUploaded int64
	err = db.
		Model(&dbContract{}).
		Select("COALESCE(SUM(size), 0)").
		Scan(&totalUploaded).
		Error
	if err != nil {
		return api.ObjectsStatsResponse{}, err
	}

	return api.ObjectsStatsResponse{
		MinHealth:                  objInfo.MinHealth,
		NumObjects:                 objInfo.NumObjects,
		NumUnfinishedObjects:       unfinishedObjects,
		TotalUnfinishedObjectsSize: totalUnfinishedObjectsSize,
		TotalObjectsSize:           objInfo.TotalObjectsSize,
		TotalSectorsSize:           uint64(totalSectors) * rhpv2.SectorSize,
		TotalUploadedSize:          uint64(totalUploaded),
	}, nil
}

func (s *SQLStore) SlabBuffers(ctx context.Context) ([]api.SlabBuffer, error) {
	// Slab buffer info from the database.
	var bufferedSlabs []dbBufferedSlab
	err := s.db.Model(&dbBufferedSlab{}).
		Joins("DBSlab").
		Joins("DBSlab.DBContractSet").
		Find(&bufferedSlabs).
		Error
	if err != nil {
		return nil, err
	}
	// Translate buffers to contract set.
	fileNameToContractSet := make(map[string]string)
	for _, slab := range bufferedSlabs {
		fileNameToContractSet[slab.Filename] = slab.DBSlab.DBContractSet.Name
	}
	// Fetch in-memory buffer info and fill in contract set name.
	buffers := s.slabBufferMgr.SlabBuffers()
	for i := range buffers {
		buffers[i].ContractSet = fileNameToContractSet[buffers[i].Filename]
	}
	return buffers, nil
}

func (s *SQLStore) AddContract(ctx context.Context, c rhpv2.ContractRevision, contractPrice, totalCost types.Currency, startHeight uint64, state string) (_ api.ContractMetadata, err error) {
	var cs contractState
	if err := cs.LoadString(state); err != nil {
		return api.ContractMetadata{}, err
	}
	var added dbContract
	if err = s.retryTransaction(ctx, func(tx *gorm.DB) error {
		added, err = addContract(tx, c, contractPrice, totalCost, startHeight, types.FileContractID{}, cs)
		return err
	}); err != nil {
		return
	}

	s.addKnownContract(types.FileContractID(added.FCID))
	return added.convert(), nil
}

func (s *SQLStore) Contracts(ctx context.Context, opts api.ContractsOpts) ([]api.ContractMetadata, error) {
	db := s.db.WithContext(ctx)

	// helper to check whether a contract set exists
	hasContractSet := func() error {
		if opts.ContractSet == "" {
			return nil
		}
		err := db.Where("name", opts.ContractSet).Take(&dbContractSet{}).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return api.ErrContractSetNotFound
		}
		return err
	}

	// fetch all contracts, their hosts and the contract set name
	var rows []struct {
		Contract dbContract `gorm:"embedded"`
		Host     dbHost     `gorm:"embedded"`
		Name     string
	}
	tx := db
	if opts.ContractSet == "" {
		// no filter, use all contracts
		tx = tx.Table("contracts")
	} else {
		// filter contracts by contract set first
		tx = tx.Table("(?) contracts", db.Model(&dbContract{}).
			Select("contracts.*").
			Joins("INNER JOIN hosts h ON h.id = contracts.host_id").
			Joins("INNER JOIN contract_set_contracts csc ON csc.db_contract_id = contracts.id").
			Joins("INNER JOIN contract_sets cs ON cs.id = csc.db_contract_set_id AND cs.name = ?", opts.ContractSet))
	}
	err := tx.
		Select("contracts.*, h.*, cs.name as Name").
		Joins("INNER JOIN hosts h ON h.id = contracts.host_id").
		Joins("LEFT JOIN contract_set_contracts csc ON csc.db_contract_id = contracts.id").
		Joins("LEFT JOIN contract_sets cs ON cs.id = csc.db_contract_set_id").
		Order("contracts.id ASC").
		Scan(&rows).
		Error
	if err != nil {
		return nil, err
	} else if len(rows) == 0 {
		return nil, hasContractSet()
	}

	// merge 'Host', 'Name' and 'Contract' into dbContracts
	var dbContracts []dbContract
	for i := range rows {
		dbContract := rows[i].Contract
		dbContract.Host = rows[i].Host
		if rows[i].Name != "" {
			dbContract.ContractSets = append(dbContract.ContractSets, dbContractSet{Name: rows[i].Name})
		}
		dbContracts = append(dbContracts, dbContract)
	}

	// merge contract sets
	var contracts []api.ContractMetadata
	current, dbContracts := dbContracts[0], dbContracts[1:]
	for {
		if len(dbContracts) == 0 {
			contracts = append(contracts, current.convert())
			break
		} else if current.ID != dbContracts[0].ID {
			contracts = append(contracts, current.convert())
		} else if len(dbContracts[0].ContractSets) > 0 {
			current.ContractSets = append(current.ContractSets, dbContracts[0].ContractSets...)
		}
		current, dbContracts = dbContracts[0], dbContracts[1:]
	}

	// if no contracts are left, check if the set existed in the first place
	if len(contracts) == 0 {
		return nil, hasContractSet()
	}
	return contracts, nil
}

// AddRenewedContract adds a new contract which was created as the result of a renewal to the store.
// The old contract specified as 'renewedFrom' will be deleted from the active
// contracts and moved to the archive. Both new and old contract will be linked
// to each other through the RenewedFrom and RenewedTo fields respectively.
func (s *SQLStore) AddRenewedContract(ctx context.Context, c rhpv2.ContractRevision, contractPrice, totalCost types.Currency, startHeight uint64, renewedFrom types.FileContractID, state string) (api.ContractMetadata, error) {
	var cs contractState
	if err := cs.LoadString(state); err != nil {
		return api.ContractMetadata{}, err
	}
	var renewed dbContract
	if err := s.retryTransaction(ctx, func(tx *gorm.DB) error {
		// Fetch contract we renew from.
		oldContract, err := contract(tx, fileContractID(renewedFrom))
		if err != nil {
			return err
		}

		// Create copy in archive.
		err = tx.Create(&dbArchivedContract{
			Host:      publicKey(oldContract.Host.PublicKey),
			Reason:    api.ContractArchivalReasonRenewed,
			RenewedTo: fileContractID(c.ID()),

			ContractCommon: oldContract.ContractCommon,
		}).Error
		if err != nil {
			return err
		}

		// Overwrite the old contract with the new one.
		newContract := newContract(oldContract.HostID, c.ID(), renewedFrom, contractPrice, totalCost, startHeight, c.Revision.WindowStart, c.Revision.WindowEnd, oldContract.Size, cs)
		newContract.Model = oldContract.Model
		newContract.CreatedAt = time.Now()
		err = tx.Save(&newContract).Error
		if err != nil {
			return err
		}

		s.addKnownContract(c.ID())
		renewed = newContract
		return nil
	}); err != nil {
		return api.ContractMetadata{}, err
	}

	return renewed.convert(), nil
}

func (s *SQLStore) AncestorContracts(ctx context.Context, id types.FileContractID, startHeight uint64) ([]api.ArchivedContract, error) {
	var ancestors []dbArchivedContract
	err := s.db.WithContext(ctx).Raw("WITH RECURSIVE ancestors AS (SELECT * FROM archived_contracts WHERE renewed_to = ? UNION ALL SELECT archived_contracts.* FROM ancestors, archived_contracts WHERE archived_contracts.renewed_to = ancestors.fcid) SELECT * FROM ancestors WHERE start_height >= ?", fileContractID(id), startHeight).
		Scan(&ancestors).
		Error
	if err != nil {
		return nil, err
	}
	contracts := make([]api.ArchivedContract, len(ancestors))
	for i, ancestor := range ancestors {
		contracts[i] = ancestor.convert()
	}
	return contracts, nil
}

func (s *SQLStore) ArchiveContract(ctx context.Context, id types.FileContractID, reason string) error {
	return s.ArchiveContracts(ctx, map[types.FileContractID]string{id: reason})
}

func (s *SQLStore) ArchiveContracts(ctx context.Context, toArchive map[types.FileContractID]string) error {
	// fetch ids
	var ids []types.FileContractID
	for id := range toArchive {
		ids = append(ids, id)
	}

	// fetch contracts
	cs, err := contracts(s.db, ids)
	if err != nil {
		return err
	}

	// archive them
	if err := s.retryTransaction(ctx, func(tx *gorm.DB) error {
		return archiveContracts(tx, cs, toArchive)
	}); err != nil {
		return err
	}

	return nil
}

func (s *SQLStore) ArchiveAllContracts(ctx context.Context, reason string) error {
	// fetch contract ids
	var fcids []fileContractID
	if err := s.db.
		WithContext(ctx).
		Model(&dbContract{}).
		Pluck("fcid", &fcids).
		Error; err != nil {
		return err
	}

	// create map
	toArchive := make(map[types.FileContractID]string)
	for _, fcid := range fcids {
		toArchive[types.FileContractID(fcid)] = reason
	}

	return s.ArchiveContracts(ctx, toArchive)
}

func (s *SQLStore) Contract(ctx context.Context, id types.FileContractID) (api.ContractMetadata, error) {
	contract, err := s.contract(ctx, fileContractID(id))
	if err != nil {
		return api.ContractMetadata{}, err
	}
	return contract.convert(), nil
}

func (s *SQLStore) ContractRoots(ctx context.Context, id types.FileContractID) (roots []types.Hash256, err error) {
	if !s.isKnownContract(id) {
		return nil, api.ErrContractNotFound
	}

	var dbRoots []hash256
	if err = s.db.
		WithContext(ctx).
		Raw(`
SELECT sec.root
FROM contracts c
INNER JOIN contract_sectors cs ON cs.db_contract_id = c.id
INNER JOIN sectors sec ON cs.db_sector_id = sec.id
WHERE c.fcid = ?
`, fileContractID(id)).
		Scan(&dbRoots).
		Error; err == nil {
		for _, r := range dbRoots {
			roots = append(roots, *(*types.Hash256)(&r))
		}
	}
	return
}

func (s *SQLStore) ContractSets(ctx context.Context) ([]string, error) {
	var sets []string
	err := s.db.WithContext(ctx).Raw("SELECT name FROM contract_sets").
		Scan(&sets).
		Error
	return sets, err
}

func (s *SQLStore) ContractSizes(ctx context.Context) (map[types.FileContractID]api.ContractSize, error) {
	type size struct {
		Fcid     fileContractID `json:"fcid"`
		Size     uint64         `json:"size"`
		Prunable uint64         `json:"prunable"`
	}

	var nullContracts []size
	var dataContracts []size
	if err := s.retryTransaction(ctx, func(tx *gorm.DB) error {
		// first, we fetch all contracts without sectors and consider their
		// entire size as prunable
		if err := tx.
			Raw(`
SELECT c.fcid, c.size, c.size as prunable FROM contracts c WHERE NOT EXISTS (SELECT 1 FROM contract_sectors cs WHERE cs.db_contract_id = c.id)`).
			Scan(&nullContracts).
			Error; err != nil {
			return err
		}

		// second, we fetch how much data can be pruned from all contracts that
		// do have sectors, we take a two-step approach because it allows us to
		// use an INNER JOIN on contract_sectors, drastically improving the
		// performance of the query
		return tx.
			Raw(`
SELECT fcid, contract_size as size, CASE WHEN contract_size > sector_size THEN contract_size - sector_size ELSE 0 END as prunable FROM (
SELECT c.fcid, MAX(c.size) as contract_size, COUNT(cs.db_sector_id) * ? as sector_size FROM contracts c INNER JOIN contract_sectors cs ON cs.db_contract_id = c.id GROUP BY c.fcid
) i`, rhpv2.SectorSize).
			Scan(&dataContracts).
			Error
	}); err != nil {
		return nil, err
	}

	sizes := make(map[types.FileContractID]api.ContractSize)
	for _, row := range append(nullContracts, dataContracts...) {
		if types.FileContractID(row.Fcid) == (types.FileContractID{}) {
			return nil, errors.New("invalid file contract id")
		}
		sizes[types.FileContractID(row.Fcid)] = api.ContractSize{
			Size:     row.Size,
			Prunable: row.Prunable,
		}
	}
	return sizes, nil
}

func (s *SQLStore) ContractSize(ctx context.Context, id types.FileContractID) (api.ContractSize, error) {
	if !s.isKnownContract(id) {
		return api.ContractSize{}, api.ErrContractNotFound
	}

	var size struct {
		Size     uint64 `json:"size"`
		Prunable uint64 `json:"prunable"`
	}

	if err := s.db.
		WithContext(ctx).
		Raw(`
SELECT contract_size as size, CASE WHEN contract_size > sector_size THEN contract_size - sector_size ELSE 0 END as prunable FROM (
SELECT MAX(c.size) as contract_size, COUNT(cs.db_sector_id) * ? as sector_size FROM contracts c LEFT JOIN contract_sectors cs ON cs.db_contract_id = c.id WHERE c.fcid = ?
) i
`, rhpv2.SectorSize, fileContractID(id)).
		Take(&size).
		Error; err != nil {
		return api.ContractSize{}, err
	}

	return api.ContractSize{
		Size:     size.Size,
		Prunable: size.Prunable,
	}, nil
}

func (s *SQLStore) SetContractSet(ctx context.Context, name string, contractIds []types.FileContractID) error {
	var wantedIds []fileContractID
	wanted := make(map[fileContractID]struct{})
	for _, fcid := range contractIds {
		wantedIds = append(wantedIds, fileContractID(fcid))
		wanted[fileContractID(fcid)] = struct{}{}
	}

	var diff []fileContractID
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
				diff = append(diff, contract.FCID)
			}
			delete(wanted, contract.FCID)
		}

		// add additions to the diff
		for fcid := range wanted {
			diff = append(diff, fcid)
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
	return s.db.
		WithContext(ctx).
		Where(dbContractSet{Name: name}).
		Delete(&dbContractSet{}).
		Error
}

func (s *SQLStore) RenewedContract(ctx context.Context, renewedFrom types.FileContractID) (_ api.ContractMetadata, err error) {
	var contract dbContract

	err = s.db.
		WithContext(ctx).
		Where(&dbContract{ContractCommon: ContractCommon{RenewedFrom: fileContractID(renewedFrom)}}).
		Joins("Host").
		Take(&contract).
		Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		err = api.ErrContractNotFound
		return
	}

	return contract.convert(), nil
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

func replaceAnyValue(query string) string {
	re := regexp.MustCompile(`ANY_VALUE\((.*?)\)`)
	return re.ReplaceAllString(query, "$1")
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

	indexHint := ""
	if !isSQLite(s.db) {
		indexHint = "USE INDEX (idx_object_bucket, idx_objects_created_at)"
	}

	onameExpr := fmt.Sprintf("CASE INSTR(SUBSTR(object_id, ?), '/') WHEN 0 THEN %s ELSE %s END",
		sqlConcat(s.db, "?", "SUBSTR(object_id, ?)"),
		sqlConcat(s.db, "?", "substr(SUBSTR(object_id, ?), 1, INSTR(SUBSTR(object_id, ?), '/'))"),
	)

	// build objects query & parameters
	objectsQuery := fmt.Sprintf(`
SELECT ETag, ModTime, oname as Name, Size, Health, MimeType
FROM (
	SELECT
	ANY_VALUE(etag) AS ETag,
	MAX(objects.created_at) AS ModTime,
	%s AS oname,
	SUM(size) AS Size,
	MIN(health) as Health,
	ANY_VALUE(mime_type) as MimeType
	FROM objects %s
	INNER JOIN buckets b ON objects.db_bucket_id = b.id
	WHERE object_id LIKE ? AND SUBSTR(object_id, 1, ?) = ? AND b.name = ? AND SUBSTR(%s, 1, ?) = ? AND %s != ?
	GROUP BY oname
) baseQuery
`,
		onameExpr,
		indexHint,
		onameExpr,
		onameExpr,
	)

	if isSQLite(s.db) {
		objectsQuery = replaceAnyValue(objectsQuery)
	}

	objectsQueryParams := []interface{}{
		utf8.RuneCountInString(path) + 1,       // onameExpr
		path, utf8.RuneCountInString(path) + 1, // onameExpr
		path, utf8.RuneCountInString(path) + 1, utf8.RuneCountInString(path) + 1, // onameExpr

		path + "%",

		utf8.RuneCountInString(path), // WHERE SUBSTR(%s, 1, ?) = ? AND %s != ? AND b.name = ?
		path,                         // WHERE SUBSTR(%s, 1, ?) = ? AND %s != ? AND b.name = ?
		bucket,                       // WHERE SUBSTR(%s, 1, ?) = ? AND %s != ? AND b.name = ?

		utf8.RuneCountInString(path) + 1,       // onameExpr
		path, utf8.RuneCountInString(path) + 1, // onameExpr
		path, utf8.RuneCountInString(path) + 1, utf8.RuneCountInString(path) + 1, // onameExpr

		utf8.RuneCountInString(path + prefix),  // WHERE SUBSTR(%s, 1, ?) = ? AND %s != ? AND b.name = ?
		path + prefix,                          // WHERE SUBSTR(%s, 1, ?) = ? AND %s != ? AND b.name = ?
		utf8.RuneCountInString(path) + 1,       // onameExpr
		path, utf8.RuneCountInString(path) + 1, // onameExpr
		path, utf8.RuneCountInString(path) + 1, utf8.RuneCountInString(path) + 1, // onameExpr
		path, // WHERE SUBSTR(%s, 1, ?) = ? AND %s != ? AND b.name = ?
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
				Raw(fmt.Sprintf(`SELECT Health FROM (%s WHERE oname >= ? ORDER BY oname LIMIT 1) as n`, objectsQuery), append(objectsQueryParams, marker)...).
				Scan(&markerHealth).
				Error; err != nil {
				return
			}

			if sortDir == api.ObjectSortDirAsc {
				markerExpr = "(Health > ? OR (Health = ? AND Name > ?))"
				markerParams = []interface{}{markerHealth, markerHealth, marker}
			} else {
				markerExpr = "(Health = ? AND Name > ?) OR Health < ?"
				markerParams = []interface{}{markerHealth, marker, markerHealth}
			}
		case api.ObjectSortBySize:
			var markerSize float64
			if err = s.db.
				WithContext(ctx).
				Raw(fmt.Sprintf(`SELECT Size FROM (%s WHERE oname >= ? ORDER BY oname LIMIT 1) as n`, objectsQuery), append(objectsQueryParams, marker)...).
				Scan(&markerSize).
				Error; err != nil {
				return
			}

			if sortDir == api.ObjectSortDirAsc {
				markerExpr = "(Size > ? OR (Size = ? AND Name > ?))"
				markerParams = []interface{}{markerSize, markerSize, marker}
			} else {
				markerExpr = "(Size = ? AND Name > ?) OR Size < ?"
				markerParams = []interface{}{markerSize, marker, markerSize}
			}
		case api.ObjectSortByName:
			if sortDir == api.ObjectSortDirAsc {
				markerExpr = "Name > ?"
			} else {
				markerExpr = "Name < ?"
			}
			markerParams = []interface{}{marker}
		default:
			panic("unhandled sortBy") // developer error
		}
	}

	// build order clause
	orderByClause := fmt.Sprintf("%s %s", sortBy, sortDir)
	if sortBy != api.ObjectSortByName {
		orderByClause += ", Name"
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
	err = s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
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
				Where("fcid = ?", fileContractID(fcid)).
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
			return tx.Model(&contract).Updates(updates).Error
		})
		if err != nil {
			return err
		}
	}
	if err := s.RecordContractMetric(ctx, metrics...); err != nil {
		s.logger.Errorw("failed to record contract metrics", zap.Error(err))
	}
	return nil
}

func (s *SQLStore) addKnownContract(fcid types.FileContractID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.knownContracts[fcid] = struct{}{}
}

func (s *SQLStore) isKnownContract(fcid types.FileContractID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, found := s.knownContracts[fcid]
	return found
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
	return s.retryTransaction(ctx, func(tx *gorm.DB) error {
		if force {
			// delete potentially existing object at destination
			if _, err := s.deleteObject(tx, bucket, keyNew); err != nil {
				return fmt.Errorf("RenameObject: failed to delete object: %w", err)
			}
		}
		tx = tx.Exec(`UPDATE objects SET object_id = ? WHERE object_id = ? AND ?`, keyNew, keyOld, sqlWhereBucket("objects", bucket))
		if tx.Error != nil &&
			(strings.Contains(tx.Error.Error(), "UNIQUE constraint failed") || strings.Contains(tx.Error.Error(), "Duplicate entry")) {
			return api.ErrObjectExists
		} else if tx.Error != nil {
			return tx.Error
		}
		if tx.RowsAffected == 0 {
			return fmt.Errorf("%w: key %v", api.ErrObjectNotFound, keyOld)
		}
		return nil
	})
}

func (s *SQLStore) RenameObjects(ctx context.Context, bucket, prefixOld, prefixNew string, force bool) error {
	return s.retryTransaction(ctx, func(tx *gorm.DB) error {
		if force {
			// delete potentially existing objects at destination
			inner := tx.Raw("SELECT ? FROM objects WHERE object_id LIKE ? AND SUBSTR(object_id, 1, ?) = ? AND ?",
				gorm.Expr(sqlConcat(tx, "?", "SUBSTR(object_id, ?)")), prefixNew,
				utf8.RuneCountInString(prefixOld)+1, prefixOld+"%",
				utf8.RuneCountInString(prefixOld), prefixOld, sqlWhereBucket("objects", bucket))

			if !isSQLite(tx) {
				inner = tx.Raw("SELECT * FROM (?) as i", inner)
			}
			resp := tx.Model(&dbObject{}).
				Where("object_id IN (?)", inner).
				Delete(&dbObject{})
			if err := resp.Error; err != nil {
				return err
			}
		}
		tx = tx.Exec("UPDATE objects SET object_id = "+sqlConcat(tx, "?", "SUBSTR(object_id, ?)")+" WHERE object_id LIKE ? AND SUBSTR(object_id, 1, ?) = ? AND ?",
			prefixNew, utf8.RuneCountInString(prefixOld)+1, prefixOld+"%", utf8.RuneCountInString(prefixOld), prefixOld, sqlWhereBucket("objects", bucket))
		if tx.Error != nil &&
			(strings.Contains(tx.Error.Error(), "UNIQUE constraint failed") || strings.Contains(tx.Error.Error(), "Duplicate entry")) {
			return api.ErrObjectExists
		} else if tx.Error != nil {
			return tx.Error
		}
		if tx.RowsAffected == 0 {
			return fmt.Errorf("%w: prefix %v", api.ErrObjectNotFound, prefixOld)
		}
		return nil
	})
}

func (s *SQLStore) FetchPartialSlab(ctx context.Context, ec object.EncryptionKey, offset, length uint32) ([]byte, error) {
	return s.slabBufferMgr.FetchPartialSlab(ctx, ec, offset, length)
}

func (s *SQLStore) AddPartialSlab(ctx context.Context, data []byte, minShards, totalShards uint8, contractSet string) ([]object.SlabSlice, int64, error) {
	var contractSetID uint
	if err := s.db.Raw("SELECT id FROM contract_sets WHERE name = ?", contractSet).Scan(&contractSetID).Error; err != nil {
		return nil, 0, err
	}
	return s.slabBufferMgr.AddPartialSlab(ctx, data, minShards, totalShards, contractSetID)
}

func (s *SQLStore) CopyObject(ctx context.Context, srcBucket, dstBucket, srcPath, dstPath, mimeType string, metadata api.ObjectUserMetadata) (om api.ObjectMetadata, err error) {
	err = s.retryTransaction(ctx, func(tx *gorm.DB) error {
		if srcBucket != dstBucket || srcPath != dstPath {
			_, err = s.deleteObject(tx, dstBucket, dstPath)
			if err != nil {
				return fmt.Errorf("CopyObject: failed to delete object: %w", err)
			}
		}

		var srcObj dbObject
		err = tx.Where("objects.object_id = ? AND DBBucket.name = ?", srcPath, srcBucket).
			Joins("DBBucket").
			Take(&srcObj).
			Error
		if err != nil {
			return fmt.Errorf("failed to fetch src object: %w", err)
		}

		if srcBucket == dstBucket && srcPath == dstPath {
			// No copying is happening. We just update the metadata on the src
			// object.
			srcObj.MimeType = mimeType
			om = newObjectMetadata(
				srcObj.ObjectID,
				srcObj.Etag,
				srcObj.MimeType,
				srcObj.Health,
				srcObj.CreatedAt,
				srcObj.Size,
			)
			if err := s.updateUserMetadata(tx, srcObj.ID, metadata); err != nil {
				return fmt.Errorf("failed to update user metadata: %w", err)
			}
			return tx.Save(&srcObj).Error
		}

		var srcSlices []dbSlice
		err = tx.Where("db_object_id = ?", srcObj.ID).
			Find(&srcSlices).
			Error
		if err != nil {
			return fmt.Errorf("failed to fetch src slices: %w", err)
		}
		for i := range srcSlices {
			srcSlices[i].Model = Model{}  // clear model
			srcSlices[i].DBObjectID = nil // clear object id
		}

		var bucket dbBucket
		err = tx.Where("name = ?", dstBucket).
			Take(&bucket).
			Error
		if err != nil {
			return fmt.Errorf("failed to fetch dst bucket: %w", err)
		}

		dstObj := srcObj
		dstObj.Model = Model{}        // clear model
		dstObj.DBBucket = bucket      // set dst bucket
		dstObj.ObjectID = dstPath     // set dst path
		dstObj.DBBucketID = bucket.ID // set dst bucket id
		dstObj.Slabs = srcSlices      // set slices
		if mimeType != "" {
			dstObj.MimeType = mimeType // override mime type
		}
		if err := tx.Create(&dstObj).Error; err != nil {
			return fmt.Errorf("failed to create copy of object: %w", err)
		}

		if err := s.createUserMetadata(tx, dstObj.ID, metadata); err != nil {
			return fmt.Errorf("failed to create object metadata: %w", err)
		}

		om = newObjectMetadata(
			dstObj.ObjectID,
			dstObj.Etag,
			dstObj.MimeType,
			dstObj.Health,
			dstObj.CreatedAt,
			dstObj.Size,
		)
		return nil
	})
	return
}

func (s *SQLStore) DeleteHostSector(ctx context.Context, hk types.PublicKey, root types.Hash256) (int, error) {
	var deletedSectors int
	err := s.retryTransaction(ctx, func(tx *gorm.DB) error {
		// Fetch contract_sectors to delete.
		var sectors []dbContractSector
		err := tx.Raw(`
			SELECT contract_sectors.*
			FROM contract_sectors
			INNER JOIN sectors s ON s.id = contract_sectors.db_sector_id
			INNER JOIN contracts c ON c.id = contract_sectors.db_contract_id
			INNER JOIN hosts h ON h.id = c.host_id
			WHERE s.root = ? AND h.public_key = ?
			`, root[:], publicKey(hk)).
			Scan(&sectors).
			Error
		if err != nil {
			return fmt.Errorf("failed to fetch contract sectors for deletion: %w", err)
		}

		if len(sectors) > 0 {
			// Update the affected slabs.
			var sectorIDs []uint
			uniqueIDs := make(map[uint]struct{})
			for _, s := range sectors {
				if _, exists := uniqueIDs[s.DBSectorID]; !exists {
					uniqueIDs[s.DBSectorID] = struct{}{}
					sectorIDs = append(sectorIDs, s.DBSectorID)
				}
			}
			err = tx.Exec("UPDATE slabs SET health_valid_until = ? WHERE id IN (SELECT db_slab_id FROM sectors WHERE id IN (?))", time.Now().Unix(), sectorIDs).Error
			if err != nil {
				return fmt.Errorf("failed to invalidate slab health: %w", err)
			}

			// Delete contract_sectors.
			res := tx.Delete(&sectors)
			if err := res.Error; err != nil {
				return fmt.Errorf("failed to delete contract sectors: %w", err)
			} else if res.RowsAffected != int64(len(sectors)) {
				return fmt.Errorf("expected %v affected rows but got %v", len(sectors), res.RowsAffected)
			}
			deletedSectors = len(sectors)

			// Increment the host's lostSectors by the number of lost sectors.
			if err := tx.Exec("UPDATE hosts SET lost_sectors = lost_sectors + ? WHERE public_key = ?", len(sectors), publicKey(hk)).Error; err != nil {
				return fmt.Errorf("failed to increment lost sectors: %w", err)
			}
		}

		// Fetch the sector and update the latest_host field if the host for
		// which we remove the sector is the latest_host.
		var sector dbSector
		err = tx.Where("root", root[:]).
			Preload("Contracts.Host").
			Find(&sector).
			Error
		if err != nil {
			return fmt.Errorf("failed to fetch sectors: %w", err)
		}
		if sector.LatestHost == publicKey(hk) {
			if len(sector.Contracts) == 0 {
				sector.LatestHost = publicKey{} // no more hosts
			} else {
				sector.LatestHost = sector.Contracts[len(sector.Contracts)-1].Host.PublicKey // most recent contract
			}
			return tx.Save(sector).Error
		}
		return nil
	})
	return deletedSectors, err
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

	// collect all used contracts
	usedContracts := o.Contracts()

	// UpdateObject is ACID.
	return s.retryTransaction(ctx, func(tx *gorm.DB) error {
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
		_, err := s.deleteObject(tx, bucket, path)
		if err != nil {
			return fmt.Errorf("UpdateObject: failed to delete object: %w", err)
		}

		// Insert a new object.
		objKey, err := o.Key.MarshalBinary()
		if err != nil {
			return fmt.Errorf("failed to marshal object key: %w", err)
		}
		// fetch bucket id
		var bucketID uint
		err = s.db.Table("(SELECT id from buckets WHERE buckets.name = ?) bucket_id", bucket).
			Take(&bucketID).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("bucket %v not found: %w", bucket, api.ErrBucketNotFound)
		} else if err != nil {
			return fmt.Errorf("failed to fetch bucket id: %w", err)
		}

		obj := dbObject{
			DBBucketID: bucketID,
			ObjectID:   path,
			Key:        objKey,
			Size:       o.TotalSize(),
			MimeType:   mimeType,
			Etag:       eTag,
		}
		err = tx.Create(&obj).Error
		if err != nil {
			return fmt.Errorf("failed to create object: %w", err)
		}

		// Fetch contract set.
		var cs dbContractSet
		if err := tx.Take(&cs, "name = ?", contractSet).Error; err != nil {
			return fmt.Errorf("contract set %v not found: %w", contractSet, err)
		}

		// Fetch the used contracts.
		contracts, err := fetchUsedContracts(tx, usedContracts)
		if err != nil {
			return fmt.Errorf("failed to fetch used contracts: %w", err)
		}

		// Create all slices. This also creates any missing slabs or sectors.
		if err := s.createSlices(tx, &obj.ID, nil, cs.ID, contracts, o.Slabs); err != nil {
			return fmt.Errorf("failed to create slices: %w", err)
		}

		// Create all user metadata.
		if err := s.createUserMetadata(tx, obj.ID, metadata); err != nil {
			return fmt.Errorf("failed to create user metadata: %w", err)
		}

		return nil
	})
}

func (s *SQLStore) RemoveObject(ctx context.Context, bucket, path string) error {
	var rowsAffected int64
	var err error
	err = s.retryTransaction(ctx, func(tx *gorm.DB) error {
		rowsAffected, err = s.deleteObject(tx, bucket, path)
		if err != nil {
			return fmt.Errorf("RemoveObject: failed to delete object: %w", err)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		return fmt.Errorf("%w: key: %s", api.ErrObjectNotFound, path)
	}
	return nil
}

func (s *SQLStore) RemoveObjects(ctx context.Context, bucket, prefix string) error {
	var rowsAffected int64
	var err error
	rowsAffected, err = s.deleteObjects(ctx, bucket, prefix)
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		return fmt.Errorf("%w: prefix: %s", api.ErrObjectNotFound, prefix)
	}
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

	// extract the slab key
	key, err := s.Key.MarshalBinary()
	if err != nil {
		return err
	}

	// collect all used contracts
	usedContracts := s.Contracts()

	// Update slab.
	return ss.retryTransaction(ctx, func(tx *gorm.DB) (err error) {
		// update slab
		if err := tx.Model(&dbSlab{}).
			Where("key", key).
			Updates(map[string]interface{}{
				"db_contract_set_id": gorm.Expr("(SELECT id FROM contract_sets WHERE name = ?)", contractSet),
				"health_valid_until": time.Now().Unix(),
				"health":             1,
			}).
			Error; err != nil {
			return err
		}

		// find all used contracts
		contracts, err := fetchUsedContracts(tx, usedContracts)
		if err != nil {
			return err
		}

		// find existing slab
		var slab dbSlab
		if err = tx.
			Where(&dbSlab{Key: key}).
			Preload("Shards").
			Take(&slab).
			Error; err == gorm.ErrRecordNotFound {
			return fmt.Errorf("slab with key '%s' not found: %w", string(key), err)
		} else if err != nil {
			return err
		}

		// make sure the number of shards doesn't change.
		// NOTE: check both the slice as well as the TotalShards field to be
		// safe.
		if len(s.Shards) != int(slab.TotalShards) {
			return fmt.Errorf("%w: expected %v shards (TotalShards) but got %v", errInvalidNumberOfShards, slab.TotalShards, len(s.Shards))
		} else if len(s.Shards) != len(slab.Shards) {
			return fmt.Errorf("%w: expected %v shards (Shards) but got %v", errInvalidNumberOfShards, len(slab.Shards), len(s.Shards))
		}

		// make sure the roots stay the same.
		for i, shard := range s.Shards {
			if shard.Root != types.Hash256(slab.Shards[i].Root) {
				return fmt.Errorf("%w: shard %v has changed root from %v to %v", errShardRootChanged, i, slab.Shards[i].Root, shard.Root[:])
			}
		}

		// prepare sectors to update
		sectors := make([]dbSector, len(s.Shards))
		for i := range s.Shards {
			sectors[i] = dbSector{
				DBSlabID:   slab.ID,
				SlabIndex:  i + 1,
				LatestHost: publicKey(s.Shards[i].LatestHost),
				Root:       s.Shards[i].Root[:],
			}
		}

		// ensure the sectors exists
		sectorIDs, err := upsertSectors(tx, sectors)
		if err != nil {
			return fmt.Errorf("failed to create sector: %w", err)
		}

		// build contract <-> sector links
		var contractSectors []dbContractSector
		for i, shard := range s.Shards {
			sectorID := sectorIDs[i]

			// ensure the associations are updated
			for _, fcids := range shard.Contracts {
				for _, fcid := range fcids {
					if _, ok := contracts[fcid]; ok {
						contractSectors = append(contractSectors, dbContractSector{
							DBSectorID:   sectorID,
							DBContractID: contracts[fcid].ID,
						})
					}
				}
			}
		}

		// if there are no associations we are done
		if len(contractSectors) == 0 {
			return nil
		}

		// create associations
		if err := tx.Table("contract_sectors").
			Clauses(clause.OnConflict{
				DoNothing: true,
			}).
			Create(&contractSectors).Error; err != nil {
			return err
		}
		return nil
	})
}

func (s *SQLStore) RefreshHealth(ctx context.Context) error {
	var nSlabs int64
	if err := s.db.Model(&dbSlab{}).Count(&nSlabs).Error; err != nil {
		return err
	}
	if nSlabs == 0 {
		return nil // nothing to do
	}

	// Update slab health in batches.
	now := time.Now()

	// build health query
	healthQuery := s.db.Raw(`
SELECT slabs.id, slabs.db_contract_set_id, CASE WHEN (slabs.min_shards = slabs.total_shards)
THEN
    CASE WHEN (COUNT(DISTINCT(CASE WHEN cs.name IS NULL THEN NULL ELSE c.host_id END)) < slabs.min_shards)
    THEN -1
    ELSE 1
    END
ELSE (CAST(COUNT(DISTINCT(CASE WHEN cs.name IS NULL THEN NULL ELSE c.host_id END)) AS FLOAT) - CAST(slabs.min_shards AS FLOAT)) / Cast(slabs.total_shards - slabs.min_shards AS FLOAT)
END AS health
FROM slabs
INNER JOIN sectors s ON s.db_slab_id = slabs.id
LEFT JOIN contract_sectors se ON s.id = se.db_sector_id
LEFT JOIN contracts c ON se.db_contract_id = c.id
LEFT JOIN contract_set_contracts csc ON csc.db_contract_id = c.id AND csc.db_contract_set_id = slabs.db_contract_set_id
LEFT JOIN contract_sets cs ON cs.id = csc.db_contract_set_id
WHERE slabs.health_valid_until <= ?
GROUP BY slabs.id
LIMIT ?
`, now.Unix(), refreshHealthBatchSize)

	for {
		var rowsAffected int64
		err := s.retryTransaction(ctx, func(tx *gorm.DB) error {
			var res *gorm.DB
			if isSQLite(s.db) {
				res = tx.Exec("UPDATE slabs SET health = inner.health, health_valid_until = (?) FROM (?) AS inner WHERE slabs.id=inner.id", sqlRandomTimestamp(s.db, now, refreshHealthMinHealthValidity, refreshHealthMaxHealthValidity), healthQuery)
			} else {
				res = tx.Exec("UPDATE slabs sla INNER JOIN (?) h ON sla.id = h.id SET sla.health = h.health, health_valid_until = (?)", healthQuery, sqlRandomTimestamp(s.db, now, refreshHealthMinHealthValidity, refreshHealthMaxHealthValidity))
			}
			if res.Error != nil {
				return res.Error
			}
			rowsAffected = res.RowsAffected

			// Update the health of objects with outdated health.
			return tx.Exec(`
UPDATE objects SET health = (
	SELECT MIN(slabs.health)
	FROM slabs
	INNER JOIN slices ON slices.db_slab_id = slabs.id AND slices.db_object_id = objects.id
) WHERE health != (
	SELECT MIN(slabs.health)
	FROM slabs
	INNER JOIN slices ON slices.db_slab_id = slabs.id AND slices.db_object_id = objects.id
)`).Error
		})
		if err != nil {
			return err
		} else if rowsAffected < refreshHealthBatchSize {
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

func (s *SQLStore) createUserMetadata(tx *gorm.DB, objID uint, metadata api.ObjectUserMetadata) error {
	entities := make([]*dbObjectUserMetadata, 0, len(metadata))
	for k, v := range metadata {
		metadata := &dbObjectUserMetadata{
			DBObjectID: &objID,
			Key:        k,
			Value:      v,
		}
		entities = append(entities, metadata)
	}
	return tx.CreateInBatches(&entities, 1000).Error
}

func (s *SQLStore) createMultipartMetadata(tx *gorm.DB, multipartUploadID uint, metadata api.ObjectUserMetadata) error {
	entities := make([]*dbObjectUserMetadata, 0, len(metadata))
	for k, v := range metadata {
		metadata := &dbObjectUserMetadata{
			DBMultipartUploadID: &multipartUploadID,
			Key:                 k,
			Value:               v,
		}
		entities = append(entities, metadata)
	}
	return tx.CreateInBatches(&entities, 1000).Error
}

func (s *SQLStore) updateUserMetadata(tx *gorm.DB, objID uint, metadata api.ObjectUserMetadata) error {
	// delete all existing metadata
	err := tx.
		Where("db_object_id = ?", objID).
		Delete(&dbObjectUserMetadata{}).
		Error
	if err != nil {
		return err
	}

	return s.createUserMetadata(tx, objID, metadata)
}

func (s *SQLStore) createSlices(tx *gorm.DB, objID, multiPartID *uint, contractSetID uint, contracts map[types.FileContractID]dbContract, slices []object.SlabSlice) error {
	if (objID == nil && multiPartID == nil) || (objID != nil && multiPartID != nil) {
		return fmt.Errorf("either objID or multiPartID must be set")
	} else if len(slices) == 0 {
		return nil // nothing to do
	}

	// build slabs
	slabs := make([]dbSlab, len(slices))
	for i := range slices {
		slabKey, err := slices[i].Key.MarshalBinary()
		if err != nil {
			return fmt.Errorf("failed to marshal slab key: %w", err)
		}
		slabs[i] = dbSlab{
			Key:             slabKey,
			DBContractSetID: contractSetID,
			MinShards:       slices[i].MinShards,
			TotalShards:     uint8(len(slices[i].Shards)),
		}
	}

	// create slabs that don't exist yet
	err := tx.
		Clauses(clause.OnConflict{
			DoNothing: true,
			Columns:   []clause.Column{{Name: "key"}},
		}).
		Create(&slabs).Error
	if err != nil {
		return fmt.Errorf("failed to create slabs %w", err)
	}

	// fetch the upserted slabs
	for i := range slabs {
		if err := tx.Raw("SELECT * FROM slabs WHERE `key` = ?", slabs[i].Key).
			Scan(&slabs[i]).
			Error; err != nil {
			return fmt.Errorf("failed to fetch slab: %w", err)
		} else if slabs[i].DBContractSetID != contractSetID {
			return fmt.Errorf("slab already exists in another contract set %v != %v", slabs[i].DBContractSetID, contractSetID)
		}
	}

	// build slices
	dbSlices := make([]dbSlice, len(slices))
	for i := range slices {
		slab := slabs[i]
		dbSlices[i] = dbSlice{
			DBSlabID:          slab.ID,
			DBObjectID:        objID,
			ObjectIndex:       uint(i + 1),
			DBMultipartPartID: multiPartID,
			Offset:            slices[i].Offset,
			Length:            slices[i].Length,
		}
	}

	// if there are no slices we are done
	if len(dbSlices) == 0 {
		return nil
	}

	// create slices
	err = tx.Create(&dbSlices).Error
	if err != nil {
		return fmt.Errorf("failed to create slice %w", err)
	}

	// build sectors
	var sectors []dbSector
	for i, ss := range slices {
		slab := slabs[i]
		for j := range ss.Shards {
			sectors = append(sectors, dbSector{
				DBSlabID:   slab.ID,
				SlabIndex:  j + 1,
				LatestHost: publicKey(ss.Shards[j].LatestHost),
				Root:       ss.Shards[j].Root[:],
			})
		}
	}

	// create sector that don't exist yet
	sectorIDs, err := upsertSectors(tx, sectors)
	if err != nil {
		return fmt.Errorf("failed to create sectors: %w", err)
	}

	// build contract <-> sector links
	sectorIdx := 0
	var contractSectors []dbContractSector
	for _, ss := range slices {
		for _, shard := range ss.Shards {
			sectorID := sectorIDs[sectorIdx]
			for _, fcids := range shard.Contracts {
				for _, fcid := range fcids {
					if _, ok := contracts[fcid]; ok {
						contractSectors = append(contractSectors, dbContractSector{
							DBSectorID:   sectorID,
							DBContractID: contracts[fcid].ID,
						})
					} else {
						s.logger.Warn("missing contract for shard",
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

	// if there are no associations we are done
	if len(contractSectors) == 0 {
		return nil
	}

	// create associations
	if err := tx.
		Table("contract_sectors").
		Clauses(clause.OnConflict{
			DoNothing: true,
		}).
		CreateInBatches(&contractSectors, sectorInsertionBatchSize).Error; err != nil {
		return err
	}
	return nil
}

// object retrieves an object from the store.
func (s *SQLStore) object(tx *gorm.DB, bucket, path string) (api.Object, error) {
	// fetch raw object data
	raw, err := s.objectRaw(tx, bucket, path)
	if errors.Is(err, gorm.ErrRecordNotFound) || len(raw) == 0 {
		return api.Object{}, api.ErrObjectNotFound
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

// contract retrieves a contract from the store.
func (s *SQLStore) contract(ctx context.Context, id fileContractID) (dbContract, error) {
	return contract(s.db.WithContext(ctx), id)
}

// PackedSlabsForUpload returns up to 'limit' packed slabs that are ready for
// uploading. They are locked for 'lockingDuration' time before being handed out
// again.
func (s *SQLStore) PackedSlabsForUpload(ctx context.Context, lockingDuration time.Duration, minShards, totalShards uint8, set string, limit int) ([]api.PackedSlab, error) {
	var contractSetID uint
	if err := s.db.WithContext(ctx).Raw("SELECT id FROM contract_sets WHERE name = ?", set).
		Scan(&contractSetID).Error; err != nil {
		return nil, err
	}
	return s.slabBufferMgr.SlabsForUpload(ctx, lockingDuration, minShards, totalShards, contractSetID, limit)
}

func (s *SQLStore) ObjectsBySlabKey(ctx context.Context, bucket string, slabKey object.EncryptionKey) (metadata []api.ObjectMetadata, err error) {
	var rows []rawObjectMetadata
	key, err := slabKey.MarshalBinary()
	if err != nil {
		return nil, err
	}

	err = s.retryTransaction(ctx, func(tx *gorm.DB) error {
		return tx.Raw(`
SELECT DISTINCT obj.object_id as Name, obj.size as Size, obj.mime_type as MimeType, sla.health as Health
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
	var buffer dbBufferedSlab
	if err := tx.Take(&buffer, "id = ?", slab.BufferID).Error; err != nil {
		return "", err
	}
	fileName := buffer.Filename
	err = tx.Delete(&buffer).
		Error
	if err != nil {
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

// contracts retrieves all contracts for the given ids from the store.
func contracts(tx *gorm.DB, ids []types.FileContractID) (dbContracts []dbContract, err error) {
	fcids := make([]fileContractID, len(ids))
	for i, fcid := range ids {
		fcids[i] = fileContractID(fcid)
	}

	// fetch contracts
	err = tx.
		Model(&dbContract{}).
		Where("fcid IN (?)", fcids).
		Joins("Host").
		Find(&dbContracts).
		Error
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

func newContract(hostID uint, fcid, renewedFrom types.FileContractID, contractPrice, totalCost types.Currency, startHeight, windowStart, windowEnd, size uint64, state contractState) dbContract {
	return dbContract{
		HostID:       hostID,
		ContractSets: nil, // new contract isn't in a set yet

		ContractCommon: ContractCommon{
			FCID:        fileContractID(fcid),
			RenewedFrom: fileContractID(renewedFrom),

			ContractPrice:  currency(contractPrice),
			State:          state,
			TotalCost:      currency(totalCost),
			RevisionNumber: "0",
			Size:           size,
			StartHeight:    startHeight,
			WindowStart:    windowStart,
			WindowEnd:      windowEnd,

			UploadSpending:      zeroCurrency,
			DownloadSpending:    zeroCurrency,
			FundAccountSpending: zeroCurrency,
			DeleteSpending:      zeroCurrency,
			ListSpending:        zeroCurrency,
		},
	}
}

// addContract adds a contract to the store.
func addContract(tx *gorm.DB, c rhpv2.ContractRevision, contractPrice, totalCost types.Currency, startHeight uint64, renewedFrom types.FileContractID, state contractState) (dbContract, error) {
	fcid := c.ID()

	// Find host.
	var host dbHost
	err := tx.Model(&dbHost{}).Where(&dbHost{PublicKey: publicKey(c.HostKey())}).
		Find(&host).Error
	if err != nil {
		return dbContract{}, err
	}

	// Create contract.
	contract := newContract(host.ID, fcid, renewedFrom, contractPrice, totalCost, startHeight, c.Revision.WindowStart, c.Revision.WindowEnd, c.Revision.Filesize, state)

	// Insert contract.
	err = tx.Create(&contract).Error
	if err != nil {
		return dbContract{}, err
	}
	// Populate host.
	contract.Host = host
	return contract, nil
}

// archiveContracts archives the given contracts and uses the given reason as
// archival reason
//
// NOTE: this function archives the contracts without setting a renewed ID
func archiveContracts(tx *gorm.DB, contracts []dbContract, toArchive map[types.FileContractID]string) error {
	var toInvalidate []fileContractID
	for _, contract := range contracts {
		toInvalidate = append(toInvalidate, contract.FCID)
	}
	// Invalidate the health on the slabs before deleting the contracts to avoid
	// breaking the relations beforehand.
	if err := invalidateSlabHealthByFCID(tx, toInvalidate); err != nil {
		return fmt.Errorf("invalidating slab health failed: %w", err)
	}
	for _, contract := range contracts {
		// sanity check the host is populated
		if contract.Host.ID == 0 {
			return fmt.Errorf("host not populated for contract %v", contract.FCID)
		}

		// create a copy in the archive
		if err := tx.Create(&dbArchivedContract{
			Host:   publicKey(contract.Host.PublicKey),
			Reason: toArchive[types.FileContractID(contract.FCID)],

			ContractCommon: contract.ContractCommon,
		}).Error; err != nil {
			return err
		}

		// remove the contract
		res := tx.Delete(&contract)
		if err := res.Error; err != nil {
			return err
		}
		if res.RowsAffected != 1 {
			return fmt.Errorf("expected to delete 1 row, deleted %d", res.RowsAffected)
		}
	}
	return nil
}

func (s *SQLStore) pruneSlabsLoop() {
	for {
		select {
		case <-s.slabPruneSigChan:
		case <-s.shutdownCtx.Done():
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second+sumDurations(s.retryTransactionIntervals))
		err := s.retryTransaction(ctx, pruneSlabs)
		if err != nil {
			s.logger.Errorw("failed to prune slabs", zap.Error(err))
			s.alerts.RegisterAlert(s.shutdownCtx, alerts.Alert{
				ID:        pruneSlabsAlertID,
				Severity:  alerts.SeverityWarning,
				Message:   "Failed to prune slabs from database",
				Timestamp: time.Now(),
				Data: map[string]interface{}{
					"error": err.Error(),
					"hint":  "This might happen when your database is under a lot of load due to deleting objects rapidly. This alert will disappear the next time slabs are pruned successfully.",
				},
			})
		} else {
			s.alerts.DismissAlerts(s.shutdownCtx, pruneSlabsAlertID)

			s.mu.Lock()
			s.lastPrunedAt = time.Now()
			s.mu.Unlock()
		}
		cancel()
	}
}

func pruneSlabs(tx *gorm.DB) error {
	return tx.Exec(`
DELETE
FROM slabs
WHERE NOT EXISTS (SELECT 1 FROM slices WHERE slices.db_slab_id = slabs.id)
AND slabs.db_buffered_slab_id IS NULL
`).Error
}

func (s *SQLStore) triggerSlabPruning() {
	select {
	case s.slabPruneSigChan <- struct{}{}:
	default:
	}
}

// deleteObject deletes an object from the store and prunes all slabs which are
// without an obect after the deletion. That means in case of packed uploads,
// the slab is only deleted when no more objects point to it.
func (s *SQLStore) deleteObject(tx *gorm.DB, bucket string, path string) (int64, error) {
	// check if the object exists first to avoid unnecessary locking for the
	// common case
	var objID uint
	resp := tx.Model(&dbObject{}).
		Where("object_id = ? AND ?", path, sqlWhereBucket("objects", bucket)).
		Select("id").
		Limit(1).
		Scan(&objID)
	if err := resp.Error; err != nil {
		return 0, err
	} else if resp.RowsAffected == 0 {
		return 0, nil
	}

	tx = tx.Where("id", objID).
		Delete(&dbObject{})
	if tx.Error != nil {
		return 0, tx.Error
	}
	numDeleted := tx.RowsAffected
	if numDeleted == 0 {
		return 0, nil // nothing to prune if no object was deleted
	}
	s.triggerSlabPruning()
	return numDeleted, nil
}

// deleteObjects deletes a batch of objects from the database. The order of
// deletion goes from largest to smallest. That's because the batch size is
// dynamically increased and the smaller objects get the faster we can delete
// them meaning it makes sense to increase the batch size over time.
func (s *SQLStore) deleteObjects(ctx context.Context, bucket string, path string) (numDeleted int64, _ error) {
	batchSizeIdx := 0
	for {
		var duration time.Duration
		var rowsAffected int64
		if err := s.retryTransaction(ctx, func(tx *gorm.DB) error {
			start := time.Now()
			res := tx.Exec(`
			DELETE FROM objects
			WHERE id IN (
				SELECT id FROM (
					SELECT id FROM objects
					WHERE object_id LIKE ? AND SUBSTR(object_id, 1, ?) = ? AND ?
					ORDER BY size DESC
					LIMIT ?
					) tmp
				)`,
				path+"%", utf8.RuneCountInString(path), path, sqlWhereBucket("objects", bucket),
				objectDeleteBatchSizes[batchSizeIdx])
			if err := res.Error; err != nil {
				return res.Error
			}
			// prune slabs if we deleted an object
			rowsAffected = res.RowsAffected
			if rowsAffected > 0 {
				s.triggerSlabPruning()
			}
			duration = time.Since(start)
			return nil
		}); err != nil {
			return 0, fmt.Errorf("failed to delete objects: %w", err)
		}

		// if nothing got deleted we are done
		if rowsAffected == 0 {
			break
		}
		numDeleted += rowsAffected

		// increase the batch size if deletion was faster than the threshold
		if duration < batchDurationThreshold && batchSizeIdx < len(objectDeleteBatchSizes)-1 {
			batchSizeIdx++
		}
	}
	return numDeleted, nil
}

func invalidateSlabHealthByFCID(tx *gorm.DB, fcids []fileContractID) error {
	if len(fcids) == 0 {
		return nil
	}

	for {
		now := time.Now().Unix()
		if resp := tx.Exec(`
		UPDATE slabs SET health_valid_until = ? WHERE id in (
			   SELECT *
			   FROM (
					   SELECT slabs.id
					   FROM slabs
					   INNER JOIN sectors se ON se.db_slab_id = slabs.id
					   INNER JOIN contract_sectors cs ON cs.db_sector_id = se.id
					   INNER JOIN contracts c ON c.id = cs.db_contract_id
					   WHERE c.fcid IN (?) AND slabs.health_valid_until >= ?
					   LIMIT ?
			   ) slab_ids
		)`, now, fcids, now, refreshHealthBatchSize); resp.Error != nil {
			return fmt.Errorf("failed to invalidate slab health: %w", resp.Error)
		} else if resp.RowsAffected < refreshHealthBatchSize {
			break // done
		}
		time.Sleep(time.Second)
	}
	return nil
}

func (s *SQLStore) invalidateSlabHealthByFCID(ctx context.Context, fcids []fileContractID) error {
	return s.retryTransaction(ctx, func(tx *gorm.DB) error {
		return invalidateSlabHealthByFCID(tx, fcids)
	})
}

// nolint:unparam
func sqlConcat(db *gorm.DB, a, b string) string {
	if isSQLite(db) {
		return fmt.Sprintf("%s || %s", a, b)
	}
	return fmt.Sprintf("CONCAT(%s, %s)", a, b)
}

func sqlRandomTimestamp(db *gorm.DB, now time.Time, min, max time.Duration) clause.Expr {
	if isSQLite(db) {
		return gorm.Expr("ABS(RANDOM()) % (? - ?) + ?", int(max.Seconds()), int(min.Seconds()), now.Add(min).Unix())
	}
	return gorm.Expr("FLOOR(? + RAND() * (? - ?))", now.Add(min).Unix(), int(max.Seconds()), int(min.Seconds()))
}

// nolint:unparam
func sqlWhereBucket(objTable string, bucket string) clause.Expr {
	return gorm.Expr(fmt.Sprintf("%s.db_bucket_id = (SELECT id FROM buckets WHERE buckets.name = ?)", objTable), bucket)
}

// TODO: we can use ObjectEntries instead of ListObject if we want to use '/' as
// a delimiter for now (see backend.go) but it would be interesting to have
// arbitrary 'delim' support in ListObjects.
func (s *SQLStore) ListObjects(ctx context.Context, bucket, prefix, sortBy, sortDir, marker string, limit int) (api.ObjectsListResponse, error) {
	// fetch one more to see if there are more entries
	if limit <= -1 {
		limit = math.MaxInt
	} else {
		limit++
	}

	// build prefix expr
	prefixExpr := buildPrefixExpr(prefix)

	// build order clause
	orderBy, err := buildOrderClause(sortBy, sortDir)
	if err != nil {
		return api.ObjectsListResponse{}, err
	}

	// build marker expr
	markerExpr, markerOrderBy, err := buildMarkerExpr(s.db, bucket, prefix, marker, sortBy, sortDir)
	if err != nil {
		return api.ObjectsListResponse{}, err
	}
	var rows []rawObjectMetadata
	if err := s.db.
		Select("o.object_id as Name, o.size as Size, o.health as Health, o.mime_type as MimeType, o.created_at as ModTime, o.etag as ETag").
		Model(&dbObject{}).
		Table("objects o").
		Joins("INNER JOIN buckets b ON o.db_bucket_id = b.id").
		Where("b.name = ? AND ? AND ?", bucket, prefixExpr, markerExpr).
		Order(orderBy).
		Order(markerOrderBy).
		Order("Name ASC").
		Limit(int(limit)).
		Scan(&rows).Error; err != nil {
		return api.ObjectsListResponse{}, err
	}

	var hasMore bool
	var nextMarker string
	if len(rows) == limit {
		hasMore = true
		rows = rows[:len(rows)-1]
		nextMarker = rows[len(rows)-1].Name
	}

	var objects []api.ObjectMetadata
	for _, row := range rows {
		objects = append(objects, row.convert())
	}

	return api.ObjectsListResponse{
		HasMore:    hasMore,
		NextMarker: nextMarker,
		Objects:    objects,
	}, nil
}

func (ss *SQLStore) processConsensusChangeContracts(cc modules.ConsensusChange) {
	height := uint64(cc.InitialHeight())
	for _, sb := range cc.RevertedBlocks {
		var b types.Block
		convertToCore(sb, (*types.V1Block)(&b))

		// revert contracts that got reorged to "pending".
		for _, txn := range b.Transactions {
			// handle contracts
			for i := range txn.FileContracts {
				fcid := txn.FileContractID(i)
				if ss.isKnownContract(fcid) {
					ss.unappliedContractState[fcid] = contractStatePending // revert from 'active' to 'pending'
					ss.logger.Infow("contract state changed: active -> pending",
						"fcid", fcid,
						"reason", "contract reverted")
				}
			}
			// handle contract revision
			for _, rev := range txn.FileContractRevisions {
				if ss.isKnownContract(rev.ParentID) {
					if rev.RevisionNumber == math.MaxUint64 && rev.Filesize == 0 {
						ss.unappliedContractState[rev.ParentID] = contractStateActive // revert from 'complete' to 'active'
						ss.logger.Infow("contract state changed: complete -> active",
							"fcid", rev.ParentID,
							"reason", "final revision reverted")
					}
				}
			}
			// handle storage proof
			for _, sp := range txn.StorageProofs {
				if ss.isKnownContract(sp.ParentID) {
					ss.unappliedContractState[sp.ParentID] = contractStateActive // revert from 'complete' to 'active'
					ss.logger.Infow("contract state changed: complete -> active",
						"fcid", sp.ParentID,
						"reason", "storage proof reverted")
				}
			}
		}
		height--
	}

	for _, sb := range cc.AppliedBlocks {
		var b types.Block
		convertToCore(sb, (*types.V1Block)(&b))

		// Update RevisionHeight and RevisionNumber for our contracts.
		for _, txn := range b.Transactions {
			// handle contracts
			for i := range txn.FileContracts {
				fcid := txn.FileContractID(i)
				if ss.isKnownContract(fcid) {
					ss.unappliedContractState[fcid] = contractStateActive // 'pending' -> 'active'
					ss.logger.Infow("contract state changed: pending -> active",
						"fcid", fcid,
						"reason", "contract confirmed")
				}
			}
			// handle contract revision
			for _, rev := range txn.FileContractRevisions {
				if ss.isKnownContract(rev.ParentID) {
					ss.unappliedRevisions[types.FileContractID(rev.ParentID)] = revisionUpdate{
						height: height,
						number: rev.RevisionNumber,
						size:   rev.Filesize,
					}
					if rev.RevisionNumber == math.MaxUint64 && rev.Filesize == 0 {
						ss.unappliedContractState[rev.ParentID] = contractStateComplete // renewed: 'active' -> 'complete'
						ss.logger.Infow("contract state changed: active -> complete",
							"fcid", rev.ParentID,
							"reason", "final revision confirmed")
					}
				}
			}
			// handle storage proof
			for _, sp := range txn.StorageProofs {
				if ss.isKnownContract(sp.ParentID) {
					ss.unappliedProofs[sp.ParentID] = height
					ss.unappliedContractState[sp.ParentID] = contractStateComplete // storage proof: 'active' -> 'complete'
					ss.logger.Infow("contract state changed: active -> complete",
						"fcid", sp.ParentID,
						"reason", "storage proof confirmed")
				}
			}
		}
		height++
	}
}

func buildMarkerExpr(db *gorm.DB, bucket, prefix, marker, sortBy, sortDir string) (markerExpr clause.Expr, orderBy clause.OrderBy, err error) {
	// no marker
	if marker == "" {
		return exprTRUE, clause.OrderBy{}, nil
	}

	// for markers to work we need to order by object_id
	orderBy = clause.OrderBy{
		Columns: []clause.OrderByColumn{
			{
				Column: clause.Column{Name: "object_id"},
				Desc:   false,
			},
		},
	}

	desc := strings.EqualFold(sortDir, api.ObjectSortDirDesc)
	switch sortBy {
	case "", api.ObjectSortByName:
		if desc {
			markerExpr = gorm.Expr("object_id < ?", marker)
		} else {
			markerExpr = gorm.Expr("object_id > ?", marker)
		}
	case api.ObjectSortByHealth:
		// fetch marker health
		var markerHealth float64
		if marker != "" && sortBy == api.ObjectSortByHealth {
			if err := db.
				Select("o.health").
				Model(&dbObject{}).
				Table("objects o").
				Joins("INNER JOIN buckets b ON o.db_bucket_id = b.id").
				Where("b.name = ? AND ? AND ?", bucket, buildPrefixExpr(prefix), gorm.Expr("o.object_id >= ?", marker)).
				Limit(1).
				Scan(&markerHealth).
				Error; err != nil {
				return exprTRUE, clause.OrderBy{}, err
			}
		}

		if desc {
			markerExpr = gorm.Expr("(Health <= ? AND object_id > ?) OR Health < ?", markerHealth, marker, markerHealth)
		} else {
			markerExpr = gorm.Expr("Health > ? OR (Health >= ? AND object_id > ?)", markerHealth, markerHealth, marker)
		}
	case api.ObjectSortBySize:
		// fetch marker size
		var markerSize float64
		if marker != "" && sortBy == api.ObjectSortBySize {
			if err := db.
				Select("o.size").
				Model(&dbObject{}).
				Table("objects o").
				Joins("INNER JOIN buckets b ON o.db_bucket_id = b.id").
				Where("b.name = ? AND ? AND ?", bucket, buildPrefixExpr(prefix), gorm.Expr("o.object_id >= ?", marker)).
				Limit(1).
				Scan(&markerSize).
				Error; err != nil {
				return exprTRUE, clause.OrderBy{}, err
			}
		}

		if desc {
			markerExpr = gorm.Expr("(Size <= ? AND object_id > ?) OR Size < ?", markerSize, marker, markerSize)
		} else {
			markerExpr = gorm.Expr("Size > ? OR (Size >= ? AND object_id > ?)", markerSize, markerSize, marker)
		}
	default:
		err = fmt.Errorf("unhandled sortBy parameter '%s'", sortBy)
	}
	return
}

func buildOrderClause(sortBy, sortDir string) (clause.OrderByColumn, error) {
	if err := validateSort(sortBy, sortDir); err != nil {
		return clause.OrderByColumn{}, err
	}

	orderByColumns := map[string]string{
		"":                     "object_id",
		api.ObjectSortByName:   "object_id",
		api.ObjectSortByHealth: "Health",
		api.ObjectSortBySize:   "Size",
	}

	return clause.OrderByColumn{
		Column: clause.Column{Name: orderByColumns[sortBy]},
		Desc:   strings.EqualFold(sortDir, api.ObjectSortDirDesc),
	}, nil
}

func buildPrefixExpr(prefix string) clause.Expr {
	if prefix != "" {
		return gorm.Expr("o.object_id LIKE ? AND SUBSTR(o.object_id, 1, ?) = ?", prefix+"%", utf8.RuneCountInString(prefix), prefix)
	} else {
		return exprTRUE
	}
}

func updateAllObjectsHealth(tx *gorm.DB) error {
	return tx.Exec(`
UPDATE objects
SET health = (
	SELECT COALESCE(MIN(slabs.health), 1)
	FROM slabs
	INNER JOIN slices sli ON sli.db_slab_id = slabs.id
	WHERE sli.db_object_id = objects.id)
`).Error
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
