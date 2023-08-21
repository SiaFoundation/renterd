package stores

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/alerts"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/object"
	"gorm.io/gorm"
	"lukechampine.com/frand"
)

type (
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
	}

	ContractCommon struct {
		FCID        fileContractID `gorm:"unique;index;NOT NULL;column:fcid;size:32"`
		RenewedFrom fileContractID `gorm:"index;size:32"`

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

		Name      string       `gorm:"unique;index:length:255;"`
		Contracts []dbContract `gorm:"many2many:contract_set_contracts;constraint:OnDelete:CASCADE"`
	}

	dbObject struct {
		Model

		Key      []byte
		ObjectID string    `gorm:"index;unique"`
		Slabs    []dbSlice `gorm:"constraint:OnDelete:CASCADE"` // CASCADE to delete slices too
		Size     int64
	}

	dbSlice struct {
		Model
		DBObjectID uint `gorm:"index"`

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

		Health      float64 `gorm:"index; default:1.0; NOT NULL"`
		Key         []byte  `gorm:"unique;NOT NULL;size:68"` // json string
		MinShards   uint8   `gorm:"index"`
		TotalShards uint8   `gorm:"index"`

		Slices []dbSlice
		Shards []dbSector `gorm:"constraint:OnDelete:CASCADE"` // CASCADE to delete shards too
	}

	dbBufferedSlab struct {
		Model
		DBSlab dbSlab

		Complete    bool `gorm:"index"`
		Filename    string
		Size        int64
		LockID      int64 `gorm:"column:lock_id"`
		LockedUntil int64 // unix timestamp
	}

	dbSector struct {
		Model

		DBSlabID   uint      `gorm:"index"`
		LatestHost publicKey `gorm:"NOT NULL"`
		Root       []byte    `gorm:"index;unique;NOT NULL;size:32"`

		Contracts []dbContract `gorm:"many2many:contract_sectors;constraint:OnDelete:CASCADE"`
	}

	// dbContractSector is a join table between dbContract and dbSector.
	dbContractSector struct {
		DBContractID uint `gorm:"primaryKey"`
		DBSectorID   uint `gorm:"primaryKey"`
	}

	// rawObject is used for hydration and is made up of one or many raw sectors.
	rawObject []rawObjectSector

	// rawObjectRow contains all necessary information to reconstruct the object.
	rawObjectSector struct {
		// object
		ObjectKey  []byte
		ObjectName string
		ObjectSize int64

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
		SectorID   uint
		SectorRoot []byte
		SectorHost publicKey
	}
)

// TableName implements the gorm.Tabler interface.
func (dbArchivedContract) TableName() string { return "archived_contracts" }

// TableName implements the gorm.Tabler interface.
func (dbContract) TableName() string { return "contracts" }

// TableName implements the gorm.Tabler interface.
func (dbContractSector) TableName() string { return "contract_sectors" }

// TableName implements the gorm.Tabler interface.
func (dbContractSet) TableName() string { return "contract_sets" }

// TableName implements the gorm.Tabler interface.
func (dbObject) TableName() string { return "objects" }

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
	return api.ContractMetadata{
		ID:         types.FileContractID(c.FCID),
		HostIP:     c.Host.NetAddress,
		HostKey:    types.PublicKey(c.Host.PublicKey),
		SiamuxAddr: c.Host.Settings.convert().SiamuxAddr(),

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
		Size:           c.Size,
		StartHeight:    c.StartHeight,
		WindowStart:    c.WindowStart,
		WindowEnd:      c.WindowEnd,
	}
}

// convert turns a dbObject into a object.Slab.
func (s dbSlab) convert() (slab object.Slab, err error) {
	// unmarshal key
	err = slab.Key.UnmarshalText(s.Key)
	if err != nil {
		return
	}

	// set shards
	slab.MinShards = s.MinShards
	slab.Shards = make([]object.Sector, len(s.Shards))

	// hydrate shards
	for i, shard := range s.Shards {
		slab.Shards[i].Host = types.PublicKey(shard.LatestHost)
		slab.Shards[i].Root = *(*types.Hash256)(shard.Root)
	}

	return
}

func (o dbObject) metadata() api.ObjectMetadata {
	return api.ObjectMetadata{
		Name: o.ObjectID,
		Size: o.Size,
	}
}

func (raw rawObject) convert(tx *gorm.DB, partialSlabDir string) (api.Object, error) {
	if len(raw) == 0 {
		return api.Object{}, errors.New("no slabs found")
	}

	// parse object key
	var key object.EncryptionKey
	if err := key.UnmarshalText(raw[0].ObjectKey); err != nil {
		return api.Object{}, err
	}

	// filter out slabs without slab ID and buffered slabs - this is expected
	// for an empty object or objects that end with a partial slab.
	var filtered rawObject
	var partialSlabSector *rawObjectSector
	minHealth := math.MaxFloat64
	for i, sector := range raw {
		if sector.SlabID != 0 && !sector.SlabBuffered {
			filtered = append(filtered, sector)
			if sector.SlabHealth < minHealth {
				minHealth = sector.SlabHealth
			}
		} else if sector.SlabBuffered {
			partialSlabSector = &raw[i]
		}
	}

	// hydrate all slabs
	slabs := make([]object.SlabSlice, 0, len(filtered))
	if len(filtered) > 0 {
		curr := filtered[0].SlabID
		var start int
		// create a helper function to add a slab and update the state
		addSlab := func(end int, id uint) error {
			if filtered[start].SlabBuffered {
				return nil // ignore partial slabs
			}
			if slab, err := filtered[start:end].toSlabSlice(); err != nil {
				return err
			} else {
				slabs = append(slabs, slab)
				curr = id
				start = end
			}
			return nil
		}

		for j, sector := range filtered {
			if sector.SectorID == 0 {
				return api.Object{}, api.ErrObjectCorrupted
			}
			if sector.SlabID != curr {
				if err := addSlab(j, sector.SlabID); err != nil {
					return api.Object{}, err
				}
			}
		}
		if err := addSlab(len(filtered), 0); err != nil {
			return api.Object{}, err
		}
	} else {
		minHealth = 1 // empty object
	}

	// fetch a potential partial slab from the buffer.
	var partialSlab *object.PartialSlab
	if partialSlabSector != nil {
		var buffer dbBufferedSlab
		err := tx.Joins("DBSlab").
			Where("DBSlab.id", partialSlabSector.SlabID).
			Take(&buffer).Error
		if err != nil {
			return api.Object{}, err
		}
		file, err := os.Open(filepath.Join(partialSlabDir, buffer.Filename))
		if err != nil {
			return api.Object{}, err
		}
		defer file.Close()
		data := make([]byte, partialSlabSector.SliceLength)
		_, err = file.ReadAt(data, int64(partialSlabSector.SliceOffset))
		if err != nil {
			return api.Object{}, err
		}
		partialSlab = &object.PartialSlab{
			MinShards:   buffer.DBSlab.MinShards,
			TotalShards: buffer.DBSlab.TotalShards,
			Data:        data,
		}
	}

	// return object
	return api.Object{
		ObjectMetadata: api.ObjectMetadata{
			Name:   raw[0].ObjectName,
			Size:   raw[0].ObjectSize,
			Health: minHealth,
		},
		Object: object.Object{
			Key:         key,
			PartialSlab: partialSlab,
			Slabs:       slabs,
		},
	}, nil
}

func (raw rawObject) toSlabSlice() (slice object.SlabSlice, _ error) {
	if len(raw) == 0 {
		return object.SlabSlice{}, errors.New("no sectors found")
	}

	// unmarshal key
	if err := slice.Slab.Key.UnmarshalText(raw[0].SlabKey); err != nil {
		return object.SlabSlice{}, err
	}

	// hydrate all sectors
	slabID := raw[0].SlabID
	sectors := make([]object.Sector, 0, len(raw))
	for _, sector := range raw {
		if sector.SlabID != slabID {
			return object.SlabSlice{}, errors.New("sectors from different slabs") // developer error
		}
		sectors = append(sectors, object.Sector{
			Host: types.PublicKey(sector.SectorHost),
			Root: *(*types.Hash256)(sector.SectorRoot),
		})
	}

	// hydrate all fields
	slice.Slab.Health = raw[0].SlabHealth
	slice.Slab.Shards = sectors
	slice.Slab.MinShards = raw[0].SlabMinShards
	slice.Offset = raw[0].SliceOffset
	slice.Length = raw[0].SliceLength
	return slice, nil
}

// ObjectsStats returns some info related to the objects stored in the store. To
// reduce locking and make sure all results are consistent, everything is done
// within a single transaction.
func (s *SQLStore) ObjectsStats(ctx context.Context) (api.ObjectsStatsResponse, error) {
	var resp api.ObjectsStatsResponse
	return resp, s.db.Transaction(func(tx *gorm.DB) error {
		// Number of objects.
		var objInfo struct {
			NumObjects       uint64
			TotalObjectsSize uint64
		}
		err := tx.
			Model(&dbObject{}).
			Select("COUNT(*) AS NumObjects, SUM(size) AS TotalObjectsSize").
			Scan(&objInfo).
			Error
		if err != nil {
			return err
		}
		resp.NumObjects = objInfo.NumObjects
		resp.TotalObjectsSize = objInfo.TotalObjectsSize

		// Size of sectors
		var sectorSizes struct {
			SectorsSize  uint64
			UploadedSize uint64
		}
		err = tx.
			Model(&dbContractSector{}).
			Select("COUNT(DISTINCT db_sector_id) * ? as sectors_size, COUNT(*) * ? as uploaded_size", rhpv2.SectorSize, rhpv2.SectorSize).
			Scan(&sectorSizes).
			Error
		if err != nil {
			return err
		}
		resp.TotalSectorsSize = sectorSizes.SectorsSize
		resp.TotalUploadedSize = sectorSizes.UploadedSize
		return nil
	})
}

func (s *SQLStore) SlabBuffers(ctx context.Context) ([]api.SlabBuffer, error) {
	// Slab buffer info.
	var bufferedSlabs []dbBufferedSlab
	err := s.db.Model(&dbBufferedSlab{}).
		Joins("DBSlab").
		Joins("DBSlab.DBContractSet").
		Find(&bufferedSlabs).
		Error
	if err != nil {
		return nil, err
	}
	var buffers []api.SlabBuffer
	for _, buf := range bufferedSlabs {
		buffers = append(buffers, api.SlabBuffer{
			ContractSet: buf.DBSlab.DBContractSet.Name,
			Complete:    buf.Complete,
			Filename:    buf.Filename,
			Size:        buf.Size,
			MaxSize:     int64(bufferedSlabSize(buf.DBSlab.MinShards)),
			Locked:      buf.LockedUntil > time.Now().Unix(),
		})
	}
	return buffers, nil
}

func (s *SQLStore) AddContract(ctx context.Context, c rhpv2.ContractRevision, totalCost types.Currency, startHeight uint64) (_ api.ContractMetadata, err error) {
	var added dbContract
	if err = s.retryTransaction(func(tx *gorm.DB) error {
		added, err = addContract(tx, c, totalCost, startHeight, types.FileContractID{})
		return err
	}); err != nil {
		return
	}

	s.addKnownContract(types.FileContractID(added.FCID))
	return added.convert(), nil
}

func (s *SQLStore) Contracts(ctx context.Context) ([]api.ContractMetadata, error) {
	var dbContracts []dbContract
	err := s.db.
		Model(&dbContract{}).
		Preload("Host").
		Find(&dbContracts).
		Error
	if err != nil {
		return nil, err
	}

	contracts := make([]api.ContractMetadata, len(dbContracts))
	for i, c := range dbContracts {
		contracts[i] = c.convert()
	}
	return contracts, nil
}

// AddRenewedContract adds a new contract which was created as the result of a renewal to the store.
// The old contract specified as 'renewedFrom' will be deleted from the active
// contracts and moved to the archive. Both new and old contract will be linked
// to each other through the RenewedFrom and RenewedTo fields respectively.
func (s *SQLStore) AddRenewedContract(ctx context.Context, c rhpv2.ContractRevision, totalCost types.Currency, startHeight uint64, renewedFrom types.FileContractID) (api.ContractMetadata, error) {
	var renewed dbContract

	if err := s.retryTransaction(func(tx *gorm.DB) error {
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
		newContract := newContract(oldContract.HostID, c.ID(), renewedFrom, totalCost, startHeight, c.Revision.WindowStart, c.Revision.WindowEnd, oldContract.Size)
		newContract.Model = oldContract.Model
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
	err := s.db.Raw("WITH RECURSIVE ancestors AS (SELECT * FROM archived_contracts WHERE renewed_to = ? UNION ALL SELECT archived_contracts.* FROM ancestors, archived_contracts WHERE archived_contracts.renewed_to = ancestors.fcid) SELECT * FROM ancestors WHERE start_height >= ?", fileContractID(id), startHeight).
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
	if err := s.retryTransaction(func(tx *gorm.DB) error {
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

func (s *SQLStore) ContractSetContracts(ctx context.Context, set string) ([]api.ContractMetadata, error) {
	dbContracts, err := s.contracts(ctx, set)
	if err != nil {
		return nil, err
	}
	contracts := make([]api.ContractMetadata, len(dbContracts))
	for i, c := range dbContracts {
		contracts[i] = c.convert()
	}
	return contracts, nil
}

func (s *SQLStore) ContractSets(ctx context.Context) ([]string, error) {
	var sets []string
	err := s.db.Raw("SELECT name FROM contract_sets").
		Scan(&sets).
		Error
	return sets, err
}

func (s *SQLStore) ContractSizes(ctx context.Context) (map[types.FileContractID]api.ContractSize, error) {
	rows := make([]struct {
		Fcid     fileContractID `json:"fcid"`
		Size     uint64         `json:"size"`
		Prunable uint64         `json:"prunable"`
	}, 0)

	if err := s.db.
		Raw(`
SELECT fcid, size, CASE SIGN(bytes) WHEN -1 THEN 0 ELSE bytes END as prunable FROM (
	SELECT fcid, MAX(c.size) as size, IFNULL(MAX(c.size) - COUNT(cs.db_sector_id) * ?, 0) as bytes
	FROM contracts c
	LEFT JOIN contract_sectors cs ON cs.db_contract_id = c.id
	GROUP BY c.fcid
) as i
	`, rhpv2.SectorSize).
		Scan(&rows).
		Error; err != nil {
		return nil, err
	}

	sizes := make(map[types.FileContractID]api.ContractSize, len(rows))
	for _, row := range rows {
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
		Raw(`
SELECT size, CASE SIGN(bytes) WHEN -1 THEN 0 ELSE bytes END as prunable FROM (
    SELECT IFNULL(MAX(c.size), 0) as size, IFNULL(MAX(c.size) - COUNT(cs.db_sector_id) * ?, 0) as bytes
    FROM contracts c
    LEFT JOIN contract_sectors cs ON cs.db_contract_id = c.id
    WHERE c.fcid = ?
) as i
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
	fcids := make([]fileContractID, len(contractIds))
	for i, fcid := range contractIds {
		fcids[i] = fileContractID(fcid)
	}

	return s.retryTransaction(func(tx *gorm.DB) error {
		// fetch contracts
		var dbContracts []dbContract
		err := tx.
			Model(&dbContract{}).
			Where("fcid IN (?)", fcids).
			Find(&dbContracts).
			Error
		if err != nil {
			return err
		}

		// create contract set
		var contractset dbContractSet
		err = tx.
			Where(dbContractSet{Name: name}).
			FirstOrCreate(&contractset).
			Error
		if err != nil {
			return err
		}

		// update contracts
		return tx.Model(&contractset).Association("Contracts").Replace(&dbContracts)
	})
}

func (s *SQLStore) RemoveContractSet(ctx context.Context, name string) error {
	return s.db.
		Where(dbContractSet{Name: name}).
		Delete(&dbContractSet{}).
		Error
}

func (s *SQLStore) RenewedContract(ctx context.Context, renewedFrom types.FileContractID) (_ api.ContractMetadata, err error) {
	var contract dbContract

	err = s.db.
		Where(&dbContract{ContractCommon: ContractCommon{RenewedFrom: fileContractID(renewedFrom)}}).
		Preload("Host").
		Take(&contract).
		Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		err = api.ErrContractNotFound
		return
	}

	return contract.convert(), nil
}

func (s *SQLStore) SearchObjects(ctx context.Context, substring string, offset, limit int) ([]api.ObjectMetadata, error) {
	if limit <= -1 {
		limit = math.MaxInt
	}

	var objects []api.ObjectMetadata
	err := s.db.
		Select("o.object_id as name, MAX(o.size) as size, MIN(sla.health) as health").
		Model(&dbObject{}).
		Table("objects o").
		Joins("LEFT JOIN slices sli ON o.id = sli.`db_object_id`").
		Joins("LEFT JOIN slabs sla ON sli.db_slab_id = sla.`id`").
		Where("INSTR(o.object_id, ?) > 0", substring).
		Group("o.object_id").
		Offset(offset).
		Limit(limit).
		Scan(&objects).Error
	if err != nil {
		return nil, err
	}

	return objects, nil
}

func sqlConcat(db *gorm.DB, a, b string) string {
	if isSQLite(db) {
		return fmt.Sprintf("%s || %s", a, b)
	}
	return fmt.Sprintf("CONCAT(%s, %s)", a, b)
}

func (s *SQLStore) ObjectEntries(ctx context.Context, path, prefix string, offset, limit int) ([]api.ObjectMetadata, error) {
	// sanity check we are passing a directory
	if !strings.HasSuffix(path, "/") {
		panic("path must end in /")
	}

	if limit <= -1 {
		limit = math.MaxInt
	}

	query := s.db.Raw(fmt.Sprintf(`
SELECT
	SUM(size) AS size,
	CASE slashindex
	WHEN 0 THEN %s
	ELSE %s
	END AS name,
	MIN(health) as health
FROM (
	SELECT size, health, trimmed, INSTR(trimmed, "/") AS slashindex
	FROM (
		SELECT MAX(size) AS size, MIN(slabs.health) as health, SUBSTR(object_id, ?) AS trimmed
		FROM objects
		LEFT JOIN slices ON objects.id = slices.db_object_id
		LEFT JOIN slabs ON slices.db_slab_id = slabs.id
		WHERE SUBSTR(object_id, 1, ?) = ?
		GROUP BY object_id
	) AS i
) AS m
GROUP BY name
HAVING SUBSTR(name, 1, ?) = ? AND name != ?
ORDER BY name ASC
LIMIT ? OFFSET ?`,
		sqlConcat(s.db, "?", "trimmed"),
		sqlConcat(s.db, "?", "substr(trimmed, 1, slashindex)")),
		path,
		path,
		utf8.RuneCountInString(path)+1,
		utf8.RuneCountInString(path),
		path,
		utf8.RuneCountInString(path+prefix),
		path+prefix,
		path,
		limit,
		offset)

	var metadata []api.ObjectMetadata
	err := query.Scan(&metadata).Error
	if err != nil {
		return nil, err
	}
	return metadata, nil
}

func (s *SQLStore) Object(ctx context.Context, path string) (api.Object, error) {
	var obj api.Object
	err := s.db.Transaction(func(tx *gorm.DB) error {
		o, err := s.object(ctx, tx, path)
		if err != nil {
			return err
		}
		obj, err = o.convert(tx, s.partialSlabDir)
		return err
	})
	return obj, err
}

func (s *SQLStore) RecordContractSpending(ctx context.Context, records []api.ContractSpendingRecord) error {
	if len(records) == 0 {
		return nil // nothing to do
	}

	// Only allow for applying one batch of spending records at a time.
	s.spendingMu.Lock()
	defer s.spendingMu.Unlock()

	squashedRecords := make(map[types.FileContractID]api.ContractSpending)
	latestRevision := make(map[types.FileContractID]uint64)
	latestSize := make(map[types.FileContractID]uint64)
	for _, r := range records {
		squashedRecords[r.ContractID] = squashedRecords[r.ContractID].Add(r.ContractSpending)
		if r.RevisionNumber > latestRevision[r.ContractID] {
			latestRevision[r.ContractID] = r.RevisionNumber
			latestSize[r.ContractID] = r.Size
		}
	}
	for fcid, newSpending := range squashedRecords {
		err := s.retryTransaction(func(tx *gorm.DB) error {
			var contract dbContract
			err := tx.Model(&dbContract{}).
				Where("fcid = ?", fileContractID(fcid)).
				Take(&contract).Error
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil // contract not found, continue with next one
			} else if err != nil {
				return err
			}
			updates := make(map[string]interface{})
			if !newSpending.Uploads.IsZero() {
				updates["upload_spending"] = currency(types.Currency(contract.UploadSpending).Add(newSpending.Uploads))
			}
			if !newSpending.Downloads.IsZero() {
				updates["download_spending"] = currency(types.Currency(contract.DownloadSpending).Add(newSpending.Downloads))
			}
			if !newSpending.FundAccount.IsZero() {
				updates["fund_account_spending"] = currency(types.Currency(contract.FundAccountSpending).Add(newSpending.FundAccount))
			}
			if !newSpending.Deletions.IsZero() {
				updates["delete_spending"] = currency(types.Currency(contract.DeleteSpending).Add(newSpending.Deletions))
			}
			if !newSpending.SectorRoots.IsZero() {
				updates["list_spending"] = currency(types.Currency(contract.ListSpending).Add(newSpending.SectorRoots))
			}
			updates["revision_number"] = latestRevision[fcid]
			updates["size"] = latestSize[fcid]
			return tx.Model(&contract).Updates(updates).Error
		})
		if err != nil {
			return err
		}
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

func pruneSlabs(tx *gorm.DB) error {
	return tx.Exec(`DELETE FROM slabs WHERE slabs.id IN (SELECT * FROM (SELECT sla.id FROM slabs sla
		LEFT JOIN slices sli ON sli.db_slab_id  = sla.id
		WHERE db_object_id IS NULL) toDelete)`).Error
}

func fetchUsedContracts(tx *gorm.DB, usedContracts map[types.PublicKey]types.FileContractID) (map[types.PublicKey]dbContract, error) {
	fcids := make([]fileContractID, 0, len(usedContracts))
	hostForFCID := make(map[types.FileContractID]types.PublicKey, len(usedContracts))
	for hk, fcid := range usedContracts {
		fcids = append(fcids, fileContractID(fcid))
		hostForFCID[fcid] = hk
	}
	var contracts []dbContract
	err := tx.Model(&dbContract{}).
		Where("fcid IN (?)", fcids).
		Find(&contracts).Error
	if err != nil {
		return nil, err
	}
	fetchedContracts := make(map[types.PublicKey]dbContract, len(contracts))
	for _, c := range contracts {
		fetchedContracts[hostForFCID[types.FileContractID(c.FCID)]] = c
	}
	return fetchedContracts, nil
}

func (s *SQLStore) RenameObject(ctx context.Context, keyOld, keyNew string) error {
	tx := s.db.Exec(`UPDATE objects SET object_id = ? WHERE object_id = ?`, keyNew, keyOld)
	if tx.Error != nil {
		return tx.Error
	}
	if tx.RowsAffected == 0 {
		return fmt.Errorf("%w: key %v", api.ErrObjectNotFound, keyOld)
	}
	return nil
}

func (s *SQLStore) RenameObjects(ctx context.Context, prefixOld, prefixNew string) error {
	tx := s.db.Exec("UPDATE objects SET object_id = "+sqlConcat(s.db, "?", "SUBSTR(object_id, ?)")+" WHERE SUBSTR(object_id, 1, ?) = ?",
		prefixNew, utf8.RuneCountInString(prefixOld)+1, utf8.RuneCountInString(prefixOld), prefixOld)
	if tx.Error != nil {
		return tx.Error
	}
	if tx.RowsAffected == 0 {
		return fmt.Errorf("%w: prefix %v", api.ErrObjectNotFound, prefixOld)
	}
	return nil
}

func (s *SQLStore) UpdateObject(ctx context.Context, path, contractSet string, o object.Object, usedContracts map[types.PublicKey]types.FileContractID) error {
	s.objectsMu.Lock()
	defer s.objectsMu.Unlock()

	// Sanity check input.
	for _, ss := range o.Slabs {
		for _, shard := range ss.Shards {
			// Verify that all hosts have a contract.
			_, exists := usedContracts[shard.Host]
			if !exists {
				return fmt.Errorf("missing contract for host %v", shard.Host)
			}
		}
	}

	// UpdateObject is ACID.
	return s.retryTransaction(func(tx *gorm.DB) error {
		// Fetch contract set.
		var cs dbContractSet
		if err := tx.Take(&cs, "name = ?", contractSet).Error; err != nil {
			return err
		}

		// Try to delete. We want to get rid of the object and its
		// slices if it exists.
		_, err := deleteObject(tx, path)
		if err != nil {
			return err
		}

		// Insert a new object.
		objKey, err := o.Key.MarshalText()
		if err != nil {
			return err
		}
		obj := dbObject{
			ObjectID: path,
			Key:      objKey,
			Size:     o.TotalSize(),
		}
		err = tx.Create(&obj).Error
		if err != nil {
			return err
		}

		// Fetch the used contracts.
		contracts, err := fetchUsedContracts(tx, usedContracts)
		if err != nil {
			return err
		}

		for _, ss := range o.Slabs {
			// Create Slab if it doesn't exist yet.
			slabKey, err := ss.Key.MarshalText()
			if err != nil {
				return err
			}
			slab := &dbSlab{
				Key:         slabKey,
				MinShards:   ss.MinShards,
				TotalShards: uint8(len(ss.Shards)),
			}
			err = tx.Where(dbSlab{Key: slabKey}).
				Assign(dbSlab{
					DBContractSetID: cs.ID,
				}).
				FirstOrCreate(&slab).Error
			if err != nil {
				return err
			}

			// Create Slice.
			slice := dbSlice{
				DBSlabID:   slab.ID,
				DBObjectID: obj.ID,
				Offset:     ss.Offset,
				Length:     ss.Length,
			}
			err = tx.Create(&slice).Error
			if err != nil {
				return err
			}

			for _, shard := range ss.Shards {
				// Create sector if it doesn't exist yet.
				var sector dbSector
				err := tx.
					Where(dbSector{Root: shard.Root[:]}).
					Assign(dbSector{
						DBSlabID:   slab.ID,
						LatestHost: publicKey(shard.Host),
					}).
					FirstOrCreate(&sector).
					Error
				if err != nil {
					return err
				}

				// Add contract and host to join tables.
				contract, contractFound := contracts[shard.Host]
				if contractFound {
					err = tx.Model(&sector).Association("Contracts").Append(&contract)
					if err != nil {
						return err
					}
				}
			}
		}

		// Handle partial slab.
		partialSlab := o.PartialSlab
		if partialSlab == nil {
			return nil
		}

		// Find a buffer that is not yet marked as complete.
		// NOTE: We don't need to fetch the data of the buffer since we are just
		// going to append to it. Instead we omit the data and select the length
		// of the data.
		var buffer struct {
			ID       uint
			Filename string
			SlabID   uint
			Size     int64
		}
		err = tx.
			Table("buffered_slabs").
			Select("buffered_slabs.id AS ID, buffered_slabs.size AS Size, buffered_slabs.filename AS Filename, sla.id AS SlabID").
			Joins("INNER JOIN slabs sla ON buffered_slabs.id = sla.db_buffered_slab_id AND sla.min_shards = ? AND sla.total_shards = ?", partialSlab.MinShards, partialSlab.TotalShards).
			Joins("INNER JOIN contract_sets cs ON sla.db_contract_set_id = cs.id AND cs.name = ?", contractSet).
			Where("buffered_slabs.complete = ?",
				false).
			Take(&buffer).
			Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		} else if errors.Is(err, gorm.ErrRecordNotFound) {
			// No buffer found, create a new one.
			return createSlabBuffer(tx, obj.ID, cs.ID, *partialSlab, s.partialSlabDir)
		}

		// We have a buffer. Sanity check it.
		slabSize := bufferedSlabSize(partialSlab.MinShards)
		if buffer.Size >= int64(slabSize) {
			return fmt.Errorf("incomplete buffer with ID %v has no space left, this should never happen", buffer.ID)
		}

		// Create the slice.
		remainingSpace := int64(slabSize) - buffer.Size
		slice := dbSlice{
			DBObjectID: obj.ID,
			DBSlabID:   buffer.SlabID,
			Offset:     uint32(buffer.Size),
		}
		if remainingSpace <= int64(len(partialSlab.Data)) {
			slice.Length = uint32(remainingSpace)
		} else {
			slice.Length = uint32(len(partialSlab.Data))
		}
		if err := tx.Create(&slice).Error; err != nil {
			return err
		}

		// Add the data to the buffer and remember the overflowing data.
		toAppend := partialSlab.Data[:slice.Length]
		overflow := partialSlab.Data[slice.Length:]

		// Append to buffer.
		file, err := os.OpenFile(filepath.Join(s.partialSlabDir, buffer.Filename), os.O_WRONLY, 0600)
		if err != nil {
			return err
		}
		defer file.Close()
		_, err = file.WriteAt(toAppend, buffer.Size)
		if err != nil {
			return err
		}
		if err := file.Sync(); err != nil {
			return err
		}

		// Update buffer.
		err = tx.Model(&dbBufferedSlab{}).
			Where("ID", buffer.ID).
			Updates(map[string]interface{}{
				"complete": buffer.Size+int64(len(toAppend)) >= int64(slabSize)-s.bufferedSlabCompletionThreshold,
				"size":     buffer.Size + int64(len(toAppend)),
			}).Error
		if err != nil {
			return err
		}

		// If there is no overflow, we are done.
		if len(overflow) == 0 {
			return nil
		}

		// Otherwise, create a new buffer with a new slab and slice.
		overflowSlab := *partialSlab
		overflowSlab.Data = overflow
		return createSlabBuffer(tx, obj.ID, cs.ID, overflowSlab, s.partialSlabDir)
	})
}

func bufferedSlabSize(minShards uint8) int {
	return int(rhpv2.SectorSize) * int(minShards)
}

func createSlabBuffer(tx *gorm.DB, objectID, contractSetID uint, partialSlab object.PartialSlab, partialSlabDir string) error {
	if partialSlab.TotalShards == 0 || partialSlab.MinShards == 0 {
		return fmt.Errorf("min shards and total shards must be greater than 0: %v, %v", partialSlab.MinShards, partialSlab.TotalShards)
	}
	if partialSlab.MinShards > partialSlab.TotalShards {
		return fmt.Errorf("min shards must be less than or equal to total shards: %v > %v", partialSlab.MinShards, partialSlab.TotalShards)
	}
	if slabSize := int(rhpv2.SectorSize) * int(partialSlab.MinShards); len(partialSlab.Data) >= slabSize {
		return fmt.Errorf("partial slab data size must be less than %v to count as a partial slab: %v", slabSize, len(partialSlab.Data))
	}
	key, err := object.GenerateEncryptionKey().MarshalText()
	if err != nil {
		return err
	}
	// Create a new buffer and slab.
	identifier := frand.Entropy256()
	fileName := fmt.Sprintf("%v:%v-%v", partialSlab.MinShards, partialSlab.TotalShards, hex.EncodeToString(identifier[:]))
	file, err := os.Create(filepath.Join(partialSlabDir, fileName))
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.Write(partialSlab.Data)
	if err != nil {
		return err
	}
	return tx.Create(&dbBufferedSlab{
		DBSlab: dbSlab{
			DBContractSetID: contractSetID,
			Key:             key, // random placeholder key
			MinShards:       partialSlab.MinShards,
			TotalShards:     partialSlab.TotalShards,
			Slices: []dbSlice{
				{
					DBObjectID: objectID,
					Offset:     0,
					Length:     uint32(len(partialSlab.Data)),
				},
			},
		},
		Complete:    false,
		Size:        int64(len(partialSlab.Data)),
		Filename:    fileName,
		LockedUntil: 0,
	}).Error
}

func (s *SQLStore) RemoveObject(ctx context.Context, key string) error {
	var rowsAffected int64
	var err error
	err = s.retryTransaction(func(tx *gorm.DB) error {
		rowsAffected, err = deleteObject(tx, key)
		return err
	})
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		return fmt.Errorf("%w: key: %s", api.ErrObjectNotFound, key)
	}
	return nil
}

func (s *SQLStore) RemoveObjects(ctx context.Context, prefix string) error {
	var rowsAffected int64
	var err error
	err = s.retryTransaction(func(tx *gorm.DB) error {
		rowsAffected, err = deleteObjects(tx, prefix)
		return err
	})
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		return fmt.Errorf("%w: prefix: %s", api.ErrObjectNotFound, prefix)
	}
	return nil
}

func (s *SQLStore) Slab(ctx context.Context, key object.EncryptionKey) (object.Slab, error) {
	k, err := key.MarshalText()
	if err != nil {
		return object.Slab{}, err
	}
	var slab dbSlab
	tx := s.db.Where(&dbSlab{Key: k}).
		Preload("Shards.Contracts.Host").
		Take(&slab)
	if errors.Is(tx.Error, gorm.ErrRecordNotFound) {
		return object.Slab{}, api.ErrObjectNotFound
	}
	return slab.convert()
}

func (ss *SQLStore) UpdateSlab(ctx context.Context, s object.Slab, contractSet string, usedContracts map[types.PublicKey]types.FileContractID) error {
	ss.objectsMu.Lock()
	defer ss.objectsMu.Unlock()

	// sanity check the shards don't contain an empty root
	for _, s := range s.Shards {
		if s.Root == (types.Hash256{}) {
			return errors.New("shard root can never be the empty root")
		}
	}
	// Sanity check input.
	for _, shard := range s.Shards {
		// Verify that all hosts have a contract.
		_, exists := usedContracts[shard.Host]
		if !exists {
			return fmt.Errorf("missing contract for host %v", shard.Host)
		}
	}

	// extract the slab key
	key, err := s.Key.MarshalText()
	if err != nil {
		return err
	}

	// Update slab.
	return ss.retryTransaction(func(tx *gorm.DB) (err error) {
		// Fetch contract set.
		var cs dbContractSet
		if err := tx.Take(&cs, "name = ?", contractSet).Error; err != nil {
			return err
		}

		// find all contracts
		contracts, err := fetchUsedContracts(tx, usedContracts)
		if err != nil {
			return err
		}

		// find existing slab
		var slab dbSlab
		if err = tx.
			Where(&dbSlab{Key: key}).
			Assign(&dbSlab{
				DBContractSetID: cs.ID,
				TotalShards:     uint8(len(slab.Shards)),
			}).
			Preload("Shards").
			Take(&slab).
			Error; err == gorm.ErrRecordNotFound {
			return fmt.Errorf("slab with key '%s' not found: %w", string(key), err)
		} else if err != nil {
			return err
		}

		// loop updated shards
		for _, shard := range s.Shards {
			// ensure the sector exists
			var sector dbSector
			if err := tx.
				Where(dbSector{Root: shard.Root[:]}).
				Assign(dbSector{
					DBSlabID:   slab.ID,
					LatestHost: publicKey(shard.Host)},
				).
				FirstOrCreate(&sector).
				Error; err != nil {
				return err
			}

			// ensure the associations are updated
			contract, contractFound := contracts[shard.Host]
			if contractFound {
				if err := tx.
					Model(&sector).
					Association("Contracts").
					Append(&contract); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func (s *SQLStore) RefreshHealth(ctx context.Context) error {
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
GROUP BY slabs.id
`)
	return s.retryTransaction(func(tx *gorm.DB) error {
		if isSQLite(s.db) {
			return s.db.Exec("UPDATE slabs SET health = COALESCE((SELECT health FROM (?) WHERE slabs.id = id), 1)", healthQuery).Error
		} else {
			return s.db.Exec("UPDATE slabs sla INNER JOIN (?) h ON sla.id = h.id SET sla.health = h.health", healthQuery).Error
		}
	})
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

	if err := s.db.
		Select("slabs.key, slabs.health").
		Joins("INNER JOIN contract_sets cs ON slabs.db_contract_set_id = cs.id").
		Model(&dbSlab{}).
		Where("health <= ? AND cs.name = ?", healthCutoff, set).
		Order("health ASC").
		Limit(limit).
		Find(&rows).
		Error; err != nil {
		return nil, err
	}

	slabs := make([]api.UnhealthySlab, len(rows))
	for i, row := range rows {
		var key object.EncryptionKey
		if err := key.UnmarshalText(row.Key); err != nil {
			return nil, err
		}
		slabs[i] = api.UnhealthySlab{
			Key:    key,
			Health: row.Health,
		}
	}
	return slabs, nil
}

// object retrieves a raw object from the store.
func (s *SQLStore) object(ctx context.Context, txn *gorm.DB, path string) (rawObject, error) {
	// NOTE: we LEFT JOIN here because empty objects are valid and need to be
	// included in the result set, when we convert the rawObject before
	// returning it we'll check for SlabID and/or SectorID being 0 and act
	// accordingly
	var rows rawObject
	tx := s.db.
		Select("o.key as ObjectKey, o.object_id as ObjectName, o.size as ObjectSize, sli.id as SliceID, sli.offset as SliceOffset, sli.length as SliceLength, sla.id as SlabID, sla.health as SlabHealth, sla.key as SlabKey, sla.min_shards as SlabMinShards, bs.id IS NOT NULL AS SlabBuffered, sec.id as SectorID, sec.root as SectorRoot, sec.latest_host as SectorHost").
		Model(&dbObject{}).
		Table("objects o").
		Joins("LEFT JOIN slices sli ON o.id = sli.`db_object_id`").
		Joins("LEFT JOIN slabs sla ON sli.db_slab_id = sla.`id`").
		Joins("LEFT JOIN sectors sec ON sla.id = sec.`db_slab_id`").
		Joins("LEFT JOIN buffered_slabs bs ON sla.db_buffered_slab_id = bs.`id`").
		Where("o.object_id = ?", path).
		Order("sli.id ASC").
		Order("sec.id ASC").
		Scan(&rows)
	if errors.Is(tx.Error, gorm.ErrRecordNotFound) || len(rows) == 0 {
		return nil, api.ErrObjectNotFound
	}

	return rows, nil
}

// contract retrieves a contract from the store.
func (s *SQLStore) contract(ctx context.Context, id fileContractID) (dbContract, error) {
	return contract(s.db, id)
}

// contracts retrieves all contracts in the given set.
func (s *SQLStore) contracts(ctx context.Context, set string) ([]dbContract, error) {
	var cs dbContractSet
	err := s.db.
		Where(&dbContractSet{Name: set}).
		Preload("Contracts.Host").
		Take(&cs).
		Error

	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, fmt.Errorf("%w '%s'", api.ErrContractSetNotFound, set)
	} else if err != nil {
		return nil, err
	}

	return cs.Contracts, nil
}

// packedSlabsForUpload retrieves up to 'limit' dbBufferedSlab that have their
// 'Complete' flag set to true and locks them using the 'LockedUntil' field
func (s *SQLStore) packedSlabsForUpload(lockingDuration time.Duration, minShards, totalShards uint8, set string, limit int) ([]dbBufferedSlab, error) {
	now := time.Now().Unix()
	lockID := int64(frand.Uint64n(math.MaxInt64))
	err := s.db.Exec(`
		UPDATE buffered_slabs SET locked_until = ?, lock_id = ? WHERE id IN (
			SELECT * FROM (
				SELECT buffered_slabs.id FROM buffered_slabs
				INNER JOIN slabs sla ON sla.db_buffered_slab_id = buffered_slabs.id
				INNER JOIN contract_sets cs ON cs.id = sla.db_contract_set_id
				WHERE complete = ? AND locked_until < ? AND min_shards = ? AND total_shards = ? AND cs.name = ?
				LIMIT ?
			) AS buffer_ids
		)`,
		now+int64(lockingDuration.Seconds()), lockID, true, now, minShards, totalShards, set, limit).
		Error
	if err != nil {
		return nil, err
	}
	var buffers []dbBufferedSlab
	if err := s.db.Find(&buffers, "lock_id = ?", lockID).Error; err != nil {
		return nil, err
	}
	return buffers, nil
}

// PackedSlabsForUpload returns up to 'limit' packed slabs that are ready for
// uploading. They are locked for 'lockingDuration' time before being handed out
// again.
func (s *SQLStore) PackedSlabsForUpload(ctx context.Context, lockingDuration time.Duration, minShards, totalShards uint8, set string, limit int) ([]api.PackedSlab, error) {
	buffers, err := s.packedSlabsForUpload(lockingDuration, minShards, totalShards, set, limit)
	if err != nil {
		return nil, err
	}
	slabs := make([]api.PackedSlab, len(buffers))
	for i, buf := range buffers {
		data, err := os.ReadFile(filepath.Join(s.partialSlabDir, buf.Filename))
		if os.IsNotExist(err) {
			s.alerts.RegisterAlert(ctx, alerts.Alert{
				ID:       types.HashBytes([]byte(buf.Filename)),
				Severity: alerts.SeverityCritical,
				Message:  "buffered slab for upload not found on disk",
				Data: map[string]interface{}{
					"filename": buf.Filename,
					"slabKey":  buf.DBSlab.Key,
				},
				Timestamp: time.Now(),
			})
			s.logger.Error(ctx, fmt.Sprintf("buffered slab %v doesn't exist", buf.Filename))
			continue
		}
		if err != nil {
			return nil, err
		}
		slabs[i] = api.PackedSlab{
			BufferID: buf.ID,
			Data:     data,
		}
	}
	return slabs, nil
}

func (s *SQLStore) ObjectsBySlabKey(ctx context.Context, slabKey object.EncryptionKey) ([]api.ObjectMetadata, error) {
	var objs []struct {
		Name   string
		Size   int64
		Health float64
	}
	key, err := slabKey.MarshalText()
	if err != nil {
		return nil, err
	}
	var query string
	if isSQLite(s.db) {
		query = `
SELECT o.object_id as Name, o.size as Size, sla.health as Health
FROM slabs sla
LEFT JOIN slices sli ON sli.db_slab_id = sla.id
INNER JOIN objects o ON o.id = sli.db_object_id
GROUP BY o.object_id
HAVING sla.key = ?
	`
	} else {
		query = `
SELECT o.object_id as Name, ANY_VALUE(o.size) as Size, ANY_VALUE(sla.health) as Health, ANY_VALUE(sla.key) as slabKey
FROM slabs sla
LEFT JOIN slices sli ON sli.db_slab_id = sla.id
INNER JOIN objects o ON o.id = sli.db_object_id
GROUP BY o.object_id
HAVING slabKey = ?
	`
	}
	err = s.db.Raw(query, key).
		Scan(&objs).
		Error
	if err != nil {
		return nil, err
	}
	metadata := make([]api.ObjectMetadata, len(objs))
	for i, obj := range objs {
		metadata[i] = api.ObjectMetadata{
			Name:   obj.Name,
			Size:   obj.Size,
			Health: obj.Health,
		}
	}
	return metadata, nil
}

// MarkPackedSlabsUploaded marks the given slabs as uploaded and deletes them
// from the buffer.
func (s *SQLStore) MarkPackedSlabsUploaded(ctx context.Context, slabs []api.UploadedPackedSlab, usedContracts map[types.PublicKey]types.FileContractID) error {
	// Sanity check input.
	for _, ss := range slabs {
		for _, shard := range ss.Shards {
			// Verify that all hosts have a contract.
			_, exists := usedContracts[shard.Host]
			if !exists {
				return fmt.Errorf("missing contract for host %v", shard.Host)
			}
		}
	}
	var filename string
	err := s.retryTransaction(func(tx *gorm.DB) error {
		contracts, err := fetchUsedContracts(tx, usedContracts)
		if err != nil {
			return err
		}
		for _, slab := range slabs {
			filename, err = markPackedSlabUploaded(tx, slab, contracts)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("marking slabs as uploaded in the db failed: %w", err)
	}
	if err := os.Remove(filepath.Join(s.partialSlabDir, filename)); err != nil {
		return fmt.Errorf("removing buffer after marking it as uploaded failed: %w", err)
	}
	return nil
}

func markPackedSlabUploaded(tx *gorm.DB, slab api.UploadedPackedSlab, contracts map[types.PublicKey]dbContract) (string, error) {
	// find the slab
	var sla dbSlab
	if err := tx.Where("db_buffered_slab_id", slab.BufferID).
		Take(&sla).Error; err != nil {
		return "", err
	}

	// update the slab
	key, err := slab.Key.MarshalText()
	if err != nil {
		return "", err
	}
	if err := tx.Model(&dbSlab{}).
		Where("id", sla.ID).
		Updates(map[string]interface{}{
			"key":                 key,
			"db_buffered_slab_id": nil,
		}).Error; err != nil {
		return "", err
	}

	// delete buffer
	var buffer dbBufferedSlab
	if err := tx.Take(&buffer, "id = ?", slab.BufferID).Error; err != nil {
		return "", err
	}
	err = tx.Delete(&buffer).
		Error
	if err != nil {
		return "", err
	}

	// add the shards to the slab
	var shards []dbSector
	for i := range slab.Shards {
		contract, exists := contracts[slab.Shards[i].Host]
		if !exists {
			return "", fmt.Errorf("missing contract for host %v", slab.Shards[i].Host)
		}
		shards = append(shards, dbSector{
			DBSlabID:   sla.ID,
			LatestHost: publicKey(slab.Shards[i].Host),
			Root:       slab.Shards[i].Root[:],
			Contracts:  []dbContract{contract},
		})
	}
	return buffer.Filename, tx.Create(shards).Error
}

// contract retrieves a contract from the store.
func contract(tx *gorm.DB, id fileContractID) (contract dbContract, err error) {
	err = tx.
		Where(&dbContract{ContractCommon: ContractCommon{FCID: id}}).
		Preload("Host").
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
		Preload("Host").
		Find(&dbContracts).
		Error
	return
}

// contractsForHost retrieves all contracts for the given host
func contractsForHost(tx *gorm.DB, host dbHost) (contracts []dbContract, err error) {
	err = tx.
		Where(&dbContract{HostID: host.ID}).
		Preload("Host").
		Find(&contracts).
		Error
	return
}

func newContract(hostID uint, fcid, renewedFrom types.FileContractID, totalCost types.Currency, startHeight, windowStart, windowEnd, size uint64) dbContract {
	return dbContract{
		HostID: hostID,

		ContractCommon: ContractCommon{
			FCID:        fileContractID(fcid),
			RenewedFrom: fileContractID(renewedFrom),

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
func addContract(tx *gorm.DB, c rhpv2.ContractRevision, totalCost types.Currency, startHeight uint64, renewedFrom types.FileContractID) (dbContract, error) {
	fcid := c.ID()

	// Find host.
	var host dbHost
	err := tx.Model(&dbHost{}).Where(&dbHost{PublicKey: publicKey(c.HostKey())}).
		Find(&host).Error
	if err != nil {
		return dbContract{}, err
	}

	// Create contract.
	contract := newContract(host.ID, fcid, renewedFrom, totalCost, startHeight, c.Revision.WindowStart, c.Revision.WindowEnd, c.Revision.Filesize)

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

// deleteObject deletes an object from the store and prunes all slabs which are
// without an obect after the deletion. That means in case of packed uploads,
// the slab is only deleted when no more objects point to it.
func deleteObject(tx *gorm.DB, path string) (numDeleted int64, _ error) {
	tx = tx.Where(&dbObject{ObjectID: path}).Delete(&dbObject{})
	if tx.Error != nil {
		return 0, tx.Error
	}
	numDeleted = tx.RowsAffected
	if numDeleted == 0 {
		return 0, nil // nothing to prune if no object was deleted
	}
	if err := pruneSlabs(tx); err != nil {
		return 0, err
	}
	return
}

func deleteObjects(tx *gorm.DB, path string) (numDeleted int64, _ error) {
	tx = tx.Exec("DELETE FROM objects WHERE SUBSTR(object_id, 1, ?) = ?", utf8.RuneCountInString(path), path)
	if tx.Error != nil {
		return 0, tx.Error
	}
	numDeleted = tx.RowsAffected
	if err := pruneSlabs(tx); err != nil {
		return 0, err
	}
	return
}
