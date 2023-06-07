package stores

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"unicode/utf8"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/object"
	"gorm.io/gorm"
)

const (
	// slabRetrievalBatchSize is the number of slabs we fetch from the
	// database per batch
	// NOTE: This value can't be too big or otherwise UnhealthySlabs will fail
	// due to "too many SQL variables".
	slabRetrievalBatchSize = 100
)

var (
	// ErrSlabNotFound is returned if get is unable to retrieve a slab from the
	// database.
	ErrSlabNotFound = errors.New("slab not found in database")

	// ErrContractNotFound is returned when a contract can't be retrieved from
	// the database.
	ErrContractNotFound = errors.New("couldn't find contract")
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
		StartHeight    uint64 `gorm:"index;NOT NULL"`
		WindowStart    uint64 `gorm:"index;NOT NULL;default:0"`
		WindowEnd      uint64 `gorm:"index;NOT NULL;default:0"`

		// spending fields
		UploadSpending      currency
		DownloadSpending    currency
		FundAccountSpending currency
	}

	dbContractSet struct {
		Model

		Name      string       `gorm:"unique;index"`
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

		Key         []byte `gorm:"unique;NOT NULL;size:68"` // json string
		MinShards   uint8
		TotalShards uint8

		Slices []dbSlice  `gorm:"constraint:OnDelete:SET NULL"`
		Shards []dbSector `gorm:"constraint:OnDelete:CASCADE"` // CASCADE to delete shards too
	}

	dbSector struct {
		Model

		DBSlabID   uint      `gorm:"index;NOT NULL"`
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
		ObjectKey []byte

		// slice
		SliceOffset uint32
		SliceLength uint32

		// slab
		SlabID        uint
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
		StartHeight:    c.StartHeight,
		WindowStart:    c.WindowStart,
		WindowEnd:      c.WindowEnd,

		Spending: api.ContractSpending{
			Uploads:     types.Currency(c.UploadSpending),
			Downloads:   types.Currency(c.DownloadSpending),
			FundAccount: types.Currency(c.FundAccountSpending),
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
		},
		ProofHeight:    c.ProofHeight,
		RevisionHeight: c.RevisionHeight,
		RevisionNumber: revisionNumber,
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

func (raw rawObject) convert() (obj object.Object, _ error) {
	if len(raw) == 0 {
		return object.Object{}, errors.New("no slabs found")
	}

	// unmarshal key
	if err := obj.Key.UnmarshalText(raw[0].ObjectKey); err != nil {
		return object.Object{}, err
	}

	// create a helper function to add a slab and update the state
	slabs := make([]object.SlabSlice, 0, len(raw))
	curr := raw[0].SlabID
	var start int
	addSlab := func(end int, id uint) error {
		if slab, err := raw[start:end].toSlabSlice(); err != nil {
			return err
		} else {
			slabs = append(slabs, slab)
			curr = id
			start = end
		}
		return nil
	}

	// hydrate all slabs
	for j, sector := range raw {
		if sector.SlabID != curr {
			if err := addSlab(j, sector.SlabID); err != nil {
				return object.Object{}, err
			}
		}
	}
	if err := addSlab(len(raw), 0); err != nil {
		return object.Object{}, err
	}

	// hydrate all fields
	obj.Slabs = slabs
	return obj, nil
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
	slice.Slab.Shards = sectors
	slice.Slab.MinShards = raw[0].SlabMinShards
	slice.Offset = raw[0].SliceOffset
	slice.Length = raw[0].SliceLength
	return slice, nil
}

// ObjectsStats returns some info related to the objects stored in the store. To
// reduce locking and make sure all results are consistent, everything is done
// within a single transaction.
func (s *SQLStore) ObjectsStats(ctx context.Context) (api.ObjectsStats, error) {
	var resp api.ObjectsStats
	return resp, s.db.Transaction(func(tx *gorm.DB) error {
		// Number of objects.
		err := tx.
			Model(&dbObject{}).
			Select("COUNT(*)").
			Scan(&resp.NumObjects).
			Error
		if err != nil {
			return err
		}

		// Size of objects.
		if resp.NumObjects > 0 {
			err = tx.
				Model(&dbSlice{}).
				Select("SUM(length)").
				Scan(&resp.TotalObjectsSize).
				Error
			if err != nil {
				return err
			}
		}

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

func (s *SQLStore) AddContract(ctx context.Context, c rhpv2.ContractRevision, totalCost types.Currency, startHeight uint64) (_ api.ContractMetadata, err error) {
	var added dbContract
	if err = s.retryTransaction(func(tx *gorm.DB) error {
		added, err = addContract(tx, c, totalCost, startHeight, types.FileContractID{})
		return err
	}); err != nil {
		return
	}

	s.knownContracts[types.FileContractID(added.FCID)] = struct{}{}
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

		// Add the new contract.
		renewed, err = addContract(tx, c, totalCost, startHeight, renewedFrom)
		if err != nil {
			return err
		}
		s.knownContracts[c.ID()] = struct{}{}

		// Update the old contract in the contract set to the new one.
		err = tx.Table("contract_set_contracts").
			Where("db_contract_id = ?", oldContract.ID).
			Update("db_contract_id", renewed.ID).Error
		if err != nil {
			return err
		}

		// Update the contract_sectors table from the old contract to the new
		// one.
		err = tx.Table("contract_sectors").
			Where("db_contract_id = ?", oldContract.ID).
			Update("db_contract_id", renewed.ID).Error
		if err != nil {
			return err
		}

		// Finally delete the old contract.
		res := tx.Delete(&oldContract)
		if err := res.Error; err != nil {
			return err
		}
		if res.RowsAffected != 1 {
			return fmt.Errorf("expected to delete 1 row, deleted %d", res.RowsAffected)
		}

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

func (s *SQLStore) SearchObjects(ctx context.Context, substring string, offset, limit int) ([]api.ObjectMetadata, error) {
	if limit <= -1 {
		limit = math.MaxInt
	}

	var objects []dbObject
	err := s.db.Model(&dbObject{}).
		Where("object_id LIKE ?", "%"+substring+"%").
		Offset(offset).
		Limit(limit).
		Find(&objects).Error
	if err != nil {
		return nil, err
	}
	metadata := make([]api.ObjectMetadata, len(objects))
	for i, entry := range objects {
		metadata[i] = entry.metadata()
	}
	return metadata, nil
}

func (s *SQLStore) ObjectEntries(ctx context.Context, path, prefix string, offset, limit int) ([]api.ObjectMetadata, error) {
	if limit <= -1 {
		limit = math.MaxInt
	}
	if !strings.HasSuffix(path, "/") {
		panic("path must end in /")
	}

	concat := func(a, b string) string {
		if isSQLite(s.db) {
			return fmt.Sprintf("%s || %s", a, b)
		}
		return fmt.Sprintf("CONCAT(%s, %s)", a, b)
	}

	// base query
	query := s.db.Raw(fmt.Sprintf(`SELECT SUM(size) AS size, CASE slashindex WHEN 0 THEN %s ELSE %s END AS name
	FROM (
		SELECT size, trimmed, INSTR(trimmed, ?) AS slashindex
		FROM (
			SELECT size, SUBSTR(object_id, ?) AS trimmed
			FROM objects
			WHERE object_id LIKE ?
		) AS i
	) AS m
	GROUP BY name
	LIMIT ? OFFSET ?`, concat("?", "trimmed"), concat("?", "substr(trimmed, 1, slashindex)")), path, path, "/", utf8.RuneCountInString(path)+1, path+"%", limit, offset)

	// apply prefix
	if prefix != "" {
		query = s.db.Raw(fmt.Sprintf("SELECT * FROM (?) AS i WHERE name LIKE %s", concat("?", "?")), query, path, prefix+"%")
	}

	var metadata []api.ObjectMetadata
	err := query.Scan(&metadata).Error
	if err != nil {
		return nil, err
	}
	return metadata, nil
}

func (s *SQLStore) Object(ctx context.Context, key string) (object.Object, error) {
	obj, err := s.object(ctx, key)
	if err != nil {
		return object.Object{}, err
	}
	return obj.convert()
}

func (s *SQLStore) RecordContractSpending(ctx context.Context, records []api.ContractSpendingRecord) error {
	if len(records) == 0 {
		return nil // nothing to do
	}
	squashedRecords := make(map[types.FileContractID]api.ContractSpending)
	latestRevision := make(map[types.FileContractID]uint64)
	for _, r := range records {
		squashedRecords[r.ContractID] = squashedRecords[r.ContractID].Add(r.ContractSpending)
		if r.RevisionNumber > latestRevision[r.ContractID] {
			latestRevision[r.ContractID] = r.RevisionNumber
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
			updates["revision_number"] = latestRevision[fcid]
			return tx.Model(&contract).Updates(updates).Error
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func pruneSlabs(tx *gorm.DB) error {
	return tx.Exec(`DELETE FROM slabs WHERE slabs.id IN (SELECT * FROM (SELECT sla.id FROM slabs sla
		LEFT JOIN slices sli ON sli.db_slab_id  = sla.id
		WHERE db_object_id IS NULL) toDelete)`).Error
}

func (s *SQLStore) UpdateObject(ctx context.Context, key string, o object.Object, usedContracts map[types.PublicKey]types.FileContractID) error {
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
		// Try to delete first. We want to get rid of the object and its
		// slices if it exists.
		err := deleteObject(tx, key)
		if err != nil {
			return err
		}

		// Insert a new object.
		objKey, err := o.Key.MarshalText()
		if err != nil {
			return err
		}
		obj := dbObject{
			ObjectID: key,
			Key:      objKey,
			Size:     o.Size(),
		}
		err = tx.Create(&obj).Error
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
				// Translate pubkey to contract.
				fcid := usedContracts[shard.Host]

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

				// Look for the contract referenced by the shard.
				contractFound := true
				var contract dbContract
				err = tx.Model(&dbContract{}).
					Where(&dbContract{ContractCommon: ContractCommon{FCID: fileContractID(fcid)}}).
					Take(&contract).Error
				if errors.Is(err, gorm.ErrRecordNotFound) {
					contractFound = false
				} else if err != nil {
					return err
				}

				// Add contract and host to join tables.
				if contractFound {
					err = tx.Model(&sector).Association("Contracts").Append(&contract)
					if err != nil {
						return err
					}
				}
			}
		}
		return nil
	})
}

func (s *SQLStore) RemoveObject(ctx context.Context, key string) error {
	return deleteObject(s.db, key)
}

func (ss *SQLStore) UpdateSlab(ctx context.Context, s object.Slab, usedContracts map[types.PublicKey]types.FileContractID) error {
	// sanity check the shards don't contain an empty root
	for _, s := range s.Shards {
		if s.Root == (types.Hash256{}) {
			return errors.New("shard root can never be the empty root")
		}
	}

	// extract the slab key
	key, err := s.Key.MarshalText()
	if err != nil {
		return err
	}

	// extract file contract ids
	fcids := make([]fileContractID, 0, len(usedContracts))
	for _, fcid := range usedContracts {
		fcids = append(fcids, fileContractID(fcid))
	}

	// Update slab.
	return ss.retryTransaction(func(tx *gorm.DB) (err error) {
		// find all contracts
		var dbContracts []dbContract
		if err := tx.
			Model(&dbContract{}).
			Where("fcid IN (?)", fcids).
			Find(&dbContracts).
			Error; err != nil {
			return err
		}

		// make a contracts map
		contracts := make(map[fileContractID]*dbContract)
		for i := range dbContracts {
			contracts[fileContractID(dbContracts[i].FCID)] = &dbContracts[i]
		}

		// find existing slab
		var slab dbSlab
		if err = tx.
			Where(&dbSlab{Key: key}).
			Assign(&dbSlab{TotalShards: uint8(len(slab.Shards))}).
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
			if contract := contracts[fileContractID(usedContracts[shard.Host])]; contract != nil {
				if err := tx.
					Model(&sector).
					Association("Contracts").
					Append(contract); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// UnhealthySlabs returns up to 'limit' slabs that do not reach full redundancy
// in the given contract set. These slabs need to be migrated to good contracts
// so they are restored to full health.
func (s *SQLStore) UnhealthySlabs(ctx context.Context, healthCutoff float64, set string, limit int) ([]object.Slab, error) {
	if limit <= -1 {
		limit = math.MaxInt
	}

	var dbBatch []dbSlab
	var slabs []object.Slab

	if err := s.db.
		Select(`slabs.*,
		        CASE
				  WHEN (slabs.min_shards = slabs.total_shards)
				  THEN
				    CASE
					WHEN (COUNT(DISTINCT(c.host_id)) < slabs.min_shards)
					THEN
					  -1
					ELSE
					  1
					END
				  ELSE
				  (CAST(COUNT(DISTINCT(c.host_id)) AS FLOAT) - CAST(slabs.min_shards AS FLOAT)) / Cast(slabs.total_shards - slabs.min_shards AS FLOAT)
				  END AS health`).
		Model(&dbSlab{}).
		Joins("INNER JOIN sectors s ON s.db_slab_id = slabs.id").
		Joins("LEFT JOIN contract_sectors se ON s.id = se.db_sector_id").
		Joins("LEFT JOIN contracts c ON se.db_contract_id = c.id").
		Joins("INNER JOIN contract_set_contracts csc ON csc.db_contract_id = c.id").
		Joins("INNER JOIN contract_sets cs ON cs.id = csc.db_contract_set_id").
		Where("cs.name = ?", set).
		Group("slabs.id").
		Having("health >= 0 AND health <= ?", healthCutoff).
		Order("health ASC").
		Limit(limit).
		Preload("Shards").
		FindInBatches(&dbBatch, slabRetrievalBatchSize, func(tx *gorm.DB, batch int) error {
			for _, dbSlab := range dbBatch {
				if slab, err := dbSlab.convert(); err == nil {
					slabs = append(slabs, slab)
				} else {
					panic(err)
				}
			}
			return nil
		}).
		Error; err != nil {
		return nil, err
	}

	return slabs, nil
}

// object retrieves a raw object from the store.
func (s *SQLStore) object(ctx context.Context, key string) (rawObject, error) {
	var rows rawObject
	tx := s.db.
		Select("o.key as ObjectKey, sli.id as SliceID, sli.offset as SliceOffset, sli.length as SliceLength, sla.id as SlabID, sla.key as SlabKey, sla.min_shards as SlabMinShards, sec.id as SectorID, sec.root as SectorRoot, sec.latest_host as SectorHost").
		Model(&dbObject{}).
		Table("objects o").
		Joins("INNER JOIN slices sli ON o.id = sli.`db_object_id`").
		Joins("INNER JOIN slabs sla ON sli.db_slab_id = sla.`id`").
		Joins("INNER JOIN sectors sec ON sla.id = sec.`db_slab_id`").
		Where("o.object_id = ?", key).
		Order("o.id ASC").
		Order("sla.id ASC").
		Order("sli.offset ASC").
		Order("sec.id ASC").
		Scan(&rows)

	if errors.Is(tx.Error, gorm.ErrRecordNotFound) {
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

// contract retrieves a contract from the store.
func contract(tx *gorm.DB, id fileContractID) (contract dbContract, err error) {
	err = tx.
		Where(&dbContract{ContractCommon: ContractCommon{FCID: id}}).
		Preload("Host").
		Take(&contract).
		Error

	if errors.Is(err, gorm.ErrRecordNotFound) {
		err = ErrContractNotFound
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
	contract := dbContract{
		HostID: host.ID,

		ContractCommon: ContractCommon{
			FCID:        fileContractID(fcid),
			RenewedFrom: fileContractID(renewedFrom),

			TotalCost:      currency(totalCost),
			RevisionNumber: "0",
			StartHeight:    startHeight,
			WindowStart:    c.Revision.WindowStart,
			WindowEnd:      c.Revision.WindowEnd,

			UploadSpending:      zeroCurrency,
			DownloadSpending:    zeroCurrency,
			FundAccountSpending: zeroCurrency,
		},
	}

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
func deleteObject(tx *gorm.DB, key string) error {
	if err := tx.Where(&dbObject{ObjectID: key}).Delete(&dbObject{}).Error; err != nil {
		return err
	}
	return pruneSlabs(tx)
}
