package stores

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/object"
	"gorm.io/gorm"
)

const (
	// archivalReasonRenewed describes why a contract was archived
	archivalReasonRenewed = "renewed"

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

	// ErrContractSetNotFound is returned when a contract can't be retrieved
	// from the database.
	ErrContractSetNotFound = errors.New("couldn't find contract set")
)

type (
	dbArchivedContract struct {
		Model

		ContractCommon
		RenewedTo fileContractID `gorm:"unique;index;size:32"`

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
	}

	dbSlice struct {
		Model
		DBObjectID uint `gorm:"index"`

		// Slice related fields.
		Slab   dbSlab `gorm:"constraint:OnDelete:CASCADE"` // CASCADE to delete slabs too
		Offset uint32
		Length uint32
	}

	dbSlab struct {
		Model
		DBSliceID uint `gorm:"index"`

		Key         []byte `gorm:"unique;NOT NULL;size:68"` // json string
		MinShards   uint8
		TotalShards uint8
		Shards      []dbShard `gorm:"constraint:OnDelete:CASCADE"` // CASCADE to delete shards too
	}

	dbSector struct {
		Model

		LatestHost publicKey `gorm:"NOT NULL"`
		Root       []byte    `gorm:"index;unique;NOT NULL;size:32"`

		Contracts []dbContract `gorm:"many2many:contract_sectors;constraint:OnDelete:CASCADE"`
		Hosts     []dbHost     `gorm:"many2many:host_sectors;constraint:OnDelete:CASCADE"`
	}

	// dbContractSector is a join table between dbContract and dbSector.
	dbContractSector struct {
		DBContractID uint `gorm:"primaryKey"`
		DBSectorID   uint `gorm:"primaryKey"`
	}

	// dbShard is a join table between dbSlab and dbSector.
	dbShard struct {
		ID         uint `gorm:"primaryKey"`
		DBSlabID   uint `gorm:"index"`
		DBSector   dbSector
		DBSectorID uint `gorm:"index"`
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
func (dbShard) TableName() string { return "shards" }

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

	// hydrate shards if possible
	for i, shard := range s.Shards {
		if shard.DBSector.ID == 0 {
			continue // sector wasn't preloaded
		}

		slab.Shards[i].Host = types.PublicKey(shard.DBSector.LatestHost)
		slab.Shards[i].Root = *(*types.Hash256)(shard.DBSector.Root)
	}

	return
}

// convert turns a dbObject into a object.Object.
func (o dbObject) convert() (object.Object, error) {
	var objKey object.EncryptionKey
	if err := objKey.UnmarshalText(o.Key); err != nil {
		return object.Object{}, err
	}
	obj := object.Object{
		Key:   objKey,
		Slabs: make([]object.SlabSlice, len(o.Slabs)),
	}
	for i, sl := range o.Slabs {
		slab, err := sl.Slab.convert()
		if err != nil {
			return object.Object{}, err
		}
		obj.Slabs[i] = object.SlabSlice{
			Slab:   slab,
			Offset: sl.Offset,
			Length: sl.Length,
		}
	}
	return obj, nil
}

func (s *SQLStore) AddContract(ctx context.Context, c rhpv2.ContractRevision, totalCost types.Currency, startHeight uint64) (_ api.ContractMetadata, err error) {
	added, err := addContract(s.db, c, totalCost, startHeight, types.FileContractID{})
	if err != nil {
		return api.ContractMetadata{}, err
	}
	s.knownContracts[c.ID()] = struct{}{}
	return added.convert(), nil
}

func (s *SQLStore) ActiveContracts(ctx context.Context) ([]api.ContractMetadata, error) {
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
			Reason:    archivalReasonRenewed,
			RenewedTo: fileContractID(c.ID()),

			ContractCommon: ContractCommon{
				FCID:        oldContract.FCID,
				RenewedFrom: oldContract.RenewedFrom,

				TotalCost:      oldContract.TotalCost,
				ProofHeight:    oldContract.ProofHeight,
				RevisionHeight: oldContract.RevisionHeight,
				RevisionNumber: oldContract.RevisionNumber,
				StartHeight:    oldContract.StartHeight,
				WindowStart:    oldContract.WindowStart,
				WindowEnd:      oldContract.WindowEnd,

				UploadSpending:      oldContract.UploadSpending,
				DownloadSpending:    oldContract.DownloadSpending,
				FundAccountSpending: oldContract.FundAccountSpending,
			},
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
		return removeContract(tx, fileContractID(renewedFrom))
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

func (s *SQLStore) Contract(ctx context.Context, id types.FileContractID) (api.ContractMetadata, error) {
	contract, err := s.contract(ctx, fileContractID(id))
	if err != nil {
		return api.ContractMetadata{}, err
	}
	return contract.convert(), nil
}

func (s *SQLStore) Contracts(ctx context.Context, set string) ([]api.ContractMetadata, error) {
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

	// fetch contracts
	var dbContracts []dbContract
	err := s.db.
		Model(&dbContract{}).
		Where("fcid IN (?)", fcids).
		Find(&dbContracts).
		Error
	if err != nil {
		return err
	}

	// create contract set
	var contractset dbContractSet
	err = s.db.
		Where(dbContractSet{Name: name}).
		FirstOrCreate(&contractset).
		Error
	if err != nil {
		return err
	}

	// update contracts
	return s.db.Model(&contractset).Association("Contracts").Replace(&dbContracts)
}

func (s *SQLStore) DeleteContractSet(ctx context.Context, name string) error {
	return s.db.
		Where(dbContractSet{Name: name}).
		Delete(&dbContractSet{}).
		Error
}

func (s *SQLStore) RemoveContract(ctx context.Context, id types.FileContractID) error {
	err := removeContract(s.db, fileContractID(id))
	if err != nil {
		return err
	}
	delete(s.knownContracts, id)
	return nil
}

func (s *SQLStore) RemoveContracts(ctx context.Context) error {
	if err := s.db.Where("TRUE").Delete(&dbContract{}).Error; err != nil {
		return err
	}
	s.knownContracts = make(map[types.FileContractID]struct{})
	return nil
}

func (s *SQLStore) SearchObjects(ctx context.Context, substring string, offset, limit int) ([]string, error) {
	if limit <= -1 {
		limit = math.MaxInt
	}

	var ids []string
	err := s.db.Model(&dbObject{}).
		Select("object_id").
		Where("object_id LIKE ?", "%"+substring+"%").
		Offset(offset).
		Limit(limit).
		Scan(&ids).Error
	return ids, err
}

func (s *SQLStore) ObjectEntries(ctx context.Context, path, prefix string, offset, limit int) ([]string, error) {
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
	query := s.db.Raw(fmt.Sprintf(`SELECT CASE slashindex WHEN 0 THEN %s ELSE %s END AS result
	FROM (
		SELECT trimmed, INSTR(trimmed, ?) AS slashindex
		FROM (
			SELECT SUBSTR(object_id, ?) AS trimmed
			FROM objects
			WHERE object_id LIKE ?
		) AS i
	) AS m
	GROUP BY result
	LIMIT ? OFFSET ?`, concat("?", "trimmed"), concat("?", "substr(trimmed, 1, slashindex)")), path, path, "/", len(path)+1, path+"%", limit, offset)

	// apply prefix
	if prefix != "" {
		query = s.db.Raw(fmt.Sprintf("SELECT * FROM (?) AS i WHERE result LIKE %s", concat("?", "?")), query, path, prefix+"%")
	}

	var entries []string
	err := query.Scan(&entries).Error
	if err != nil {
		return nil, err
	}
	return entries, nil
}

func (s *SQLStore) Object(ctx context.Context, key string) (object.Object, error) {
	obj, err := s.object(ctx, key)
	if err != nil {
		return object.Object{}, err
	}
	return obj.convert()
}

func (s *SQLStore) RecordContractSpending(ctx context.Context, records []api.ContractSpendingRecord) error {
	squashedRecords := make(map[types.FileContractID]api.ContractSpending)
	for _, r := range records {
		squashedRecords[r.ContractID] = squashedRecords[r.ContractID].Add(r.ContractSpending)
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
			return tx.Model(&contract).Updates(updates).Error
		})
		if err != nil {
			return err
		}
	}
	return nil
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
		// slabs if it exists.
		err := removeObject(tx, key)
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
		}
		err = tx.Create(&obj).Error
		if err != nil {
			return err
		}

		for _, ss := range o.Slabs {
			// Create Slice.
			slice := dbSlice{
				DBObjectID: obj.ID,
				Offset:     ss.Offset,
				Length:     ss.Length,
			}
			err = tx.Create(&slice).Error
			if err != nil {
				return err
			}

			// Create Slab.
			slabKey, err := ss.Key.MarshalText()
			if err != nil {
				return err
			}
			slab := &dbSlab{
				DBSliceID:   slice.ID,
				Key:         slabKey,
				MinShards:   ss.MinShards,
				TotalShards: uint8(len(ss.Shards)),
			}
			err = tx.Create(&slab).Error
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
					Assign(dbSector{LatestHost: publicKey(shard.Host)}).
					FirstOrCreate(&sector).
					Error
				if err != nil {
					return err
				}

				// Add the slab-sector link to the sector to the
				// shards table.
				err = tx.Create(&dbShard{
					DBSlabID:   slab.ID,
					DBSectorID: sector.ID,
				}).Error
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

				// Look for the host referenced by the shard.
				hostFound := true
				var host dbHost
				err = tx.Model(&dbHost{}).
					Where(&dbHost{PublicKey: publicKey(shard.Host)}).
					Take(&host).Error
				if errors.Is(err, gorm.ErrRecordNotFound) {
					hostFound = false
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
				if hostFound {
					err = tx.Model(&sector).Association("Hosts").Append(&host)
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
	return removeObject(s.db, key)
}

func (ss *SQLStore) UpdateSlab(ctx context.Context, s object.Slab, usedContracts map[types.PublicKey]types.FileContractID) error {
	// extract the slab key
	key, err := s.Key.MarshalText()
	if err != nil {
		return err
	}

	// extract host keys
	hostkeys := make([]publicKey, 0, len(usedContracts))
	for key := range usedContracts {
		hostkeys = append(hostkeys, publicKey(key))
	}

	// extract file contract ids
	fcids := make([]fileContractID, 0, len(usedContracts))
	for _, fcid := range usedContracts {
		fcids = append(fcids, fileContractID(fcid))
	}

	// find all hosts
	var dbHosts []dbHost
	if err := ss.db.
		Model(&dbHost{}).
		Where("public_key IN (?)", hostkeys).
		Find(&dbHosts).
		Error; err != nil {
		return err
	}

	// find all contracts
	var dbContracts []dbContract
	if err := ss.db.
		Model(&dbContract{}).
		Where("fcid IN (?)", fcids).
		Find(&dbContracts).
		Error; err != nil {
		return err
	}

	// make a hosts map
	hosts := make(map[publicKey]*dbHost)
	for i := range dbHosts {
		hosts[dbHosts[i].PublicKey] = &dbHosts[i]
	}

	// make a contracts map
	contracts := make(map[fileContractID]*dbContract)
	for i := range dbContracts {
		contracts[fileContractID(dbContracts[i].FCID)] = &dbContracts[i]
	}

	// find existing slab
	var slab dbSlab
	if err = ss.db.
		Where(&dbSlab{Key: key}).
		Assign(&dbSlab{TotalShards: uint8(len(slab.Shards))}).
		Preload("Shards.DBSector").
		Take(&slab).
		Error; err == gorm.ErrRecordNotFound {
		return fmt.Errorf("slab with key '%s' not found: %w", string(key), err)
	} else if err != nil {
		return err
	}

	// Update slab.
	return ss.retryTransaction(func(tx *gorm.DB) (err error) {
		// build map out of current shards
		shards := make(map[uint]struct{})
		for _, shard := range slab.Shards {
			shards[shard.DBSectorID] = struct{}{}
		}

		// loop updated shards
		for _, shard := range s.Shards {
			// ensure the sector exists
			var sector dbSector
			if err := tx.
				Where(dbSector{Root: shard.Root[:]}).
				Assign(dbSector{LatestHost: publicKey(shard.Host)}).
				FirstOrCreate(&sector).
				Error; err != nil {
				return err
			}

			// ensure the join table has an entry
			_, exists := shards[sector.ID]
			if !exists {
				if err := tx.
					Create(&dbShard{
						DBSlabID:   slab.ID,
						DBSectorID: sector.ID,
					}).Error; err != nil {
					return err
				}
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
			if host := hosts[publicKey(shard.Host)]; host != nil {
				if err := tx.
					Model(&sector).
					Association("Hosts").
					Append(host); err != nil {
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
					  0
					ELSE
					  1
					END
				  ELSE
				  CAST((COUNT(DISTINCT(c.host_id)) - slabs.min_shards) AS FLOAT) / Cast(slabs.total_shards - slabs.min_shards AS FLOAT)
				  END AS health`).
		Model(&dbSlab{}).
		Joins("INNER JOIN shards sh ON sh.db_slab_id = slabs.id").
		Joins("INNER JOIN sectors s ON sh.db_sector_id = s.id").
		Joins("LEFT JOIN contract_sectors se USING (db_sector_id)").
		Joins("LEFT JOIN contracts c ON se.db_contract_id = c.id").
		Joins("INNER JOIN contract_set_contracts csc ON csc.db_contract_id = c.id").
		Joins("INNER JOIN contract_sets cs ON cs.id = csc.db_contract_set_id").
		Where("cs.name = ?", set).
		Group("slabs.id").
		Having("health <= ?", healthCutoff).
		Order("health ASC").
		Limit(limit).
		Preload("Shards.DBSector").
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

// object retrieves an object from the store.
func (s *SQLStore) object(ctx context.Context, key string) (dbObject, error) {
	var obj dbObject
	tx := s.db.Where(&dbObject{ObjectID: key}).
		Preload("Slabs.Slab.Shards.DBSector.Contracts.Host").
		Take(&obj)
	if errors.Is(tx.Error, gorm.ErrRecordNotFound) {
		return dbObject{}, api.ErrObjectNotFound
	}
	return obj, nil
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
		return nil, ErrContractSetNotFound
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

// removeObject removes an object from the store.
func removeObject(tx *gorm.DB, key string) error {
	return tx.Where(&dbObject{ObjectID: key}).Delete(&dbObject{}).Error
}

// removeContract removes a contract from the store.
func removeContract(tx *gorm.DB, id fileContractID) error {
	return tx.
		Where(&dbContract{ContractCommon: ContractCommon{FCID: id}}).
		Delete(&dbContract{}).
		Error
}
