package stores

import (
	"errors"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"gorm.io/gorm"
)

const archivalReasonRenewed = "renewed"

var (
	// ErrContractNotFound is returned when a contract can't be retrieved from the
	// database.
	ErrContractNotFound = errors.New("couldn't find contract")

	// ErrContractSetNotFound is returned when a contract can't be retrieved from the
	// database.
	ErrContractSetNotFound = errors.New("couldn't find contract set")
)

type (
	dbContract struct {
		Model

		FCID                fileContractID `gorm:"unique;index;NOT NULL;column:fcid"`
		HostID              uint           `gorm:"index"`
		Host                dbHost
		RenewedFrom         fileContractID `gorm:"index"`
		StartHeight         uint64         `gorm:"index;NOT NULL"`
		TotalCost           currency
		UploadSpending      currency
		DownloadSpending    currency
		FundAccountSpending currency
	}

	dbContractSet struct {
		Model

		Name      string       `gorm:"unique;index"`
		Contracts []dbContract `gorm:"many2many:contract_set_contracts;constraint:OnDelete:CASCADE"`
	}

	dbContractSetContract struct {
		DBContractID    uint `gorm:"primaryKey"`
		DBContractSetID uint `gorm:"primaryKey"`
	}

	dbArchivedContract struct {
		Model
		FCID                fileContractID `gorm:"unique;index;NOT NULL;column:fcid"`
		Host                publicKey      `gorm:"index;NOT NULL"`
		RenewedTo           fileContractID `gorm:"unique;index"`
		Reason              string
		UploadSpending      currency
		DownloadSpending    currency
		FundAccountSpending currency
		StartHeight         uint64 `gorm:"index;NOT NULL"`
	}

	dbContractSector struct {
		DBContractID uint `gorm:"primaryKey"`
		DBSectorID   uint `gorm:"primaryKey"`
	}
)

// TableName implements the gorm.Tabler interface.
func (dbArchivedContract) TableName() string { return "archived_contracts" }

// TableName implements the gorm.Tabler interface.
func (dbContractSector) TableName() string { return "contract_sectors" }

// TableName implements the gorm.Tabler interface.
func (dbContract) TableName() string { return "contracts" }

// TableName implements the gorm.Tabler interface.
func (dbContractSet) TableName() string { return "contract_sets" }

// convert converts a dbContract to a ContractMetadata.
func (c dbContract) convert() api.ContractMetadata {
	return api.ContractMetadata{
		ID:          types.FileContractID(c.FCID),
		HostIP:      c.Host.NetAddress,
		HostKey:     types.PublicKey(c.Host.PublicKey),
		StartHeight: c.StartHeight,
		RenewedFrom: types.FileContractID(c.RenewedFrom),
		TotalCost:   types.Currency(c.TotalCost),
		Spending: api.ContractSpending{
			Uploads:     types.Currency(c.UploadSpending),
			Downloads:   types.Currency(c.DownloadSpending),
			FundAccount: types.Currency(c.FundAccountSpending),
		},
	}
}

// convert converts a dbContract to an ArchivedContract.
func (c dbArchivedContract) convert() api.ArchivedContract {
	return api.ArchivedContract{
		ID:        types.FileContractID(c.FCID),
		HostKey:   types.PublicKey(c.Host),
		RenewedTo: types.FileContractID(c.RenewedTo),

		Spending: api.ContractSpending{
			Uploads:     types.Currency(c.UploadSpending),
			Downloads:   types.Currency(c.DownloadSpending),
			FundAccount: types.Currency(c.FundAccountSpending),
		},
	}
}

// addContract implements the bus.ContractStore interface.
func addContract(tx *gorm.DB, c rhpv2.ContractRevision, totalCost types.Currency, startHeight uint64, renewedFrom types.FileContractID) (dbContract, error) {
	fcid := c.ID()

	// Find host.
	var host dbHost
	err := tx.Where(&dbHost{PublicKey: publicKey(c.HostKey())}).
		Take(&host).Error
	if err != nil {
		return dbContract{}, err
	}

	// Create contract.
	contract := dbContract{
		FCID:        fileContractID(fcid),
		HostID:      host.ID,
		RenewedFrom: fileContractID(renewedFrom),
		StartHeight: startHeight,
		TotalCost:   currency(totalCost),

		// Spending starts at 0.
		UploadSpending:      zeroCurrency,
		DownloadSpending:    zeroCurrency,
		FundAccountSpending: zeroCurrency,
	}

	// Insert contract.
	err = tx.Where(&dbHost{PublicKey: publicKey(c.HostKey())}).
		Create(&contract).Error
	if err != nil {
		return dbContract{}, err
	}
	return contract, nil
}

// AddContract implements the bus.ContractStore interface.
func (s *SQLStore) AddContract(c rhpv2.ContractRevision, totalCost types.Currency, startHeight uint64) (_ api.ContractMetadata, err error) {
	added, err := addContract(s.db, c, totalCost, startHeight, types.FileContractID{})
	if err != nil {
		return api.ContractMetadata{}, err
	}
	return added.convert(), nil
}

// ActiveContracts implements the bus.ContractStore interface.
func (s *SQLStore) ActiveContracts() ([]api.ContractMetadata, error) {
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
func (s *SQLStore) AddRenewedContract(c rhpv2.ContractRevision, totalCost types.Currency, startHeight uint64, renewedFrom types.FileContractID) (api.ContractMetadata, error) {
	var renewed dbContract

	if err := s.db.Transaction(func(tx *gorm.DB) error {
		// Fetch contract we renew from.
		oldContract, err := contract(tx, renewedFrom)
		if err != nil {
			return err
		}

		// Create copy in archive.
		err = tx.Create(&dbArchivedContract{
			FCID:        oldContract.FCID,
			Host:        publicKey(oldContract.Host.PublicKey),
			Reason:      archivalReasonRenewed,
			RenewedTo:   fileContractID(c.ID()),
			StartHeight: oldContract.StartHeight,

			UploadSpending:      oldContract.UploadSpending,
			DownloadSpending:    oldContract.DownloadSpending,
			FundAccountSpending: oldContract.FundAccountSpending,
		}).Error
		if err != nil {
			return err
		}

		// Delete the contract from the regular table.
		err = removeContract(tx, renewedFrom)
		if err != nil {
			return err
		}

		// Add the new contract.
		renewed, err = addContract(tx, c, totalCost, startHeight, renewedFrom)
		return err
	}); err != nil {
		return api.ContractMetadata{}, err
	}

	return renewed.convert(), nil
}

func (s *SQLStore) AncestorContracts(id types.FileContractID, startHeight uint64) ([]api.ArchivedContract, error) {
	var ancestors []dbArchivedContract
	err := s.db.Raw("WITH ancestors AS (SELECT * FROM archived_contracts WHERE renewed_to = ? UNION ALL SELECT archived_contracts.* FROM ancestors, archived_contracts WHERE archived_contracts.renewed_to = ancestors.fcid) SELECT * FROM ancestors WHERE start_height >= ?", fileContractID(id), startHeight).
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

// Contract implements the bus.ContractStore interface.
func (s *SQLStore) Contract(id types.FileContractID) (api.ContractMetadata, error) {
	// Fetch contract.
	contract, err := s.contract(id)
	if err != nil {
		return api.ContractMetadata{}, err
	}
	return contract.convert(), nil
}

// Contracts implements the bus.ContractStore interface.
func (s *SQLStore) Contracts(set string) ([]api.ContractMetadata, error) {
	dbContracts, err := s.contracts(set)
	if err != nil {
		return nil, err
	}
	contracts := make([]api.ContractMetadata, len(dbContracts))
	for i, c := range dbContracts {
		contracts[i] = c.convert()
	}
	return contracts, nil
}

// SetContractSet implements the bus.ContractStore interface.
func (s *SQLStore) SetContractSet(name string, contractIds []types.FileContractID) error {
	encIds := make([]fileContractID, len(contractIds))
	for i, fcid := range contractIds {
		encIds[i] = fileContractID(fcid)
	}

	return s.db.Transaction(func(tx *gorm.DB) error {
		// Delete existing set.
		err := tx.Model(&dbContractSet{}).
			Where("name", name).
			Delete(&dbContractSet{}).
			Error
		if err != nil {
			return err
		}
		// Fetch contracts.
		var dbContracts []dbContract
		err = tx.Model(&dbContract{}).
			Where("fcid in ?", encIds).
			Find(&dbContracts).Error
		if err != nil {
			return err
		}

		// Create set.
		return tx.Create(&dbContractSet{
			Name:      name,
			Contracts: dbContracts,
		}).Error
	})
}

// RemoveContract implements the bus.ContractStore interface.
func (s *SQLStore) RemoveContract(id types.FileContractID) error {
	return removeContract(s.db, id)
}

func (s *SQLStore) contract(id types.FileContractID) (dbContract, error) {
	return contract(s.db, id)
}

func (s *SQLStore) contracts(set string) ([]dbContract, error) {
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

func contract(tx *gorm.DB, id types.FileContractID) (dbContract, error) {
	var contract dbContract
	err := tx.Where(&dbContract{FCID: fileContractID(id)}).
		Preload("Host").
		Take(&contract).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return contract, ErrContractNotFound
	}
	return contract, err
}

func removeContract(tx *gorm.DB, id types.FileContractID) error {
	var contract dbContract
	if err := tx.Where(&dbContract{FCID: fileContractID(id)}).
		Take(&contract).Error; err != nil {
		return err
	}
	return tx.Where(&dbContract{Model: Model{ID: contract.ID}}).
		Delete(&contract).Error
}
