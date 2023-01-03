package stores

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"math/big"
	"time"

	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/consensus"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"go.sia.tech/siad/types"
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

		FCID                types.FileContractID `gorm:"unique;index;type:bytes;serializer:gob;NOT NULL;column:fcid"`
		HostID              uint                 `gorm:"index"`
		Host                dbHost
		LockedUntil         time.Time
		RenewedFrom         types.FileContractID `gorm:"index;type:bytes;serializer:gob"`
		StartHeight         uint64               `gorm:"index;NOT NULL"`
		TotalCost           *big.Int             `gorm:"type:bytes;serializer:gob"`
		UploadSpending      *big.Int             `gorm:"type:bytes;serializer:gob"`
		DownloadSpending    *big.Int             `gorm:"type:bytes;serializer:gob"`
		FundAccountSpending *big.Int             `gorm:"type:bytes;serializer:gob"`
	}

	dbArchivedContract struct {
		Model
		FCID                types.FileContractID `gorm:"unique;index;type:bytes;serializer:gob;NOT NULL;column:fcid"`
		Host                consensus.PublicKey  `gorm:"index;type:bytes;serializer:gob;NOT NULL"`
		RenewedTo           types.FileContractID `gorm:"unique;index;type:bytes;serializer:gob"`
		Reason              string
		UploadSpending      *big.Int `gorm:"type:bytes;serializer:gob"`
		DownloadSpending    *big.Int `gorm:"type:bytes;serializer:gob"`
		FundAccountSpending *big.Int `gorm:"type:bytes;serializer:gob"`
		StartHeight         uint64   `gorm:"index;NOT NULL"`
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

// convert converts a dbContract to a Contract type.
func (c dbContract) convert() api.ContractMetadata {
	return api.ContractMetadata{
		ID:          c.FCID,
		HostIP:      c.Host.NetAddress(),
		HostKey:     c.Host.PublicKey,
		StartHeight: c.StartHeight,
		RenewedFrom: c.RenewedFrom,
		TotalCost:   types.NewCurrency(c.TotalCost),
		Spending: api.ContractSpending{
			Uploads:     types.NewCurrency(c.UploadSpending),
			Downloads:   types.NewCurrency(c.DownloadSpending),
			FundAccount: types.NewCurrency(c.FundAccountSpending),
		},
	}
}

// convert converts a dbContract to a ArchivedContract.
func (c dbArchivedContract) convert() api.ArchivedContract {
	return api.ArchivedContract{
		ID:        c.FCID,
		HostKey:   c.Host,
		RenewedTo: c.RenewedTo,

		Spending: api.ContractSpending{
			Uploads:     types.NewCurrency(c.UploadSpending),
			Downloads:   types.NewCurrency(c.DownloadSpending),
			FundAccount: types.NewCurrency(c.FundAccountSpending),
		},
	}
}

func gobEncode(i interface{}) []byte {
	buf := bytes.NewBuffer(nil)
	if err := gob.NewEncoder(buf).Encode(i); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

// AcquireContract acquires a contract assuming that the contract exists and
// that it isn't locked right now. The returned bool indicates whether locking
// the contract was successful.
func (s *SQLStore) AcquireContract(fcid types.FileContractID, duration time.Duration) (bool, error) {
	var contract dbContract
	var locked bool

	err := s.db.Transaction(func(tx *gorm.DB) error {
		// Get revision.
		err := tx.Model(&dbContract{}).
			Where("fcid", gobEncode(fcid)).
			Take(&contract).
			Error
		if err != nil {
			return err
		}
		// See if it is locked.
		locked = time.Now().Before(contract.LockedUntil)
		if locked {
			return nil
		}

		// Update lock.
		return tx.Model(&dbContract{}).
			Where("fcid", gobEncode(fcid)).
			Update("locked_until", time.Now().Add(duration).UTC()).
			Error
	})
	if err != nil {
		return false, fmt.Errorf("failed to lock contract: %w", err)
	}
	if locked {
		return false, nil
	}
	return true, nil
}

// ReleaseContract releases a contract by setting its locked_until field to 0.
func (s *SQLStore) ReleaseContract(fcid types.FileContractID) error {
	return s.db.Model(&dbContract{}).
		Where("fcid", gobEncode(fcid)).
		Update("locked_until", time.Time{}).
		Error
}

// addContract implements the api.ContractStore interface.
func addContract(tx *gorm.DB, c rhpv2.ContractRevision, totalCost types.Currency, startHeight uint64, renewedFrom types.FileContractID) (dbContract, error) {
	fcid := c.ID()

	// Find host.
	var host dbHost
	err := tx.Where(&dbHost{PublicKey: c.HostKey()}).
		Take(&host).Error
	if err != nil {
		return dbContract{}, err
	}

	// Create contract.
	contract := dbContract{
		FCID:        fcid,
		HostID:      host.ID,
		RenewedFrom: renewedFrom,
		StartHeight: startHeight,
		TotalCost:   totalCost.Big(),

		// Spending starts at 0.
		UploadSpending:      big.NewInt(0),
		DownloadSpending:    big.NewInt(0),
		FundAccountSpending: big.NewInt(0),
	}

	// Insert contract.
	err = tx.Where(&dbHost{PublicKey: c.HostKey()}).
		Create(&contract).Error
	if err != nil {
		return dbContract{}, err
	}
	return contract, nil
}

// AddContract implements the api.ContractStore interface.
func (s *SQLStore) AddContract(c rhpv2.ContractRevision, totalCost types.Currency, startHeight uint64) (_ api.ContractMetadata, err error) {
	var added dbContract

	if err := s.db.Transaction(func(tx *gorm.DB) error {
		added, err = addContract(tx, c, totalCost, startHeight, types.FileContractID{})
		return err
	}); err != nil {
		return api.ContractMetadata{}, err
	}

	return added.convert(), nil
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
			Host:        oldContract.Host.PublicKey,
			Reason:      archivalReasonRenewed,
			RenewedTo:   c.ID(),
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

// Contract implements the api.ContractStore interface.
func (s *SQLStore) Contract(id types.FileContractID) (api.ContractMetadata, error) {
	// Fetch contract.
	contract, err := s.contract(id)
	if err != nil {
		return api.ContractMetadata{}, err
	}
	return contract.convert(), nil
}

// Contracts implements the api.ContractStore interface.
func (s *SQLStore) Contracts() ([]api.ContractMetadata, error) {
	dbContracts, err := s.contracts()
	if err != nil {
		return nil, err
	}
	contracts := make([]api.ContractMetadata, len(dbContracts))
	for i, c := range dbContracts {
		contracts[i] = c.convert()
	}
	return contracts, nil
}

// removeContract implements the api.ContractStore interface.
func removeContract(tx *gorm.DB, id types.FileContractID) error {
	var contract dbContract
	if err := tx.Where(&dbContract{FCID: id}).
		Take(&contract).Error; err != nil {
		return err
	}
	return tx.Where(&dbContract{Model: Model{ID: contract.ID}}).
		Delete(&contract).Error
}

// RemoveContract implements the api.ContractStore interface.
func (s *SQLStore) RemoveContract(id types.FileContractID) error {
	return s.db.Transaction(func(tx *gorm.DB) error {
		return removeContract(tx, id)
	})
}

func contract(tx *gorm.DB, id types.FileContractID) (dbContract, error) {
	var contract dbContract
	err := tx.Where(&dbContract{FCID: id}).
		Preload("Host.Announcements").
		Take(&contract).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return contract, ErrContractNotFound
	}
	return contract, err
}

func (s *SQLStore) contract(id types.FileContractID) (dbContract, error) {
	return contract(s.db, id)
}

func (s *SQLStore) contracts() ([]dbContract, error) {
	var contracts []dbContract
	err := s.db.Model(&dbContract{}).
		Preload("Host.Announcements").
		Find(&contracts).Error
	return contracts, err
}

func (s *SQLStore) AncestorContracts(id types.FileContractID, startHeight uint64) ([]api.ArchivedContract, error) {
	var ancestors []dbArchivedContract
	err := s.db.Raw("WITH ancestors AS (SELECT * FROM archived_contracts WHERE renewed_to = ? UNION ALL SELECT archived_contracts.* FROM ancestors, archived_contracts WHERE archived_contracts.renewed_to = ancestors.fcid) SELECT * FROM ancestors WHERE start_height >= ?", gobEncode(id), startHeight).
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
