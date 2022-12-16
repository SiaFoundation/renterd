package stores

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"math/big"
	"time"

	"go.sia.tech/renterd/bus"
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

		FCID        types.FileContractID `gorm:"unique;index,type:bytes;serializer:gob;NOT NULL;column:fcid"`
		HostID      uint                 `gorm:"index"`
		Host        dbHost
		LockedUntil time.Time
		RenewedFrom types.FileContractID `gorm:"index,type:bytes;serializer:gob"`
		StartHeight uint64               `gorm:"NOT NULL"`
		TotalCost   *big.Int             `gorm:"type:bytes;serializer:gob"`
	}

	dbArchivedContract struct {
		Model
		FCID      types.FileContractID `gorm:"unique;index,type:bytes;serializer:gob;NOT NULL;column:fcid"`
		Host      consensus.PublicKey  `gorm:"index;type:bytes;serializer:gob;NOT NULL"`
		RenewedTo types.FileContractID `gorm:"unique;index,type:bytes;serializer:gob"`
		Reason    string
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

// convert converts a dbContractRHPv2 to a rhpv2.Contract type.
func (c dbContract) convert() (bus.Contract, error) {
	return bus.Contract{
		ID:          c.FCID,
		HostIP:      c.Host.NetAddress(),
		HostKey:     c.Host.PublicKey,
		StartHeight: c.StartHeight,
		ContractMetadata: bus.ContractMetadata{
			RenewedFrom: c.RenewedFrom,
			TotalCost:   types.NewCurrency(c.TotalCost),
			Spending:    bus.ContractSpending{}, // TODO
		},
	}, nil
}

// AcquireContract acquires a contract assuming that the contract exists and
// that it isn't locked right now. The returned bool indicates whether locking
// the contract was successful.
func (s *SQLStore) AcquireContract(fcid types.FileContractID, duration time.Duration) (bool, error) {
	var contract dbContract
	var locked bool

	fcidGob := bytes.NewBuffer(nil)
	if err := gob.NewEncoder(fcidGob).Encode(fcid); err != nil {
		return false, err
	}

	err := s.db.Transaction(func(tx *gorm.DB) error {
		// Get revision.
		err := tx.Model(&dbContract{}).
			Where("fcid", fcidGob.Bytes()).
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
			Where("fcid", fcidGob.Bytes()).
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
	fcidGob := bytes.NewBuffer(nil)
	if err := gob.NewEncoder(fcidGob).Encode(fcid); err != nil {
		return err
	}
	return s.db.Model(&dbContract{}).
		Where("fcid", fcidGob.Bytes()).
		Update("locked_until", time.Time{}).
		Error
}

// addContract implements the bus.ContractStore interface.
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
	}

	// Insert contract.
	err = tx.Where(&dbHost{PublicKey: c.HostKey()}).
		Create(&contract).Error
	if err != nil {
		return dbContract{}, err
	}
	return contract, nil
}

// AddContract implements the bus.ContractStore interface.
func (s *SQLStore) AddContract(c rhpv2.ContractRevision, totalCost types.Currency, startHeight uint64) (_ bus.Contract, err error) {
	var added dbContract

	if err := s.db.Transaction(func(tx *gorm.DB) error {
		added, err = addContract(tx, c, totalCost, startHeight, types.FileContractID{})
		return err
	}); err != nil {
		return bus.Contract{}, err
	}

	return added.convert()
}

// AddRenewedContract adds a new contract which was created as the result of a renewal to the store.
// The old contract specified as 'renewedFrom' will be deleted from the active
// contracts and moved to the archive. Both new and old contract will be linked
// to each other through the RenewedFrom and RenewedTo fields respectively.
func (s *SQLStore) AddRenewedContract(c rhpv2.ContractRevision, totalCost types.Currency, startHeight uint64, renewedFrom types.FileContractID) (bus.Contract, error) {
	var renewed dbContract

	if err := s.db.Transaction(func(tx *gorm.DB) error {
		// Fetch contract we renew from.
		oldContract, err := contract(tx, renewedFrom)
		if err != nil {
			return err
		}

		// Create copy in archive.
		err = tx.Create(&dbArchivedContract{
			FCID:      oldContract.FCID,
			Host:      oldContract.Host.PublicKey,
			Reason:    archivalReasonRenewed,
			RenewedTo: c.ID(),
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
		return bus.Contract{}, err
	}

	return renewed.convert()
}

// Contract implements the bus.ContractStore interface.
func (s *SQLStore) Contract(id types.FileContractID) (bus.Contract, error) {
	// Fetch contract.
	contract, err := s.contract(id)
	if err != nil {
		return bus.Contract{}, err
	}
	return contract.convert()
}

// Contracts implements the bus.ContractStore interface.
func (s *SQLStore) Contracts() ([]bus.Contract, error) {
	dbContracts, err := s.contracts()
	if err != nil {
		return nil, err
	}
	contracts := make([]bus.Contract, len(dbContracts))
	for i, c := range dbContracts {
		contract, err := c.convert()
		if err != nil {
			return nil, err
		}
		contracts[i] = contract
	}
	return contracts, nil
}

// removeContract implements the bus.ContractStore interface.
func removeContract(tx *gorm.DB, id types.FileContractID) error {
	var contract dbContract
	if err := tx.Where(&dbContract{FCID: id}).
		Take(&contract).Error; err != nil {
		return err
	}
	return tx.Where(&dbContract{Model: Model{ID: contract.ID}}).
		Delete(&contract).Error
}

// RemoveContract implements the bus.ContractStore interface.
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
