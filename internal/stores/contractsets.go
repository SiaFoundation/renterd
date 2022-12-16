package stores

import (
	"bytes"
	"encoding/gob"
	"errors"

	"go.sia.tech/renterd/bus"
	"go.sia.tech/siad/types"
	"gorm.io/gorm"
)

type (
	dbContractSet struct {
		Model

		Name      string       `gorm:"unique;index"`
		Contracts []dbContract `gorm:"many2many:contract_set_contracts;constraint:OnDelete:CASCADE"`
	}

	dbContractSetContract struct {
		DBContractID    uint `gorm:"primaryKey"`
		DBContractSetID uint `gorm:"primaryKey"`
	}
)

// TableName implements the gorm.Tabler interface.
func (dbContractSetContract) TableName() string { return "contract_set_contracts" }

// ContractSets implements the bus.ContractSetStore interface.
func (s *SQLStore) ContractSets() ([]string, error) {
	var setNames []string
	tx := s.db.Model(&dbContractSet{}).
		Select("Name").
		Find(&setNames)
	return setNames, tx.Error
}

// HostSet implements the bus.ContractSetStore interface.
func (s *SQLStore) ContractSet(name string) ([]bus.Contract, error) {
	var hostSet dbContractSet
	err := s.db.Where(&dbContractSet{Name: name}).
		Preload("Contracts.Host.Announcements").
		Take(&hostSet).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrContractSetNotFound
	} else if err != nil {
		return nil, err
	}
	contracts := make([]bus.Contract, len(hostSet.Contracts))
	for i, c := range hostSet.Contracts {
		contracts[i], err = c.convert()
		if err != nil {
			return nil, err
		}
	}
	return contracts, nil
}

// SetContractSet implements the bus.ContractSetStore interface.
func (s *SQLStore) SetContractSet(name string, contracts []types.FileContractID) error {
	contractIDs := make([][]byte, len(contracts))
	for i, fcid := range contracts {
		fcidGob := bytes.NewBuffer(nil)
		if err := gob.NewEncoder(fcidGob).Encode(fcid); err != nil {
			return err
		}
		contractIDs[i] = fcidGob.Bytes()
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
			Where("fcid in ?", contractIDs).
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
