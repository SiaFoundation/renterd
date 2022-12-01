package stores

import (
	"errors"

	"go.sia.tech/siad/types"
	"gorm.io/gorm"
)

type (
	dbContractSet struct {
		Model

		Name      string               `gorm:"unique;index"`
		Contracts []dbContractSetEntry `gorm:"constraint:OnDelete:CASCADE"`
	}

	dbContractSetEntry struct {
		Model
		DBContractSetID uint
		FCID            types.FileContractID `gorm:"NOT NULL;type:bytes;serializer:gob"`
	}
)

// ContractSets implements the bus.ContractSetStore interface.
func (s *SQLStore) ContractSets() ([]string, error) {
	var setNames []string
	tx := s.db.Model(&dbContractSet{}).
		Select("Name").
		Find(&setNames)
	return setNames, tx.Error
}

// HostSet implements the bus.ContractSetStore interface.
func (s *SQLStore) ContractSet(name string) ([]types.FileContractID, error) {
	var hostSet dbContractSet
	err := s.db.Where(&dbContractSet{Name: name}).
		Preload("Contracts").
		Take(&hostSet).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrContractSetNotFound
	} else if err != nil {
		return nil, err
	}
	fcids := make([]types.FileContractID, len(hostSet.Contracts))
	for i, entry := range hostSet.Contracts {
		fcids[i] = entry.FCID
	}
	return fcids, nil
}

// SetHostSet implements the bus.ContractSetStore interface.
func (s *SQLStore) SetContractSet(name string, contracts []types.FileContractID) error {
	contractSetEntries := make([]dbContractSetEntry, len(contracts))
	for i, fcid := range contracts {
		contractSetEntries[i] = dbContractSetEntry{
			FCID: fcid,
		}
	}
	return s.db.Create(&dbContractSet{
		Name:      name,
		Contracts: contractSetEntries,
	}).Error
}
