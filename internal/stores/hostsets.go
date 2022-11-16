package stores

import (
	"errors"

	"go.sia.tech/renterd/internal/consensus"
	"gorm.io/gorm"
)

// HostSets implements the bus.HostSetStore interface.
func (s *SQLStore) HostSets() ([]string, error) {
	var setNames []string
	tx := s.db.Model(&dbHostSet{}).
		Select("Name").
		Find(&setNames)
	return setNames, tx.Error
}

// HostSet implements the bus.HostSetStore interface.
func (s *SQLStore) HostSet(name string) ([]consensus.PublicKey, error) {
	var hostSet dbHostSet
	err := s.db.Where(&dbHostSet{Name: name}).
		Preload("Hosts").
		Take(&hostSet).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrHostSetNotFound
	} else if err != nil {
		return nil, err
	}
	pks := make([]consensus.PublicKey, len(hostSet.Hosts))
	for i, entry := range hostSet.Hosts {
		pks[i] = entry.PublicKey
	}
	return pks, nil
}

// SetHostSet implements the bus.HostSetStore interface.
func (s *SQLStore) SetHostSet(name string, hosts []consensus.PublicKey) error {
	hostSetEntries := make([]dbHostSetEntry, len(hosts))
	for i, pk := range hosts {
		hostSetEntries[i] = dbHostSetEntry{
			HostSetName: name,
			PublicKey:   pk,
		}
	}
	return s.db.Create(&dbHostSet{
		Name:  name,
		Hosts: hostSetEntries,
	}).Error
}
