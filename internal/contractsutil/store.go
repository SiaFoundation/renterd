package contractsutil

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"

	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"go.sia.tech/siad/types"
)

// EphemeralStore implements api.ContractStore in memory.
type EphemeralStore struct {
	mu        sync.Mutex
	contracts map[types.FileContractID]rhpv2.Contract
}

// Contracts implements api.ContractStore.
func (s *EphemeralStore) Contracts() ([]rhpv2.Contract, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var cs []rhpv2.Contract
	for _, c := range s.contracts {
		cs = append(cs, c)
	}
	return cs, nil
}

// Contract implements api.ContractStore.
func (s *EphemeralStore) Contract(id types.FileContractID) (rhpv2.Contract, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.contracts[id]
	if !ok {
		return rhpv2.Contract{}, errors.New("no contract with that ID")
	}
	return c, nil
}

// AddContract implements api.ContractStore.
func (s *EphemeralStore) AddContract(c rhpv2.Contract) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.contracts[c.ID()] = c
	return nil
}

// RemoveContract implements api.ContractStore.
func (s *EphemeralStore) RemoveContract(id types.FileContractID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.contracts, id)
	return nil
}

// NewEphemeralStore returns a new EphemeralStore.
func NewEphemeralStore() *EphemeralStore {
	return &EphemeralStore{
		contracts: make(map[types.FileContractID]rhpv2.Contract),
	}
}

// JSONStore implements api.ContractStore in memory, backed by a JSON file.
type JSONStore struct {
	*EphemeralStore
	dir string
}

type jsonPersistData struct {
	Contracts []rhpv2.Contract
}

func (s *JSONStore) save() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var p jsonPersistData
	for _, c := range s.contracts {
		p.Contracts = append(p.Contracts, c)
	}
	js, _ := json.MarshalIndent(p, "", "  ")

	// atomic save
	dst := filepath.Join(s.dir, "contracts.json")
	f, err := os.OpenFile(dst+"_tmp", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0660)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err = f.Write(js); err != nil {
		return err
	} else if f.Sync(); err != nil {
		return err
	} else if f.Close(); err != nil {
		return err
	} else if err := os.Rename(dst+"_tmp", dst); err != nil {
		return err
	}
	return nil
}

func (s *JSONStore) load() error {
	var p jsonPersistData
	if js, err := os.ReadFile(filepath.Join(s.dir, "contracts.json")); os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	} else if err := json.Unmarshal(js, &p); err != nil {
		return err
	}
	for _, c := range p.Contracts {
		s.contracts[c.ID()] = c
	}
	return nil
}

// AddContract implements api.ContractStore.
func (s *JSONStore) AddContract(c rhpv2.Contract) error {
	s.EphemeralStore.AddContract(c)
	return s.save()
}

// RemoveContract implements api.ContractStore.
func (s *JSONStore) RemoveContract(id types.FileContractID) error {
	s.EphemeralStore.RemoveContract(id)
	return s.save()
}

// NewJSONStore returns a new JSONStore.
func NewJSONStore(dir string) (*JSONStore, error) {
	s := &JSONStore{
		EphemeralStore: NewEphemeralStore(),
		dir:            dir,
	}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}
