package autopilot

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"

	api "go.sia.tech/renterd/api/autopilot"
	"go.sia.tech/siad/modules"
)

// EphemeralStore implements Store in memory.
type EphemeralStore struct {
	mu     sync.Mutex
	config api.Config
}

// Config implements Store.
func (s *EphemeralStore) Config() api.Config {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.config
}

// SetConfig implements Store.
func (s *EphemeralStore) SetConfig(c api.Config) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.config = c
	return nil
}

// ProcessConsensusChange implements chain.Subscriber.
func (s *EphemeralStore) ProcessConsensusChange(cc modules.ConsensusChange) {
	panic("not implemented")
}

// NewEphemeralStore returns a new EphemeralStore.
func NewEphemeralStore() *EphemeralStore {
	return &EphemeralStore{}
}

// JSONStore implements Store in memory, backed by a JSON file.
type JSONStore struct {
	*EphemeralStore
	dir      string
	lastSave time.Time
}

type jsonPersistData struct {
	Config api.Config
}

func (s *JSONStore) save() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var p jsonPersistData
	p.Config = s.config
	js, _ := json.MarshalIndent(p, "", "  ")

	// atomic save
	dst := filepath.Join(s.dir, "json")
	f, err := os.OpenFile(dst+"_tmp", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0660)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write(js); err != nil {
		return err
	} else if err := f.Sync(); err != nil {
		return err
	} else if err := f.Close(); err != nil {
		return err
	} else if err := os.Rename(dst+"_tmp", dst); err != nil {
		return err
	}
	return nil
}

func (s *JSONStore) load() error {
	var p jsonPersistData
	if js, err := os.ReadFile(filepath.Join(s.dir, "json")); os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	} else if err := json.Unmarshal(js, &p); err != nil {
		return err
	}
	s.config = p.Config
	return nil
}

// SetConfig implements Store.
func (s *JSONStore) SetConfig(c api.Config) error {
	s.EphemeralStore.SetConfig(c)
	return s.save()
}

// NewJSONStore returns a new JSONStore.
func NewJSONStore(dir string) (*JSONStore, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}

	s := &JSONStore{
		EphemeralStore: NewEphemeralStore(),
		dir:            dir,
		lastSave:       time.Now(),
	}
	err := s.load()
	if err != nil {
		return nil, err
	}
	return s, nil
}
