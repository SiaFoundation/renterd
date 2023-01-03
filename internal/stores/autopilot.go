package stores

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.sia.tech/renterd/api"
	"go.sia.tech/siad/modules"
)

// EphemeralAutopilotStore implements Store in memory.
type EphemeralAutopilotStore struct {
	mu     sync.Mutex
	config api.Config
}

// Config implements Store.
func (s *EphemeralAutopilotStore) Config() api.Config {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.config
}

// SetConfig implements Store.
func (s *EphemeralAutopilotStore) SetConfig(c api.Config) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.config = c
	return nil
}

// ProcessConsensusChange implements chain.Subscriber.
func (s *EphemeralAutopilotStore) ProcessConsensusChange(cc modules.ConsensusChange) {
	panic("not implemented")
}

// NewEphemeralAutopilotStore returns a new EphemeralStore.
func NewEphemeralAutopilotStore() *EphemeralAutopilotStore {
	return &EphemeralAutopilotStore{}
}

// JSONAutopilotStore implements Store in memory, backed by a JSON file.
type JSONAutopilotStore struct {
	*EphemeralAutopilotStore
	dir      string
	lastSave time.Time
}

type jsonPersistData struct {
	Config api.Config
}

func (s *JSONAutopilotStore) save() error {
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

func (s *JSONAutopilotStore) load() error {
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
func (s *JSONAutopilotStore) SetConfig(c api.Config) error {
	s.EphemeralAutopilotStore.SetConfig(c)
	return s.save()
}

// NewJSONAutopilotStore returns a new JSONStore.
func NewJSONAutopilotStore(dir string) (*JSONAutopilotStore, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}

	s := &JSONAutopilotStore{
		EphemeralAutopilotStore: NewEphemeralAutopilotStore(),
		dir:                     dir,
		lastSave:                time.Now(),
	}
	err := s.load()
	if err != nil {
		return nil, err
	}
	return s, nil
}
