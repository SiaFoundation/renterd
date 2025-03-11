package bus

import (
	"fmt"
	"sync"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/v2/api"
)

const (
	// cacheExpiry is the amount of time after which an upload is pruned from
	// the cache, since the workers are expected to finish their uploads this is
	// there to prevent leaking memory, which is why it's set at 24h
	cacheExpiry = 24 * time.Hour
)

type (
	SectorsCache struct {
		mu      sync.Mutex
		uploads map[api.UploadID]*ongoingUpload
	}

	ongoingUpload struct {
		started time.Time
		sectors []types.Hash256
	}
)

func NewSectorsCache() *SectorsCache {
	return &SectorsCache{
		uploads: make(map[api.UploadID]*ongoingUpload),
	}
}

func (sc *SectorsCache) AddSectors(uID api.UploadID, roots ...types.Hash256) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	ongoing, ok := sc.uploads[uID]
	if !ok {
		return fmt.Errorf("%w; id '%v'", api.ErrUnknownUpload, uID)
	}

	ongoing.sectors = append(ongoing.sectors, roots...)
	return nil
}

func (sc *SectorsCache) FinishUpload(uID api.UploadID) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	delete(sc.uploads, uID)

	// prune expired uploads
	for uID, ongoing := range sc.uploads {
		if time.Since(ongoing.started) > cacheExpiry {
			delete(sc.uploads, uID)
		}
	}
}

func (sc *SectorsCache) Sectors() (sectors []types.Hash256) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	for _, ongoing := range sc.uploads {
		sectors = append(sectors, ongoing.sectors...)
	}
	return
}

func (sc *SectorsCache) StartUpload(uID api.UploadID) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	// check if upload already exists
	if _, exists := sc.uploads[uID]; exists {
		return fmt.Errorf("%w; id '%v'", api.ErrUploadAlreadyExists, uID)
	}

	sc.uploads[uID] = &ongoingUpload{
		started: time.Now(),
	}
	return nil
}
