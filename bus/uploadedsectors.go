package bus

import (
	"sync"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
)

const cacheExpiry = 12 * time.Hour

type uploadedSectorsCache struct {
	mu      sync.Mutex
	uploads map[api.UploadID]time.Time
	sectors map[api.UploadID]map[types.FileContractID][]types.Hash256
}

func newUploadedSectorsCache() *uploadedSectorsCache {
	return &uploadedSectorsCache{
		uploads: make(map[api.UploadID]time.Time),
		sectors: make(map[api.UploadID]map[types.FileContractID][]types.Hash256),
	}
}

func (usc *uploadedSectorsCache) addUploadedSector(uID api.UploadID, fcid types.FileContractID, root types.Hash256) {
	usc.mu.Lock()
	defer usc.mu.Unlock()

	// add sector
	if _, exists := usc.uploads[uID]; !exists {
		usc.uploads[uID] = time.Now()
		usc.sectors[uID] = make(map[types.FileContractID][]types.Hash256)
	}
	usc.sectors[uID][fcid] = append(usc.sectors[uID][fcid], root)

	// prune expired uploads
	for uID, t := range usc.uploads {
		if time.Since(t) > cacheExpiry {
			delete(usc.uploads, uID)
			delete(usc.sectors, uID)
		}
	}
}

func (usc *uploadedSectorsCache) cachedSectors(fcid types.FileContractID) (roots []types.Hash256) {
	usc.mu.Lock()
	defer usc.mu.Unlock()
	for _, entry := range usc.sectors {
		roots = append(roots, entry[fcid]...)
	}
	return
}

func (usc *uploadedSectorsCache) finishUpload(uID api.UploadID) {
	usc.mu.Lock()
	defer usc.mu.Unlock()
	delete(usc.uploads, uID)
	delete(usc.sectors, uID)
}
