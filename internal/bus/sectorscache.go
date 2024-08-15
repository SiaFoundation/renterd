package bus

import (
	"fmt"
	"sync"
	"time"

	"go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
)

const (
	// cacheExpiry is the amount of time after which an upload is pruned from
	// the cache, since the workers are expected to finish their uploads this is
	// there to prevent leaking memory, which is why it's set at 24h
	cacheExpiry = 24 * time.Hour
)

type (
	sectorsCache struct {
		mu        sync.Mutex
		uploads   map[api.UploadID]*ongoingUpload
		renewedTo map[types.FileContractID]types.FileContractID
	}

	ongoingUpload struct {
		started         time.Time
		contractSectors map[types.FileContractID][]types.Hash256
	}
)

func (ou *ongoingUpload) addSector(fcid types.FileContractID, root types.Hash256) {
	ou.contractSectors[fcid] = append(ou.contractSectors[fcid], root)
}

func (ou *ongoingUpload) sectors(fcid types.FileContractID) (roots []types.Hash256) {
	if sectors, exists := ou.contractSectors[fcid]; exists && time.Since(ou.started) < cacheExpiry {
		roots = append(roots, sectors...)
	}
	return
}

func NewSectorsCache() *sectorsCache {
	return &sectorsCache{
		uploads:   make(map[api.UploadID]*ongoingUpload),
		renewedTo: make(map[types.FileContractID]types.FileContractID),
	}
}

func (sc *sectorsCache) AddSector(uID api.UploadID, fcid types.FileContractID, root types.Hash256) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	ongoing, ok := sc.uploads[uID]
	if !ok {
		return fmt.Errorf("%w; id '%v'", api.ErrUnknownUpload, uID)
	}

	fcid = sc.latestFCID(fcid)
	ongoing.addSector(fcid, root)
	return nil
}

func (sc *sectorsCache) FinishUpload(uID api.UploadID) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	delete(sc.uploads, uID)

	// prune expired uploads
	for uID, ongoing := range sc.uploads {
		if time.Since(ongoing.started) > cacheExpiry {
			delete(sc.uploads, uID)
		}
	}

	// prune renewed to map
	for old, new := range sc.renewedTo {
		if _, exists := sc.renewedTo[new]; exists {
			delete(sc.renewedTo, old)
		}
	}
}

func (sc *sectorsCache) HandleRenewal(fcid, renewedFrom types.FileContractID) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	for _, upload := range sc.uploads {
		if _, exists := upload.contractSectors[renewedFrom]; exists {
			upload.contractSectors[fcid] = upload.contractSectors[renewedFrom]
			upload.contractSectors[renewedFrom] = nil
		}
	}
	sc.renewedTo[renewedFrom] = fcid
}

func (sc *sectorsCache) Pending(fcid types.FileContractID) (size uint64) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	fcid = sc.latestFCID(fcid)
	for _, ongoing := range sc.uploads {
		size += uint64(len(ongoing.sectors(fcid))) * rhp.SectorSize
	}
	return
}

func (sc *sectorsCache) Sectors(fcid types.FileContractID) (roots []types.Hash256) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	fcid = sc.latestFCID(fcid)
	for _, ongoing := range sc.uploads {
		roots = append(roots, ongoing.sectors(fcid)...)
	}
	return
}

func (sc *sectorsCache) StartUpload(uID api.UploadID) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	// check if upload already exists
	if _, exists := sc.uploads[uID]; exists {
		return fmt.Errorf("%w; id '%v'", api.ErrUploadAlreadyExists, uID)
	}

	sc.uploads[uID] = &ongoingUpload{
		started:         time.Now(),
		contractSectors: make(map[types.FileContractID][]types.Hash256),
	}
	return nil
}

func (um *sectorsCache) latestFCID(fcid types.FileContractID) types.FileContractID {
	if latest, ok := um.renewedTo[fcid]; ok {
		return latest
	}
	return fcid
}
