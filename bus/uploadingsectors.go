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
	uploadingSectorsCache struct {
		mu        sync.Mutex
		uploads   map[api.UploadID]*ongoingUpload
		renewedTo map[types.FileContractID]types.FileContractID
	}

	ongoingUpload struct {
		started         time.Time
		contractSectors map[types.FileContractID][]types.Hash256
	}
)

func newUploadingSectorsCache() *uploadingSectorsCache {
	return &uploadingSectorsCache{
		uploads:   make(map[api.UploadID]*ongoingUpload),
		renewedTo: make(map[types.FileContractID]types.FileContractID),
	}
}

func (ou *ongoingUpload) addSector(fcid types.FileContractID, root types.Hash256) {
	ou.contractSectors[fcid] = append(ou.contractSectors[fcid], root)
}

func (ou *ongoingUpload) sectors(fcid types.FileContractID) (roots []types.Hash256) {
	if sectors, exists := ou.contractSectors[fcid]; exists && time.Since(ou.started) < cacheExpiry {
		roots = append(roots, sectors...)
	}
	return
}

func (usc *uploadingSectorsCache) AddSector(uID api.UploadID, fcid types.FileContractID, root types.Hash256) error {
	usc.mu.Lock()
	defer usc.mu.Unlock()

	ongoing, ok := usc.uploads[uID]
	if !ok {
		return fmt.Errorf("%w; id '%v'", api.ErrUnknownUpload, uID)
	}

	fcid = usc.latestFCID(fcid)
	ongoing.addSector(fcid, root)
	return nil
}

func (usc *uploadingSectorsCache) FinishUpload(uID api.UploadID) {
	usc.mu.Lock()
	defer usc.mu.Unlock()
	delete(usc.uploads, uID)

	// prune expired uploads
	for uID, ongoing := range usc.uploads {
		if time.Since(ongoing.started) > cacheExpiry {
			delete(usc.uploads, uID)
		}
	}

	// prune renewed to map
	for old, new := range usc.renewedTo {
		if _, exists := usc.renewedTo[new]; exists {
			delete(usc.renewedTo, old)
		}
	}
}

func (usc *uploadingSectorsCache) HandleRenewal(fcid, renewedFrom types.FileContractID) {
	usc.mu.Lock()
	defer usc.mu.Unlock()

	for _, upload := range usc.uploads {
		if _, exists := upload.contractSectors[renewedFrom]; exists {
			upload.contractSectors[fcid] = upload.contractSectors[renewedFrom]
			upload.contractSectors[renewedFrom] = nil
		}
	}
	usc.renewedTo[renewedFrom] = fcid
}

func (usc *uploadingSectorsCache) Pending(fcid types.FileContractID) (size uint64) {
	usc.mu.Lock()
	defer usc.mu.Unlock()

	fcid = usc.latestFCID(fcid)
	for _, ongoing := range usc.uploads {
		size += uint64(len(ongoing.sectors(fcid))) * rhp.SectorSize
	}
	return
}

func (usc *uploadingSectorsCache) Sectors(fcid types.FileContractID) (roots []types.Hash256) {
	usc.mu.Lock()
	defer usc.mu.Unlock()

	fcid = usc.latestFCID(fcid)
	for _, ongoing := range usc.uploads {
		roots = append(roots, ongoing.sectors(fcid)...)
	}
	return
}

func (usc *uploadingSectorsCache) StartUpload(uID api.UploadID) error {
	usc.mu.Lock()
	defer usc.mu.Unlock()

	// check if upload already exists
	if _, exists := usc.uploads[uID]; exists {
		return fmt.Errorf("%w; id '%v'", api.ErrUploadAlreadyExists, uID)
	}

	usc.uploads[uID] = &ongoingUpload{
		started:         time.Now(),
		contractSectors: make(map[types.FileContractID][]types.Hash256),
	}
	return nil
}

func (usc *uploadingSectorsCache) latestFCID(fcid types.FileContractID) types.FileContractID {
	if latest, ok := usc.renewedTo[fcid]; ok {
		return latest
	}
	return fcid
}
