package api

import (
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/object"
)

type (
	PackedSlab struct {
		BufferID      uint                 `json:"bufferID"`
		Data          []byte               `json:"data"`
		EncryptionKey object.EncryptionKey `json:"encryptionKey"`
	}

	SlabBuffer struct {
		ContractSet string `json:"contractSet"` // contract set that be buffer will be uploaded to
		Complete    bool   `json:"complete"`    // whether the slab buffer is complete and ready to upload
		Filename    string `json:"filename"`    // name of the buffer on disk
		Size        int64  `json:"size"`        // size of the buffer
		MaxSize     int64  `json:"maxSize"`     // maximum size of the buffer
		Locked      bool   `json:"locked"`      // whether the slab buffer is locked for uploading
	}

	UnhealthySlab struct {
		EncryptionKey object.EncryptionKey `json:"encryptionKey"`
		Health        float64              `json:"health"`
	}

	UploadedPackedSlab struct {
		BufferID uint
		Shards   []UploadedSector
	}

	UploadedSector struct {
		ContractID types.FileContractID `json:"contractID"`
		Root       types.Hash256        `json:"root"`
	}
)

type (
	AddPartialSlabResponse struct {
		SlabBufferMaxSizeSoftReached bool               `json:"slabBufferMaxSizeSoftReached"`
		Slabs                        []object.SlabSlice `json:"slabs"`
	}

	// MigrationSlabsRequest is the request type for the /slabs/migration endpoint.
	MigrationSlabsRequest struct {
		ContractSet  string  `json:"contractSet"`
		HealthCutoff float64 `json:"healthCutoff"`
		Limit        int     `json:"limit"`
	}

	PackedSlabsRequestGET struct {
		LockingDuration DurationMS `json:"lockingDuration"`
		MinShards       uint8      `json:"minShards"`
		TotalShards     uint8      `json:"totalShards"`
		ContractSet     string     `json:"contractSet"`
		Limit           int        `json:"limit"`
	}

	PackedSlabsRequestPOST struct {
		Slabs []UploadedPackedSlab `json:"slabs"`
	}

	UnhealthySlabsResponse struct {
		Slabs []UnhealthySlab `json:"slabs"`
	}

	// UpdateSlabRequest is the request type for the PUT /slab/:key endpoint.
	UpdateSlabRequest []UploadedSector
)

func (s UploadedPackedSlab) Contracts() (fcids []types.FileContractID) {
	seen := make(map[types.FileContractID]struct{})
	for _, sector := range s.Shards {
		_, ok := seen[sector.ContractID]
		if !ok {
			seen[sector.ContractID] = struct{}{}
			fcids = append(fcids, sector.ContractID)
		}
	}
	return
}
