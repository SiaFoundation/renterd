package object

import "go.sia.tech/core/types"

type (
	PinnedSector struct {
		Root    types.Hash256   `json:"root"`
		HostKey types.PublicKey `json:"hostKey"`
	}

	PinnedSlab struct {
		EncryptionKey [32]byte       `json:"encryptionKey"`
		MinShards     uint8          `json:"minShards"`
		Sectors       []PinnedSector `json:"sectors"`
		Offset        uint32         `json:"offset"`
		Length        uint32         `json:"length"`
	}

	PinnedObject struct {
		EncryptionKey [32]byte     `json:"encryptionKey"`
		Slabs         []PinnedSlab `json:"slabs"`
	}
)
