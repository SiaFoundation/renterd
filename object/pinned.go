package object

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	"go.sia.tech/core/types"
)

type (
	PinnedSector struct {
		Root    types.Hash256   `json:"root"`
		HostKey types.PublicKey `json:"hostKey"`
	}

	RawEncryptionKey [32]byte

	PinnedSlab struct {
		EncryptionKey RawEncryptionKey `json:"encryptionKey"`
		MinShards     uint8            `json:"minShards"`
		Sectors       []PinnedSector   `json:"sectors"`
		Offset        uint32           `json:"offset"`
		Length        uint32           `json:"length"`
	}

	PinnedObject struct {
		Key      string `json:"key"`
		MimeType string `json:"mimeType"`

		EncryptionKey RawEncryptionKey `json:"encryptionKey"`
		Slabs         []PinnedSlab     `json:"slabs"`
	}
)

// UnmarshalText implements [encoding.TextUnmarshaler].
func (rk *RawEncryptionKey) UnmarshalText(data []byte) error {
	const encodedSize = 64
	if len(data) < encodedSize {
		return io.ErrUnexpectedEOF
	} else if len(data) > encodedSize {
		return errors.New("input too long")
	}
	n, err := hex.Decode(rk[:], data)
	if err != nil {
		return fmt.Errorf("decoding %q failed: %w", data, err)
	} else if n < len(rk) {
		return fmt.Errorf("input too short: got %d, want %d", n, len(rk))
	}
	return nil
}

// MarshalText implements [encoding.TextMarshaler].
func (rk RawEncryptionKey) MarshalText() ([]byte, error) {
	buf := make([]byte, 64)
	hex.Encode(buf, rk[:])
	return buf, nil
}
