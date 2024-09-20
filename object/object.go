package object

import (
	"crypto/cipher"
	"io"

	"go.sia.tech/core/types"
)

// An Object is a unit of data that has been stored on a host.
// NOTE: Object is embedded in the API's Object type, so all fields should be
// tagged omitempty to make sure responses where no object is returned remain
// clean.
type Object struct {
	Key   EncryptionKey `json:"encryptionKey,omitempty"`
	Slabs SlabSlices    `json:"slabs,omitempty"`
}

// NewObject returns a new Object with a random key.
func NewObject(ec EncryptionKey) Object {
	return Object{
		Key: ec,
	}
}

func (o Object) Contracts() []types.FileContractID {
	return o.Slabs.Contracts()
}

// TotalSize returns the total size of the object.
func (o Object) TotalSize() int64 {
	var n int64
	for _, ss := range o.Slabs {
		n += int64(ss.Length)
	}
	return n
}

// Encrypt wraps the given reader with a reader that encrypts the stream using
// the object's key.
func (o Object) Encrypt(r io.Reader, offset uint64) (cipher.StreamReader, error) {
	return o.Key.Encrypt(r, offset)
}
