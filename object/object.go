package object

import (
	"go.sia.tech/renterd/slab"
)

// An Object is a unit of data that has been stored on a host.
type Object struct {
	Key   slab.EncryptionKey
	Slabs []slab.Slice
}

// Size returns the total size of the object.
func (o Object) Size() int64 {
	var n int64
	for _, ss := range o.Slabs {
		n += int64(ss.Length)
	}
	return n
}
