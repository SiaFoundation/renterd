package object

import (
	"go.sia.tech/renterd/slab"
)

type Object struct {
	Key   slab.EncryptionKey
	Slabs []slab.SlabSlice
}

func (o Object) Size() int64 {
	var n int64
	for _, ss := range o.Slabs {
		n += int64(ss.Length)
	}
	return n
}
