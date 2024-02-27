package worker

import (
	"bytes"
	"io"

	"github.com/gabriel-vasile/mimetype"
	"go.sia.tech/renterd/object"
)

func encryptPartialSlab(data []byte, key object.EncryptionKey, minShards, totalShards uint8) [][]byte {
	slab := object.Slab{
		Key:       key,
		MinShards: minShards,
		Shards:    make([]object.Sector, totalShards),
	}
	encodedShards := make([][]byte, totalShards)
	slab.Encode(data, encodedShards)
	slab.Encrypt(encodedShards)
	return encodedShards
}

func newMimeReader(r io.Reader) (mimeType string, recycled io.Reader, err error) {
	buf := bytes.NewBuffer(nil)
	mtype, err := mimetype.DetectReader(io.TeeReader(r, buf))
	recycled = io.MultiReader(buf, r)
	return mtype.String(), recycled, err
}
