package worker

import (
	"bytes"
	"encoding/hex"
	"io"

	"github.com/gabriel-vasile/mimetype"
	"go.sia.tech/core/types"
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

type hashReader struct {
	r io.Reader
	h *types.Hasher
}

func newHashReader(r io.Reader) *hashReader {
	return &hashReader{
		r: r,
		h: types.NewHasher(),
	}
}

func (e *hashReader) Read(p []byte) (int, error) {
	n, err := e.r.Read(p)
	if _, wErr := e.h.E.Write(p[:n]); wErr != nil {
		return 0, wErr
	}
	return n, err
}

func (e *hashReader) Hash() string {
	sum := e.h.Sum()
	return hex.EncodeToString(sum[:])
}
