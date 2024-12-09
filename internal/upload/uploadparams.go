package upload

import (
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/object"
)

type Parameters struct {
	Bucket string
	Key    string

	Multipart  bool
	UploadID   string
	PartNumber int

	EC               object.EncryptionKey
	EncryptionOffset uint64

	RS       api.RedundancySettings
	BH       uint64
	Packing  bool
	MimeType string

	Metadata api.ObjectUserMetadata
}

func DefaultParameters(bucket, key string, rs api.RedundancySettings) Parameters {
	return Parameters{
		Bucket: bucket,
		Key:    key,

		EC:               object.GenerateEncryptionKey(object.EncryptionKeyTypeSalted), // random key
		EncryptionOffset: 0,                                                            // from the beginning

		RS: rs,
	}
}

type Option func(*Parameters)

func WithBlockHeight(bh uint64) Option {
	return func(up *Parameters) {
		up.BH = bh
	}
}

func WithCustomKey(ec object.EncryptionKey) Option {
	return func(up *Parameters) {
		up.EC = ec
	}
}

func WithCustomEncryptionOffset(offset uint64) Option {
	return func(up *Parameters) {
		up.EncryptionOffset = offset
	}
}

func WithMimeType(mimeType string) Option {
	return func(up *Parameters) {
		up.MimeType = mimeType
	}
}

func WithPacking(packing bool) Option {
	return func(up *Parameters) {
		up.Packing = packing
	}
}

func WithPartNumber(partNumber int) Option {
	return func(up *Parameters) {
		up.PartNumber = partNumber
	}
}

func WithUploadID(uploadID string) Option {
	return func(up *Parameters) {
		up.UploadID = uploadID
		up.Multipart = true
	}
}

func WithObjectUserMetadata(metadata api.ObjectUserMetadata) Option {
	return func(up *Parameters) {
		up.Metadata = metadata
	}
}
