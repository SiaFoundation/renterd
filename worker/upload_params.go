package worker

import (
	"fmt"

	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/object"
)

type uploadParameters struct {
	bucket string
	path   string

	multipart  bool
	uploadID   string
	partNumber int

	ec               object.EncryptionKey
	encryptionOffset uint64

	rs          api.RedundancySettings
	bh          uint64
	contractSet string
	packing     bool
	mimeType    string

	metadata api.ObjectUserMetadata
}

func (up uploadParameters) String() string {
	return fmt.Sprintf("bucket: %s, path: %s, multipart: %t, uploadID: %s, partNumber: %d, ec: %v, encryptionOffset: %d, rs: %v, bh: %d, contractSet: %s, packing: %t, mimeType: %s, metadata: %v",
		up.bucket, up.path, up.multipart, up.uploadID, up.partNumber, up.ec, up.encryptionOffset, up.rs, up.bh, up.contractSet, up.packing, up.mimeType, up.metadata)
}

func defaultParameters(bucket, path string, rs api.RedundancySettings) uploadParameters {
	return uploadParameters{
		bucket: bucket,
		path:   path,

		ec:               object.GenerateEncryptionKey(), // random key
		encryptionOffset: 0,                              // from the beginning

		rs: rs,
	}
}

type UploadOption func(*uploadParameters)

func WithBlockHeight(bh uint64) UploadOption {
	return func(up *uploadParameters) {
		up.bh = bh
	}
}

func WithContractSet(contractSet string) UploadOption {
	return func(up *uploadParameters) {
		up.contractSet = contractSet
	}
}

func WithCustomKey(ec object.EncryptionKey) UploadOption {
	return func(up *uploadParameters) {
		up.ec = ec
	}
}

func WithCustomEncryptionOffset(offset uint64) UploadOption {
	return func(up *uploadParameters) {
		up.encryptionOffset = offset
	}
}

func WithMimeType(mimeType string) UploadOption {
	return func(up *uploadParameters) {
		up.mimeType = mimeType
	}
}

func WithPacking(packing bool) UploadOption {
	return func(up *uploadParameters) {
		up.packing = packing
	}
}

func WithPartNumber(partNumber int) UploadOption {
	return func(up *uploadParameters) {
		up.partNumber = partNumber
	}
}

func WithUploadID(uploadID string) UploadOption {
	return func(up *uploadParameters) {
		up.uploadID = uploadID
		up.multipart = true
	}
}

func WithObjectUserMetadata(metadata api.ObjectUserMetadata) UploadOption {
	return func(up *uploadParameters) {
		up.metadata = metadata
	}
}
