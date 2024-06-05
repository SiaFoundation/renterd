package stores

import (
	"context"
	"fmt"
	"sort"

	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/object"
	sql "go.sia.tech/renterd/stores/sql"
)

type (
	dbMultipartUpload struct {
		Model

		Key        secretKey
		UploadID   string                 `gorm:"uniqueIndex;NOT NULL;size:64"`
		ObjectID   string                 `gorm:"index:idx_multipart_uploads_object_id;NOT NULL"`
		DBBucket   dbBucket               `gorm:"constraint:OnDelete:CASCADE"` // CASCADE to delete uploads when bucket is deleted
		DBBucketID uint                   `gorm:"index:idx_multipart_uploads_db_bucket_id;NOT NULL"`
		Parts      []dbMultipartPart      // no CASCADE, parts are deleted via trigger
		Metadata   []dbObjectUserMetadata `gorm:"constraint:OnDelete:SET NULL"` // CASCADE to delete parts too
		MimeType   string                 `gorm:"index:idx_multipart_uploads_mime_type"`
	}

	dbMultipartPart struct {
		Model
		Etag                string `gorm:"index"`
		PartNumber          int    `gorm:"index"`
		Size                uint64
		DBMultipartUploadID uint      `gorm:"index;NOT NULL"`
		Slabs               []dbSlice // no CASCADE, slices are deleted via trigger
	}
)

func (dbMultipartUpload) TableName() string {
	return "multipart_uploads"
}

func (dbMultipartPart) TableName() string {
	return "multipart_parts"
}

func (s *SQLStore) CreateMultipartUpload(ctx context.Context, bucket, path string, ec object.EncryptionKey, mimeType string, metadata api.ObjectUserMetadata) (api.MultipartCreateResponse, error) {
	var uploadID string
	err := s.bMain.Transaction(ctx, func(tx sql.DatabaseTx) (err error) {
		uploadID, err = tx.InsertMultipartUpload(ctx, bucket, path, ec, mimeType, metadata)
		return
	})
	if err != nil {
		return api.MultipartCreateResponse{}, err
	}
	return api.MultipartCreateResponse{
		UploadID: uploadID,
	}, err
}

func (s *SQLStore) AddMultipartPart(ctx context.Context, bucket, path, contractSet, eTag, uploadID string, partNumber int, slices []object.SlabSlice) (err error) {
	return s.bMain.Transaction(ctx, func(tx sql.DatabaseTx) error {
		return tx.AddMultipartPart(ctx, bucket, path, contractSet, eTag, uploadID, partNumber, slices)
	})
}

func (s *SQLStore) MultipartUpload(ctx context.Context, uploadID string) (resp api.MultipartUpload, err error) {
	err = s.bMain.Transaction(ctx, func(tx sql.DatabaseTx) (err error) {
		resp, err = tx.MultipartUpload(ctx, uploadID)
		return
	})
	return
}

func (s *SQLStore) MultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker string, limit int) (resp api.MultipartListUploadsResponse, err error) {
	err = s.bMain.Transaction(ctx, func(tx sql.DatabaseTx) (err error) {
		resp, err = tx.MultipartUploads(ctx, bucket, prefix, keyMarker, uploadIDMarker, limit)
		return
	})
	return
}

func (s *SQLStore) MultipartUploadParts(ctx context.Context, bucket, object string, uploadID string, marker int, limit int64) (resp api.MultipartListPartsResponse, _ error) {
	err := s.bMain.Transaction(ctx, func(tx sql.DatabaseTx) (err error) {
		resp, err = tx.MultipartUploadParts(ctx, bucket, object, uploadID, marker, limit)
		return
	})
	return resp, err
}

func (s *SQLStore) AbortMultipartUpload(ctx context.Context, bucket, path string, uploadID string) error {
	err := s.bMain.Transaction(ctx, func(tx sql.DatabaseTx) error {
		return tx.AbortMultipartUpload(ctx, bucket, path, uploadID)
	})
	if err != nil {
		return err
	}
	s.triggerSlabPruning()
	return nil
}

func (s *SQLStore) CompleteMultipartUpload(ctx context.Context, bucket, path string, uploadID string, parts []api.MultipartCompletedPart, opts api.CompleteMultipartOptions) (_ api.MultipartCompleteResponse, err error) {
	// Sanity check input parts.
	if !sort.SliceIsSorted(parts, func(i, j int) bool {
		return parts[i].PartNumber < parts[j].PartNumber
	}) {
		return api.MultipartCompleteResponse{}, fmt.Errorf("provided parts are not sorted")
	}
	for i := 0; i < len(parts)-1; i++ {
		if parts[i].PartNumber == parts[i+1].PartNumber {
			return api.MultipartCompleteResponse{}, fmt.Errorf("duplicate part number %v", parts[i].PartNumber)
		}
	}

	var eTag string
	var prune bool
	err = s.bMain.Transaction(ctx, func(tx sql.DatabaseTx) error {
		// Delete potentially existing object.
		prune, err = tx.DeleteObject(ctx, bucket, path)
		if err != nil {
			return fmt.Errorf("failed to delete object: %w", err)
		}

		// Complete upload
		eTag, err = tx.CompleteMultipartUpload(ctx, bucket, path, uploadID, parts, opts)
		if err != nil {
			return fmt.Errorf("failed to complete multipart upload: %w", err)
		}
		return nil
	})
	if err != nil {
		return api.MultipartCompleteResponse{}, err
	} else if prune {
		s.triggerSlabPruning()
	}
	return api.MultipartCompleteResponse{
		ETag: eTag,
	}, nil
}
