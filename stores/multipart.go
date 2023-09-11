package stores

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/SiaFoundation/gofakes3"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/object"
	"gorm.io/gorm"
	"lukechampine.com/frand"
)

type (
	dbMultipartUpload struct {
		Model

		UploadID string            `gorm:"uniqueIndex"`
		ObjectID string            `gorm:"index"`
		Parts    []dbMultipartPart `gorm:"constraint:OnDelete:CASCADE"` // CASCADE to delete parts too
	}

	dbMultipartPart struct {
		Model
		Etag                string    `gorm:"index"`
		PartNumber          int       `gorm:"index"`
		DBMultipartUploadID uint      `gorm:"index;NOT NULL"`
		Slabs               []dbSlice `gorm:"constraint:OnDelete:CASCADE"` // CASCADE to delete slices too
	}
)

func (s *SQLStore) CreateMultipartUpload(ctx context.Context, bucket, path string) (api.MultipartCreateResponse, error) {
	var uploadID string
	err := s.retryTransaction(func(tx *gorm.DB) error {
		// Get bucket id.
		var bucketID uint
		err := tx.Table("(SELECT id from buckets WHERE buckets.name = ?) bucket_id", bucket).
			Take(&bucketID).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("bucket %v not found: %w", bucket, api.ErrBucketNotFound)
		} else if err != nil {
			return fmt.Errorf("failed to fetch bucket id: %w", err)
		}
		// Create multipart upload
		uploadIDEntropy := frand.Entropy256()
		uploadID = hex.EncodeToString(uploadIDEntropy[:])
		if err := s.db.Create(&dbMultipartUpload{
			UploadID: uploadID,
			ObjectID: path,
		}).Error; err != nil {
			return fmt.Errorf("failed to create multipart upload: %w", err)
		}
		return nil
	})
	return api.MultipartCreateResponse{
		UploadID: uploadID,
	}, err
}

func (s *SQLStore) AddMultipartPart(ctx context.Context, bucket, path, contractSet, uploadID string, partNumber int, slices []object.SlabSlice, partialSlabs []object.PartialSlab, etag string, usedContracts map[types.PublicKey]types.FileContractID) (err error) {
	err = s.retryTransaction(func(tx *gorm.DB) error {
		// Fetch contract set.
		var cs dbContractSet
		if err := tx.Take(&cs, "name = ?", contractSet).Error; err != nil {
			return fmt.Errorf("contract set %v not found: %w", contractSet, err)
		}
		// Fetch the used contracts.
		contracts, err := fetchUsedContracts(tx, usedContracts)
		if err != nil {
			return fmt.Errorf("failed to fetch used contracts: %w", err)
		}
		// Find multipart upload.
		var mu dbMultipartUpload
		err = tx.Where("upload_id", uploadID).
			Take(&mu).
			Error
		if err != nil {
			return fmt.Errorf("failed to fetch multipart upload: %w", err)
		}
		// Delete a potentially existing part.
		err = tx.Model(&dbMultipartPart{}).
			Where("db_multipart_upload_id = ? AND part_number = ?", mu.ID, partNumber).
			Delete(&dbMultipartPart{}).
			Error
		if err != nil {
			return fmt.Errorf("failed to delete existing part: %w", err)
		}
		// Create a new part.
		part := dbMultipartPart{
			Etag:                etag,
			PartNumber:          partNumber,
			DBMultipartUploadID: mu.ID,
		}
		err = tx.Create(&part).Error
		if err != nil {
			return fmt.Errorf("failed to create part: %w", err)
		}
		// Create the slices.
		err = s.createSlices(tx, nil, &part.ID, cs.ID, contracts, slices, partialSlabs)
		if err != nil {
			return fmt.Errorf("failed to create slices: %w", err)
		}
		return nil
	})
	return err
}

// TODO: f/u with support for 'prefix', 'keyMarker' and 'uploadIDMarker'
func (s *SQLStore) ListMultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker string, maxUploads int) (resp api.MultipartListUploadsResponse, _ error) {
	err := s.retryTransaction(func(tx *gorm.DB) error {
		var dbUploads []dbMultipartUpload
		err := tx.Limit(int(maxUploads)).
			Find(&dbUploads).
			Error
		if err != nil {
			return err
		}
		for _, upload := range dbUploads {
			resp.Uploads = append(resp.Uploads, api.MultipartListUploadItem{
				Path:      upload.ObjectID,
				UploadID:  upload.UploadID,
				CreatedAt: upload.CreatedAt.UTC(),
			})
		}
		return nil
	})
	return resp, err
}
func (s *SQLStore) ListParts(bucket, object string, uploadID string, marker int, limit int64) (api.MultipartListPartsResponse, error) {
	panic("not implemented")
}

func (s *SQLStore) AbortMultipartUpload(bucket, object string, uploadID string) (api.MultipartAbortResponse, error) {
	panic("not implemented")
}
func (s *SQLStore) CompleteMultipartUpload(bucket, object string, uploadID string, input *gofakes3.CompleteMultipartUploadRequest) (_ api.MultipartCompleteResponse, err error) {
	panic("not implemented")
}
