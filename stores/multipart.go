package stores

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"unicode/utf8"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/object"
	"gorm.io/gorm"
	"lukechampine.com/frand"
)

type (
	dbMultipartUpload struct {
		Model

		Key        []byte
		UploadID   string `gorm:"uniqueIndex;NOT NULL;size:64"`
		ObjectID   string `gorm:"index;NOT NULL"`
		DBBucket   dbBucket
		DBBucketID uint              `gorm:"index;NOT NULL"`
		Parts      []dbMultipartPart `gorm:"constraint:OnDelete:CASCADE"` // CASCADE to delete parts too
		MimeType   string            `gorm:"index"`
	}

	dbMultipartPart struct {
		Model
		Etag                string `gorm:"index"`
		PartNumber          int    `gorm:"index"`
		Size                uint64
		DBMultipartUploadID uint      `gorm:"index;NOT NULL"`
		Slabs               []dbSlice `gorm:"constraint:OnDelete:CASCADE"` // CASCADE to delete slices too

	}
)

func (dbMultipartUpload) TableName() string {
	return "multipart_uploads"
}

func (dbMultipartPart) TableName() string {
	return "multipart_parts"
}

func (s *SQLStore) CreateMultipartUpload(ctx context.Context, bucket, path string, ec object.EncryptionKey, mimeType string) (api.MultipartCreateResponse, error) {
	// Marshal key
	key, err := ec.MarshalText()
	if err != nil {
		return api.MultipartCreateResponse{}, err
	}
	var uploadID string
	err = s.retryTransaction(func(tx *gorm.DB) error {
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
			DBBucketID: bucketID,
			Key:        key,
			UploadID:   uploadID,
			ObjectID:   path,
			MimeType:   mimeType,
		}).Error; err != nil {
			return fmt.Errorf("failed to create multipart upload: %w", err)
		}
		return nil
	})
	return api.MultipartCreateResponse{
		UploadID: uploadID,
	}, err
}

func (s *SQLStore) AddMultipartPart(ctx context.Context, bucket, path, contractSet, eTag, uploadID string, partNumber int, slices []object.SlabSlice, partialSlabs []object.PartialSlab, usedContracts map[types.PublicKey]types.FileContractID) (err error) {
	return s.retryTransaction(func(tx *gorm.DB) error {
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
		var size uint64
		for _, slice := range slices {
			size += uint64(slice.Length)
		}
		for _, ps := range partialSlabs {
			size += uint64(ps.Length)
		}
		// Create a new part.
		part := dbMultipartPart{
			Etag:                eTag,
			PartNumber:          partNumber,
			DBMultipartUploadID: mu.ID,
			Size:                size,
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
}

func (s *SQLStore) MultipartUpload(ctx context.Context, uploadID string) (resp api.MultipartUpload, err error) {
	err = s.retryTransaction(func(tx *gorm.DB) error {
		var dbUpload dbMultipartUpload
		err := tx.
			Model(&dbMultipartUpload{}).
			Joins("DBBucket").
			Where("upload_id", uploadID).
			Take(&dbUpload).
			Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return api.ErrMultipartUploadNotFound
		} else if err != nil {
			return err
		}
		resp, err = dbUpload.convert()
		return err
	})
	return
}

func (s *SQLStore) MultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker string, limit int) (resp api.MultipartListUploadsResponse, err error) {
	limitUsed := limit > 0
	if !limitUsed {
		limit = math.MaxInt64
	} else {
		limit++
	}

	prefixExpr := gorm.Expr("TRUE")
	if prefix != "" {
		prefixExpr = gorm.Expr("SUBSTR(object_id, 1, ?) = ?", utf8.RuneCountInString(prefix), prefix)
	}
	keyMarkerExpr := gorm.Expr("TRUE")
	if keyMarker != "" {
		keyMarkerExpr = gorm.Expr("object_id > ?", keyMarker)
	}
	uploadIDMarkerExpr := gorm.Expr("TRUE")
	if uploadIDMarker != "" {
		uploadIDMarkerExpr = gorm.Expr("upload_id > ?", keyMarker)
	}

	err = s.retryTransaction(func(tx *gorm.DB) error {
		var dbUploads []dbMultipartUpload
		err := tx.
			Model(&dbMultipartUpload{}).
			Joins("DBBucket").
			Where("? AND ? AND ?", prefixExpr, keyMarkerExpr, uploadIDMarkerExpr).
			Limit(limit).
			Find(&dbUploads).
			Error
		if err != nil {
			return err
		}
		// Check if there are more uploads beyond 'limit'.
		if limitUsed && len(dbUploads) == int(limit) {
			resp.HasMore = true
			dbUploads = dbUploads[:len(dbUploads)-1]
			resp.NextPathMarker = dbUploads[len(dbUploads)-1].ObjectID
			resp.NextUploadIDMarker = dbUploads[len(dbUploads)-1].UploadID
		}
		for _, upload := range dbUploads {
			u, err := upload.convert()
			if err != nil {
				return err
			}
			resp.Uploads = append(resp.Uploads, u)
		}
		return nil
	})
	return
}

func (s *SQLStore) MultipartUploadParts(ctx context.Context, bucket, object string, uploadID string, marker int, limit int64) (resp api.MultipartListPartsResponse, _ error) {
	limitUsed := limit > 0
	if !limitUsed {
		limit = math.MaxInt64
	} else {
		limit++
	}

	err := s.retryTransaction(func(tx *gorm.DB) error {
		var dbParts []dbMultipartPart
		err := tx.
			Model(&dbMultipartPart{}).
			Joins("INNER JOIN multipart_uploads mus ON mus.id = multipart_parts.db_multipart_upload_id").
			Joins("INNER JOIN buckets b ON b.name = ? AND b.id = mus.db_bucket_id", bucket).
			Where("mus.object_id = ? AND mus.upload_id = ? AND part_number > ?", object, uploadID, marker).
			Order("part_number ASC").
			Limit(int(limit)).
			Find(&dbParts).
			Error
		if err != nil {
			return err
		}
		// Check if there are more parts beyond 'limit'.
		if limitUsed && len(dbParts) == int(limit) {
			resp.HasMore = true
			dbParts = dbParts[:len(dbParts)-1]
			resp.NextMarker = dbParts[len(dbParts)-1].PartNumber
		}
		for _, part := range dbParts {
			resp.Parts = append(resp.Parts, api.MultipartListPartItem{
				PartNumber:   part.PartNumber,
				LastModified: part.CreatedAt.UTC(),
				ETag:         part.Etag,
				Size:         int64(part.Size),
			})
		}
		return nil
	})
	return resp, err
}

func (s *SQLStore) AbortMultipartUpload(ctx context.Context, bucket, path string, uploadID string) error {
	return s.retryTransaction(func(tx *gorm.DB) error {
		// Find multipart upload.
		var mu dbMultipartUpload
		err := tx.Where("upload_id = ?", uploadID).
			Preload("Parts").
			Joins("DBBucket").
			Take(&mu).
			Error
		if err != nil {
			return fmt.Errorf("failed to fetch multipart upload: %w", err)
		}
		if mu.ObjectID != path {
			// Check object id.
			return fmt.Errorf("object id mismatch: %v != %v: %w", mu.ObjectID, path, api.ErrObjectNotFound)
		} else if mu.DBBucket.Name != bucket {
			// Check bucket name.
			return fmt.Errorf("bucket name mismatch: %v != %v: %w", mu.DBBucket.Name, bucket, api.ErrBucketNotFound)
		}
		err = tx.Delete(&mu).Error
		if err != nil {
			return fmt.Errorf("failed to delete multipart upload: %w", err)
		}
		return pruneSlabs(tx)
	})
}

func (s *SQLStore) CompleteMultipartUpload(ctx context.Context, bucket, path string, uploadID string, parts []api.MultipartCompletedPart) (_ api.MultipartCompleteResponse, err error) {
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
	err = s.retryTransaction(func(tx *gorm.DB) error {
		// Find multipart upload.
		var mu dbMultipartUpload
		err = tx.Where("upload_id = ?", uploadID).
			Preload("Parts").
			Joins("DBBucket").
			Take(&mu).
			Error
		if err != nil {
			return fmt.Errorf("failed to fetch multipart upload: %w", err)
		}
		// Check object id.
		if mu.ObjectID != path {
			return fmt.Errorf("object id mismatch: %v != %v: %w", mu.ObjectID, path, api.ErrObjectNotFound)
		}

		// Check bucket name.
		if mu.DBBucket.Name != bucket {
			return fmt.Errorf("bucket name mismatch: %v != %v: %w", mu.DBBucket.Name, bucket, api.ErrBucketNotFound)
		}

		// Delete potentially existing object.
		_, err := deleteObject(tx, bucket, path)
		if err != nil {
			return fmt.Errorf("failed to delete object: %w", err)
		}

		// Sort the parts.
		sort.Slice(mu.Parts, func(i, j int) bool {
			return mu.Parts[i].PartNumber < mu.Parts[j].PartNumber
		})
		// Find relevant parts.
		var dbParts []dbMultipartPart
		var size uint64
		j := 0
		for _, part := range parts {
			for {
				if j >= len(mu.Parts) {
					// ran out of parts in the database
					return api.ErrPartNotFound
				} else if mu.Parts[j].PartNumber > part.PartNumber {
					// missing part
					return api.ErrPartNotFound
				} else if mu.Parts[j].PartNumber == part.PartNumber && mu.Parts[j].Etag == strings.Trim(part.ETag, "\"") {
					// found a match
					dbParts = append(dbParts, mu.Parts[j])
					size += mu.Parts[j].Size
					j++
					break
				} else {
					// try next
					j++
				}
			}
		}

		// Fetch all the slices in the right order.
		var slices []dbSlice
		h := types.NewHasher()
		for _, part := range dbParts {
			var partSlices []dbSlice
			err = tx.Model(&dbSlice{}).
				Joins("INNER JOIN multipart_parts mp ON mp.id = slices.db_multipart_part_id AND mp.id = ?", part.ID).
				Joins("INNER JOIN multipart_uploads mus ON mus.id = mp.db_multipart_upload_id").
				Find(&partSlices).
				Error
			if err != nil {
				return fmt.Errorf("failed to fetch slices: %w", err)
			}
			slices = append(slices, partSlices...)
			if _, err = h.E.Write([]byte(part.Etag)); err != nil {
				return fmt.Errorf("failed to hash etag: %w", err)
			}
		}

		// Compute ETag.
		sum := h.Sum()
		eTag = hex.EncodeToString(sum[:])

		// Create the object.
		obj := dbObject{
			DBBucketID: mu.DBBucketID,
			ObjectID:   path,
			Key:        mu.Key,
			Size:       int64(size),
			MimeType:   mu.MimeType,
			Etag:       eTag,
		}
		if err := tx.Create(&obj).Error; err != nil {
			return fmt.Errorf("failed to create object: %w", err)
		}

		// Assign the right object id and unassign the multipart upload.  Also
		// clear the ID to make sure new slices are created with IDs in
		// ascending order.
		for i := range slices {
			slices[i].ID = 0
			slices[i].DBObjectID = &obj.ID
			slices[i].DBMultipartPartID = nil
		}

		// Save updated slices.
		if err := tx.CreateInBatches(slices, 100).Error; err != nil {
			return fmt.Errorf("failed to save slices: %w", err)
		}

		// Delete the multipart upload.
		if err := tx.Delete(&mu).Error; err != nil {
			return fmt.Errorf("failed to delete multipart upload: %w", err)
		}
		return nil
	})
	if err != nil {
		return api.MultipartCompleteResponse{}, err
	}
	return api.MultipartCompleteResponse{
		ETag: eTag,
	}, nil
}

func (u dbMultipartUpload) convert() (api.MultipartUpload, error) {
	var key object.EncryptionKey
	if err := key.UnmarshalText(u.Key); err != nil {
		return api.MultipartUpload{}, fmt.Errorf("failed to unmarshal key: %w", err)
	}
	return api.MultipartUpload{
		Bucket:    u.DBBucket.Name,
		Key:       key,
		Path:      u.ObjectID,
		UploadID:  u.UploadID,
		CreatedAt: u.CreatedAt.UTC(),
	}, nil
}
