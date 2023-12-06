package api

import (
	"errors"

	"go.sia.tech/renterd/object"
)

var (
	// ErrMultipartUploadNotFound is returned if the specified multipart upload
	// wasn't found.
	ErrMultipartUploadNotFound = errors.New("multipart upload not found")

	// ErrPartNotFound is returned if the specified part of a multipart upload
	// wasn't found.
	ErrPartNotFound = errors.New("multipart upload part not found")

	// ErrUploadAlreadyExists is returned when starting an upload with an id
	// that's already in use.
	ErrUploadAlreadyExists = errors.New("upload already exists")

	// ErrUnknownUpload is returned when adding sectors for an upload id that's
	// not known.
	ErrUnknownUpload = errors.New("unknown upload")
)

type (
	MultipartUpload struct {
		Bucket    string               `json:"bucket"`
		Key       object.EncryptionKey `json:"key"`
		Path      string               `json:"path"`
		UploadID  string               `json:"uploadID"`
		CreatedAt TimeRFC3339          `json:"createdAt"`
	}

	MultipartListPartItem struct {
		PartNumber   int         `json:"partNumber"`
		LastModified TimeRFC3339 `json:"lastModified"`
		ETag         string      `json:"eTag"`
		Size         int64       `json:"size"`
	}

	MultipartCompletedPart struct {
		PartNumber int    `json:"partNumber"`
		ETag       string `json:"eTag"`
	}

	CreateMultipartOptions struct {
		Key      object.EncryptionKey
		MimeType string
	}
)

type (
	MultipartAbortRequest struct {
		Bucket   string `json:"bucket"`
		Path     string `json:"path"`
		UploadID string `json:"uploadID"`
	}

	MultipartAddPartRequest struct {
		Bucket      string             `json:"bucket"`
		ETag        string             `json:"eTag"`
		Path        string             `json:"path"`
		ContractSet string             `json:"contractSet"`
		UploadID    string             `json:"uploadID"`
		PartNumber  int                `json:"partNumber"`
		Slices      []object.SlabSlice `json:"slices"`
	}

	MultipartCompleteResponse struct {
		ETag string `json:"eTag"`
	}

	MultipartCompleteRequest struct {
		Bucket   string `json:"bucket"`
		Path     string `json:"path"`
		UploadID string `json:"uploadID"`
		Parts    []MultipartCompletedPart
	}

	MultipartCreateRequest struct {
		Bucket   string               `json:"bucket"`
		Path     string               `json:"path"`
		Key      object.EncryptionKey `json:"key"`
		MimeType string               `json:"mimeType"`
	}

	MultipartCreateResponse struct {
		UploadID string `json:"uploadID"`
	}

	MultipartListPartsRequest struct {
		Bucket           string `json:"bucket"`
		Path             string `json:"path"`
		UploadID         string `json:"uploadID"`
		PartNumberMarker int    `json:"partNumberMarker"`
		Limit            int64  `json:"limit"`
	}

	MultipartListPartsResponse struct {
		HasMore    bool                    `json:"hasMore"`
		NextMarker int                     `json:"nextMarker"`
		Parts      []MultipartListPartItem `json:"parts"`
	}

	MultipartListUploadsRequest struct {
		Bucket         string `json:"bucket"`
		Prefix         string `json:"prefix"`
		PathMarker     string `json:"pathMarker"`
		UploadIDMarker string `json:"uploadIDMarker"`
		Limit          int    `json:"limit"`
	}

	MultipartListUploadsResponse struct {
		HasMore            bool              `json:"hasMore"`
		NextPathMarker     string            `json:"nextMarker"`
		NextUploadIDMarker string            `json:"nextUploadIDMarker"`
		Uploads            []MultipartUpload `json:"uploads"`
	}
)
