package api

import (
	"errors"
	"mime"
	"net/http"
	"path/filepath"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/object"
)

const (
	ObjectsRenameModeSingle = "single"
	ObjectsRenameModeMulti  = "multi"
)

var (
	// ErrObjectNotFound is returned when an object can't be retrieved from the
	// database.
	ErrObjectNotFound = errors.New("object not found")

	// ErrObjectCorrupted is returned if we were unable to retrieve the object
	// from the database.
	ErrObjectCorrupted = errors.New("object corrupted")
)

type (
	// Object wraps an object.Object with its metadata.
	Object struct {
		ObjectMetadata
		object.Object
	}

	// ObjectMetadata contains various metadata about an object.
	ObjectMetadata struct {
		ETag     string    `json:"eTag,omitempty"`
		Health   float64   `json:"health"`
		MimeType string    `json:"mimeType,omitempty"`
		ModTime  time.Time `json:"modTime"`
		Name     string    `json:"name"`
		Size     int64     `json:"size"`
	}

	// ObjectAddRequest is the request type for the /object/*key endpoint.
	ObjectAddRequest struct {
		Bucket        string                                   `json:"bucket"`
		ContractSet   string                                   `json:"contractSet"`
		Object        object.Object                            `json:"object"`
		UsedContracts map[types.PublicKey]types.FileContractID `json:"usedContracts"`
		MimeType      string                                   `json:"mimeType"`
		ETag          string                                   `json:"eTag"`
	}

	// ObjectsResponse is the response type for the /objects endpoint.
	ObjectsResponse struct {
		HasMore bool             `json:"hasMore"`
		Entries []ObjectMetadata `json:"entries,omitempty"`
		Object  *Object          `json:"object,omitempty"`
	}

	// ObjectsCopyRequest is the request type for the /objects/copy endpoint.
	ObjectsCopyRequest struct {
		SourceBucket string `json:"sourceBucket"`
		SourcePath   string `json:"sourcePath"`

		DestinationBucket string `json:"destinationBucket"`
		DestinationPath   string `json:"destinationPath"`

		MimeType string `json:"mimeType"`
	}

	// ObjectsDeleteRequest is the request type for the /objects/list endpoint.
	ObjectsListRequest struct {
		Bucket string `json:"bucket"`
		Limit  int    `json:"limit"`
		Prefix string `json:"prefix"`
		Marker string `json:"marker"`
	}

	// ObjectsListResponse is the response type for the /objects/list endpoint.
	ObjectsListResponse struct {
		HasMore    bool             `json:"hasMore"`
		NextMarker string           `json:"nextMarker"`
		Objects    []ObjectMetadata `json:"objects"`
	}

	// ObjectsRenameRequest is the request type for the /objects/rename endpoint.
	ObjectsRenameRequest struct {
		Bucket string `json:"bucket"`
		From   string `json:"from"`
		To     string `json:"to"`
		Mode   string `json:"mode"`
	}

	// ObjectsStatsResponse is the response type for the /stats/objects endpoint.
	ObjectsStatsResponse struct {
		NumObjects        uint64 `json:"numObjects"`        // number of objects
		TotalObjectsSize  uint64 `json:"totalObjectsSize"`  // size of all objects
		TotalSectorsSize  uint64 `json:"totalSectorsSize"`  // uploaded size of all objects
		TotalUploadedSize uint64 `json:"totalUploadedSize"` // uploaded size of all objects including redundant sectors
	}
)

// LastModified returns the object's ModTime formatted for use in the
// 'Last-Modified' header
func (o ObjectMetadata) LastModified() string {
	return o.ModTime.UTC().Format(http.TimeFormat)
}

// ContentType returns the object's MimeType for use in the 'Content-Type'
// header, if the object's mime type is empty we try and deduce it from the
// extension in the object's name.
func (o ObjectMetadata) ContentType() string {
	if o.MimeType != "" {
		return o.MimeType
	}

	if ext := filepath.Ext(o.Name); ext != "" {
		return mime.TypeByExtension(ext)
	}

	return ""
}
