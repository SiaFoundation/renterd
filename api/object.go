package api

import (
	"errors"
	"fmt"
	"mime"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"go.sia.tech/renterd/object"
)

const (
	ObjectsRenameModeSingle = "single"
	ObjectsRenameModeMulti  = "multi"

	ObjectSortByHealth = "health"
	ObjectSortByName   = "name"

	ObjectSortDirAsc  = "asc"
	ObjectSortDirDesc = "desc"
)

var (
	// ErrObjectExists is returned when an operation fails because an object
	// already exists.
	ErrObjectExists = errors.New("object already exists")

	// ErrObjectNotFound is returned when an object can't be retrieved from the
	// database.
	ErrObjectNotFound = errors.New("object not found")

	// ErrObjectCorrupted is returned if we were unable to retrieve the object
	// from the database.
	ErrObjectCorrupted = errors.New("object corrupted")

	// ErrInvalidObjectSortParameters is returned when invalid sort parameters
	// were provided
	ErrInvalidObjectSortParameters = errors.New("invalid sort parameters")
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

	// ObjectAddRequest is the request type for the /bus/object/*key endpoint.
	ObjectAddRequest struct {
		Bucket      string        `json:"bucket"`
		ContractSet string        `json:"contractSet"`
		Object      object.Object `json:"object"`
		MimeType    string        `json:"mimeType"`
		ETag        string        `json:"eTag"`
	}

	// ObjectsResponse is the response type for the /bus/objects endpoint.
	ObjectsResponse struct {
		HasMore bool             `json:"hasMore"`
		Entries []ObjectMetadata `json:"entries,omitempty"`
		Object  *Object          `json:"object,omitempty"`
	}

	// ObjectsCopyRequest is the request type for the /bus/objects/copy endpoint.
	ObjectsCopyRequest struct {
		SourceBucket string `json:"sourceBucket"`
		SourcePath   string `json:"sourcePath"`

		DestinationBucket string `json:"destinationBucket"`
		DestinationPath   string `json:"destinationPath"`

		MimeType string `json:"mimeType"`
	}

	// ObjectsDeleteRequest is the request type for the /bus/objects/list endpoint.
	ObjectsListRequest struct {
		Bucket  string `json:"bucket"`
		Limit   int    `json:"limit"`
		SortBy  string `json:"sortBy"`
		SortDir string `json:"sortDir"`
		Prefix  string `json:"prefix"`
		Marker  string `json:"marker"`
	}

	// ObjectsListResponse is the response type for the /bus/objects/list endpoint.
	ObjectsListResponse struct {
		HasMore    bool             `json:"hasMore"`
		NextMarker string           `json:"nextMarker"`
		Objects    []ObjectMetadata `json:"objects"`
	}

	// ObjectsRenameRequest is the request type for the /bus/objects/rename endpoint.
	ObjectsRenameRequest struct {
		Bucket string `json:"bucket"`
		Force  bool   `json:"force"`
		From   string `json:"from"`
		To     string `json:"to"`
		Mode   string `json:"mode"`
	}

	// ObjectsStatsResponse is the response type for the /bus/stats/objects endpoint.
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

type (
	AddObjectOptions struct {
		MimeType string
		ETag     string
	}

	CopyObjectOptions struct {
		MimeType string
	}

	DeleteObjectOptions struct {
		Batch bool
	}

	DownloadObjectOptions struct {
		Prefix string
		Offset int
		Limit  int
		Range  DownloadRange
	}

	ObjectEntriesOptions struct {
		Prefix string
		Offset int
		Limit  int
	}

	GetObjectOptions struct {
		Prefix      string
		Offset      int
		Limit       int
		IgnoreDelim bool
		Marker      string
	}

	ListObjectOptions struct {
		Prefix string
		Marker string
		Limit  int
	}

	SearchObjectOptions struct {
		Key    string
		Offset int
		Limit  int
	}

	UploadObjectOptions struct {
		Offset                       int
		MinShards                    int
		TotalShards                  int
		ContractSet                  string
		MimeType                     string
		DisablePreshardingEncryption bool
	}

	UploadMultipartUploadPartOptions struct {
		DisablePreshardingEncryption bool
		EncryptionOffset             int
	}
)

func (opts UploadObjectOptions) Apply(values url.Values) {
	if opts.Offset != 0 {
		values.Set("offset", fmt.Sprint(opts.Offset))
	}
	if opts.MinShards != 0 {
		values.Set("minshards", fmt.Sprint(opts.MinShards))
	}
	if opts.TotalShards != 0 {
		values.Set("totalshards", fmt.Sprint(opts.TotalShards))
	}
	if opts.ContractSet != "" {
		values.Set("contractset", opts.ContractSet)
	}
	if opts.MimeType != "" {
		values.Set("mimetype", opts.MimeType)
	}
	if opts.DisablePreshardingEncryption {
		values.Set("disablepreshardingencryption", "true")
	}
}

func (opts UploadMultipartUploadPartOptions) Apply(values url.Values) {
	if opts.DisablePreshardingEncryption {
		values.Set("disablepreshardingencryption", "true")
	}
	if !opts.DisablePreshardingEncryption || opts.EncryptionOffset != 0 {
		values.Set("offset", fmt.Sprint(opts.EncryptionOffset))
	}
}

func (opts DownloadObjectOptions) ApplyValues(values url.Values) {
	if opts.Prefix != "" {
		values.Set("prefix", opts.Prefix)
	}
	if opts.Offset != 0 {
		values.Set("offset", fmt.Sprint(opts.Offset))
	}
	if opts.Limit != 0 {
		values.Set("limit", fmt.Sprint(opts.Limit))
	}
}

func (opts DownloadObjectOptions) ApplyHeaders(h http.Header) {
	if opts.Range != (DownloadRange{}) {
		if opts.Range.Length == -1 {
			h.Set("Range", fmt.Sprintf("bytes=%v-", opts.Range.Offset))
		} else {
			h.Set("Range", fmt.Sprintf("bytes=%v-%v", opts.Range.Offset, opts.Range.Offset+opts.Range.Length-1))
		}
	}
}

func (opts DeleteObjectOptions) Apply(values url.Values) {
	if opts.Batch {
		values.Set("batch", "true")
	}
}

func (opts GetObjectOptions) Apply(values url.Values) {
	if opts.Prefix != "" {
		values.Set("prefix", opts.Prefix)
	}
	if opts.Offset != 0 {
		values.Set("offset", fmt.Sprint(opts.Offset))
	}
	if opts.Limit != 0 {
		values.Set("limit", fmt.Sprint(opts.Limit))
	}
	if opts.IgnoreDelim {
		values.Set("ignoreDelim", "true")
	}
	if opts.Marker != "" {
		values.Set("marker", opts.Marker)
	}
}

func (opts SearchObjectOptions) Apply(values url.Values) {
	if opts.Key != "" {
		values.Set("key", opts.Key)
	}
	if opts.Offset != 0 {
		values.Set("offset", fmt.Sprint(opts.Offset))
	}
	if opts.Limit != 0 {
		values.Set("limit", fmt.Sprint(opts.Limit))
	}
}

func FormatETag(ETag string) string {
	return fmt.Sprintf("\"%s\"", ETag)
}

func ObjectPathEscape(path string) string {
	return url.PathEscape(strings.TrimPrefix(path, "/"))
}
