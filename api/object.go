package api

import (
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"

	"go.sia.tech/renterd/object"
)

const (
	ObjectMetadataPrefix = "X-Sia-Meta-"

	ObjectsRenameModeSingle = "single"
	ObjectsRenameModeMulti  = "multi"

	ObjectSortByHealth = "health"
	ObjectSortByName   = "name"
	ObjectSortBySize   = "size"

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

	// ErrSlabNotFound is returned when a slab can't be retrieved from the
	// database.
	ErrSlabNotFound = errors.New("slab not found")
)

type (
	// Object wraps an object.Object with its metadata.
	Object struct {
		Metadata ObjectUserMetadata `json:"metadata,omitempty"`
		ObjectMetadata
		*object.Object
	}

	// ObjectMetadata contains various metadata about an object.
	ObjectMetadata struct {
		ETag     string      `json:"eTag,omitempty"`
		Health   float64     `json:"health"`
		ModTime  TimeRFC3339 `json:"modTime"`
		Name     string      `json:"name"`
		Size     int64       `json:"size"`
		MimeType string      `json:"mimeType,omitempty"`
	}

	// ObjectUserMetadata contains user-defined metadata about an object and can
	// be provided through `X-Sia-Meta-` meta headers.
	//
	// NOTE: `X-Amz-Meta-` headers are supported and will be converted to sia
	// metadata headers internally, this means that S3 clients can safely keep
	// using Amazon headers and find the metadata will be persisted in Sia as
	// well
	ObjectUserMetadata map[string]string

	// ObjectsResponse is the response type for the /bus/objects endpoint.
	ObjectsResponse struct {
		HasMore bool             `json:"hasMore"`
		Entries []ObjectMetadata `json:"entries,omitempty"`
		Object  *Object          `json:"object,omitempty"`
	}

	// GetObjectResponse is the response type for the GET /worker/object endpoint.
	GetObjectResponse struct {
		Content io.ReadCloser `json:"content"`
		HeadObjectResponse
	}

	// HeadObjectResponse is the response type for the HEAD /worker/object endpoint.
	HeadObjectResponse struct {
		ContentType  string             `json:"contentType"`
		Etag         string             `json:"eTag"`
		LastModified string             `json:"lastModified"`
		Range        *DownloadRange     `json:"range,omitempty"`
		Size         int64              `json:"size"`
		Metadata     ObjectUserMetadata `json:"metadata"`
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

	ObjectsStatsOpts struct {
		Bucket string
	}

	// ObjectsStatsResponse is the response type for the /bus/stats/objects endpoint.
	ObjectsStatsResponse struct {
		NumObjects                 uint64  `json:"numObjects"`                 // number of objects
		NumUnfinishedObjects       uint64  `json:"numUnfinishedObjects"`       // number of unfinished objects
		MinHealth                  float64 `json:"minHealth"`                  // minimum health of all objects
		TotalObjectsSize           uint64  `json:"totalObjectsSize"`           // size of all objects
		TotalUnfinishedObjectsSize uint64  `json:"totalUnfinishedObjectsSize"` // size of all unfinished objects
		TotalSectorsSize           uint64  `json:"totalSectorsSize"`           // uploaded size of all objects
		TotalUploadedSize          uint64  `json:"totalUploadedSize"`          // uploaded size of all objects including redundant sectors
	}
)

func ExtractObjectUserMetadataFrom(metadata map[string]string) ObjectUserMetadata {
	oum := make(map[string]string)
	for k, v := range metadata {
		if strings.HasPrefix(strings.ToLower(k), strings.ToLower(ObjectMetadataPrefix)) {
			oum[k[len(ObjectMetadataPrefix):]] = v
		}
	}
	return oum
}

// LastModified returns the object's ModTime formatted for use in the
// 'Last-Modified' header
func (o ObjectMetadata) LastModified() string {
	return o.ModTime.Std().Format(http.TimeFormat)
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
	// AddObjectOptions is the options type for the bus client.
	AddObjectOptions struct {
		ETag     string
		MimeType string
		Metadata ObjectUserMetadata
	}

	// AddObjectRequest is the request type for the /bus/object/*key endpoint.
	AddObjectRequest struct {
		Bucket      string             `json:"bucket"`
		ContractSet string             `json:"contractSet"`
		Object      object.Object      `json:"object"`
		ETag        string             `json:"eTag"`
		MimeType    string             `json:"mimeType"`
		Metadata    ObjectUserMetadata `json:"metadata"`
	}

	// CopyObjectOptions is the options type for the bus client.
	CopyObjectOptions struct {
		MimeType string
		Metadata ObjectUserMetadata
	}

	// CopyObjectsRequest is the request type for the /bus/objects/copy endpoint.
	CopyObjectsRequest struct {
		SourceBucket string `json:"sourceBucket"`
		SourcePath   string `json:"sourcePath"`

		DestinationBucket string `json:"destinationBucket"`
		DestinationPath   string `json:"destinationPath"`

		MimeType string             `json:"mimeType"`
		Metadata ObjectUserMetadata `json:"metadata"`
	}

	DeleteObjectOptions struct {
		Batch bool
	}

	HeadObjectOptions struct {
		IgnoreDelim bool
		Range       DownloadRange
	}

	DownloadObjectOptions struct {
		GetObjectOptions
		Range DownloadRange
	}

	GetObjectOptions struct {
		Prefix       string
		Offset       int
		Limit        int
		IgnoreDelim  bool
		Marker       string
		OnlyMetadata bool
		SortBy       string
		SortDir      string
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

	// UploadObjectOptions is the options type for the worker client.
	UploadObjectOptions struct {
		Offset        int
		MinShards     int
		TotalShards   int
		ContractSet   string
		ContentLength int64
		MimeType      string
		Metadata      ObjectUserMetadata
	}

	UploadMultipartUploadPartOptions struct {
		EncryptionOffset *int
		ContentLength    int64
	}
)

func (opts UploadObjectOptions) ApplyValues(values url.Values) {
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
}

func (opts UploadObjectOptions) ApplyHeaders(h http.Header) {
	for k, v := range opts.Metadata {
		h.Set(ObjectMetadataPrefix+k, v)
	}
}

func (opts UploadMultipartUploadPartOptions) Apply(values url.Values) {
	if opts.EncryptionOffset != nil {
		values.Set("offset", fmt.Sprint(*opts.EncryptionOffset))
	}
}

func (opts DownloadObjectOptions) ApplyValues(values url.Values) {
	opts.GetObjectOptions.Apply(values)
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

func (opts HeadObjectOptions) Apply(values url.Values) {
	if opts.IgnoreDelim {
		values.Set("ignoreDelim", "true")
	}
}

func (opts HeadObjectOptions) ApplyHeaders(h http.Header) {
	if opts.Range != (DownloadRange{}) {
		if opts.Range.Length == -1 {
			h.Set("Range", fmt.Sprintf("bytes=%v-", opts.Range.Offset))
		} else {
			h.Set("Range", fmt.Sprintf("bytes=%v-%v", opts.Range.Offset, opts.Range.Offset+opts.Range.Length-1))
		}
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
	if opts.OnlyMetadata {
		values.Set("onlymetadata", "true")
	}
	if opts.SortBy != "" {
		values.Set("sortBy", opts.SortBy)
	}
	if opts.SortDir != "" {
		values.Set("sortDir", opts.SortDir)
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

func FormatETag(eTag string) string {
	return fmt.Sprintf("%q", eTag)
}

func ObjectPathEscape(path string) string {
	return url.PathEscape(strings.TrimPrefix(path, "/"))
}
