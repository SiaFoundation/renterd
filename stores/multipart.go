package stores

import (
	"io"
	"time"

	"github.com/Mikubill/gofakes3"
)

type (
	dbMultipartUpload struct {
		Model

		DBObjectID uint
		DBObject   dbObject
	}
)

func CreateMultipartUpload(bucket, object string, meta map[string]string, initiated time.Time) (string, error) {
	panic("not implemented")
}

func UploadPart(bucket, object string, uploadID string, partNumber int, at time.Time, contentLength int64, input io.Reader) (etag string, err error) {
	panic("not implemented")
}

func ListMultipartUploads(bucket string, marker *gofakes3.UploadListMarker, prefix gofakes3.Prefix, limit int64) (*gofakes3.ListMultipartUploadsResult, error) {
	panic("not implemented")
}
func ListParts(bucket, object string, uploadID string, marker int, limit int64) (*gofakes3.ListMultipartUploadPartsResult, error) {
	panic("not implemented")
}

func AbortMultipartUpload(bucket, object string, uploadID string) error {
	panic("not implemented")
}
func CompleteMultipartUpload(bucket, object string, uploadID string, input *gofakes3.CompleteMultipartUploadRequest) (versionID gofakes3.VersionID, etag string, err error) {
	panic("not implemented")
}
