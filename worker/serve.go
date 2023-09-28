package worker

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"

	"github.com/gabriel-vasile/mimetype"
	"github.com/gotd/contrib/http_range"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
)

type (
	// contentReader implements io.ReadSeeker but really Seek only returns the
	// object's size, this reader can be used to pass to http.ServeContent but
	// only if the caller made sure to manipulate the request in such a way that
	// the only seeks are to find out the object's size
	contentReader struct {
		r    io.Reader
		size int64
	}
)

func newContentReader(r io.Reader, obj api.Object) (io.ReadSeeker, string, error) {
	contentType := obj.ContentType()
	if contentType == "" {
		header := bytes.NewBuffer(nil)
		mtype, err := mimetype.DetectReader(io.TeeReader(r, header))
		if err != nil {
			return nil, "", err
		}
		r = io.MultiReader(header, r)
		contentType = mtype.String()
	}

	return &contentReader{
		r:    r,
		size: obj.Size,
	}, contentType, nil
}

func (cr *contentReader) Seek(offset int64, whence int) (int64, error) {
	return cr.size, nil
}

func (cr *contentReader) Read(p []byte) (int, error) {
	return cr.r.Read(p)
}

func serveContent(rw http.ResponseWriter, req *http.Request, obj api.Object, downloadFn func(w io.Writer, offset, length int64) error) (int, error) {
	// parse offset and length from the request range header
	offset, length, err := parseRangeHeader(req, obj)
	if err != nil {
		return http.StatusRequestedRangeNotSatisfiable, err
	}

	// launch the download in a goroutine
	pr, pw := io.Pipe()
	go func() {
		if err := downloadFn(pw, offset, length); err != nil {
			pw.CloseWithError(err)
		} else {
			pw.Close()
		}
	}()

	// create a content reader, this will return the object's content type
	rs, contentType, err := newContentReader(pr, obj)
	if err != nil {
		return http.StatusInternalServerError, err
	}

	// set the response headers, no need to set Last-Modified header as
	// serveContent does that for us
	rw.Header().Set("ETag", api.FormatETag(buildETag(req, obj.ETag)))
	rw.Header().Set("Content-Type", contentType)

	// override the range request header to avoid seeks in http.ServeContent
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))

	http.ServeContent(rw, req, obj.Name, obj.ModTime, rs)
	return http.StatusOK, nil
}

func parseRangeHeader(req *http.Request, obj api.Object) (int64, int64, error) {
	// parse the request range
	ranges, err := http_range.ParseRange(req.Header.Get("Range"), obj.Size)
	if err != nil {
		return 0, 0, err
	}

	// extract requested offset and length
	offset := int64(0)
	length := obj.Size
	if len(ranges) > 0 {
		offset, length = ranges[0].Start, ranges[0].Length
		if offset < 0 || length < 0 || offset+length > obj.Size {
			return 0, 0, fmt.Errorf("invalid range: %v %v", offset, length)
		}
	}
	return offset, length, nil
}

func buildETag(req *http.Request, objETag string) string {
	rh := req.Header.Get("Range")
	if rh == "" {
		return objETag
	}

	h := types.NewHasher()
	h.E.Write([]byte(rh))
	h.E.Write([]byte(objETag))
	sum := h.Sum()
	return hex.EncodeToString(sum[:])
}
