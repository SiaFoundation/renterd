package worker

import (
	"errors"
	"fmt"
	"io"
	"net/http"

	"go.sia.tech/renterd/api"
)

type (
	// contentReader implements io.ReadSeeker but really Seek only returns the
	// object's size, this reader can be used to pass to http.ServeContent but
	// only if the caller made sure to manipulate the request in such a way that
	// the only seeks are to find out the object's size
	contentReader struct {
		r           io.Reader
		readStarted bool
		size        int64
		seekOffset  int64
		dataOffset  int64
	}
)

func newContentReader(r io.Reader, size int64, offset int64) io.ReadSeeker {
	return &contentReader{
		r:          r,
		dataOffset: offset,
		seekOffset: offset,
		size:       size,
	}
}

func (cr *contentReader) Seek(offset int64, whence int) (int64, error) {
	if cr.readStarted {
		return 0, errors.New("can't call Seek after calling Read")
	} else if offset == 0 && whence == io.SeekEnd {
		cr.seekOffset = cr.size
	} else if offset == 0 && whence == io.SeekStart {
		cr.seekOffset = 0
	} else if offset == cr.dataOffset && whence == io.SeekStart {
		cr.seekOffset = cr.dataOffset
	} else {
		return 0, errors.New("unexpected seek")
	}
	return cr.seekOffset, nil
}

func (cr *contentReader) Read(p []byte) (int, error) {
	if !cr.readStarted && cr.seekOffset != cr.dataOffset {
		return 0, fmt.Errorf("contentReader: Read called but offset doesn't match data offset %v != %v", cr.seekOffset, cr.dataOffset)
	}
	cr.readStarted = true
	return cr.r.Read(p)
}

func serveContent(rw http.ResponseWriter, req *http.Request, name string, content io.Reader, hor api.HeadObjectResponse) {
	// set content type and etag
	rw.Header().Set("Content-Type", hor.ContentType)
	rw.Header().Set("ETag", api.FormatETag(hor.Etag))

	// set the user metadata headers
	for k, v := range hor.Metadata {
		rw.Header().Set(fmt.Sprintf("%s%s", api.ObjectMetadataPrefix, k), v)
	}

	// create a content reader
	rs := newContentReader(content, hor.Size, hor.Range.Offset)

	http.ServeContent(rw, req, name, hor.LastModified.Std(), rs)
}
