package worker

import (
	"bytes"
	"io"

	rhp4 "go.sia.tech/coreutils/rhp/v4"
)

type readerLen struct {
	r      io.Reader
	length int
}

func NewReaderLen(buf []byte) rhp4.ReaderLen {
	return &readerLen{r: bytes.NewReader(buf), length: len(buf)}
}

func (r *readerLen) Len() (int, error) {
	return r.length, nil
}

func (r *readerLen) Read(p []byte) (int, error) {
	return r.r.Read(p)
}
