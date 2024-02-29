package worker

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestUploaderStopped(t *testing.T) {
	w := newTestWorker(t)
	w.AddHosts(1)

	um := w.uploadManager
	um.refreshUploaders(w.Contracts(), 1)

	ul := um.uploaders[0]
	ul.Stop(errors.New("test"))

	req := sectorUploadReq{
		responseChan: make(chan sectorUploadResp),
		sector:       &sectorUpload{ctx: context.Background()},
	}
	ul.enqueue(&req)

	select {
	case res := <-req.responseChan:
		if !errors.Is(res.err, errUploaderStopped) {
			t.Fatal("expected error response")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no response")
	}
}
