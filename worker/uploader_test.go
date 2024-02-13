package worker

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestUploaderStopped(t *testing.T) {
	w := newMockWorker()
	w.addHost()
	w.ul.refreshUploaders(w.contracts(), 1)

	ul := w.ul.uploaders[0]
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
	case <-time.After(time.Second):
		t.Fatal("no response")
	}
}
