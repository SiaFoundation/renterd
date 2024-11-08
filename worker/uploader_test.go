package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.sia.tech/renterd/internal/upload/uploader"
)

func TestUploaderStopped(t *testing.T) {
	w := newTestWorker(t)
	w.AddHosts(1)

	um := w.uploadManager
	um.refreshUploaders(w.Contracts(), 1)

	ul := um.uploaders[0]
	ul.Stop(errors.New("test"))

	req := uploader.SectorUploadReq{
		Ctx:          context.Background(),
		ResponseChan: make(chan uploader.SectorUploadResp),
	}
	ul.Enqueue(&req)

	select {
	case res := <-req.ResponseChan:
		if !errors.Is(res.Err, uploader.ErrStopped) {
			t.Fatal("expected error response")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no response")
	}
}
