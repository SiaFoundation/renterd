package worker

import (
	"errors"
	"testing"
	"time"
)

func TestDownloaderStopped(t *testing.T) {
	w := newMockWorker()
	h := w.addHost()
	w.dl.refreshDownloaders(w.contracts())

	dl := w.dl.downloaders[h.PublicKey()]
	dl.Stop()

	req := sectorDownloadReq{
		resps: &sectorResponses{
			c: make(chan struct{}),
		},
	}
	dl.enqueue(&req)

	select {
	case <-req.resps.c:
		if err := req.resps.responses[0].err; !errors.Is(err, errDownloaderStopped) {
			t.Fatal("unexpected error response", err)
		}
	case <-time.After(time.Second):
		t.Fatal("no response")
	}
}
