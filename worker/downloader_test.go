package worker

import (
	"errors"
	"testing"
	"time"
)

func TestDownloaderStopped(t *testing.T) {
	w := newTestWorker(t)
	hosts := w.AddHosts(1)

	// convenience variables
	dm := w.downloadManager
	h := hosts[0]

	dm.refreshDownloaders(w.Contracts())
	dl := w.downloadManager.downloaders[h.PublicKey()]
	dl.Stop()

	req := sectorDownloadReq{
		resps: &sectorResponses{
			c: make(chan struct{}, 1),
		},
	}
	dl.enqueue(&req)

	select {
	case <-req.resps.c:
		if err := req.resps.responses[0].err; !errors.Is(err, errDownloaderStopped) {
			t.Fatal("unexpected error response", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no response")
	}
}
