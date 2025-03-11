package downloader

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.sia.tech/renterd/v2/api"
	"go.sia.tech/renterd/v2/internal/test/mocks"
)

func TestDownloaderStopped(t *testing.T) {
	assertErr := func(t *testing.T, req SectorDownloadReq, expected error) {
		select {
		case <-req.Resps.Received():
			if err := req.Resps.responses[0].Err; !errors.Is(err, expected) {
				t.Fatal("unexpected error response", err)
			}
		case <-time.After(time.Second):
			t.Fatal("no response")
		}
	}

	t.Run("stop before enqueue", func(t *testing.T) {
		hm := mocks.NewHostManager()
		dl := New(context.Background(), hm.Downloader(api.HostInfo{}))
		req := SectorDownloadReq{
			Ctx:   context.Background(),
			Resps: NewSectorResponses(),
		}

		dl.Stop(errors.New(t.Name()))
		dl.Enqueue(&req)

		assertErr(t, req, ErrStopped)
	})

	t.Run("stop after enqueue", func(t *testing.T) {
		hm := mocks.NewHostManager()
		dl := New(context.Background(), hm.Downloader(api.HostInfo{}))
		req := SectorDownloadReq{
			Ctx:   context.Background(),
			Resps: NewSectorResponses(),
		}

		err := errors.New(t.Name())
		dl.Enqueue(&req)
		dl.Stop(err)

		assertErr(t, req, err)
	})
}
