package worker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/stats"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
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

func TestUploaderTrackSectorUpload(t *testing.T) {
	// create test worker
	w := newTestWorker(t)
	h := w.AddHosts(1)[0]

	// convenience variables
	um := w.uploadManager

	// create custom logger to capture logs
	observedZapCore, observedLogs := observer.New(zap.DebugLevel)
	um.logger = zap.New(observedZapCore).Sugar()

	// create uploader
	um.refreshUploaders(w.Contracts(), 1)
	ul := um.uploaders[0]
	ul.statsSectorUploadEstimateInMS = stats.NoDecay()

	// executeReq is a helper that enqueues a sector upload request and returns
	// a response, we don't need to alter the request because we'll configure
	// the test host to see whether we properly handle sector uploads
	executeReq := func(overdrive bool) sectorUploadResp {
		t.Helper()
		responseChan := make(chan sectorUploadResp)
		ul.enqueue(&sectorUploadReq{
			contractLockDuration: time.Second,
			contractLockPriority: lockingPriorityUpload,
			overdrive:            overdrive,
			responseChan:         responseChan,
			sector:               &sectorUpload{ctx: context.Background()},
		})
		return <-responseChan
	}

	// assert successful uploads reset consecutive failures and track duration
	h.uploadDelay = 100 * time.Millisecond
	ul.consecutiveFailures = 1
	res := executeReq(false)
	if res.err != nil {
		t.Fatal(res.err)
	} else if ul.consecutiveFailures != 0 {
		t.Fatal("unexpected consecutive failures")
	} else if ul.statsSectorUploadSpeedBytesPerMS.Len() != 1 {
		t.Fatal("unexpected number of data points")
	} else if ul.statsSectorUploadEstimateInMS.Len() != 1 {
		t.Fatal("unexpected number of data points")
	} else if avg := ul.statsSectorUploadEstimateInMS.Average(); !(100 <= avg && avg < 200) {
		t.Fatal("unexpected average", avg)
	}

	// assert we refresh the underlying contract when max rev. is reached
	h.rev.RevisionNumber = types.MaxRevisionNumber
	res = executeReq(false)
	if res.err == nil {
		t.Fatal("expected error")
	} else if logs := observedLogs.TakeAll(); len(logs) == 0 {
		t.Fatal("missing log entry")
	} else if !strings.Contains(logs[0].Message, "failed to refresh the uploader's contract") {
		t.Fatal("ununexpected log line")
	}

	// assert we punish the host if he was slower than one of the other hosts
	// overdriving his sector, also assert we don't track consecutive failures
	ul.consecutiveFailures = 1
	h.rev.RevisionNumber = 0 // reset
	h.uploadErr = errSectorUploadFinished
	res = executeReq(false)
	if res.err == nil {
		t.Fatal("expected error")
	} else if ul.consecutiveFailures != 1 {
		t.Fatal("unexpected consecutive failures")
	} else if ul.statsSectorUploadSpeedBytesPerMS.Len() != 1 {
		t.Fatal("unexpected number of data points")
	} else if ul.statsSectorUploadEstimateInMS.Len() != 2 {
		t.Fatal("unexpected number of data points")
	} else if avg := ul.statsSectorUploadEstimateInMS.Average(); !(500 <= avg && avg < 750) {
		t.Fatal("unexpected average", avg)
	}

	// assert we don't punish the host if it itself was overdriving the sector
	h.uploadErr = errSectorUploadFinished
	res = executeReq(true)
	if res.err == nil {
		t.Fatal("expected error")
	} else if ul.consecutiveFailures != 1 {
		t.Fatal("unexpected consecutive failures")
	} else if ul.statsSectorUploadSpeedBytesPerMS.Len() != 1 {
		t.Fatal("unexpected number of data points")
	} else if ul.statsSectorUploadEstimateInMS.Len() != 2 {
		t.Fatal("unexpected number of data points")
	} else if logs := observedLogs.TakeAll(); len(logs) != 1 {
		t.Fatal("missing log entry")
	} else if !strings.Contains(logs[0].Message, "skip tracking sector upload") {
		t.Fatal("ununexpected log line")
	}

	// assert we punish the host if it was overdriving and it was slow to dial
	// the host, also assert we track it as a failure
	h.uploadDelay = time.Second
	h.uploadErr = fmt.Errorf("%w;%w", errDialTransport, errSectorUploadFinished)
	res = executeReq(true)
	if res.err == nil {
		t.Fatal("expected error")
	} else if ul.consecutiveFailures != 2 {
		t.Fatal("unexpected consecutive failures")
	} else if ul.statsSectorUploadSpeedBytesPerMS.Len() != 1 {
		t.Fatal("unexpected number of data points")
	} else if ul.statsSectorUploadEstimateInMS.Len() != 3 {
		t.Fatal("unexpected number of data points")
	} else if logs := observedLogs.TakeAll(); len(logs) != 0 {
		t.Fatal("unexpected log entry")
	} else if avg := ul.statsSectorUploadEstimateInMS.Average(); avg < float64((10*time.Second).Milliseconds())/3 {
		t.Fatal("unexpected average", avg)
	}

	// assert we punish the host if it failed the upload for any other reason
	h.uploadErr = errors.New("host error")
	res = executeReq(false)
	if res.err == nil {
		t.Fatal("expected error")
	} else if ul.consecutiveFailures != 3 {
		t.Fatal("unexpected consecutive failures")
	} else if ul.statsSectorUploadSpeedBytesPerMS.Len() != 1 {
		t.Fatal("unexpected number of data points")
	} else if ul.statsSectorUploadEstimateInMS.Len() != 4 {
		t.Fatal("unexpected number of data points")
	} else if avg := ul.statsSectorUploadEstimateInMS.Average(); avg < float64((time.Hour).Milliseconds())/4 {
		t.Fatal("unexpected average", avg)
	}
}
