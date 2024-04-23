package worker

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"lukechampine.com/frand"
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
	w := newTestWorker(t)

	// convenience variables
	um := w.uploadManager
	rs := testRedundancySettings

	// create custom logger to capture logs
	observedZapCore, observedLogs := observer.New(zap.DebugLevel)
	um.logger = zap.New(observedZapCore).Sugar()

	// overdrive immediately after 50ms
	um.overdriveTimeout = 50 * time.Millisecond
	um.maxOverdrive = uint64(rs.TotalShards) + 1

	// add hosts and add arificial delay of 150ms
	hosts := w.AddHosts(rs.TotalShards)
	for _, host := range hosts {
		host.uploadDelay = 150 * time.Millisecond
	}

	// create test data
	data := frand.Bytes(128)

	// create upload params
	params := testParameters(t.Name())

	// upload data
	_, _, err := um.Upload(context.Background(), bytes.NewReader(data), w.Contracts(), params, lockingPriorityUpload)
	if err != nil {
		t.Fatal(err)
	}

	// define a helper function to fetch an uploader for given host key
	uploaderr := func(hk types.PublicKey) *uploader {
		t.Helper()
		um.refreshUploaders(w.Contracts(), 1)
		for _, uploader := range um.uploaders {
			if uploader.hk == hk {
				return uploader
			}
		}
		t.Fatal("uploader not found")
		return nil
	}

	// define a helper function to fetch uploader stats
	stats := func(u *uploader) (uint64, float64) {
		t.Helper()
		u.mu.Lock()
		defer u.mu.Unlock()
		return u.consecutiveFailures, u.statsSectorUploadEstimateInMS.P90()
	}

	// assert all uploaders have 0 failures and an estimate that roughly equals
	// the upload delay
	for _, h := range hosts {
		if failures, estimate := stats(uploaderr(h.hk)); failures != 0 {
			t.Fatal("unexpected failures", failures)
		} else if !(estimate >= 150 && estimate < 155) {
			t.Fatal("unexpected estimate", estimate)
		}
	}

	// add a host with a 250ms delay
	h := w.AddHost()
	h.uploadDelay = 250 * time.Millisecond

	// make sure its estimate is not 0 and thus is not used for the upload, but
	// instead it is used for the overdrive
	ul := uploaderr(h.hk)
	ul.statsSectorUploadEstimateInMS.Track(float64(h.uploadDelay.Milliseconds()))
	ul.statsSectorUploadEstimateInMS.Recompute()
	if ul.statsSectorUploadEstimateInMS.P90() == 0 {
		t.Fatal("unexpected p90")
	}

	// upload data
	_, _, err = um.Upload(context.Background(), bytes.NewReader(data), w.Contracts(), params, lockingPriorityUpload)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(h.uploadDelay)

	// assert the new host has 0 failures and that we logged an entry indicating
	// we skipped tracking the metric
	if failures, _ := stats(uploaderr(h.hk)); failures != 0 {
		t.Fatal("unexpected failures", failures)
	} else if observedLogs.Filter(func(entry observer.LoggedEntry) bool {
		return strings.Contains(entry.Message, "not tracking sector upload metric")
	}).Len() == 0 {
		t.Fatal("missing log entry")
	}

	// upload data again but now have the host return an error
	h.uploadErr = errors.New("host error")
	_, _, err = um.Upload(context.Background(), bytes.NewReader(data), w.Contracts(), params, lockingPriorityUpload)
	if err != nil {
		t.Fatal(err)
	}

	// assert the new host has 1 failure and its estimate includes the penalty
	uploaderr(h.hk).statsSectorUploadEstimateInMS.Recompute()
	if failures, estimate := stats(uploaderr(h.hk)); failures != 1 {
		t.Fatal("unexpected failures", failures)
	} else if estimate < float64(time.Minute.Milliseconds()) {
		t.Fatal("unexpected estimate", estimate)
	}
}
