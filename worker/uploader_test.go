package worker

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.uber.org/zap"
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

func TestHandleSectorUpload(t *testing.T) {
	ms := time.Millisecond
	ss := float64(rhpv2.SectorSize)
	overdrive := true
	regular := false

	errHostError := errors.New("some host error")
	errSectorUploadFinishedAndDial := fmt.Errorf("%w;%w", errDialTransport, errSectorUploadFinished)

	cases := []struct {
		// input
		uploadErr error
		uploadDur time.Duration
		totalDur  time.Duration
		overdrive bool

		// expected output
		success               bool
		failure               bool
		uploadEstimateMS      float64
		uploadSpeedBytesPerMS float64
	}{
		// happy case
		{nil, ms, ms, regular, true, false, 1, ss},
		{nil, ms, ms, overdrive, true, false, 1, ss},

		// renewed contract case
		{errMaxRevisionReached, 0, ms, regular, false, false, 0, 0},
		{errMaxRevisionReached, 0, ms, overdrive, false, false, 0, 0},

		// payment failure case
		{errFailedToCreatePayment, 0, ms, regular, false, false, 0, 0},
		{errFailedToCreatePayment, 0, ms, overdrive, false, false, 0, 0},

		// context canceled case
		{context.Canceled, 0, ms, regular, false, false, 0, 0},
		{context.Canceled, 0, ms, overdrive, false, false, 0, 0},

		// sector already uploaded case
		{errSectorUploadFinished, ms, ms, regular, false, false, 10, 0},
		{errSectorUploadFinished, ms, ms, overdrive, false, false, 0, 0},
		{errSectorUploadFinishedAndDial, ms, ms, overdrive, false, false, 0, 0},
		{errSectorUploadFinishedAndDial, ms, 1001 * ms, overdrive, false, true, 10010, 0},

		// host failure
		{errHostError, ms, ms, regular, false, true, 3600000, 0},
		{errHostError, ms, ms, overdrive, false, true, 3600000, 0},
	}

	for i, c := range cases {
		success, failure, uploadEstimateMS, uploadSpeedBytesPerMS := handleSectorUpload(c.uploadErr, c.uploadDur, c.totalDur, c.overdrive, zap.NewNop().Sugar())
		if success != c.success {
			t.Fatalf("case %d failed: expected success %v, got %v", i+1, c.success, success)
		} else if failure != c.failure {
			t.Fatalf("case %d failed: expected failure %v, got %v", i+1, c.failure, failure)
		} else if uploadEstimateMS != c.uploadEstimateMS {
			t.Fatalf("case %d failed: expected uploadEstimateMS %v, got %v", i+1, c.uploadEstimateMS, uploadEstimateMS)
		} else if uploadSpeedBytesPerMS != c.uploadSpeedBytesPerMS {
			t.Fatalf("case %d failed: expected uploadSpeedBytesPerMS %v, got %v", i+1, c.uploadSpeedBytesPerMS, uploadSpeedBytesPerMS)
		}
	}
}
