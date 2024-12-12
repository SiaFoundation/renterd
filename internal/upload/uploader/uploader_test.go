package uploader

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	rhp3 "go.sia.tech/renterd/internal/rhp/v3"
	"go.sia.tech/renterd/internal/test/mocks"
	"go.uber.org/zap"
)

func TestUploaderStopped(t *testing.T) {
	cs := mocks.NewContractStore()
	hm := mocks.NewHostManager()
	cl := mocks.NewContractLocker()

	c := mocks.NewContract(types.PublicKey{1}, types.FileContractID{1})
	md := c.Metadata()

	ul := New(context.Background(), cl, cs, hm, api.HostInfo{}, md.ID, md.WindowEnd, zap.NewNop().Sugar())
	ul.Stop(errors.New("test"))

	req := SectorUploadReq{
		Ctx:          context.Background(),
		ResponseChan: make(chan SectorUploadResp),
	}

	ul.Enqueue(&req)

	select {
	case res := <-req.ResponseChan:
		if !errors.Is(res.Err, ErrStopped) {
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
	errSectorUploadFinishedAndDial := fmt.Errorf("%w;%w", rhp3.ErrDialTransport, ErrSectorUploadFinished)

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
		{rhp3.ErrMaxRevisionReached, 0, ms, regular, false, false, 0, 0},
		{rhp3.ErrMaxRevisionReached, 0, ms, overdrive, false, false, 0, 0},

		// context canceled case
		{context.Canceled, 0, ms, regular, false, false, 0, 0},
		{context.Canceled, 0, ms, overdrive, false, false, 0, 0},

		// sector already uploaded case
		{ErrSectorUploadFinished, ms, ms, regular, false, false, 10, 0},
		{ErrSectorUploadFinished, ms, ms, overdrive, false, false, 0, 0},
		{errSectorUploadFinishedAndDial, ms, ms, overdrive, false, false, 0, 0},
		{errSectorUploadFinishedAndDial, ms, 1001 * ms, overdrive, false, true, 10010, 0},

		// payment failure case
		{rhp3.ErrFailedToCreatePayment, 0, ms, regular, false, false, 3600000, 0},
		{rhp3.ErrFailedToCreatePayment, 0, ms, overdrive, false, false, 3600000, 0},

		// host failure
		{errHostError, ms, ms, regular, false, true, 3600000, 0},
		{errHostError, ms, ms, overdrive, false, true, 3600000, 0},
	}

	for i, c := range cases {
		success, failure, uploadEstimateMS, uploadSpeedBytesPerMS := handleSectorUpload(c.uploadErr, c.uploadDur, c.totalDur, c.overdrive)
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

func TestRefreshUploader(t *testing.T) {
	cs := mocks.NewContractStore()
	hm := mocks.NewHostManager()
	cl := mocks.NewContractLocker()

	// create uploader
	hk := types.PublicKey{1}
	c1 := cs.AddContract(hk).Metadata()
	ul := New(context.Background(), cl, cs, hm, api.HostInfo{}, c1.ID, c1.WindowEnd, zap.NewNop().Sugar())

	// renew the first contract
	c1Renewed, err := cs.RenewContract(hk)
	if err != nil {
		t.Fatal(err)
	}

	// refresh uploader to cause it to expire
	if !ul.tryRefresh(context.Background()) {
		t.Fatal("uploader wasn't refreshed")
	}

	if ul.fcid != c1Renewed.ID() {
		t.Fatalf("expected uploader to be using renewed contract, got %v", ul.fcid)
	}
}
