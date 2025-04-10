package uploader

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/v2/api"
	rhp3 "go.sia.tech/renterd/v2/internal/rhp/v3"
	"go.sia.tech/renterd/v2/internal/test/mocks"
	"go.uber.org/zap"
)

func TestUploaderStopped(t *testing.T) {
	cs := mocks.NewContractStore()
	hm := mocks.NewHostManager()
	cl := mocks.NewContractLocker()

	c := mocks.NewContract(types.PublicKey{1}, types.FileContractID{1})
	md := c.Metadata()

	ul := New(context.Background(), cl, cs, hm, api.HostInfo{}, md.ID, md.WindowEnd, zap.NewNop().Sugar())

	// enqueue a request
	respChan := make(chan SectorUploadResp, 1)
	if ok := ul.Enqueue(&SectorUploadReq{
		UploadCtx:    context.Background(),
		SectorCtx:    context.Background(),
		ResponseChan: respChan,
	}); !ok {
		t.Fatal("failed to enqueue request")
	}

	// stop the uploader
	ul.Stop(ErrStopped)

	// assert we received a response indicating the uploader was stopped
	resp := <-respChan
	if !errors.Is(resp.Err, ErrStopped) {
		t.Fatal("expected error to be ErrStopped", resp.Err)
	}

	// enqueue another request
	if ok := ul.Enqueue(&SectorUploadReq{
		UploadCtx:    context.Background(),
		SectorCtx:    context.Background(),
		ResponseChan: respChan,
	}); ok {
		t.Fatal("expected enqueue to fail")
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

	// create contract
	hi := api.HostInfo{
		PublicKey:  types.PublicKey{1},
		SiamuxAddr: "localhost:1234",
	}
	c := cs.AddContract(hi.PublicKey).Metadata()

	// create uploader
	ul := New(context.Background(), cl, cs, hm, hi, c.ID, c.WindowEnd, zap.NewNop().Sugar())

	// assert state
	if ul.expiry != c.WindowEnd {
		t.Fatal("endheight was not initialized", ul.expiry)
	} else if ul.fcid != c.ID {
		t.Fatal("contract id was not initialized", ul.fcid, c.ID)
	} else if !reflect.DeepEqual(ul.host, hi) {
		t.Fatal("host info was not initialized", ul.host, hi)
	}

	// renew the contract
	cr := cs.RenewContract(hi.PublicKey).Metadata()

	// refresh uploader
	if !ul.tryRefresh(context.Background()) {
		t.Fatal("refresh failed unexpectedly")
	}

	// assert state
	if ul.expiry != cr.WindowEnd {
		t.Fatal("endheight was not updated", ul.expiry, cr.WindowEnd)
	} else if ul.fcid != cr.ID {
		t.Fatal("contract id was not updated", ul.fcid, cr.ID)
	} else if !reflect.DeepEqual(ul.host, hi) {
		t.Fatal("host info was not updated", ul.host, hi)
	}

	// refresh uploader with new host info
	update := hi
	update.SiamuxAddr = "localhost:5678"
	ul.Refresh(&update, cr.ID, cr.WindowEnd)

	// assert host info got updated
	if !reflect.DeepEqual(ul.host, update) {
		t.Fatal("host info was not updated", ul.host, update)
	}
}

func TestUploaderQueue(t *testing.T) {
	// mock dependencies
	cs := mocks.NewContractStore()
	hm := mocks.NewHostManager()
	cl := mocks.NewContractLocker()

	// create uploader
	hk := types.PublicKey{1}
	fcid := types.FileContractID{1}
	ul := New(context.Background(), cl, cs, hm, api.HostInfo{PublicKey: hk}, fcid, 0, zap.NewNop().Sugar())

	respChan := make(chan SectorUploadResp, 3)
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	req1 := &SectorUploadReq{
		UploadCtx:    context.Background(), // slab upload ongoing
		SectorCtx:    context.Background(), // sector not finished
		ResponseChan: respChan,
	}
	req2 := &SectorUploadReq{
		UploadCtx:    context.Background(), // slab upload ongoing
		SectorCtx:    cancelledCtx,         // sector got finished
		ResponseChan: respChan,
	}
	req3 := &SectorUploadReq{
		UploadCtx:    cancelledCtx, // slab upload finished
		SectorCtx:    cancelledCtx, // sector also finished
		ResponseChan: respChan,
	}

	stoppedChan := make(chan struct{})
	go func() {
		ul.Start()
		close(stoppedChan)
	}()

	ul.Enqueue(req1) // expect normal response
	ul.Enqueue(req2) // expect cancel response
	ul.Enqueue(req3) // expect no response
	time.Sleep(100 * time.Millisecond)

	if len(respChan) != 2 {
		t.Fatal("response channel was not filled", len(respChan))
	} else if r1 := <-respChan; r1.FCID != fcid || r1.HK != hk || r1.Err != nil || !reflect.DeepEqual(r1.Req, req1) {
		t.Fatal("unexpected response", r1)
	} else if r2 := <-respChan; r2.FCID != (types.FileContractID{}) || r2.HK != hk || r2.Err != nil || !reflect.DeepEqual(r2.Req, req2) {
		t.Fatal("unexpected response", r2)
	}

	ul.Stop(ErrStopped)
	select {
	case <-time.After(time.Second):
		t.Fatal("uploader did not stop in time")
	case <-stoppedChan: // asserts the uploader stopped
	}
}
