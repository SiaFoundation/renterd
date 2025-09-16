package upload

import (
	"context"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/v2/api"
	"go.sia.tech/renterd/v2/internal/host"
	"go.uber.org/zap"
)

type hostManager struct{}

func (hm *hostManager) Downloader(hi api.HostInfo) host.Downloader { return nil }
func (hm *hostManager) Uploader(hi api.HostInfo, fcid types.FileContractID) host.Uploader {
	return nil
}
func (hm *hostManager) Host(hk types.PublicKey, fcid types.FileContractID, siamuxAddr string) host.Host {
	return nil
}

func TestRefreshUploaders(t *testing.T) {
	hm := &hostManager{}
	ul := NewManager(context.Background(), nil, hm, nil, nil, nil, nil, 0, 0, time.Minute, zap.NewNop())

	// prepare host info
	hi := HostInfo{
		HostInfo:            api.HostInfo{PublicKey: types.PublicKey{1}},
		ContractEndHeight:   1,
		ContractID:          types.FileContractID{1},
		ContractRenewedFrom: types.FileContractID{},
	}

	// refresh uploaders & assert it got added
	ul.refreshUploaders([]HostInfo{hi}, 0)
	if len(ul.uploaders) != 1 {
		t.Fatalf("unexpected number of uploaders, %v != 1", len(ul.uploaders))
	}

	// update contract
	hi.ContractRenewedFrom = hi.ContractID
	hi.ContractID = types.FileContractID{2}
	hi.ContractEndHeight = 10

	// assert we still have one
	ul.refreshUploaders([]HostInfo{hi}, 0)
	if len(ul.uploaders) != 1 {
		t.Fatalf("unexpected number of uploaders, %v != 1", len(ul.uploaders))
	}
	ull := ul.uploaders[0]
	if ull.ContractID() != hi.ContractID {
		t.Fatalf("unexpected contract id, %v != %v", ull.ContractID(), hi.ContractID)
	}

	// refresh right before expiry
	ul.refreshUploaders([]HostInfo{hi}, 9)
	if len(ul.uploaders) != 1 {
		t.Fatalf("unexpected number of uploaders, %v != 1", len(ul.uploaders))
	}

	// refresh at expiry height
	ul.refreshUploaders([]HostInfo{hi}, 10)
	if len(ul.uploaders) != 0 {
		t.Fatalf("unexpected number of uploaders, %v != 0", len(ul.uploaders))
	}
}
