package bus

import (
	"testing"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/v2/api"
)

func TestCheckRevision(t *testing.T) {
	renterKey := types.GeneratePrivateKey().PublicKey()
	hostKey := types.GeneratePrivateKey().PublicKey()
	known := types.V2FileContract{
		ProofHeight:      100,
		ExpirationHeight: 200,
		RenterOutput:     types.SiacoinOutput{Value: types.Siacoins(10), Address: types.Address{1}},
		HostOutput:       types.SiacoinOutput{Value: types.Siacoins(5), Address: types.Address{2}},
		MissedHostValue:  types.Siacoins(4),
		TotalCollateral:  types.Siacoins(4),
		RenterPublicKey:  renterKey,
		HostPublicKey:    hostKey,
		RevisionNumber:   1,
	}
	cm := api.ContractMetadata{
		HostKey:        hostKey,
		RevisionNumber: 2,
		WindowStart:    100,
		WindowEnd:      200,
	}

	// a valid revision moves funds from the renter to the host
	valid := known
	valid.RevisionNumber = 3
	valid.RenterOutput.Value = types.Siacoins(8)
	valid.HostOutput.Value = types.Siacoins(7)
	valid.MissedHostValue = types.Siacoins(3)
	valid.Filesize = 1
	valid.Capacity = 1
	if err := checkRevision(cm, renterKey, &known, valid); err != nil {
		t.Fatal(err)
	} else if err := checkRevision(cm, renterKey, nil, valid); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name  string
		mod   func(rev *types.V2FileContract)
		known bool
	}{
		{"revision number", func(rev *types.V2FileContract) { rev.RevisionNumber = 1 }, false},
		{"host key", func(rev *types.V2FileContract) { rev.HostPublicKey = types.PublicKey{1} }, false},
		{"renter key", func(rev *types.V2FileContract) { rev.RenterPublicKey = types.PublicKey{1} }, false},
		{"proof height", func(rev *types.V2FileContract) { rev.ProofHeight = 101 }, false},
		{"expiration height", func(rev *types.V2FileContract) { rev.ExpirationHeight = 201 }, false},
		{"renter address", func(rev *types.V2FileContract) { rev.RenterOutput.Address = types.Address{3} }, true},
		{"host address", func(rev *types.V2FileContract) { rev.HostOutput.Address = types.Address{3} }, true},
		{"total collateral", func(rev *types.V2FileContract) { rev.TotalCollateral = types.Siacoins(5) }, true},
		{"payout", func(rev *types.V2FileContract) { rev.HostOutput.Value = types.Siacoins(8) }, true},
		{"payout overflow", func(rev *types.V2FileContract) { rev.HostOutput.Value = types.MaxCurrency }, true},
	}
	for _, test := range tests {
		rev := valid
		test.mod(&rev)
		if err := checkRevision(cm, renterKey, &known, rev); err == nil {
			t.Fatalf("%v: expected error", test.name)
		} else if err := checkRevision(cm, renterKey, nil, rev); !test.known && err == nil {
			t.Fatalf("%v: expected error without known contract", test.name)
		} else if test.known && err != nil {
			t.Fatalf("%v: unexpected error without known contract: %v", test.name, err)
		}
	}
}
