package worker

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
)

func TestHost(t *testing.T) {
	c := newMockContract(types.FileContractID{1})
	h := newMockHost(types.PublicKey{1}, newTestHostPriceTable(time.Now()), c)
	sector, root := newMockSector()

	// upload the sector
	uploaded, err := h.UploadSector(context.Background(), sector, types.FileContractRevision{})
	if err != nil {
		t.Fatal(err)
	} else if uploaded != root {
		t.Fatal("root mismatch")
	}

	// download entire sector
	var buf bytes.Buffer
	err = h.DownloadSector(context.Background(), &buf, root, 0, rhpv2.SectorSize, false)
	if err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(buf.Bytes(), sector[:]) {
		t.Fatal("sector mismatch")
	}

	// download part of the sector
	buf.Reset()
	err = h.DownloadSector(context.Background(), &buf, root, 64, 64, false)
	if err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(buf.Bytes(), sector[64:128]) {
		t.Fatal("sector mismatch")
	}

	// try downloading out of bounds
	err = h.DownloadSector(context.Background(), &buf, root, rhpv2.SectorSize, 64, false)
	if !errors.Is(err, errSectorOutOfBounds) {
		t.Fatal("expected out of bounds error", err)
	}
}
