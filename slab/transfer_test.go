package slab_test

import (
	"bytes"
	"context"
	"testing"

	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/renterd/internal/slabutil"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"go.sia.tech/renterd/slab"
	"lukechampine.com/frand"
)

func TestSingleSlab(t *testing.T) {
	// generate shards
	data := frand.Bytes(777)
	shards := make([][]byte, 10)
	for i := range shards {
		shards[i] = make([]byte, 0, rhpv2.SectorSize)
	}
	slab.NewRSCode(3, 10).Encode(data, shards)
	for i := range shards {
		shards[i] = shards[i][:rhpv2.SectorSize]
	}
	key := slab.GenerateEncryptionKey()
	key.EncryptShards(shards)

	// upload
	ssu := slab.SerialSlabUploader{Hosts: make(map[consensus.PublicKey]slab.SectorUploader)}
	for range shards {
		hostKey := consensus.GeneratePrivateKey().PublicKey()
		ssu.Hosts[hostKey] = slabutil.NewMockHost()
	}
	sectors, err := ssu.UploadSlab(context.Background(), shards)
	if err != nil {
		t.Fatal(err)
	}
	ss := slab.Slice{
		Slab: slab.Slab{
			Key:       key,
			MinShards: 3,
			Shards:    sectors,
		},
		Offset: 0,
		Length: uint32(len(data)),
	}

	// download
	ssd := slab.SerialSlabDownloader{Hosts: make(map[consensus.PublicKey]slab.SectorDownloader)}
	for hostKey, host := range ssu.Hosts {
		ssd.Hosts[hostKey] = host.(slab.SectorDownloader)
	}
	downloaded, err := ssd.DownloadSlab(context.Background(), ss)
	if err != nil {
		t.Fatal(err)
	}
	if len(downloaded) != len(shards) {
		t.Fatal("wrong number of shards")
	}
	var buf bytes.Buffer
	if err := slab.RecoverSlab(&buf, ss, downloaded); err != nil {
		t.Fatal(err)
	}
	exp := data
	got := buf.Bytes()
	if !bytes.Equal(exp, got) {
		t.Errorf("\nexpected: %x...%x (%v)\ngot:      %x...%x (%v)",
			exp[:20], exp[len(exp)-20:], len(exp),
			got[:20], got[len(got)-20:], len(got))
	}
}
