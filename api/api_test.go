package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"go.sia.tech/jape"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/renterd/internal/slabutil"
	"go.sia.tech/renterd/internal/stores"
	"go.sia.tech/renterd/object"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	rhpv3 "go.sia.tech/renterd/rhp/v3"
	"go.sia.tech/renterd/slab"
	"go.sia.tech/renterd/wallet"
	"go.sia.tech/siad/types"
	"lukechampine.com/frand"
)

type mockChainManager struct{}

func (mockChainManager) TipState() (cs consensus.State) { return }

type mockSyncer struct{}

func (mockSyncer) Addr() string              { return "" }
func (mockSyncer) Peers() []string           { return nil }
func (mockSyncer) Connect(addr string) error { return nil }
func (mockSyncer) BroadcastTransaction(txn types.Transaction, dependsOn []types.Transaction) {
}

type mockTxPool struct{}

func (mockTxPool) RecommendedFee() types.Currency                   { return types.ZeroCurrency }
func (mockTxPool) Transactions() []types.Transaction                { return nil }
func (mockTxPool) AddTransactionSet(txns []types.Transaction) error { return nil }
func (mockTxPool) UnconfirmedParents(txn types.Transaction) ([]types.Transaction, error) {
	return nil, nil
}

type mockRHP struct{}

func (mockRHP) Settings(ctx context.Context, hostIP string, hostKey consensus.PublicKey) (rhpv2.HostSettings, error) {
	return rhpv2.HostSettings{}, nil
}

func (mockRHP) FormContract(ctx context.Context, cs consensus.State, hostIP string, hostKey consensus.PublicKey, renterKey consensus.PrivateKey, txns []types.Transaction) (rhpv2.Contract, []types.Transaction, error) {
	txn := txns[len(txns)-1]
	fc := txn.FileContracts[0]
	return rhpv2.Contract{
		Revision: types.FileContractRevision{
			ParentID: txn.FileContractID(0),
			UnlockConditions: types.UnlockConditions{
				PublicKeys: []types.SiaPublicKey{
					{Algorithm: types.SignatureEd25519, Key: renterKey[:]},
					{Algorithm: types.SignatureEd25519, Key: hostKey[:]},
				},
				SignaturesRequired: 2,
			},
			NewRevisionNumber:     1,
			NewFileSize:           fc.FileSize,
			NewFileMerkleRoot:     fc.FileMerkleRoot,
			NewWindowStart:        fc.WindowStart,
			NewWindowEnd:          fc.WindowEnd,
			NewValidProofOutputs:  fc.ValidProofOutputs,
			NewMissedProofOutputs: fc.MissedProofOutputs,
			NewUnlockHash:         fc.UnlockHash,
		},
	}, nil, nil
}

func (mockRHP) RenewContract(ctx context.Context, cs consensus.State, hostIP string, hostKey consensus.PublicKey, renterKey consensus.PrivateKey, contractID types.FileContractID, txns []types.Transaction, finalPayment types.Currency) (rhpv2.Contract, []types.Transaction, error) {
	return rhpv2.Contract{}, nil, nil
}

func (mockRHP) FundAccount(ctx context.Context, hostIP string, hostKey consensus.PublicKey, contract types.FileContractRevision, renterKey consensus.PrivateKey, account rhpv3.Account, amount types.Currency) (rhpv2.Contract, error) {
	return rhpv2.Contract{}, nil
}

func (mockRHP) ReadRegistry(ctx context.Context, hostIP string, hostKey consensus.PublicKey, payment rhpv3.PaymentMethod, registryKey rhpv3.RegistryKey) (rhpv3.RegistryValue, error) {
	return rhpv3.RegistryValue{}, nil
}

func (mockRHP) UpdateRegistry(ctx context.Context, hostIP string, hostKey consensus.PublicKey, payment rhpv3.PaymentMethod, registryKey rhpv3.RegistryKey, registryValue rhpv3.RegistryValue) error {
	return nil
}

type mockSlabMover struct {
	hosts []slab.Host
}

func (sm *mockSlabMover) UploadSlab(ctx context.Context, r io.Reader, m, n uint8, currentHeight uint64, contracts []api.Contract) (slab.Slab, []slab.HostInteraction, error) {
	return slab.UploadSlab(r, m, n, sm.hostsForContracts(contracts))
}

func (sm *mockSlabMover) DownloadSlab(ctx context.Context, w io.Writer, s slab.Slice, contracts []api.Contract) ([]slab.HostInteraction, error) {
	return slab.DownloadSlab(w, s, sm.hostsForContracts(contracts))
}

func (sm *mockSlabMover) DeleteSlabs(ctx context.Context, slabs []slab.Slab, contracts []api.Contract) ([]slab.HostInteraction, error) {
	return slab.DeleteSlabs(slabs, sm.hostsForContracts(contracts))
}

func (sm *mockSlabMover) MigrateSlab(ctx context.Context, s *slab.Slab, currentHeight uint64, from, to []api.Contract) ([]slab.HostInteraction, error) {
	return slab.MigrateSlab(s, sm.hostsForContracts(from), sm.hostsForContracts(to))
}

func (sm *mockSlabMover) hostsForContracts(contracts []api.Contract) []slab.Host {
	hosts := make([]slab.Host, len(contracts))
	for i, c := range contracts {
		for _, h := range sm.hosts {
			if c.HostKey == h.PublicKey() {
				hosts[i] = h
				break
			}
		}
		if hosts[i] == nil {
			panic("no host found")
		}
	}
	return hosts
}

type node struct {
	w   *wallet.SingleAddressWallet
	hdb *stores.EphemeralHostDB
	cs  *stores.EphemeralContractStore
	os  *stores.EphemeralObjectStore
	sm  *mockSlabMover

	walletKey consensus.PrivateKey
}

func (n *node) addHost() consensus.PublicKey {
	h := slabutil.NewMockHost()
	n.sm.hosts = append(n.sm.hosts, h)
	return h.PublicKey()
}

func newTestNode() *node {
	walletKey := consensus.GeneratePrivateKey()
	w := wallet.NewSingleAddressWallet(walletKey, stores.NewEphemeralWalletStore(wallet.StandardAddress(walletKey.PublicKey())))
	hdb := stores.NewEphemeralHostDB()
	cs := stores.NewEphemeralContractStore()
	os := stores.NewEphemeralObjectStore()
	sm := &mockSlabMover{}
	return &node{w, hdb, cs, os, sm, walletKey}
}

func runServer(n *node) (*api.Client, func()) {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}
	go func() {
		srv := api.NewServer(mockSyncer{}, mockChainManager{}, mockTxPool{}, n.w, n.hdb, mockRHP{}, n.cs, n.sm, n.os)
		http.Serve(l, jape.BasicAuth("password")(srv))
	}()
	c := api.NewClient("http://"+l.Addr().String(), "password")
	return c, func() { l.Close() }
}

func TestObject(t *testing.T) {
	n := newTestNode()
	c, shutdown := runServer(n)
	defer shutdown()

	// add hosts
	hosts := make([]consensus.PublicKey, 3)
	for i := range hosts {
		hosts[i] = n.addHost()
	}

	// form contracts
	contracts, err := formTestContracts(c, hosts)
	if err != nil {
		t.Fatal(err)
	}

	// upload
	data := frand.Bytes(12345)
	key := object.GenerateEncryptionKey()
	s, _, err := c.UploadSlab(key.Encrypt(bytes.NewReader(data)), 2, 3, 0, contracts)
	if err != nil {
		t.Fatal(err)
	}
	o := object.Object{
		Key: key,
		Slabs: []slab.Slice{{
			Slab:   s,
			Offset: 0,
			Length: uint32(len(data)),
		}},
	}

	// store object
	if err := c.AddObject("foo", o); err != nil {
		t.Fatal(err)
	}
	// retrieve object
	o, err = c.Object("foo")
	if err != nil {
		t.Fatal(err)
	}

	// download
	var buf bytes.Buffer
	if _, err := c.DownloadSlab(key.Decrypt(&buf, 0), o.Slabs[0], contracts); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(buf.Bytes(), data) {
		t.Fatalf("data mismatch:\n%v (%v)\n%v (%v)", buf.Bytes(), len(buf.Bytes()), data, len(data))
	}

	// delete slabs
	if _, err := c.DeleteSlabs([]slab.Slab{s}, contracts); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DownloadSlab(ioutil.Discard, o.Slabs[0], contracts); err == nil {
		t.Error("slabs should no longer be retrievable")
	}

	// delete object
	if err := c.DeleteObject("foo"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Object("foo"); err == nil {
		t.Error("object should no longer be retrievable")
	}
}

// TestHostInteractions verifies slab endpoints return a set of host
// interactions that are populated with all of the expected fields and metadata
// about that particular host interaction.
func TestHostInteractions(t *testing.T) {
	isInitialized := func(hi api.HostInteraction) (bool, string) {
		if hi.HostKey.String() == "" {
			return false, "no hostkey"
		}
		if hi.Type == "" {
			return false, "no type"
		}
		if hi.Timestamp == (time.Time{}).Unix() {
			return false, "no timestamp"
		}
		var metadata struct {
			Duration int64 `json:"dur"`
		}
		json.Unmarshal(hi.Metadata, &metadata)
		if metadata.Duration == 0 {
			return false, fmt.Sprintf("no duration, %s", string(hi.Metadata))
		}
		return true, ""
	}

	n := newTestNode()
	c, shutdown := runServer(n)
	defer shutdown()

	// add hosts
	hosts := make([]consensus.PublicKey, 3)
	for i := range hosts {
		hosts[i] = n.addHost()
	}

	// form contracts
	contracts, err := formTestContracts(c, hosts)
	if err != nil {
		t.Fatal(err)
	}

	// upload
	data := frand.Bytes(12345)
	key := object.GenerateEncryptionKey()
	s, his, err := c.UploadSlab(key.Encrypt(bytes.NewReader(data)), 2, 3, 0, contracts)
	if err != nil {
		t.Fatal(err)
	}

	// assert host interactions
	if len(his) != 3 {
		t.Fatalf("unexpected number of host interactions, %v != 3", len(his))
	}
	for _, hi := range his {
		if ok, reason := isInitialized(hi); !ok || hi.Type != "UploadSector" {
			t.Fatal("unexpected", reason, hi)
		}
	}

	// prepare slice
	slice := slab.Slice{
		Slab:   s,
		Offset: 0,
		Length: uint32(len(data)),
	}

	// download
	var buf bytes.Buffer
	his, err = c.DownloadSlab(key.Decrypt(&buf, 0), slice, contracts)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf.Bytes(), data) {
		t.Fatalf("data mismatch:\n%v (%v)\n%v (%v)", buf.Bytes(), len(buf.Bytes()), data, len(data))
	}

	// assert host interactions
	if len(his) != 2 {
		t.Fatalf("unexpected number of host interactions, %v != 2", len(his))
	}
	for _, hi := range his {
		if ok, reason := isInitialized(hi); !ok || hi.Type != "DownloadSector" {
			t.Fatal("unexpected", reason, hi)
		}
	}

	// corrupt first shard
	bkp := s.Shards[0].Root
	s.Shards[0].Root = consensus.Hash256{}

	// retry download
	buf.Reset()
	his, err = c.DownloadSlab(key.Decrypt(&buf, 0), slice, contracts)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf.Bytes(), data) {
		t.Fatalf("data mismatch:\n%v (%v)\n%v (%v)", buf.Bytes(), len(buf.Bytes()), data, len(data))
	}

	// reset shards
	s.Shards[0].Root = bkp

	// assert host interactions
	if len(his) != 3 {
		t.Fatalf("unexpected number of host interactions, %v != 3", len(his))
	}
	hasError := false
	for _, hi := range his {
		hasError = hasError || strings.Contains(hi.Error, "unknown root")
		if ok, reason := isInitialized(hi); !ok || hi.Type != "DownloadSector" {
			t.Fatal("unexpected", reason, hi)
		}
	}
	if !hasError {
		t.Fatal("expected to find one error")
	}

	// form two new contracts
	newHosts := []consensus.PublicKey{n.addHost(), n.addHost()}
	to, err := formTestContracts(c, newHosts)
	if err != nil {
		t.Fatal(err)
	}

	// perform migration
	ci, err := c.ConsensusTip()
	if err != nil {
		t.Fatal(err)
	}
	his, err = c.MigrateSlab(&s, contracts[:2], append(to, contracts[2]), ci.Height)
	if err != nil {
		t.Fatal(err)
	}

	// assert migration
	s1 := s.Sector(newHosts[0])
	s2 := s.Sector(newHosts[1])
	s3 := s.Sector(contracts[2].HostKey)
	if s1 == nil || s2 == nil || s3 == nil {
		t.Fatal("unexpected")
	}

	// assert host interactions
	if len(his) != 4 {
		t.Fatalf("unexpected number of host interactions, %v != 4", len(his))
	}
	var downloads, uploads int
	for _, hi := range his {
		if ok, reason := isInitialized(hi); !ok {
			t.Fatal("unexpected", reason, hi)
		}
		if hi.Type == "DownloadSector" {
			downloads++
		}
		if hi.Type == "UploadSector" {
			uploads++
		}
	}
	if downloads != 2 || uploads != 2 {
		t.Fatal("unexpected")
	}
}

func formTestContracts(c *api.Client, hosts []consensus.PublicKey) ([]api.Contract, error) {
	var contracts []api.Contract
	for _, hostKey := range hosts {
		const hostIP = ""
		settings, err := c.RHPScan(hostKey, hostIP)
		if err != nil {
			return nil, err
		}
		renterKey := consensus.GeneratePrivateKey()
		addr, _ := c.WalletAddress()
		fc, cost, err := c.RHPPrepareForm(renterKey, hostKey, types.ZeroCurrency, addr, types.ZeroCurrency, 0, settings)
		if err != nil {
			return nil, err
		}
		txn := types.Transaction{
			FileContracts: []types.FileContract{fc},
		}
		toSign, parents, err := c.WalletFund(&txn, cost)
		if err != nil {
			return nil, err
		}
		if err := c.WalletSign(&txn, toSign, wallet.ExplicitCoveredFields(txn)); err != nil {
			return nil, err
		}
		c, _, err := c.RHPForm(renterKey, hostKey, hostIP, append(parents, txn))
		if err != nil {
			return nil, err
		}
		contracts = append(contracts, api.Contract{
			HostKey:   c.HostKey(),
			HostIP:    hostIP,
			ID:        c.ID(),
			RenterKey: renterKey,
		})
	}
	return contracts, nil
}
