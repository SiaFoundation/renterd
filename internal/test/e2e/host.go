package e2e

import (
	"fmt"
	"net"
	"os"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/gateway"
	rhpv4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	rhp4 "go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.sia.tech/coreutils/syncer"
	"go.sia.tech/coreutils/testutil"
	"go.sia.tech/coreutils/wallet"
	"go.uber.org/zap"
)

// A Host is an ephemeral host that can be used for testing.
type Host struct {
	dir     string
	privKey types.PrivateKey

	s *syncer.Syncer

	wallet      *wallet.SingleAddressWallet
	settings    *testutil.EphemeralSettingsReporter
	contractsV2 *testutil.EphemeralContractor

	rhp4Listener net.Listener
}

// Close shutsdown the host
func (h *Host) Close() error {
	h.rhp4Listener.Close()
	h.wallet.Close()
	h.contractsV2.Close()
	h.s.Close()
	return nil
}

// RHPv4Addr returns the address of the RHPv4 listener
func (h *Host) RHPv4Addr() string {
	return h.rhp4Listener.Addr().String()
}

// UpdateSettings updates the host's configuration
func (h *Host) UpdateSettings(settings rhpv4.HostSettings) {
	h.settings.Update(settings)
}

// WalletAddress returns the host's wallet address
func (h *Host) WalletAddress() types.Address {
	return h.wallet.Address()
}

// PublicKey returns the public key of the host
func (h *Host) PublicKey() types.PublicKey {
	return h.privKey.PublicKey()
}

// SyncerAddr returns the address of the host's syncer.
func (h *Host) SyncerAddr() string {
	return string(h.s.Addr())
}

// NewHost initializes a new test host.
func NewHost(privKey types.PrivateKey, cm *chain.Manager, dir string, network *consensus.Network, genesisBlock types.Block) (*Host, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create dir: %w", err)
	}
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return nil, fmt.Errorf("failed to create syncer listener: %w", err)
	}
	s := syncer.New(l, cm, testutil.NewEphemeralPeerStore(), gateway.Header{
		GenesisID:  genesisBlock.ID(),
		UniqueID:   gateway.GenerateUniqueID(),
		NetAddress: l.Addr().String(),
	},
		syncer.WithSendBlocksTimeout(2*time.Second),
		syncer.WithRPCTimeout(2*time.Second),
	)
	go s.Run()

	log := zap.NewNop()

	ws := testutil.NewEphemeralWalletStore()
	w, err := wallet.NewSingleAddressWallet(privKey, cm, ws, s)
	if err != nil {
		return nil, fmt.Errorf("failed to create wallet: %w", err)
	}
	_ = cm.OnReorg(func(_ types.ChainIndex) {
		tip, err := ws.Tip()
		if err != nil {
			panic(err)
		}
		reverted, applied, err := cm.UpdatesSince(tip, 1000)
		if err != nil {
			panic(err)
		}

		if err := ws.UpdateChainState(func(tx wallet.UpdateTx) error {
			return w.UpdateChainState(tx, reverted, applied)
		}); err != nil {
			panic(err)
		}
	})

	rhp4Listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return nil, fmt.Errorf("failed to create listener: %w", err)
	}

	settings := testutil.NewEphemeralSettingsReporter()
	settings.Update(rhpv4.HostSettings{
		Release:             "test",
		AcceptingContracts:  true,
		WalletAddress:       w.Address(),
		MaxCollateral:       types.Siacoins(10000),
		MaxContractDuration: 1000,
		RemainingStorage:    100 * rhpv4.SectorSize,
		TotalStorage:        100 * rhpv4.SectorSize,
		Prices: rhpv4.HostPrices{
			ContractPrice:   types.Siacoins(1).Div64(5), // 0.2 SC
			StoragePrice:    types.NewCurrency64(100),   // 100 H / byte / block
			IngressPrice:    types.NewCurrency64(100),   // 100 H / byte
			EgressPrice:     types.NewCurrency64(100),   // 100 H / byte
			Collateral:      types.NewCurrency64(200),
			FreeSectorPrice: types.NewCurrency64(1),
		},
	})

	contractsV2 := testutil.NewEphemeralContractor(cm)
	sectors := testutil.NewEphemeralSectorStore()
	rhpv4 := rhp4.NewServer(privKey, cm, s, contractsV2, w, settings, sectors, rhp4.WithPriceTableValidity(30*time.Minute))
	go siamux.Serve(rhp4Listener, rhpv4, log.Named("rhp4"))

	return &Host{
		dir:     dir,
		privKey: privKey,

		s: s,

		wallet:      w,
		settings:    settings,
		contractsV2: contractsV2,

		rhp4Listener: rhp4Listener,
	}, nil
}
