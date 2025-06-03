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
	"go.sia.tech/hostd/v2/host/settings"
	"go.uber.org/zap"
)

const (
	blocksPerDay   = 144
	blocksPerMonth = blocksPerDay * 30
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

// defaultHostSettings returns the default settings for the test host
var defaultHostSettings = settings.Settings{
	AcceptingContracts:  true,
	MaxContractDuration: blocksPerMonth * 3,
	MaxCollateral:       types.Siacoins(5000),

	ContractPrice: types.Siacoins(1).Div64(4),

	BaseRPCPrice:      types.NewCurrency64(100),
	SectorAccessPrice: types.NewCurrency64(100),

	CollateralMultiplier: 2,
	StoragePrice:         types.Siacoins(100).Div64(1e12).Div64(blocksPerMonth),
	EgressPrice:          types.Siacoins(100).Div64(1e12),
	IngressPrice:         types.Siacoins(100).Div64(1e12),
	WindowSize:           5,

	PriceTableValidity: 5 * time.Minute,

	AccountExpiry:     30 * 24 * time.Hour, // 1 month
	MaxAccountBalance: types.Siacoins(10),
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
	wallet, err := wallet.NewSingleAddressWallet(privKey, cm, ws, s)
	if err != nil {
		return nil, fmt.Errorf("failed to create wallet: %w", err)
	}

	rhp4Listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return nil, fmt.Errorf("failed to create rhp3 listener: %w", err)
	}

	settings := testutil.NewEphemeralSettingsReporter()
	contractsV2 := testutil.NewEphemeralContractor(cm)
	sectors := testutil.NewEphemeralSectorStore()
	rhpv4 := rhp4.NewServer(privKey, cm, s, contractsV2, wallet, settings, sectors, rhp4.WithPriceTableValidity(30*time.Minute))
	go siamux.Serve(rhp4Listener, rhpv4, log.Named("rhp4"))

	return &Host{
		dir:     dir,
		privKey: privKey,

		s: s,

		wallet:      wallet,
		settings:    settings,
		contractsV2: contractsV2,

		rhp4Listener: rhp4Listener,
	}, nil
}
