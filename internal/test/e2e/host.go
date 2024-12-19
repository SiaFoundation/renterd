package e2e

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/gateway"
	crhpv2 "go.sia.tech/core/rhp/v2"
	crhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	rhp4 "go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/syncer"
	"go.sia.tech/coreutils/testutil"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/hostd/host/accounts"
	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/hostd/host/registry"
	"go.sia.tech/hostd/host/settings"
	"go.sia.tech/hostd/host/storage"
	"go.sia.tech/hostd/index"
	"go.sia.tech/hostd/persist/sqlite"
	"go.sia.tech/hostd/rhp"
	rhpv2 "go.sia.tech/hostd/rhp/v2"
	rhpv3 "go.sia.tech/hostd/rhp/v3"
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

	s            *syncer.Syncer
	syncerCancel context.CancelFunc

	store       *sqlite.Store
	wallet      *wallet.SingleAddressWallet
	settings    *settings.ConfigManager
	storage     *storage.VolumeManager
	index       *index.Manager
	registry    *registry.Manager
	accounts    *accounts.AccountManager
	contracts   *contracts.Manager
	contractsV2 *testutil.EphemeralContractor

	rhpv2        *rhpv2.SessionHandler
	rhpv3        *rhpv3.SessionHandler
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
	h.rhpv2.Close()
	h.rhpv3.Close()
	h.rhp4Listener.Close()
	h.settings.Close()
	h.index.Close()
	h.wallet.Close()
	h.contracts.Close()
	h.storage.Close()
	h.store.Close()
	h.syncerCancel()
	h.s.Close()
	return nil
}

// RHPv2Addr returns the address of the RHPv2 listener
func (h *Host) RHPv2Addr() string {
	return h.rhpv2.LocalAddr()
}

// RHPv3Addr returns the address of the RHPv3 listener
func (h *Host) RHPv3Addr() string {
	return h.rhpv3.LocalAddr()
}

// RHPv4Addr returns the address of the RHPv4 listener
func (h *Host) RHPv4Addr() string {
	return h.rhp4Listener.Addr().String()
}

// AddVolume adds a new volume to the host
func (h *Host) AddVolume(ctx context.Context, path string, size uint64) error {
	result := make(chan error, 1)
	_, err := h.storage.AddVolume(ctx, path, size, result)
	if err != nil {
		return err
	}
	return <-result
}

// UpdateSettings updates the host's configuration
func (h *Host) UpdateSettings(settings settings.Settings) error {
	return h.settings.UpdateSettings(settings)
}

// RHPv2Settings returns the host's current RHPv2 settings
func (h *Host) RHPv2Settings() (crhpv2.HostSettings, error) {
	return h.settings.RHP2Settings()
}

// RHPv3PriceTable returns the host's current RHPv3 price table
func (h *Host) RHPv3PriceTable() (crhpv3.HostPriceTable, error) {
	return h.settings.RHP3PriceTable()
}

// WalletAddress returns the host's wallet address
func (h *Host) WalletAddress() types.Address {
	return h.wallet.Address()
}

// Contracts returns the host's contract manager
func (h *Host) Contracts() *contracts.Manager {
	return h.contracts
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
	}, syncer.WithPeerDiscoveryInterval(100*time.Millisecond), syncer.WithSyncInterval(100*time.Millisecond))
	syncErrChan := make(chan error, 1)
	syncerCtx, syncerCancel := context.WithCancel(context.Background())
	defer func() {
		if err != nil {
			syncerCancel()
		}
	}()
	go func() { syncErrChan <- s.Run(syncerCtx) }()

	log := zap.NewNop()
	db, err := sqlite.OpenDatabase(filepath.Join(dir, "hostd.db"), log.Named("sqlite"))
	if err != nil {
		return nil, fmt.Errorf("failed to create sql store: %w", err)
	}

	wallet, err := wallet.NewSingleAddressWallet(privKey, cm, db)
	if err != nil {
		return nil, fmt.Errorf("failed to create wallet: %w", err)
	}

	storage, err := storage.NewVolumeManager(db)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage manager: %w", err)
	}

	contracts, err := contracts.NewManager(db, storage, cm, s, wallet, contracts.WithRejectAfter(10), contracts.WithRevisionSubmissionBuffer(5))
	if err != nil {
		return nil, fmt.Errorf("failed to create contract manager: %w", err)
	}

	rhp2Listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return nil, fmt.Errorf("failed to create rhp2 listener: %w", err)
	}

	rhp3Listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return nil, fmt.Errorf("failed to create rhp3 listener: %w", err)
	}

	rhp4Listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return nil, fmt.Errorf("failed to create rhp3 listener: %w", err)
	}

	settings, err := settings.NewConfigManager(privKey, db, cm, s, storage, wallet,
		settings.WithValidateNetAddress(false),
		settings.WithRHP2Port(uint16(rhp2Listener.Addr().(*net.TCPAddr).Port)),
		settings.WithRHP3Port(uint16(rhp3Listener.Addr().(*net.TCPAddr).Port)),
		settings.WithRHP4Port(uint16(rhp4Listener.Addr().(*net.TCPAddr).Port)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create settings manager: %w", err)
	}

	idx, err := index.NewManager(db, cm, contracts, wallet, settings, storage, index.WithLog(log.Named("index")), index.WithBatchSize(0)) // off-by-one
	if err != nil {
		return nil, fmt.Errorf("failed to create index manager: %w", err)
	}

	registry := registry.NewManager(privKey, db, zap.NewNop())
	accounts := accounts.NewManager(db, settings)

	rhpv2 := rhpv2.NewSessionHandler(rhp2Listener, privKey, cm, s, wallet, contracts, settings, storage, log.Named("rhpv2"))
	go rhpv2.Serve()

	rhpv3 := rhpv3.NewSessionHandler(rhp3Listener, privKey, cm, s, wallet, accounts, contracts, registry, storage, settings, log.Named("rhpv3"))
	go rhpv3.Serve()

	contractsV2 := testutil.NewEphemeralContractor(cm)
	rhpv4 := rhp4.NewServer(privKey, cm, s, contractsV2, wallet, settings, storage, rhp4.WithPriceTableValidity(30*time.Minute))
	go rhp.ServeRHP4SiaMux(rhp4Listener, rhpv4, log.Named("rhp4"))

	return &Host{
		dir:     dir,
		privKey: privKey,

		s:            s,
		syncerCancel: syncerCancel,

		store:       db,
		wallet:      wallet,
		settings:    settings,
		index:       idx,
		storage:     storage,
		registry:    registry,
		accounts:    accounts,
		contracts:   contracts,
		contractsV2: contractsV2,

		rhpv2: rhpv2,
		rhpv3: rhpv3,

		rhp4Listener: rhp4Listener,
	}, nil
}
