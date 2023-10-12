package testing

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"time"

	"go.sia.tech/core/consensus"
	crhpv2 "go.sia.tech/core/rhp/v2"
	crhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/accounts"
	"go.sia.tech/hostd/host/alerts"
	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/hostd/host/registry"
	"go.sia.tech/hostd/host/settings"
	"go.sia.tech/hostd/host/storage"
	"go.sia.tech/hostd/persist/sqlite"
	"go.sia.tech/hostd/rhp"
	rhpv2 "go.sia.tech/hostd/rhp/v2"
	rhpv3 "go.sia.tech/hostd/rhp/v3"
	"go.sia.tech/hostd/wallet"
	"go.sia.tech/renterd/bus"
	"go.sia.tech/renterd/internal/node"
	"go.sia.tech/siad/modules"
	mconsensus "go.sia.tech/siad/modules/consensus"
	"go.sia.tech/siad/modules/gateway"
	"go.sia.tech/siad/modules/transactionpool"
	"go.uber.org/zap"
)

const blocksPerMonth = 144 * 30

type stubMetricReporter struct{}

func (stubMetricReporter) StartSession(conn *rhp.Conn, proto string, version int) (rhp.UID, func()) {
	return rhp.UID{}, func() {}
}
func (stubMetricReporter) StartRPC(rhp.UID, types.Specifier) (rhp.UID, func(contracts.Usage, error)) {
	return rhp.UID{}, func(contracts.Usage, error) {}
}

type stubDataMonitor struct{}

func (stubDataMonitor) ReadBytes(n int)  {}
func (stubDataMonitor) WriteBytes(n int) {}

// A Host is an ephemeral host that can be used for testing.
type Host struct {
	dir     string
	privKey types.PrivateKey

	g  modules.Gateway
	cs modules.ConsensusSet
	tp bus.TransactionPool

	store     *sqlite.Store
	wallet    *wallet.SingleAddressWallet
	settings  *settings.ConfigManager
	storage   *storage.VolumeManager
	registry  *registry.Manager
	accounts  *accounts.AccountManager
	contracts *contracts.ContractManager

	rhpv2 *rhpv2.SessionHandler
	rhpv3 *rhpv3.SessionHandler
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

	PriceTableValidity: 10 * time.Second,

	AccountExpiry:      30 * 24 * time.Hour, // 1 month
	MaxAccountBalance:  types.Siacoins(10),
	MaxRegistryEntries: 1e3,
}

// Close shutsdown the host
func (h *Host) Close() error {
	h.rhpv2.Close()
	h.rhpv3.Close()
	h.settings.Close()
	h.wallet.Close()
	h.contracts.Close()
	h.storage.Close()
	h.store.Close()
	h.tp.Close()
	h.cs.Close()
	h.g.Close()
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

// AddVolume adds a new volume to the host
func (h *Host) AddVolume(ctx context.Context, path string, size uint64) error {
	result := make(chan error)
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
	return h.rhpv2.Settings()
}

// RHPv3PriceTable returns the host's current RHPv3 price table
func (h *Host) RHPv3PriceTable() (crhpv3.HostPriceTable, error) {
	return h.rhpv3.PriceTable()
}

// WalletAddress returns the host's wallet address
func (h *Host) WalletAddress() types.Address {
	return h.wallet.Address()
}

// Contracts returns the host's contract manager
func (h *Host) Contracts() *contracts.ContractManager {
	return h.contracts
}

// PublicKey returns the public key of the host
func (h *Host) PublicKey() types.PublicKey {
	return h.privKey.PublicKey()
}

// GatewayAddr returns the address of the host's gateway.
func (h *Host) GatewayAddr() string {
	return string(h.g.Address())
}

// NewHost initializes a new test host
func NewHost(privKey types.PrivateKey, dir string, network *consensus.Network, debugLogging bool) (*Host, error) {
	g, err := gateway.New("localhost:0", false, filepath.Join(dir, "gateway"))
	if err != nil {
		return nil, fmt.Errorf("failed to create gateway: %w", err)
	}
	cs, errCh := mconsensus.New(g, false, filepath.Join(dir, "consensus"))
	if err := <-errCh; err != nil {
		return nil, fmt.Errorf("failed to create consensus set: %w", err)
	}
	cm, err := node.NewChainManager(cs, network)
	if err != nil {
		return nil, err
	}

	tpool, err := transactionpool.New(cs, g, filepath.Join(dir, "transactionpool"))
	if err != nil {
		return nil, fmt.Errorf("failed to create transaction pool: %w", err)
	}
	tp := node.NewTransactionPool(tpool)

	log := zap.NewNop()
	db, err := sqlite.OpenDatabase(filepath.Join(dir, "hostd.db"), log.Named("sqlite"))
	if err != nil {
		return nil, fmt.Errorf("failed to create sql store: %w", err)
	}

	wallet, err := wallet.NewSingleAddressWallet(privKey, cm, tp, db, log.Named("wallet"))
	if err != nil {
		return nil, fmt.Errorf("failed to create wallet: %w", err)
	}

	am := alerts.NewManager()
	storage, err := storage.NewVolumeManager(db, am, cm, log.Named("storage"), 0)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage manager: %w", err)
	}
	contracts, err := contracts.NewManager(db, am, storage, cm, tp, wallet, log.Named("contracts"))
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

	settings, err := settings.NewConfigManager(dir, privKey, rhp2Listener.Addr().String(), db, cm, tp, wallet, log.Named("settings"))
	if err != nil {
		return nil, fmt.Errorf("failed to create settings manager: %w", err)
	}

	registry := registry.NewManager(privKey, db, zap.NewNop())
	accounts := accounts.NewManager(db, settings)

	rhpv2, err := rhpv2.NewSessionHandler(rhp2Listener, privKey, rhp3Listener.Addr().String(), cm, tp, wallet, contracts, settings, storage, stubDataMonitor{}, stubMetricReporter{}, log.Named("rhpv2"))
	if err != nil {
		return nil, fmt.Errorf("failed to create rhpv2 session handler: %w", err)
	}
	go rhpv2.Serve()

	rhpv3, err := rhpv3.NewSessionHandler(rhp3Listener, privKey, cm, tp, wallet, accounts, contracts, registry, storage, settings, stubDataMonitor{}, stubMetricReporter{}, log.Named("rhpv3"))
	if err != nil {
		return nil, fmt.Errorf("failed to create rhpv3 session handler: %w", err)
	}
	go rhpv3.Serve()

	return &Host{
		dir:     dir,
		privKey: privKey,

		g:  g,
		cs: cs,
		tp: tp,

		store:     db,
		wallet:    wallet,
		settings:  settings,
		storage:   storage,
		registry:  registry,
		accounts:  accounts,
		contracts: contracts,

		rhpv2: rhpv2,
		rhpv3: rhpv3,
	}, nil
}
