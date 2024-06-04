package e2e

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"go.sia.tech/core/consensus"
	crhpv2 "go.sia.tech/core/rhp/v2"
	crhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/alerts"
	"go.sia.tech/hostd/host/accounts"
	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/hostd/host/registry"
	"go.sia.tech/hostd/host/settings"
	"go.sia.tech/hostd/host/storage"
	"go.sia.tech/hostd/persist/sqlite"
	"go.sia.tech/hostd/rhp"
	rhpv2 "go.sia.tech/hostd/rhp/v2"
	rhpv3 "go.sia.tech/hostd/rhp/v3"
	"go.sia.tech/hostd/wallet"
	"go.sia.tech/hostd/webhooks"
	"go.sia.tech/renterd/bus"
	"go.sia.tech/siad/modules"
	mconsensus "go.sia.tech/siad/modules/consensus"
	"go.sia.tech/siad/modules/gateway"
	"go.sia.tech/siad/modules/transactionpool"
	stypes "go.sia.tech/siad/types"
	"go.uber.org/zap"
)

const (
	blocksPerDay   = 144
	blocksPerMonth = blocksPerDay * 30
)

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

type txpool struct {
	tp modules.TransactionPool
}

func (tp txpool) RecommendedFee() (fee types.Currency) {
	_, maxFee := tp.tp.FeeEstimation()
	convertToCore(&maxFee, (*types.V1Currency)(&fee))
	return
}

func (tp txpool) Transactions() []types.Transaction {
	stxns := tp.tp.Transactions()
	txns := make([]types.Transaction, len(stxns))
	for i := range txns {
		convertToCore(&stxns[i], &txns[i])
	}
	return txns
}

func (tp txpool) AcceptTransactionSet(txns []types.Transaction) error {
	stxns := make([]stypes.Transaction, len(txns))
	for i := range stxns {
		convertToSiad(&txns[i], &stxns[i])
	}
	err := tp.tp.AcceptTransactionSet(stxns)
	if errors.Is(err, modules.ErrDuplicateTransactionSet) {
		err = nil
	}
	return err
}

func (tp txpool) UnconfirmedParents(txn types.Transaction) ([]types.Transaction, error) {
	return unconfirmedParents(txn, tp.Transactions()), nil
}

func (tp txpool) Subscribe(subscriber modules.TransactionPoolSubscriber) {
	tp.tp.TransactionPoolSubscribe(subscriber)
}

func (tp txpool) Close() error {
	return tp.tp.Close()
}

func unconfirmedParents(txn types.Transaction, pool []types.Transaction) []types.Transaction {
	outputToParent := make(map[types.SiacoinOutputID]*types.Transaction)
	for i, txn := range pool {
		for j := range txn.SiacoinOutputs {
			outputToParent[txn.SiacoinOutputID(j)] = &pool[i]
		}
	}
	var parents []types.Transaction
	txnsToCheck := []*types.Transaction{&txn}
	seen := make(map[types.TransactionID]bool)
	for len(txnsToCheck) > 0 {
		nextTxn := txnsToCheck[0]
		txnsToCheck = txnsToCheck[1:]
		for _, sci := range nextTxn.SiacoinInputs {
			if parent, ok := outputToParent[sci.ParentID]; ok {
				if txid := parent.ID(); !seen[txid] {
					seen[txid] = true
					parents = append(parents, *parent)
					txnsToCheck = append(txnsToCheck, parent)
				}
			}
		}
	}
	slices.Reverse(parents)
	return parents
}

func NewTransactionPool(tp modules.TransactionPool) bus.TransactionPool {
	return &txpool{tp: tp}
}

const (
	maxSyncTime = time.Hour
)

var (
	ErrBlockNotFound   = errors.New("block not found")
	ErrInvalidChangeID = errors.New("invalid change id")
)

type chainManager struct {
	cs      modules.ConsensusSet
	tp      bus.TransactionPool
	network *consensus.Network

	close  chan struct{}
	mu     sync.Mutex
	tip    consensus.State
	synced bool
}

// ProcessConsensusChange implements the modules.ConsensusSetSubscriber interface.
func (m *chainManager) ProcessConsensusChange(cc modules.ConsensusChange) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tip = consensus.State{
		Network: m.network,
		Index: types.ChainIndex{
			ID:     types.BlockID(cc.AppliedBlocks[len(cc.AppliedBlocks)-1].ID()),
			Height: uint64(cc.BlockHeight),
		},
	}
	m.synced = synced(cc.AppliedBlocks[len(cc.AppliedBlocks)-1].Timestamp)
}

// Network returns the network name.
func (m *chainManager) Network() string {
	switch m.network.Name {
	case "zen":
		return "Zen Testnet"
	case "mainnet":
		return "Mainnet"
	default:
		return m.network.Name
	}
}

// Close closes the chain manager.
func (m *chainManager) Close() error {
	select {
	case <-m.close:
		return nil
	default:
	}
	close(m.close)
	return m.cs.Close()
}

// Synced returns true if the chain manager is synced with the consensus set.
func (m *chainManager) Synced() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.synced
}

// BlockAtHeight returns the block at the given height.
func (m *chainManager) BlockAtHeight(height uint64) (types.Block, bool) {
	sb, ok := m.cs.BlockAtHeight(stypes.BlockHeight(height))
	var c types.Block
	convertToCore(sb, (*types.V1Block)(&c))
	return types.Block(c), ok
}

func (m *chainManager) LastBlockTime() time.Time {
	return time.Unix(int64(m.cs.CurrentBlock().Timestamp), 0)
}

// IndexAtHeight return the chain index at the given height.
func (m *chainManager) IndexAtHeight(height uint64) (types.ChainIndex, error) {
	block, ok := m.cs.BlockAtHeight(stypes.BlockHeight(height))
	if !ok {
		return types.ChainIndex{}, ErrBlockNotFound
	}
	return types.ChainIndex{
		ID:     types.BlockID(block.ID()),
		Height: height,
	}, nil
}

// TipState returns the current chain state.
func (m *chainManager) TipState() consensus.State {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tip
}

// AcceptBlock adds b to the consensus set.
func (m *chainManager) AcceptBlock(b types.Block) error {
	var sb stypes.Block
	convertToSiad(types.V1Block(b), &sb)
	return m.cs.AcceptBlock(sb)
}

// PoolTransactions returns all transactions in the transaction pool
func (m *chainManager) PoolTransactions() []types.Transaction {
	return m.tp.Transactions()
}

// Subscribe subscribes to the consensus set.
func (m *chainManager) Subscribe(s modules.ConsensusSetSubscriber, ccID modules.ConsensusChangeID, cancel <-chan struct{}) error {
	if err := m.cs.ConsensusSetSubscribe(s, ccID, cancel); err != nil {
		if strings.Contains(err.Error(), "consensus subscription has invalid id") {
			return ErrInvalidChangeID
		}
		return err
	}
	return nil
}

func synced(timestamp stypes.Timestamp) bool {
	return time.Since(time.Unix(int64(timestamp), 0)) <= maxSyncTime
}

// NewManager creates a new chain manager.
func NewChainManager(cs modules.ConsensusSet, tp bus.TransactionPool, network *consensus.Network) (*chainManager, error) {
	height := cs.Height()
	block, ok := cs.BlockAtHeight(height)
	if !ok {
		return nil, fmt.Errorf("failed to get block at height %d", height)
	}

	m := &chainManager{
		cs:      cs,
		tp:      tp,
		network: network,
		tip: consensus.State{
			Network: network,
			Index: types.ChainIndex{
				ID:     types.BlockID(block.ID()),
				Height: uint64(height),
			},
		},
		synced: synced(block.Timestamp),
		close:  make(chan struct{}),
	}

	if err := cs.ConsensusSetSubscribe(m, modules.ConsensusChangeRecent, m.close); err != nil {
		return nil, fmt.Errorf("failed to subscribe to consensus set: %w", err)
	}
	return m, nil
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

	AccountExpiry:     30 * 24 * time.Hour, // 1 month
	MaxAccountBalance: types.Siacoins(10),
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
	tpool, err := transactionpool.New(cs, g, filepath.Join(dir, "transactionpool"))
	if err != nil {
		return nil, fmt.Errorf("failed to create transaction pool: %w", err)
	}
	tp := NewTransactionPool(tpool)
	cm, err := NewChainManager(cs, tp, network)
	if err != nil {
		return nil, err
	}
	log := zap.NewNop()
	db, err := sqlite.OpenDatabase(filepath.Join(dir, "hostd.db"), log.Named("sqlite"))
	if err != nil {
		return nil, fmt.Errorf("failed to create sql store: %w", err)
	}

	wallet, err := wallet.NewSingleAddressWallet(privKey, cm, db, log.Named("wallet"))
	if err != nil {
		return nil, fmt.Errorf("failed to create wallet: %w", err)
	}

	wr, err := webhooks.NewManager(db, log.Named("webhooks"))
	if err != nil {
		return nil, fmt.Errorf("failed to create webhook reporter: %w", err)
	}

	am := alerts.NewManager(wr, log.Named("alerts"))
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

	settings, err := settings.NewConfigManager(
		settings.WithHostKey(privKey),
		settings.WithRHP2Addr(rhp2Listener.Addr().String()),
		settings.WithStore(db),
		settings.WithChainManager(cm),
		settings.WithTransactionPool(tp),
		settings.WithWallet(wallet),
		settings.WithAlertManager(am),
		settings.WithLog(log.Named("settings")))
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
