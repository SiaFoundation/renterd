package testing

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"gitlab.com/NebulousLabs/encoding"
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
	"go.sia.tech/renterd/build"
	"go.sia.tech/siad/modules"
	mconsensus "go.sia.tech/siad/modules/consensus"
	"go.sia.tech/siad/modules/gateway"
	"go.sia.tech/siad/modules/transactionpool"
	stypes "go.sia.tech/siad/types"
	"go.uber.org/zap"
)

const blocksPerMonth = 144 * 30

type stubMetricReporter struct{}

func (stubMetricReporter) StartSession(conn *rhp.Conn, proto string, version int) (_ rhp.UID, _ func()) {
	return
}
func (stubMetricReporter) StartRPC(rhp.UID, types.Specifier) (_ rhp.UID, _ func(contracts.Usage, error)) {
	return
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
	tp *TXPool

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

func convertToSiad(core types.EncoderTo, siad encoding.SiaUnmarshaler) {
	var buf bytes.Buffer
	e := types.NewEncoder(&buf)
	core.EncodeTo(e)
	e.Flush()
	if err := siad.UnmarshalSia(&buf); err != nil {
		panic(err)
	}
}

func convertToCore(siad encoding.SiaMarshaler, core types.DecoderFrom) {
	var buf bytes.Buffer
	siad.MarshalSia(&buf)
	d := types.NewBufDecoder(buf.Bytes())
	core.DecodeFrom(d)
	if d.Err() != nil {
		panic(d.Err())
	}
}

// TXPool wraps a siad transaction pool with core types.
type TXPool struct {
	tp modules.TransactionPool
}

// RecommendedFee returns the recommended fee for a transaction.
func (tp TXPool) RecommendedFee() (fee types.Currency) {
	_, max := tp.tp.FeeEstimation()
	convertToCore(&max, &fee)
	return
}

// Transactions returns all transactions in the pool.
func (tp TXPool) Transactions() []types.Transaction {
	stxns := tp.tp.Transactions()
	txns := make([]types.Transaction, len(stxns))
	for i := range txns {
		convertToCore(&stxns[i], &txns[i])
	}
	return txns
}

// AcceptTransactionSet adds a transaction set to the pool.
func (tp TXPool) AcceptTransactionSet(txns []types.Transaction) error {
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

// UnconfirmedParents returns the parents of a transaction in the pool.
func (tp TXPool) UnconfirmedParents(txn types.Transaction) ([]types.Transaction, error) {
	pool := tp.Transactions()
	outputToParent := make(map[types.SiacoinOutputID]*types.Transaction)
	for i, txn := range pool {
		for j := range txn.SiacoinOutputs {
			outputToParent[txn.SiacoinOutputID(j)] = &pool[i]
		}
	}
	var parents []types.Transaction
	seen := make(map[types.TransactionID]bool)
	for _, sci := range txn.SiacoinInputs {
		if parent, ok := outputToParent[sci.ParentID]; ok {
			if txid := parent.ID(); !seen[txid] {
				seen[txid] = true
				parents = append(parents, *parent)
			}
		}
	}
	return parents, nil
}

// Subscribe subscribes to the transaction pool.
func (tp TXPool) Subscribe(subscriber modules.TransactionPoolSubscriber) {
	tp.tp.TransactionPoolSubscribe(subscriber)
}

const maxSyncTime = time.Hour

var (
	// ErrBlockNotFound is returned when a block is not found.
	ErrBlockNotFound = errors.New("block not found")
	// ErrInvalidChangeID is returned to a subscriber when the change id is
	// invalid.
	ErrInvalidChangeID = errors.New("invalid change id")
)

// A Manager implements wallet.ChainManager
type Manager struct {
	cs      modules.ConsensusSet
	network *consensus.Network

	close  chan struct{}
	mu     sync.Mutex
	tip    consensus.State
	synced bool
}

// ProcessConsensusChange implements the modules.ConsensusSetSubscriber interface.
func (m *Manager) ProcessConsensusChange(cc modules.ConsensusChange) {
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
func (m *Manager) Network() string {
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
func (m *Manager) Close() error {
	select {
	case <-m.close:
		return nil
	default:
	}
	close(m.close)
	return m.cs.Close()
}

// Synced returns true if the chain manager is synced with the consensus set.
func (m *Manager) Synced() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.synced
}

// BlockAtHeight returns the block at the given height.
func (m *Manager) BlockAtHeight(height uint64) (types.Block, bool) {
	sb, ok := m.cs.BlockAtHeight(stypes.BlockHeight(height))
	var c types.V1Block
	convertToCore(sb, &c)
	return types.Block(c), ok
}

// IndexAtHeight return the chain index at the given height.
func (m *Manager) IndexAtHeight(height uint64) (types.ChainIndex, error) {
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
func (m *Manager) TipState() consensus.State {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tip
}

// AcceptBlock adds b to the consensus set.
func (m *Manager) AcceptBlock(b types.Block) error {
	var sb stypes.Block
	convertToSiad(types.V1Block(b), &sb)
	return m.cs.AcceptBlock(sb)
}

// Subscribe subscribes to the consensus set.
func (m *Manager) Subscribe(s modules.ConsensusSetSubscriber, ccID modules.ConsensusChangeID, cancel <-chan struct{}) error {
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

// NewManager creates a new manager.
func NewManager(cs modules.ConsensusSet) (*Manager, error) {
	height := cs.Height()
	block, ok := cs.BlockAtHeight(height)
	if !ok {
		return nil, fmt.Errorf("failed to get block at height %d", height)
	}
	n, _ := build.Network()
	m := &Manager{
		cs:      cs,
		network: n,
		tip: consensus.State{
			Network: n,
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

// Close shutsdown the host
func (h *Host) Close() error {
	h.rhpv2.Close()
	h.rhpv3.Close()
	h.settings.Close()
	h.wallet.Close()
	h.contracts.Close()
	h.storage.Close()
	h.store.Close()
	h.tp.tp.Close()
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
func NewHost(privKey types.PrivateKey, dir string, debugLogging bool) (*Host, error) {
	g, err := gateway.New("localhost:0", false, filepath.Join(dir, "gateway"))
	if err != nil {
		return nil, fmt.Errorf("failed to create gateway: %w", err)
	}
	cs, errCh := mconsensus.New(g, false, filepath.Join(dir, "consensus"))
	if err := <-errCh; err != nil {
		return nil, fmt.Errorf("failed to create consensus set: %w", err)
	}
	cm, err := NewManager(cs)
	if err != nil {
		return nil, err
	}
	tpool, err := transactionpool.New(cs, g, filepath.Join(dir, "transactionpool"))
	if err != nil {
		return nil, fmt.Errorf("failed to create transaction pool: %w", err)
	}
	tp := &TXPool{tpool}

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
