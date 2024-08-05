package node

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/internal/utils"
	"go.sia.tech/siad/modules"
	stypes "go.sia.tech/siad/types"
)

const (
	maxSyncTime = time.Hour
)

var (
	ErrBlockNotFound   = errors.New("block not found")
	ErrInvalidChangeID = errors.New("invalid change id")
)

type (
	// A TransactionPool can validate and relay unconfirmed transactions.
	TransactionPool interface {
		AcceptTransactionSet(txns []types.Transaction) error
		Close() error
		RecommendedFee() types.Currency
		Subscribe(subscriber modules.TransactionPoolSubscriber)
		Transactions() []types.Transaction
		UnconfirmedParents(txn types.Transaction) ([]types.Transaction, error)
	}

	chainManager struct {
		cs      modules.ConsensusSet
		tp      TransactionPool
		network *consensus.Network

		close         chan struct{}
		mu            sync.Mutex
		lastBlockTime time.Time
		tip           consensus.State
		synced        bool
	}
)

// ProcessConsensusChange implements the modules.ConsensusSetSubscriber interface.
func (m *chainManager) ProcessConsensusChange(cc modules.ConsensusChange) {
	m.mu.Lock()
	defer m.mu.Unlock()

	b := cc.AppliedBlocks[len(cc.AppliedBlocks)-1]
	m.tip = consensus.State{
		Network: m.network,
		Index: types.ChainIndex{
			ID:     types.BlockID(b.ID()),
			Height: uint64(cc.BlockHeight),
		},
	}
	m.synced = synced(b.Timestamp)
	m.lastBlockTime = time.Unix(int64(b.Timestamp), 0)
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
	utils.ConvertToCore(sb, (*types.V1Block)(&c))
	return types.Block(c), ok
}

func (m *chainManager) LastBlockTime() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastBlockTime
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
	utils.ConvertToSiad(types.V1Block(b), &sb)
	return m.cs.AcceptBlock(sb)
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

// PoolTransactions returns all transactions in the transaction pool
func (m *chainManager) PoolTransactions() []types.Transaction {
	return m.tp.Transactions()
}

func synced(timestamp stypes.Timestamp) bool {
	return time.Since(time.Unix(int64(timestamp), 0)) <= maxSyncTime
}

// NewManager creates a new chain manager.
func NewChainManager(cs modules.ConsensusSet, tp TransactionPool, network *consensus.Network) (*chainManager, error) {
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
		synced:        synced(block.Timestamp),
		lastBlockTime: time.Unix(int64(block.Timestamp), 0),
		close:         make(chan struct{}),
	}

	if err := cs.ConsensusSetSubscribe(m, modules.ConsensusChangeRecent, m.close); err != nil {
		return nil, fmt.Errorf("failed to subscribe to consensus set: %w", err)
	}
	return m, nil
}
