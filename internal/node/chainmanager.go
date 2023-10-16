package node

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
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

type chainManager struct {
	cs      modules.ConsensusSet
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
func NewChainManager(cs modules.ConsensusSet, network *consensus.Network) (*chainManager, error) {
	height := cs.Height()
	block, ok := cs.BlockAtHeight(height)
	if !ok {
		return nil, fmt.Errorf("failed to get block at height %d", height)
	}

	m := &chainManager{
		cs:      cs,
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
