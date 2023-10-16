// TODO: remove this file when we can import it from hostd
package node

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"go.sia.tech/core/types"
	"go.sia.tech/siad/crypto"
	"go.sia.tech/siad/modules"
	stypes "go.sia.tech/siad/types"
	"lukechampine.com/frand"
)

const solveAttempts = 1e4

type (
	// Consensus defines a minimal interface needed by the miner to interact
	// with the consensus set
	Consensus interface {
		AcceptBlock(context.Context, types.Block) error
	}

	// A Miner is a CPU miner that can mine blocks, sending the reward to a
	// specified address.
	Miner struct {
		consensus Consensus

		mu             sync.Mutex
		height         stypes.BlockHeight
		target         stypes.Target
		currentBlockID stypes.BlockID
		txnsets        map[modules.TransactionSetID][]stypes.TransactionID
		transactions   []stypes.Transaction
	}
)

var errFailedToSolve = errors.New("failed to solve block")

// ProcessConsensusChange implements modules.ConsensusSetSubscriber.
func (m *Miner) ProcessConsensusChange(cc modules.ConsensusChange) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.target = cc.ChildTarget
	m.currentBlockID = cc.AppliedBlocks[len(cc.AppliedBlocks)-1].ID()
	m.height = cc.BlockHeight
}

// ReceiveUpdatedUnconfirmedTransactions implements modules.TransactionPoolSubscriber
func (m *Miner) ReceiveUpdatedUnconfirmedTransactions(diff *modules.TransactionPoolDiff) {
	m.mu.Lock()
	defer m.mu.Unlock()

	reverted := make(map[stypes.TransactionID]bool)
	for _, setID := range diff.RevertedTransactions {
		for _, txnID := range m.txnsets[setID] {
			reverted[txnID] = true
		}
	}

	filtered := m.transactions[:0]
	for _, txn := range m.transactions {
		if reverted[txn.ID()] {
			continue
		}
		filtered = append(filtered, txn)
	}

	for _, txnset := range diff.AppliedTransactions {
		m.txnsets[txnset.ID] = txnset.IDs
		filtered = append(filtered, txnset.Transactions...)
	}
	m.transactions = filtered
}

// mineBlock attempts to mine a block and add it to the consensus set.
func (m *Miner) mineBlock(addr stypes.UnlockHash) error {
	m.mu.Lock()
	block := stypes.Block{
		ParentID:  m.currentBlockID,
		Timestamp: stypes.CurrentTimestamp(),
	}

	randBytes := frand.Bytes(stypes.SpecifierLen)
	randTxn := stypes.Transaction{
		ArbitraryData: [][]byte{append(modules.PrefixNonSia[:], randBytes...)},
	}
	block.Transactions = append([]stypes.Transaction{randTxn}, m.transactions...)
	block.MinerPayouts = append(block.MinerPayouts, stypes.SiacoinOutput{
		Value:      block.CalculateSubsidy(m.height + 1),
		UnlockHash: addr,
	})
	target := m.target
	m.mu.Unlock()

	merkleRoot := block.MerkleRoot()
	header := make([]byte, 80)
	copy(header, block.ParentID[:])
	binary.LittleEndian.PutUint64(header[40:48], uint64(block.Timestamp))
	copy(header[48:], merkleRoot[:])

	var nonce uint64
	var solved bool
	for i := 0; i < solveAttempts; i++ {
		id := crypto.HashBytes(header)
		if bytes.Compare(target[:], id[:]) >= 0 {
			block.Nonce = *(*stypes.BlockNonce)(header[32:40])
			solved = true
			break
		}
		binary.LittleEndian.PutUint64(header[32:], nonce)
		nonce += stypes.ASICHardforkFactor
	}
	if !solved {
		return errFailedToSolve
	}

	var b types.Block
	convertToCore(&block, (*types.V1Block)(&b))
	if err := m.consensus.AcceptBlock(context.Background(), types.Block(b)); err != nil {
		return fmt.Errorf("failed to get block accepted: %w", err)
	}
	return nil
}

// Mine mines n blocks, sending the reward to addr
func (m *Miner) Mine(addr types.Address, n int) error {
	var err error
	for mined := 1; mined <= n; {
		// return the error only if the miner failed to solve the block,
		// ignore any consensus related errors
		if err = m.mineBlock(stypes.UnlockHash(addr)); errors.Is(err, errFailedToSolve) {
			return fmt.Errorf("failed to mine block %v: %w", mined, errFailedToSolve)
		}
		mined++
	}
	return nil
}

// NewMiner initializes a new CPU miner
func NewMiner(consensus Consensus) *Miner {
	return &Miner{
		consensus: consensus,
		txnsets:   make(map[modules.TransactionSetID][]stypes.TransactionID),
	}
}
