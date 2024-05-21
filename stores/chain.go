package stores

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/chain"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	_ chain.ChainStore    = (*SQLStore)(nil)
	_ chain.ChainUpdateTx = (*chainUpdateTx)(nil)
)

// chainUpdateTx implements the ChainUpdateTx interface.
type chainUpdateTx struct {
	tx *gorm.DB

	logs []logEntry // only logged if tx was successfully committed
}

type logEntry struct {
	msg           string
	keysAndValues []interface{}
}

// ProcessChainUpdate returns a callback function that process a chain update
// inside a transaction.
func (s *SQLStore) ProcessChainUpdate(ctx context.Context, fn func(chain.ChainUpdateTx) error) error {
	return s.retryTransaction(ctx, func(tx *gorm.DB) error {
		updateTx := &chainUpdateTx{tx: tx}
		err := fn(updateTx)
		if err == nil {
			updateTx.log(s.logger.Named("ProcessChainUpdate"))
		}
		return err
	})
}

// UpdateChainState process the given revert and apply updates.
func (s *SQLStore) UpdateChainState(reverted []chain.RevertUpdate, applied []chain.ApplyUpdate) error {
	return s.ProcessChainUpdate(context.Background(), func(tx chain.ChainUpdateTx) error {
		return wallet.UpdateChainState(tx, s.walletAddress, applied, reverted)
	})
}

// ApplyIndex is called with the chain index that is being applied. Any
// transactions and siacoin elements that were created by the index should be
// added and any siacoin elements that were spent should be removed.
func (u *chainUpdateTx) ApplyIndex(index types.ChainIndex, created, spent []types.SiacoinElement, events []wallet.Event) error {
	u.debug("applying index", "height", index.Height, "block_id", index.ID)

	// remove spent outputs
	for _, e := range spent {
		u.debug(fmt.Sprintf("remove output %v", e.ID), "height", index.Height, "block_id", index.ID)
		if res := u.tx.
			Where("output_id", hash256(e.ID)).
			Delete(&dbWalletOutput{}); res.Error != nil {
			return res.Error
		} else if res.RowsAffected != 1 {
			return fmt.Errorf("spent output with id %v not found ", e.ID)
		}
	}

	// create outputs
	for _, e := range created {
		u.debug(fmt.Sprintf("create output %v", e.ID), "height", index.Height, "block_id", index.ID)
		if err := u.tx.
			Clauses(clause.OnConflict{
				DoNothing: true,
				Columns:   []clause.Column{{Name: "output_id"}},
			}).
			Create(&dbWalletOutput{
				OutputID:       hash256(e.ID),
				LeafIndex:      e.StateElement.LeafIndex,
				MerkleProof:    merkleProof{proof: e.StateElement.MerkleProof},
				Value:          currency(e.SiacoinOutput.Value),
				Address:        hash256(e.SiacoinOutput.Address),
				MaturityHeight: e.MaturityHeight,
			}).Error; err != nil {
			return nil
		}
	}

	// create events
	for _, e := range events {
		u.debug(fmt.Sprintf("create event %v", e.ID), "height", index.Height, "block_id", index.ID)
		if err := u.tx.
			Clauses(clause.OnConflict{
				DoNothing: true,
				Columns:   []clause.Column{{Name: "event_id"}},
			}).
			Create(&dbWalletEvent{
				EventID:        hash256(e.ID),
				Inflow:         currency(e.Inflow),
				Outflow:        currency(e.Outflow),
				Transaction:    e.Transaction,
				MaturityHeight: e.MaturityHeight,
				Source:         string(e.Source),
				Timestamp:      e.Timestamp.Unix(),
				Height:         e.Index.Height,
				BlockID:        hash256(e.Index.ID),
			}).Error; err != nil {
			return err
		}
	}
	return nil
}

// ContractState returns the state of a file contract.
func (u *chainUpdateTx) ContractState(fcid types.FileContractID) (api.ContractState, error) {
	// try regular contracts
	var c dbContract
	if err := u.tx.
		Model(&dbContract{}).
		Where("fcid", fileContractID(fcid)).
		Take(&c).
		Error; err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return "", err
	} else if err == nil {
		return api.ContractState(c.State.String()), nil
	}

	// try archived contracts
	var ac dbArchivedContract
	if err := u.tx.
		Model(&dbArchivedContract{}).
		Where("fcid", fileContractID(fcid)).
		Take(&ac).
		Error; err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return "", err
	} else if err == nil {
		return api.ContractState(ac.State.String()), nil
	}

	return "", api.ErrContractNotFound
}

// RevertIndex is called with the chain index that is being reverted. Any
// transactions and siacoin elements that were created by the index should be
// removed.
func (u *chainUpdateTx) RevertIndex(index types.ChainIndex, removed, unspent []types.SiacoinElement) error {
	u.debug("reverting index", "height", index.Height, "block_id", index.ID)

	// recreate unspent outputs
	for _, e := range unspent {
		if err := u.tx.
			Clauses(clause.OnConflict{
				DoNothing: true,
				Columns:   []clause.Column{{Name: "output_id"}},
			}).
			Create(&dbWalletOutput{
				OutputID:       hash256(e.ID),
				LeafIndex:      e.StateElement.LeafIndex,
				MerkleProof:    merkleProof{proof: e.StateElement.MerkleProof},
				Value:          currency(e.SiacoinOutput.Value),
				Address:        hash256(e.SiacoinOutput.Address),
				MaturityHeight: e.MaturityHeight,
			}).Error; err != nil {
			return nil
		}
		u.debug(fmt.Sprintf("recreate unspent output %v", e.ID), "height", index.Height, "block_id", index.ID)
	}

	// remove outputs created at the reverted index
	for _, e := range removed {
		if err := u.tx.
			Where("output_id", hash256(e.ID)).
			Delete(&dbWalletOutput{}).
			Error; err != nil {
			return err
		}
		u.debug(fmt.Sprintf("remove output %v", e.ID), "height", index.Height, "block_id", index.ID)
	}

	// remove events created at the reverted index
	res := u.tx.
		Model(&dbWalletEvent{}).
		Where("height = ? AND block_id = ?", index.Height, hash256(index.ID)).
		Delete(&dbWalletEvent{})
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected > 0 {
		u.debug(fmt.Sprintf("removed %d events", res.RowsAffected), "height", index.Height, "block_id", index.ID)
	}
	return nil
}

// UpdateChainIndex updates the chain index in the database.
func (u *chainUpdateTx) UpdateChainIndex(index types.ChainIndex) error {
	u.debug("updating index", "height", index.Height, "block_id", index.ID)
	return u.tx.
		Model(&dbConsensusInfo{}).
		Where(&dbConsensusInfo{Model: Model{ID: consensusInfoID}}).
		Updates(map[string]interface{}{
			"height":   index.Height,
			"block_id": hash256(index.ID),
		}).
		Error
}

// UpdateContract updates the revision height, revision number, and size the
// contract with given fcid.
func (u *chainUpdateTx) UpdateContract(fcid types.FileContractID, revisionHeight, revisionNumber, size uint64) error {
	// isUpdatedRevision indicates whether the given revision number is greater
	// than the one currently set on the contract
	isUpdatedRevision := func(currRevStr string) bool {
		var currRev uint64
		_, _ = fmt.Sscan(currRevStr, &currRev)
		return revisionNumber > currRev
	}

	// try regular contract
	var c dbContract
	err := u.tx.
		Model(&dbContract{}).
		Where("fcid", fileContractID(fcid)).
		Take(&c).
		Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return err
	} else if err == nil {
		// save old valules
		oldRevH := c.RevisionHeight
		oldRevN := c.RevisionNumber
		oldSize := c.Size

		c.RevisionHeight = revisionHeight
		if isUpdatedRevision(c.RevisionNumber) {
			c.RevisionNumber = fmt.Sprint(revisionNumber)
			c.Size = size
		}

		u.debug(fmt.Sprintf("update contract, revision number %s -> %s, revision height %d -> %d, size %d -> %d", oldRevN, c.RevisionNumber, oldRevH, c.RevisionHeight, oldSize, c.Size), "fcid", fcid)
		return u.tx.Save(&c).Error
	}

	// try archived contract
	var ac dbArchivedContract
	err = u.tx.
		Model(&dbArchivedContract{}).
		Where("fcid", fileContractID(fcid)).
		Take(&ac).
		Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return err
	} else if err == nil {
		// save old valules
		oldRevH := ac.RevisionHeight
		oldRevN := ac.RevisionNumber
		oldSize := ac.Size

		ac.RevisionHeight = revisionHeight
		if isUpdatedRevision(ac.RevisionNumber) {
			ac.RevisionNumber = fmt.Sprint(revisionNumber)
			ac.Size = size
		}

		u.debug(fmt.Sprintf("update archived contract, revision number %s -> %s, revision height %d -> %d, size %d -> %d", oldRevN, ac.RevisionNumber, oldRevH, ac.RevisionHeight, oldSize, ac.Size), "fcid", fcid)
		return u.tx.Save(&ac).Error
	}

	return api.ErrContractNotFound
}

// UpdateContractState updates the state of the contract with given fcid.
func (u *chainUpdateTx) UpdateContractState(fcid types.FileContractID, state api.ContractState) error {
	u.debug("update contract state", "fcid", fcid, "state", state)

	var cs contractState
	if err := cs.LoadString(string(state)); err != nil {
		return err
	}

	// return early if the contract is already in the desired state
	curr, err := u.ContractState(fcid)
	if err != nil {
		return err
	} else if curr == state {
		return nil
	}

	// try regular contract
	if res := u.tx.
		Model(&dbContract{}).
		Where("fcid", fileContractID(fcid)).
		Update("state", cs); res.Error != nil {
		return res.Error
	} else if res.RowsAffected > 0 {
		return nil
	}

	// try archived contract
	if res := u.tx.
		Model(&dbArchivedContract{}).
		Where("fcid", fileContractID(fcid)).
		Update("state", cs); res.Error != nil {
		return res.Error
	} else if res.RowsAffected > 0 {
		return nil
	}

	// wrap ErrContractNotFound
	return fmt.Errorf("%v %w", fcid, api.ErrContractNotFound)
}

// UpdateContractProofHeight updates the proof height of the contract with given
// fcid.
func (u *chainUpdateTx) UpdateContractProofHeight(fcid types.FileContractID, proofHeight uint64) error {
	u.debug("update contract proof height", "fcid", fcid, "proof_height", proofHeight)

	// try regular contract
	if res := u.tx.
		Model(&dbContract{}).
		Where("fcid", fileContractID(fcid)).
		Update("proof_height", proofHeight); res.Error != nil {
		return res.Error
	} else if res.RowsAffected > 0 {
		return nil
	}

	// try archived contract
	if res := u.tx.
		Model(&dbArchivedContract{}).
		Where("fcid", fileContractID(fcid)).
		Update("proof_height", proofHeight); res.Error != nil {
		return res.Error
	} else if res.RowsAffected > 0 {
		return nil
	}

	// wrap api.ErrContractNotFound
	return fmt.Errorf("%v %w", fcid, api.ErrContractNotFound)
}

// UpdateFailedContracts marks active contract as failed if the current
// blockheight surposses their window_end.
func (u *chainUpdateTx) UpdateFailedContracts(blockHeight uint64) error {
	if res := u.tx.
		Model(&dbContract{}).
		Where("window_end <= ?", blockHeight).
		Where("state", contractStateActive).
		Update("state", contractStateFailed); res.Error != nil {
		return res.Error
	} else if res.RowsAffected > 0 {
		u.debug(fmt.Sprintf("marked %d active contracts as failed", res.RowsAffected), "window_end", blockHeight)
	}
	return nil
}

// UpdateHost creates the announcement and upserts the host in the database.
func (u *chainUpdateTx) UpdateHost(hk types.PublicKey, ha chain.HostAnnouncement, bh uint64, blockID types.BlockID, ts time.Time) error {
	u.debug("updated host", "hk", hk, "netaddress", ha.NetAddress)

	// create the announcement
	if err := u.tx.Create(&dbAnnouncement{
		HostKey:     publicKey(hk),
		BlockHeight: bh,
		BlockID:     blockID.String(),
		NetAddress:  ha.NetAddress,
	}).Error; err != nil {
		return err
	}

	// create the host
	if err := u.tx.Create(&dbHost{
		PublicKey:        publicKey(hk),
		LastAnnouncement: ts.UTC(),
		NetAddress:       ha.NetAddress,
	}).Error; err != nil {
		return err
	}

	// fetch blocklists
	allowlist, blocklist, err := getBlocklists(u.tx)
	if err != nil {
		return fmt.Errorf("%w; failed to fetch blocklists", err)
	}

	// return early if there are no allowlist or blocklist entries
	if len(allowlist)+len(blocklist) == 0 {
		return nil
	}

	// update blocklist
	if err := updateBlocklist(u.tx, hk, allowlist, blocklist); err != nil {
		return fmt.Errorf("%w; failed to update blocklist for host %v", err, hk)
	}

	return nil
}

// UpdateStateElements updates the proofs of all state elements affected by the
// update.
func (u *chainUpdateTx) UpdateStateElements(elements []types.StateElement) error {
	if len(elements) == 0 {
		return nil
	}

	utxos, err := u.outputs()
	if err != nil {
		return err
	}

	lookup := make(map[types.Hash256]int)
	for i, utxo := range utxos {
		lookup[types.Hash256(utxo.OutputID)] = i
	}

	for _, el := range elements {
		if index, ok := lookup[el.ID]; ok {
			update := utxos[index]
			update.LeafIndex = el.LeafIndex
			update.MerkleProof = merkleProof{proof: el.MerkleProof}
			utxos[index] = update
		}
	}

	return u.tx.Save(utxos).Error
}

// WalletStateElements implements the ChainStore interface and returns all state
// elements in the database.
func (u *chainUpdateTx) WalletStateElements() ([]types.StateElement, error) {
	type row struct {
		ID          hash256
		LeafIndex   uint64
		MerkleProof merkleProof
	}
	var rows []row
	if err := u.tx.
		Model(&dbWalletOutput{}).
		Select("output_id AS id", "leaf_index", "merkle_proof").
		Find(&rows).
		Error; err != nil {
		return nil, err
	}
	elements := make([]types.StateElement, 0, len(rows))
	for _, r := range rows {
		elements = append(elements, types.StateElement{
			ID:          types.Hash256(r.ID),
			LeafIndex:   r.LeafIndex,
			MerkleProof: r.MerkleProof.proof,
		})
	}
	return elements, nil
}

func (s *SQLStore) ResetChainState(ctx context.Context) error {
	return s.retryTransaction(ctx, func(tx *gorm.DB) error {
		if err := s.db.Exec("DELETE FROM consensus_infos").Error; err != nil {
			return err
		} else if err := s.db.Exec("DELETE FROM wallet_events").Error; err != nil {
			return err
		} else if err := s.db.Exec("DELETE FROM wallet_outputs").Error; err != nil {
			return err
		}
		return nil
	})
}

func (u *chainUpdateTx) outputs() (outputs []dbWalletOutput, err error) {
	err = u.tx.
		Model(&dbWalletOutput{}).
		Find(&outputs).
		Error
	return
}

func (u *chainUpdateTx) debug(msg string, keysAndValues ...interface{}) {
	u.logs = append(u.logs, logEntry{msg: msg, keysAndValues: keysAndValues})
}

func (u *chainUpdateTx) log(l *zap.SugaredLogger) {
	for _, log := range u.logs {
		l.Debugw(log.msg, log.keysAndValues...)
	}
	u.logs = nil
}
