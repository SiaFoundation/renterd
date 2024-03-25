package stores

import (
	"context"
	"fmt"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/chain"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var _ chain.ChainStore = (*SQLStore)(nil)

// ApplyChainUpdate implements the ChainStore interface and applies a given
// chain update to the database. This includes host announcements, contract
// updates and all wallet related updates.
func (s *SQLStore) ApplyChainUpdate(ctx context.Context, cu *chain.Update) error {
	return s.retryTransaction(ctx, func(tx *gorm.DB) error {
		// apply contract updates
		for fcid, contractUpdate := range cu.ContractUpdates {
			if err := updateContract(tx, fcid, *contractUpdate); err != nil {
				return fmt.Errorf("%w; failed to update contract %v", err, fcid)
			}
		}

		// mark failed contracts
		if err := markFailedContracts(tx, cu.Index.Height); err != nil {
			return fmt.Errorf("%w; failed to mark failed contracts", err)
		}

		// apply host updates
		if err := updateHosts(tx, cu.HostUpdates); err != nil {
			return fmt.Errorf("%w; failed to add hosts", err)
		}

		// apply wallet event updates
		for _, weu := range cu.WalletEventUpdates {
			if err := updateWalletEvent(tx, weu); err != nil {
				return fmt.Errorf("%w; failed to update wallet event %+v", err, weu)
			}
		}

		// apply wallet output updates
		for _, wou := range cu.WalletOutputUpdates {
			if err := updateWalletOutput(tx, wou); err != nil {
				return fmt.Errorf("%w; failed to update wallet output %+v", err, wou)
			}
		}

		// update chain index
		if err := updateChainIndex(tx, cu.Index); err != nil {
			return fmt.Errorf("%w; failed to update chain index", err)
		}

		return nil
	})
}

func updateHosts(tx *gorm.DB, ann map[types.PublicKey]chain.HostUpdate) error {
	if len(ann) == 0 {
		return nil
	}

	var hosts []dbHost
	var announcements []dbAnnouncement
	for hk, ha := range ann {
		hosts = append(hosts, dbHost{
			PublicKey:        publicKey(hk),
			LastAnnouncement: ha.Timestamp.UTC(),
			NetAddress:       ha.Announcement.NetAddress,
		})
		announcements = append(announcements, dbAnnouncement{
			HostKey:     publicKey(hk),
			BlockHeight: ha.BlockHeight,
			BlockID:     ha.BlockID.String(),
			NetAddress:  ha.Announcement.NetAddress,
		})
	}

	if err := tx.Create(&announcements).Error; err != nil {
		return err
	}
	if err := tx.Create(&hosts).Error; err != nil {
		return err
	}

	// fetch blocklists
	allowlist, blocklist, err := getBlocklists(tx)
	if err != nil {
		return fmt.Errorf("%w; failed to fetch blocklists", err)
	}

	// return early if there are no allowlist or blocklist entries
	if len(allowlist)+len(blocklist) == 0 {
		return nil
	}

	// update blocklist for every host
	for hk := range ann {
		if err := updateBlocklist(tx, hk, allowlist, blocklist); err != nil {
			return fmt.Errorf("%w; failed to update blocklist for host %v", err, hk)
		}
	}

	return nil
}

func markFailedContracts(tx *gorm.DB, height uint64) error {
	return tx.
		Model(&dbContract{}).
		Where("state = ? AND ? > window_end", contractStateActive, height).
		Update("state", contractStateFailed).
		Error
}

func updateChainIndex(tx *gorm.DB, newTip types.ChainIndex) error {
	return tx.Model(&dbConsensusInfo{}).Where(&dbConsensusInfo{
		Model: Model{
			ID: consensusInfoID,
		},
	}).Updates(map[string]interface{}{
		"height":   newTip.Height,
		"block_id": hash256(newTip.ID),
	}).Error
}

func updateContract(tx *gorm.DB, fcid types.FileContractID, update chain.ContractUpdate) error {
	var cs contractState
	if err := cs.LoadString(string(update.State)); err != nil {
		return err
	}

	updates := make(map[string]interface{})
	updates["state"] = cs

	if update.RevisionHeight != nil {
		updates["revision_height"] = *update.RevisionHeight
	}
	if update.RevisionNumber != nil {
		updates["revision_number"] = fmt.Sprint(*update.RevisionNumber)
	}
	if update.ProofHeight != nil {
		updates["proof_height"] = *update.ProofHeight
	}
	if update.Size != nil {
		updates["size"] = *update.Size
	}

	if err := tx.
		Model(&dbContract{}).
		Where("fcid = ?", fileContractID(fcid)).
		Updates(updates).
		Error; err != nil {
		return err
	}
	return tx.
		Model(&dbArchivedContract{}).
		Where("fcid = ?", fileContractID(fcid)).
		Updates(updates).
		Error
}

func updateWalletOutput(tx *gorm.DB, wou chain.WalletOutputUpdate) error {
	if wou.Addition {
		return tx.
			Clauses(clause.OnConflict{
				DoNothing: true,
				Columns:   []clause.Column{{Name: "output_id"}},
			}).Create(&dbWalletOutput{
			OutputID:       hash256(wou.Element.ID),
			LeafIndex:      wou.Element.StateElement.LeafIndex,
			MerkleProof:    merkleProof{proof: wou.Element.StateElement.MerkleProof},
			Value:          currency(wou.Element.SiacoinOutput.Value),
			Address:        hash256(wou.Element.SiacoinOutput.Address),
			MaturityHeight: wou.Element.MaturityHeight,
			Height:         wou.Element.Index.Height,
			BlockID:        hash256(wou.Element.Index.ID),
		}).Error
	}
	return tx.
		Where("output_id", hash256(wou.ID)).
		Delete(&dbWalletOutput{}).
		Error
}

func updateWalletEvent(tx *gorm.DB, weu chain.WalletEventUpdate) error {
	if weu.Addition {
		return tx.
			Clauses(clause.OnConflict{
				DoNothing: true,
				Columns:   []clause.Column{{Name: "event_id"}},
			}).Create(&dbWalletEvent{
			EventID:        hash256(weu.Event.ID),
			Inflow:         currency(weu.Event.Inflow),
			Outflow:        currency(weu.Event.Outflow),
			Transaction:    weu.Event.Transaction,
			MaturityHeight: weu.Event.MaturityHeight,
			Source:         string(weu.Event.Source),
			Timestamp:      weu.Event.Timestamp.Unix(),
			Height:         weu.Event.Index.Height,
			BlockID:        hash256(weu.Event.Index.ID),
		}).Error
	}
	return tx.
		Where("event_id", hash256(weu.Event.ID)).
		Delete(&dbWalletEvent{}).
		Error
}
