package stores

import (
	"bytes"
	"math"
	"time"

	"gitlab.com/NebulousLabs/encoding"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/siad/modules"
	"gorm.io/gorm"
)

type (
	dbSiacoinElement struct {
		Model
		Value          currency
		Address        hash256 `gorm:"size:32"`
		OutputID       hash256 `gorm:"unique;index;NOT NULL;size:32"`
		MaturityHeight uint64  `gorm:"index"`
	}

	dbTransaction struct {
		Model
		Raw           types.Transaction `gorm:"serializer:json"`
		Height        uint64
		BlockID       hash256 `gorm:"size:32"`
		TransactionID hash256 `gorm:"unique;index;NOT NULL;size:32"`
		Inflow        currency
		Outflow       currency
		Timestamp     int64 `gorm:"index:idx_transactions_timestamp"`
	}

	outputChange struct {
		addition bool
		oid      hash256
		sco      dbSiacoinElement
	}

	txnChange struct {
		addition bool
		txnID    hash256
		txn      dbTransaction
	}
)

// TableName implements the gorm.Tabler interface.
func (dbSiacoinElement) TableName() string { return "siacoin_elements" }

// TableName implements the gorm.Tabler interface.
func (dbTransaction) TableName() string { return "transactions" }

func (s *SQLStore) Height() uint64 {
	s.persistMu.Lock()
	height := s.chainIndex.Height
	s.persistMu.Unlock()
	return height
}

// Tip returns the consensus change ID and block height of the last wallet
// change.
func (s *SQLStore) Tip() (types.ChainIndex, error) {
	return s.cs.Tip(), nil
}

// UnspentSiacoinElements returns a list of all unspent siacoin outputs
func (s *SQLStore) UnspentSiacoinElements() ([]wallet.SiacoinElement, error) {
	var elems []dbSiacoinElement
	if err := s.db.Find(&elems).Error; err != nil {
		return nil, err
	}

	utxo := make([]wallet.SiacoinElement, len(elems))
	for i := range elems {
		utxo[i] = wallet.SiacoinElement{
			SiacoinElement: types.SiacoinElement{
				StateElement: types.StateElement{
					ID: types.Hash256(elems[i].OutputID),
					// TODO: LeafIndex missing
					// TODO: MerkleProof missing
				},
				SiacoinOutput: types.SiacoinOutput{
					Address: types.Address(elems[i].Address),
					Value:   types.Currency(elems[i].Value),
				},
				MaturityHeight: elems[i].MaturityHeight,
			},
			// TODO: Index missing
		}
	}
	return utxo, nil
}

// WalletEvents returns a paginated list of transactions ordered by maturity
// height, descending. If no more transactions are available, (nil, nil) should
// be returned.
func (s *SQLStore) WalletEvents(offset, limit int) ([]wallet.Event, error) {
	if limit == 0 || limit == -1 {
		limit = math.MaxInt64
	}

	var dbTxns []dbTransaction
	err := s.db.Raw("SELECT * FROM transactions ORDER BY timestamp DESC LIMIT ? OFFSET ?",
		limit, offset).Scan(&dbTxns).
		Error
	if err != nil {
		return nil, err
	}

	txns := make([]wallet.Event, len(dbTxns))
	for i := range dbTxns {
		txns[i] = wallet.Event{
			Transaction: dbTxns[i].Raw,
			Index: types.ChainIndex{
				Height: dbTxns[i].Height,
				ID:     types.BlockID(dbTxns[i].BlockID),
			},
			ID:        types.Hash256(dbTxns[i].TransactionID),
			Inflow:    types.Currency(dbTxns[i].Inflow),
			Outflow:   types.Currency(dbTxns[i].Outflow),
			Timestamp: time.Unix(dbTxns[i].Timestamp, 0),
		}
	}
	return txns, nil
}

// WalletEventCount returns the total number of transactions in the wallet.
func (s *SQLStore) WalletEventCount() (uint64, error) {
	var count int64
	if err := s.db.Model(&dbTransaction{}).Count(&count).Error; err != nil {
		return 0, err
	}
	return uint64(count), nil
}

// ProcessConsensusChange implements chain.Subscriber.
func (s *SQLStore) processConsensusChangeWallet(cc modules.ConsensusChange) {
	// Add/Remove siacoin outputs.
	for _, diff := range cc.SiacoinOutputDiffs {
		var sco types.SiacoinOutput
		convertToCore(diff.SiacoinOutput, (*types.V1SiacoinOutput)(&sco))
		if sco.Address != s.walletAddress {
			continue
		}
		if diff.Direction == modules.DiffApply {
			// add new outputs
			s.unappliedOutputChanges = append(s.unappliedOutputChanges, outputChange{
				addition: true,
				oid:      hash256(diff.ID),
				sco: dbSiacoinElement{
					Address:        hash256(sco.Address),
					Value:          currency(sco.Value),
					OutputID:       hash256(diff.ID),
					MaturityHeight: uint64(cc.BlockHeight), // immediately spendable
				},
			})
		} else {
			// remove reverted outputs
			s.unappliedOutputChanges = append(s.unappliedOutputChanges, outputChange{
				addition: false,
				oid:      hash256(diff.ID),
			})
		}
	}

	// Create a 'fake' transaction for every matured siacoin output.
	for _, diff := range cc.AppliedDiffs {
		for _, dsco := range diff.DelayedSiacoinOutputDiffs {
			// if a delayed output is reverted in an applied diff, the
			// output has matured -- add a payout transaction.
			if dsco.Direction != modules.DiffRevert {
				continue
			} else if types.Address(dsco.SiacoinOutput.UnlockHash) != s.walletAddress {
				continue
			}
			var sco types.SiacoinOutput
			convertToCore(dsco.SiacoinOutput, (*types.V1SiacoinOutput)(&sco))
			s.unappliedTxnChanges = append(s.unappliedTxnChanges, txnChange{
				addition: true,
				txnID:    hash256(dsco.ID), // use output id as txn id
				txn: dbTransaction{
					Height:        uint64(dsco.MaturityHeight),
					Inflow:        currency(sco.Value),                                                         // transaction inflow is value of matured output
					TransactionID: hash256(dsco.ID),                                                            // use output as txn id
					Timestamp:     int64(cc.AppliedBlocks[dsco.MaturityHeight-cc.InitialHeight()-1].Timestamp), // use timestamp of block that caused output to mature
				},
			})
		}
	}

	// Revert transactions from reverted blocks.
	for _, block := range cc.RevertedBlocks {
		for _, stxn := range block.Transactions {
			var txn types.Transaction
			convertToCore(stxn, &txn)
			if transactionIsRelevant(txn, s.walletAddress) {
				// remove reverted txns
				s.unappliedTxnChanges = append(s.unappliedTxnChanges, txnChange{
					addition: false,
					txnID:    hash256(txn.ID()),
				})
			}
		}
	}

	// Revert 'fake' transactions.
	for _, diff := range cc.RevertedDiffs {
		for _, dsco := range diff.DelayedSiacoinOutputDiffs {
			if dsco.Direction == modules.DiffApply {
				s.unappliedTxnChanges = append(s.unappliedTxnChanges, txnChange{
					addition: false,
					txnID:    hash256(dsco.ID),
				})
			}
		}
	}

	spentOutputs := make(map[types.SiacoinOutputID]types.SiacoinOutput)
	for i, block := range cc.AppliedBlocks {
		appliedDiff := cc.AppliedDiffs[i]
		for _, diff := range appliedDiff.SiacoinOutputDiffs {
			if diff.Direction == modules.DiffRevert {
				var so types.SiacoinOutput
				convertToCore(diff.SiacoinOutput, (*types.V1SiacoinOutput)(&so))
				spentOutputs[types.SiacoinOutputID(diff.ID)] = so
			}
		}

		for _, stxn := range block.Transactions {
			var txn types.Transaction
			convertToCore(stxn, &txn)
			if transactionIsRelevant(txn, s.walletAddress) {
				var inflow, outflow types.Currency
				for _, out := range txn.SiacoinOutputs {
					if out.Address == s.walletAddress {
						inflow = inflow.Add(out.Value)
					}
				}
				for _, in := range txn.SiacoinInputs {
					if in.UnlockConditions.UnlockHash() == s.walletAddress {
						so, ok := spentOutputs[in.ParentID]
						if !ok {
							panic("spent output not found")
						}
						outflow = outflow.Add(so.Value)
					}
				}

				// add confirmed txns
				s.unappliedTxnChanges = append(s.unappliedTxnChanges, txnChange{
					addition: true,
					txnID:    hash256(txn.ID()),
					txn: dbTransaction{
						Raw:           txn,
						Height:        uint64(cc.InitialHeight()) + uint64(i) + 1,
						BlockID:       hash256(block.ID()),
						Inflow:        currency(inflow),
						Outflow:       currency(outflow),
						TransactionID: hash256(txn.ID()),
						Timestamp:     int64(block.Timestamp),
					},
				})
			}
		}
	}
}

func transactionIsRelevant(txn types.Transaction, addr types.Address) bool {
	for i := range txn.SiacoinInputs {
		if txn.SiacoinInputs[i].UnlockConditions.UnlockHash() == addr {
			return true
		}
	}
	for i := range txn.SiacoinOutputs {
		if txn.SiacoinOutputs[i].Address == addr {
			return true
		}
	}
	for i := range txn.SiafundInputs {
		if txn.SiafundInputs[i].UnlockConditions.UnlockHash() == addr {
			return true
		}
		if txn.SiafundInputs[i].ClaimAddress == addr {
			return true
		}
	}
	for i := range txn.SiafundOutputs {
		if txn.SiafundOutputs[i].Address == addr {
			return true
		}
	}
	for i := range txn.FileContracts {
		for _, sco := range txn.FileContracts[i].ValidProofOutputs {
			if sco.Address == addr {
				return true
			}
		}
		for _, sco := range txn.FileContracts[i].MissedProofOutputs {
			if sco.Address == addr {
				return true
			}
		}
	}
	for i := range txn.FileContractRevisions {
		for _, sco := range txn.FileContractRevisions[i].ValidProofOutputs {
			if sco.Address == addr {
				return true
			}
		}
		for _, sco := range txn.FileContractRevisions[i].MissedProofOutputs {
			if sco.Address == addr {
				return true
			}
		}
	}
	return false
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

func applyUnappliedOutputAdditions(tx *gorm.DB, sco dbSiacoinElement) error {
	return tx.Create(&sco).Error
}

func applyUnappliedOutputRemovals(tx *gorm.DB, oid hash256) error {
	return tx.Where("output_id", oid).
		Delete(&dbSiacoinElement{}).
		Error
}

func applyUnappliedTxnAdditions(tx *gorm.DB, txn dbTransaction) error {
	return tx.Create(&txn).Error
}

func applyUnappliedTxnRemovals(tx *gorm.DB, txnID hash256) error {
	return tx.Where("transaction_id", txnID).
		Delete(&dbTransaction{}).
		Error
}
