package stores

import (
	"bytes"
	"time"

	"gitlab.com/NebulousLabs/encoding"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/wallet"
	"go.sia.tech/siad/modules"
)

type (
	dbSiacoinElement struct {
		Model
		Value          currency
		Address        hash256 `gorm:"size:32"`
		OutputID       hash256 `gorm:"unique;index;size:32"`
		MaturityHeight uint64  `gorm:"index"`
	}

	dbTransaction struct {
		Model
		Raw           types.Transaction `gorm:"serializer:json"`
		Height        uint64
		BlockID       hash256 `gorm:"size:32"`
		TransactionID hash256 `gorm:"unique;index;size:32"`
		Inflow        currency
		Outflow       currency
		Timestamp     int64
	}
)

// TableName implements the gorm.Tabler interface.
func (dbSiacoinElement) TableName() string { return "siacoin_elements" }

// TableName implements the gorm.Tabler interface.
func (dbTransaction) TableName() string { return "transactions" }

// Balance implements wallet.SingleAddressStore.
func (s *SQLStore) Balance() (types.Currency, error) {
	var elems []dbSiacoinElement
	if err := s.db.Find(&elems).Where("maturity_height < ?", s.chainIndex.Height).Error; err != nil {
		return types.ZeroCurrency, err
	}
	var balance types.Currency
	for _, sce := range elems {
		balance = balance.Add(types.Currency(sce.Value))
	}
	return balance, nil
}

// UnspentSiacoinElements implements wallet.SingleAddressStore.
func (s *SQLStore) UnspentSiacoinElements() ([]wallet.SiacoinElement, error) {
	var elems []dbSiacoinElement
	if err := s.db.Find(&elems).Error; err != nil {
		return nil, err
	}
	utxo := make([]wallet.SiacoinElement, len(elems))
	for i := range elems {
		utxo[i] = wallet.SiacoinElement{
			ID:             types.Hash256(elems[i].OutputID),
			MaturityHeight: elems[i].MaturityHeight,
			SiacoinOutput: types.SiacoinOutput{
				Address: types.Address(elems[i].Address),
				Value:   types.Currency(elems[i].Value),
			},
		}
	}
	return utxo, nil
}

// Transactions implements wallet.SingleAddressStore.
func (s *SQLStore) Transactions(since time.Time, max int) ([]wallet.Transaction, error) {
	var dbTxns []dbTransaction
	err := s.db.Find(&dbTxns).
		Where("timestamp > ?", since.UnixNano()).
		Limit(max).
		Error
	if err != nil {
		return nil, err
	}

	txns := make([]wallet.Transaction, len(dbTxns))
	for i := range dbTxns {
		txns[i] = wallet.Transaction{
			Raw: dbTxns[i].Raw,
			Index: types.ChainIndex{
				Height: dbTxns[i].Height,
				ID:     types.BlockID(dbTxns[i].BlockID),
			},
			ID:        types.TransactionID(dbTxns[i].TransactionID),
			Inflow:    types.Currency(dbTxns[i].Inflow),
			Outflow:   types.Currency(dbTxns[i].Outflow),
			Timestamp: time.Unix(dbTxns[i].Timestamp, 0),
		}
	}
	return txns, nil
}

// ProcessConsensusChange implements chain.Subscriber.
func (s *SQLStore) processConsensusChangeWallet(cc modules.ConsensusChange) {
	for _, diff := range cc.SiacoinOutputDiffs {
		var sco types.SiacoinOutput
		convertToCore(diff.SiacoinOutput, &sco)
		if sco.Address != s.walletAddress {
			continue
		}
		if diff.Direction == modules.DiffApply {
			// add new outputs
			s.unappliedOutputAdditions = append(s.unappliedOutputAdditions, dbSiacoinElement{
				Address:        hash256(sco.Address),
				Value:          currency(sco.Value),
				OutputID:       hash256(diff.ID),
				MaturityHeight: uint64(cc.BlockHeight), // immediately spendable
			})
		} else {
			// remove reverted outputs
			s.unappliedOutputRemovals = append(s.unappliedOutputRemovals, hash256(diff.ID))
			for i := range s.unappliedOutputAdditions {
				if s.unappliedOutputAdditions[i].OutputID == hash256(diff.ID) {
					s.unappliedOutputAdditions[i] = s.unappliedOutputAdditions[len(s.unappliedOutputAdditions)-1]
					s.unappliedOutputAdditions = s.unappliedOutputAdditions[:len(s.unappliedOutputAdditions)-1]
					break
				}
			}
		}
	}

	for _, diff := range cc.DelayedSiacoinOutputDiffs {
		var sco types.SiacoinOutput
		convertToCore(diff.SiacoinOutput, &sco)
		if sco.Address != s.walletAddress {
			continue
		}
		if diff.Direction == modules.DiffApply {
			// add new outputs
			s.unappliedOutputAdditions = append(s.unappliedOutputAdditions, dbSiacoinElement{
				Address:        hash256(sco.Address),
				Value:          currency(sco.Value),
				OutputID:       hash256(diff.ID),
				MaturityHeight: uint64(diff.MaturityHeight),
			})
		} else {
			// remove reverted outputs
			s.unappliedOutputRemovals = append(s.unappliedOutputRemovals, hash256(diff.ID))
			for i := range s.unappliedOutputAdditions {
				if s.unappliedOutputAdditions[i].OutputID == hash256(diff.ID) {
					s.unappliedOutputAdditions[i] = s.unappliedOutputAdditions[len(s.unappliedOutputAdditions)-1]
					s.unappliedOutputAdditions = s.unappliedOutputAdditions[:len(s.unappliedOutputAdditions)-1]
					break
				}
			}
		}
	}

	for _, block := range cc.RevertedBlocks {
		for _, stxn := range block.Transactions {
			var txn types.Transaction
			convertToCore(stxn, &txn)
			if transactionIsRelevant(txn, s.walletAddress) {
				// remove reverted txns
				s.unappliedTxnRemovals = append(s.unappliedTxnRemovals, hash256(txn.ID()))
				for i := range s.unappliedTxnAdditions {
					if s.unappliedTxnAdditions[i].TransactionID == hash256(txn.ID()) {
						s.unappliedTxnAdditions[i] = s.unappliedTxnAdditions[len(s.unappliedTxnAdditions)-1]
						s.unappliedTxnAdditions = s.unappliedTxnAdditions[:len(s.unappliedTxnAdditions)-1]
						break
					}
				}
			}
		}
	}

	spentOutputs := make(map[types.SiacoinOutputID]types.SiacoinOutput)
	for i, block := range cc.AppliedBlocks {
		appliedDiff := cc.AppliedDiffs[i]
		for _, diff := range appliedDiff.SiacoinOutputDiffs {
			if diff.Direction == modules.DiffRevert {
				var so types.SiacoinOutput
				convertToCore(diff.SiacoinOutput, &so)
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
				s.unappliedTxnAdditions = append(s.unappliedTxnAdditions, dbTransaction{
					Raw:           txn,
					Height:        uint64(cc.InitialHeight()) + uint64(i) + 1,
					BlockID:       hash256(block.ID()),
					Inflow:        currency(inflow),
					Outflow:       currency(outflow),
					TransactionID: hash256(txn.ID()),
					Timestamp:     int64(block.Timestamp),
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
