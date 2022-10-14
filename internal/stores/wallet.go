package stores

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/renterd/wallet"
	"go.sia.tech/siad/modules"
	"go.sia.tech/siad/types"
)

// EphemeralWalletStore implements wallet.SingleAddressStore in memory.
type EphemeralWalletStore struct {
	tip     consensus.ChainIndex
	ccid    modules.ConsensusChangeID
	addr    types.UnlockHash
	scElems []wallet.SiacoinElement
	txns    []wallet.Transaction
	mu      sync.Mutex
}

// Balance implements wallet.SingleAddressStore.
func (s *EphemeralWalletStore) Balance() (sc types.Currency) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sce := range s.scElems {
		if sce.MaturityHeight < s.tip.Height {
			sc = sc.Add(sce.Value)
		}
	}
	return
}

// UnspentSiacoinElements implements wallet.SingleAddressStore.
func (s *EphemeralWalletStore) UnspentSiacoinElements() ([]wallet.SiacoinElement, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var elems []wallet.SiacoinElement
	for _, sce := range s.scElems {
		_ = sce // V2: sce.MerkleProof = append([]types.Hash256(nil), sce.MerkleProof...)
		elems = append(elems, sce)
	}
	return elems, nil
}

// Transactions implements wallet.SingleAddressStore.
func (s *EphemeralWalletStore) Transactions(since time.Time, max int) ([]wallet.Transaction, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var txns []wallet.Transaction
	for _, txn := range s.txns {
		if len(txns) == max {
			break
		} else if txn.Timestamp.After(since) {
			txns = append(txns, txn)
		}
	}
	return txns, nil
}

func transactionIsRelevant(txn types.Transaction, addr types.UnlockHash) bool {
	for i := range txn.SiacoinInputs {
		if txn.SiacoinInputs[i].UnlockConditions.UnlockHash() == addr {
			return true
		}
	}
	for i := range txn.SiacoinOutputs {
		if txn.SiacoinOutputs[i].UnlockHash == addr {
			return true
		}
	}
	for i := range txn.SiafundInputs {
		if txn.SiafundInputs[i].UnlockConditions.UnlockHash() == addr {
			return true
		}
		if txn.SiafundInputs[i].ClaimUnlockHash == addr {
			return true
		}
	}
	for i := range txn.SiafundOutputs {
		if txn.SiafundOutputs[i].UnlockHash == addr {
			return true
		}
	}
	for i := range txn.FileContracts {
		for _, sco := range txn.FileContracts[i].ValidProofOutputs {
			if sco.UnlockHash == addr {
				return true
			}
		}
		for _, sco := range txn.FileContracts[i].MissedProofOutputs {
			if sco.UnlockHash == addr {
				return true
			}
		}
	}
	for i := range txn.FileContractRevisions {
		for _, sco := range txn.FileContractRevisions[i].NewValidProofOutputs {
			if sco.UnlockHash == addr {
				return true
			}
		}
		for _, sco := range txn.FileContractRevisions[i].NewMissedProofOutputs {
			if sco.UnlockHash == addr {
				return true
			}
		}
	}
	return false
}

// ProcessConsensusChange implements modules.ConsensusSetSubscriber.
func (s *EphemeralWalletStore) ProcessConsensusChange(cc modules.ConsensusChange) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, diff := range cc.SiacoinOutputDiffs {
		if diff.SiacoinOutput.UnlockHash != s.addr {
			continue
		}
		if diff.Direction == modules.DiffApply {
			// add
			s.scElems = append(s.scElems, wallet.SiacoinElement{
				SiacoinOutput: diff.SiacoinOutput,
				ID:            types.OutputID(diff.ID),
			})
		} else {
			// remove
			for i := range s.scElems {
				if s.scElems[i].ID == types.OutputID(diff.ID) {
					s.scElems[i] = s.scElems[len(s.scElems)-1]
					s.scElems = s.scElems[:len(s.scElems)-1]
					break
				}
			}
		}
	}

	for _, block := range cc.RevertedBlocks {
		for _, txn := range block.Transactions {
			if transactionIsRelevant(txn, s.addr) {
				s.txns = s.txns[:len(s.txns)-1]
			}
		}
	}

	for _, block := range cc.AppliedBlocks {
		for _, txn := range block.Transactions {
			if transactionIsRelevant(txn, s.addr) {
				var inflow, outflow types.Currency
				for _, out := range txn.SiacoinOutputs {
					if out.UnlockHash == s.addr {
						inflow = inflow.Add(out.Value)
					}
				}
				for _, in := range txn.SiacoinInputs {
					if in.UnlockConditions.UnlockHash() == s.addr {
						inputValue := types.ZeroCurrency // V2: use in.Parent.value
						outflow = outflow.Add(inputValue)
					}
				}

				s.txns = append(s.txns, wallet.Transaction{
					Raw:       txn,
					Index:     s.tip,
					Inflow:    inflow,
					Outflow:   outflow,
					ID:        txn.ID(),
					Timestamp: time.Unix(int64(block.Timestamp), 0),
				})
			}
		}
	}

	s.tip.Height = uint64(cc.InitialHeight()) + uint64(len(cc.AppliedBlocks)) - uint64(len(cc.RevertedBlocks))
	s.tip.ID = consensus.BlockID(cc.AppliedBlocks[len(cc.AppliedBlocks)-1].ID())
	s.ccid = cc.ID
}

// NewEphemeralWalletStore returns a new EphemeralWalletStore.
func NewEphemeralWalletStore(addr types.UnlockHash) *EphemeralWalletStore {
	return &EphemeralWalletStore{
		addr: addr,
	}
}

// JSONWalletStore implements wallet.SingleAddressStore in memory, backed by a JSON file.
type JSONWalletStore struct {
	*EphemeralWalletStore
	dir      string
	lastSave time.Time
}

type jsonWalletPersistData struct {
	Tip             consensus.ChainIndex
	CCID            modules.ConsensusChangeID
	SiacoinElements []wallet.SiacoinElement
	Transactions    []wallet.Transaction
}

func (s *JSONWalletStore) save() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	js, _ := json.MarshalIndent(jsonWalletPersistData{
		Tip:             s.tip,
		CCID:            s.ccid,
		SiacoinElements: s.scElems,
		Transactions:    s.txns,
	}, "", "  ")

	// atomic save
	dst := filepath.Join(s.dir, "wallet.json")
	f, err := os.OpenFile(dst+"_tmp", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0660)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err = f.Write(js); err != nil {
		return err
	} else if f.Sync(); err != nil {
		return err
	} else if f.Close(); err != nil {
		return err
	} else if err := os.Rename(dst+"_tmp", dst); err != nil {
		return err
	}
	return nil
}

func (s *JSONWalletStore) load() (modules.ConsensusChangeID, error) {
	var p jsonWalletPersistData
	if js, err := os.ReadFile(filepath.Join(s.dir, "wallet.json")); os.IsNotExist(err) {
		// set defaults
		s.ccid = modules.ConsensusChangeBeginning
		return s.ccid, nil
	} else if err != nil {
		return modules.ConsensusChangeID{}, err
	} else if err := json.Unmarshal(js, &p); err != nil {
		return modules.ConsensusChangeID{}, err
	}
	s.tip = p.Tip
	s.ccid = p.CCID
	s.scElems = p.SiacoinElements
	s.txns = p.Transactions
	return s.ccid, nil
}

// ProcessConsensusChange implements chain.Subscriber.
func (s *JSONWalletStore) ProcessConsensusChange(cc modules.ConsensusChange) {
	s.EphemeralWalletStore.ProcessConsensusChange(cc)
	if time.Since(s.lastSave) > 2*time.Minute {
		if err := s.save(); err != nil {
			log.Fatalln("Couldn't save wallet state:", err)
		}
		s.lastSave = time.Now()
	}
}

// NewJSONWalletStore returns a new JSONWalletStore.
func NewJSONWalletStore(dir string, addr types.UnlockHash) (*JSONWalletStore, modules.ConsensusChangeID, error) {
	s := &JSONWalletStore{
		EphemeralWalletStore: NewEphemeralWalletStore(addr),
		dir:                  dir,
		lastSave:             time.Now(),
	}
	ccid, err := s.load()
	if err != nil {
		return nil, modules.ConsensusChangeID{}, err
	}
	return s, ccid, nil
}
