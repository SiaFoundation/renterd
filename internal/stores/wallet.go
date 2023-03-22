package stores

import (
	"bytes"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"gitlab.com/NebulousLabs/encoding"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/wallet"
	"go.sia.tech/siad/modules"
)

// EphemeralWalletStore implements wallet.SingleAddressStore in memory.
type EphemeralWalletStore struct {
	tip     types.ChainIndex
	ccid    modules.ConsensusChangeID
	addr    types.Address
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

// ProcessConsensusChange implements modules.ConsensusSetSubscriber.
func (s *EphemeralWalletStore) ProcessConsensusChange(cc modules.ConsensusChange) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, diff := range cc.SiacoinOutputDiffs {
		var sco types.SiacoinOutput
		convertToCore(diff.SiacoinOutput, &sco)
		if sco.Address != s.addr {
			continue
		}
		if diff.Direction == modules.DiffApply {
			// add
			s.scElems = append(s.scElems, wallet.SiacoinElement{
				SiacoinOutput: sco,
				ID:            types.Hash256(diff.ID),
			})
		} else {
			// remove
			for i := range s.scElems {
				if s.scElems[i].ID == types.Hash256(diff.ID) {
					s.scElems[i] = s.scElems[len(s.scElems)-1]
					s.scElems = s.scElems[:len(s.scElems)-1]
					break
				}
			}
		}
	}

	for _, block := range cc.RevertedBlocks {
		for _, stxn := range block.Transactions {
			var txn types.Transaction
			convertToCore(stxn, &txn)
			if transactionIsRelevant(txn, s.addr) {
				s.txns = s.txns[:len(s.txns)-1]
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
			if transactionIsRelevant(txn, s.addr) {
				var inflow, outflow types.Currency
				for _, out := range txn.SiacoinOutputs {
					if out.Address == s.addr {
						inflow = inflow.Add(out.Value)
					}
				}
				for _, in := range txn.SiacoinInputs {
					if in.UnlockConditions.UnlockHash() == s.addr {
						so, ok := spentOutputs[in.ParentID]
						if !ok {
							panic("spent output not found")
						}
						outflow = outflow.Add(so.Value)
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
	s.tip.ID = types.BlockID(cc.AppliedBlocks[len(cc.AppliedBlocks)-1].ID())
	s.ccid = cc.ID
}

// NewEphemeralWalletStore returns a new EphemeralWalletStore.
func NewEphemeralWalletStore(addr types.Address) *EphemeralWalletStore {
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
	Tip             types.ChainIndex
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
	if _, err := f.Write(js); err != nil {
		return err
	} else if err := f.Sync(); err != nil {
		return err
	} else if err := f.Close(); err != nil {
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
func NewJSONWalletStore(dir string, addr types.Address) (*JSONWalletStore, modules.ConsensusChangeID, error) {
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
