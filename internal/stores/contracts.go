package stores

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"go.sia.tech/siad/crypto"

	"go.sia.tech/renterd/internal/consensus"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"go.sia.tech/renterd/slab"
	"go.sia.tech/siad/types"
	"gorm.io/gorm"
)

// ErrContractNotFound is returned when a contract can't be retrieved from the
// database.
var ErrContractNotFound = errors.New("couldn't find contract with specified id")

// EphemeralContractStore implements api.ContractStore and api.HostSetStore in memory.
type EphemeralContractStore struct {
	mu        sync.Mutex
	contracts map[types.FileContractID]rhpv2.Contract
	hostSets  map[string][]consensus.PublicKey
}

// Contracts implements api.ContractStore.
func (s *EphemeralContractStore) Contracts() ([]rhpv2.Contract, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var cs []rhpv2.Contract
	for _, c := range s.contracts {
		cs = append(cs, c)
	}
	return cs, nil
}

// Contract implements api.ContractStore.
func (s *EphemeralContractStore) Contract(id types.FileContractID) (rhpv2.Contract, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.contracts[id]
	if !ok {
		return rhpv2.Contract{}, ErrContractNotFound
	}
	return c, nil
}

// AddContract implements api.ContractStore.
func (s *EphemeralContractStore) AddContract(c rhpv2.Contract) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.contracts[c.ID()] = c
	return nil
}

// RemoveContract implements api.ContractStore.
func (s *EphemeralContractStore) RemoveContract(id types.FileContractID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.contracts, id)
	return nil
}

// HostSets implements api.HostSetStore.
func (s *EphemeralContractStore) HostSets() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	sets := make([]string, 0, len(s.hostSets))
	for set := range s.hostSets {
		sets = append(sets, set)
	}
	return sets
}

// HostSet implements api.HostSetStore.
func (s *EphemeralContractStore) HostSet(name string) []consensus.PublicKey {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hostSets[name]
}

// SetHostSet implements api.HostSetStore.
func (s *EphemeralContractStore) SetHostSet(name string, hosts []consensus.PublicKey) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(hosts) == 0 {
		delete(s.hostSets, name)
	} else {
		s.hostSets[name] = append([]consensus.PublicKey(nil), hosts...)
	}
	return nil
}

// NewEphemeralContractStore returns a new EphemeralContractStore.
func NewEphemeralContractStore() *EphemeralContractStore {
	return &EphemeralContractStore{
		contracts: make(map[types.FileContractID]rhpv2.Contract),
	}
}

// JSONContractStore implements api.ContractStore in memory, backed by a JSON file.
type JSONContractStore struct {
	*EphemeralContractStore
	dir string
}

type jsonContractsPersistData struct {
	Contracts []rhpv2.Contract
	HostSets  map[string][]consensus.PublicKey
}

func (s *JSONContractStore) save() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var p jsonContractsPersistData
	for _, c := range s.contracts {
		p.Contracts = append(p.Contracts, c)
	}
	p.HostSets = s.hostSets
	js, _ := json.MarshalIndent(p, "", "  ")

	// atomic save
	dst := filepath.Join(s.dir, "contracts.json")
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

func (s *JSONContractStore) load() error {
	var p jsonContractsPersistData
	if js, err := os.ReadFile(filepath.Join(s.dir, "contracts.json")); os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	} else if err := json.Unmarshal(js, &p); err != nil {
		return err
	}
	for _, c := range p.Contracts {
		s.contracts[c.ID()] = c
	}
	s.hostSets = p.HostSets
	return nil
}

// AddContract implements api.ContractStore.
func (s *JSONContractStore) AddContract(c rhpv2.Contract) error {
	s.EphemeralContractStore.AddContract(c)
	return s.save()
}

// RemoveContract implements api.ContractStore.
func (s *JSONContractStore) RemoveContract(id types.FileContractID) error {
	s.EphemeralContractStore.RemoveContract(id)
	return s.save()
}

// SetHostSet implements api.HostSetStore.
func (s *JSONContractStore) SetHostSet(name string, hosts []consensus.PublicKey) error {
	s.EphemeralContractStore.SetHostSet(name, hosts)
	return s.save()
}

// NewJSONContractStore returns a new JSONContractStore.
func NewJSONContractStore(dir string) (*JSONContractStore, error) {
	s := &JSONContractStore{
		EphemeralContractStore: NewEphemeralContractStore(),
		dir:                    dir,
	}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

type (
	// SQLContractStore implements the bus.ContractStore interface using SQL as the
	// persistence backend.
	SQLContractStore struct {
		db *gorm.DB
	}

	dbContractRHPv2 struct {
		ID         types.FileContractID     `gorm:"primaryKey,type:bytes;serializer:gob"`
		Revision   dbFileContractRevision   `gorm:"constraint:OnDelete:CASCADE;foreignKey:ParentID;references:ID"` //CASCADE to delete revision too
		Signatures []dbTransactionSignature `gorm:"constraint:OnDelete:CASCADE;foreignKey:ParentID;references:ID"` // CASCADE to delete signatures too
	}

	dbFileContractRevision struct {
		ParentID              types.FileContractID `gorm:"primaryKey"` // only one revision for a given parent
		UnlockConditions      dbUnlockConditions   `gorm:"constraint:OnDelete:CASCADE;foreignKey:ParentID;references:ParentID"`
		NewRevisionNumber     uint64               `gorm:"index"`
		NewFileSize           uint64
		NewFileMerkleRoot     crypto.Hash       `gorm:"type:bytes;serializer:gob"`
		NewWindowStart        types.BlockHeight `gorm:"index"`
		NewWindowEnd          types.BlockHeight `gorm:"index"`
		NewValidProofOutputs  []dbSiacoinOutput `gorm:"constraint:OnDelete:CASCADE;foreignKey:ParentID;References:ParentID"` // CASCADE to delete output
		NewMissedProofOutputs []dbSiacoinOutput `gorm:"constraint:OnDelete:CASCADE;foreignKey:ParentID;References:ParentID"` // CASCADE to delete output
		NewUnlockHash         types.UnlockHash  `gorm:"index,type:bytes;serializer:gob"`
	}

	dbTransactionSignature struct {
		ID             uint64      `gorm:"primaryKey"`
		ParentID       crypto.Hash `gorm:"index;NOT NULL;type:bytes;serializer:gob"`
		PublicKeyIndex uint64
		Timelock       types.BlockHeight
		CoveredFields  types.CoveredFields `gorm:"type:bytes;serializer:gob"`
		Signature      []byte
	}

	dbUnlockConditions struct {
		ParentID           types.FileContractID `gorm:"primaryKey;type:bytes;serializer:gob"` // only one set of UnlockConditions for a given parent
		Timelock           types.BlockHeight
		PublicKeys         []dbSiaPublicKey `gorm:"constraint:OnDelete:CASCADE;foreignKey:UnlockConditionID;references:ParentID"` // CASCADE to delete pubkeys
		SignaturesRequired uint64
	}

	dbSiaPublicKey struct {
		ID                uint64          `gorm:"primaryKey"`
		Algorithm         types.Specifier `gorm:"type:bytes;serializer:gob"`
		Key               []byte
		UnlockConditionID types.FileContractID `gorm:"index;NOT NULL;type:bytes;serializer:gob"`
	}

	dbSiacoinOutput struct {
		ID         uint64               `gorm:"primaryKey"`
		ParentID   types.FileContractID `gorm:"index;NOT NULL;type:bytes;serializer:gob"`
		UnlockHash types.UnlockHash     `gorm:"index;type:bytes;serializer:gob"`
		Value      types.Currency       `gorm:"type:bytes;serializer:gob"`
	}
)

// TableName implements the gorm.Tabler interface.
func (dbContractRHPv2) TableName() string { return "contracts_v2" }

// TableName implements the gorm.Tabler interface.
func (dbFileContractRevision) TableName() string { return "file_contract_revisions" }

// TableName implements the gorm.Tabler interface.
func (dbTransactionSignature) TableName() string { return "transaction_signatures" }

// TableName implements the gorm.Tabler interface.
func (dbUnlockConditions) TableName() string { return "unlock_conditions" }

// TableName implements the gorm.Tabler interface.
func (dbSiaPublicKey) TableName() string { return "public_keys" }

// TableName implements the gorm.Tabler interface.
func (dbSiacoinOutput) TableName() string { return "siacoin_outputs" }

// Contract converts a dbContractRHPv2 to a rhpv2.Contract type.
func (c dbContractRHPv2) Contract() (rhpv2.Contract, error) {
	// Prepare valid and missed outputs.
	newValidOutputs := make([]types.SiacoinOutput, 0, len(c.Revision.NewValidProofOutputs))
	for _, sco := range c.Revision.NewValidProofOutputs {
		newValidOutputs = append(newValidOutputs, types.SiacoinOutput{
			Value:      sco.Value,
			UnlockHash: sco.UnlockHash,
		})
	}
	newMissedOutputs := make([]types.SiacoinOutput, 0, len(c.Revision.NewMissedProofOutputs))
	for _, sco := range c.Revision.NewMissedProofOutputs {
		newMissedOutputs = append(newMissedOutputs, types.SiacoinOutput{
			Value:      sco.Value,
			UnlockHash: sco.UnlockHash,
		})
	}

	// Prepare pubkeys.
	publickeys := make([]types.SiaPublicKey, 0, len(c.Revision.UnlockConditions.PublicKeys))
	for _, pk := range c.Revision.UnlockConditions.PublicKeys {
		publickeys = append(publickeys, types.SiaPublicKey{
			Algorithm: pk.Algorithm,
			Key:       pk.Key,
		})
	}

	// Prepare revision.
	revision := types.FileContractRevision{
		ParentID: c.Revision.ParentID,
		UnlockConditions: types.UnlockConditions{
			Timelock:           c.Revision.UnlockConditions.Timelock,
			PublicKeys:         publickeys,
			SignaturesRequired: c.Revision.UnlockConditions.SignaturesRequired,
		},
		NewRevisionNumber:     c.Revision.NewRevisionNumber,
		NewFileSize:           c.Revision.NewFileSize,
		NewFileMerkleRoot:     c.Revision.NewFileMerkleRoot,
		NewWindowStart:        c.Revision.NewWindowStart,
		NewWindowEnd:          c.Revision.NewWindowEnd,
		NewValidProofOutputs:  newValidOutputs,
		NewMissedProofOutputs: newMissedOutputs,
		NewUnlockHash:         c.Revision.NewUnlockHash,
	}

	// Prepare signatures.
	var signatures [2]types.TransactionSignature
	if len(c.Signatures) != len(signatures) {
		return rhpv2.Contract{}, fmt.Errorf("contract in db got %v signatures but expected %v", len(c.Signatures), len(signatures))
	}
	for i, sig := range c.Signatures {
		signatures[i] = types.TransactionSignature{
			ParentID:       sig.ParentID,
			PublicKeyIndex: sig.PublicKeyIndex,
			Timelock:       sig.Timelock,
			CoveredFields:  sig.CoveredFields,
			Signature:      sig.Signature,
		}
	}

	return rhpv2.Contract{
		Revision:   revision,
		Signatures: signatures,
	}, nil
}

// NewSQLContractStore creates a new SQLContractStore from a given gorm
// Dialector.
func NewSQLContractStore(conn gorm.Dialector, migrate bool) (*SQLContractStore, error) {
	db, err := gorm.Open(conn, &gorm.Config{})
	if err != nil {
		return nil, err
	}

	if migrate {
		// Create the tables.
		tables := []interface{}{
			&dbContractRHPv2{},
			&dbFileContractRevision{},
			&dbTransactionSignature{},
			&dbUnlockConditions{},
			&dbSiaPublicKey{},
			&dbSiacoinOutput{},
		}
		if err := db.AutoMigrate(tables...); err != nil {
			return nil, err
		}
		if res := db.Exec("PRAGMA foreign_keys = ON", nil); res.Error != nil {
			return nil, res.Error
		}
	}

	return &SQLContractStore{
		db: db,
	}, nil
}

// AddContract implements the bus.ContractStore interface.
func (s *SQLContractStore) AddContract(c rhpv2.Contract) error {
	return s.db.Transaction(func(tx *gorm.DB) error {
		fcid := c.ID()

		// Prepare valid and missed outputs.
		newValidOutputs := make([]dbSiacoinOutput, 0, len(c.Revision.NewValidProofOutputs))
		for _, sco := range c.Revision.NewValidProofOutputs {
			newValidOutputs = append(newValidOutputs, dbSiacoinOutput{
				ParentID:   fcid,
				UnlockHash: sco.UnlockHash,
				Value:      sco.Value,
			})
		}
		newMissedOutputs := make([]dbSiacoinOutput, 0, len(c.Revision.NewMissedProofOutputs))
		for _, sco := range c.Revision.NewMissedProofOutputs {
			newMissedOutputs = append(newMissedOutputs, dbSiacoinOutput{
				ParentID:   fcid,
				UnlockHash: sco.UnlockHash,
				Value:      sco.Value,
			})
		}

		// Prepare pubkeys for unlock conditions.
		pubkeys := make([]dbSiaPublicKey, 0, len(c.Revision.UnlockConditions.PublicKeys))
		for _, pk := range c.Revision.UnlockConditions.PublicKeys {
			pubkeys = append(pubkeys, dbSiaPublicKey{
				Algorithm:         pk.Algorithm,
				Key:               pk.Key,
				UnlockConditionID: fcid,
			})
		}

		// Prepare unlock conditions.
		unlockConditions := dbUnlockConditions{
			ParentID:           fcid,
			Timelock:           c.Revision.UnlockConditions.Timelock,
			PublicKeys:         pubkeys,
			SignaturesRequired: c.Revision.UnlockConditions.SignaturesRequired,
		}

		// Prepare contract revision.
		revision := dbFileContractRevision{
			ParentID:              fcid,
			UnlockConditions:      unlockConditions,
			NewRevisionNumber:     c.Revision.NewRevisionNumber,
			NewFileSize:           c.Revision.NewFileSize,
			NewFileMerkleRoot:     c.Revision.NewFileMerkleRoot,
			NewWindowStart:        c.Revision.NewWindowEnd,
			NewWindowEnd:          c.Revision.NewWindowEnd,
			NewValidProofOutputs:  newValidOutputs,
			NewMissedProofOutputs: newMissedOutputs,
			NewUnlockHash:         c.Revision.NewUnlockHash,
		}

		// Prepare signatures. The covered fields are stored as a blob
		// to avoid more tables which are probably not going to be
		// useful anyway.
		signatures := make([]dbTransactionSignature, 0, len(c.Signatures))
		for _, sig := range c.Signatures {
			signatures = append(signatures, dbTransactionSignature{
				ParentID:       crypto.Hash(fcid),
				PublicKeyIndex: sig.PublicKeyIndex,
				Timelock:       sig.Timelock,
				CoveredFields:  sig.CoveredFields,
				Signature:      sig.Signature,
			})
		}

		// Insert contract.
		return tx.Create(&dbContractRHPv2{
			ID:         fcid,
			Revision:   revision,
			Signatures: signatures,
		}).Error
	})
}

// Contract implements the bus.ContractStore interface.
func (s *SQLContractStore) Contract(id types.FileContractID) (rhpv2.Contract, error) {
	// Fetch contract.
	contract, err := s.contract(id)
	if err != nil {
		return rhpv2.Contract{}, err
	}
	return contract.Contract()
}

// Contracts implements the bus.ContractStore interface.
func (s *SQLContractStore) Contracts() ([]rhpv2.Contract, error) {
	panic("not implemented")
}

// RemoveContract implements the bus.ContractStore interface.
func (s *SQLContractStore) RemoveContract(id types.FileContractID) error {
	panic("not implemented")
}

// ContractsForDownload implements the worker.ContractStore interface.
func (s *SQLContractStore) ContractsForDownload(slab slab.Slab) ([]rhpv2.Contract, error) {
	panic("not implemented")
}

// ContractsForUpload implements the worker.ContractStore interface.
func (s *SQLContractStore) ContractsForUpload() ([]rhpv2.Contract, error) {
	panic("not implemented")
}

func (s *SQLContractStore) contract(id types.FileContractID) (dbContractRHPv2, error) {
	var contract dbContractRHPv2
	err := s.db.Where(&dbContractRHPv2{ID: id}).
		Take(&contract).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return contract, ErrContractNotFound
	}
	return contract, err
}
