package stores

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"sync"

	"go.sia.tech/siad/crypto"

	"go.sia.tech/renterd/internal/consensus"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"go.sia.tech/siad/types"
	"gorm.io/gorm"
)

var (
	// ErrContractNotFound is returned when a contract can't be retrieved from the
	// database.
	ErrContractNotFound = errors.New("couldn't find contract")

	// ErrHostSetNotFound is returned when a contract can't be retrieved from the
	// database.
	ErrHostSetNotFound = errors.New("couldn't find host set")
)

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
	dbContractRHPv2 struct {
		dbCommon

		GoodForUpload bool                   `gorm:"index"`
		ID            types.FileContractID   `gorm:"primaryKey,type:bytes;serializer:gob;NOT NULL"`
		Revision      dbFileContractRevision `gorm:"constraint:OnDelete:CASCADE;foreignKey:ParentID;references:ID;NOT NULL"` //CASCADE to delete revision too
		Sectors       []dbSector             `gorm:"foreignKey:Contract;references:ID;constraint:OnDelete:SET NULL"`         // Set Contract field in sector null on delete
	}

	dbFileContractRevision struct {
		dbCommon
		Signatures []types.TransactionSignature `gorm:"type:bytes;serializer:gob;NOT NULL"`

		ParentID              types.FileContractID   `gorm:"primaryKey;type:bytes;serializer:gob;NOT NULL"` // only one revision for a given parent
		UnlockConditions      types.UnlockConditions `gorm:"NOT NULL;type:bytes;serializer:gob"`
		NewRevisionNumber     uint64                 `gorm:"index"`
		NewFileSize           uint64
		NewFileMerkleRoot     crypto.Hash             `gorm:"type:bytes;serializer:gob"`
		NewWindowStart        types.BlockHeight       `gorm:"index"`
		NewWindowEnd          types.BlockHeight       `gorm:"index"`
		NewValidProofOutputs  []dbValidSiacoinOutput  `gorm:"constraint:OnDelete:CASCADE;foreignKey:ParentID;References:ParentID;NOT NULL"` // CASCADE to delete output
		NewMissedProofOutputs []dbMissedSiacoinOutput `gorm:"constraint:OnDelete:CASCADE;foreignKey:ParentID;References:ParentID;NOT NULL"` // CASCADE to delete output
		NewUnlockHash         types.UnlockHash        `gorm:"index,type:bytes;serializer:gob"`
	}

	dbTransactionSignature struct {
		dbCommon

		ID             uint64               `gorm:"primaryKey"`
		ParentID       types.FileContractID `gorm:"index;type:bytes;serializer:gob;NOT NULL"`
		PublicKeyIndex uint64
		Timelock       types.BlockHeight
		CoveredFields  types.CoveredFields `gorm:"type:bytes;serializer:gob"`
		Signature      []byte
	}

	dbValidSiacoinOutput struct {
		dbCommon

		ID         uint64               `gorm:"primaryKey"`
		ParentID   types.FileContractID `gorm:"index;type:bytes;serializer:gob;NOT NULL"`
		UnlockHash types.UnlockHash     `gorm:"index;type:bytes;serializer:gob"`
		Value      *big.Int             `gorm:"type:bytes;serializer:gob"`
	}

	dbMissedSiacoinOutput struct {
		dbCommon

		ID         uint64               `gorm:"primaryKey"`
		ParentID   types.FileContractID `gorm:"index;type:bytes;serializer:gob;NOT NULL"`
		UnlockHash types.UnlockHash     `gorm:"index;type:bytes;serializer:gob"`
		Value      *big.Int             `gorm:"type:bytes;serializer:gob"`
	}

	dbHostSet struct {
		dbCommon

		Name  string           `gorm:"primaryKey"`
		Hosts []dbHostSetEntry `gorm:"constraing:OnDelete:CASCADE;foreignKey:HostSetName;references:Name"`
	}

	dbHostSetEntry struct {
		dbCommon

		ID          uint64              `gorm:"primaryKey"`
		HostSetName string              `gorm:"index;NOT NULL"`
		PublicKey   consensus.PublicKey `gorm:"NOT NULL;type:bytes;serializer:gob"`
	}
)

// TableName implements the gorm.Tabler interface.
func (dbContractRHPv2) TableName() string { return "contracts_v2" }

// TableName implements the gorm.Tabler interface.
func (dbFileContractRevision) TableName() string { return "file_contract_revisions" }

// TableName implements the gorm.Tabler interface.
func (dbTransactionSignature) TableName() string { return "transaction_signatures" }

// TableName implements the gorm.Tabler interface.
func (dbValidSiacoinOutput) TableName() string { return "siacoin_valid_outputs" }

// TableName implements the gorm.Tabler interface.
func (dbMissedSiacoinOutput) TableName() string { return "siacoin_missed_outputs" }

// TableName implements the gorm.Tabler interface.
func (dbHostSet) TableName() string { return "host_sets" }

// TableName implements the gorm.Tabler interface.
func (dbHostSetEntry) TableName() string { return "host_set_entries" }

// convert converts a dbContractRHPv2 to a rhpv2.Contract type.
func (c dbContractRHPv2) convert() (rhpv2.Contract, error) {
	// Prepare valid and missed outputs.
	newValidOutputs := make([]types.SiacoinOutput, len(c.Revision.NewValidProofOutputs))
	for i, sco := range c.Revision.NewValidProofOutputs {
		newValidOutputs[i] = types.SiacoinOutput{
			Value:      types.NewCurrency(sco.Value),
			UnlockHash: sco.UnlockHash,
		}
	}
	newMissedOutputs := make([]types.SiacoinOutput, len(c.Revision.NewMissedProofOutputs))
	for i, sco := range c.Revision.NewMissedProofOutputs {
		newMissedOutputs[i] = types.SiacoinOutput{
			Value:      types.NewCurrency(sco.Value),
			UnlockHash: sco.UnlockHash,
		}
	}

	// Prepare pubkeys.
	publickeys := make([]types.SiaPublicKey, len(c.Revision.UnlockConditions.PublicKeys))
	for i, pk := range c.Revision.UnlockConditions.PublicKeys {
		publickeys[i] = types.SiaPublicKey{
			Algorithm: pk.Algorithm,
			Key:       pk.Key,
		}
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
	if len(c.Revision.Signatures) != len(signatures) {
		return rhpv2.Contract{}, fmt.Errorf("contract in db got %v signatures but expected %v", len(c.Revision.Signatures), len(signatures))
	}
	for i, sig := range c.Revision.Signatures {
		signatures[i] = types.TransactionSignature{
			ParentID:       crypto.Hash(sig.ParentID),
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

// AddContract implements the bus.ContractStore interface.
func (s *SQLStore) AddContract(c rhpv2.Contract) error {
	fcid := c.ID()

	// Prepare valid and missed outputs.
	newValidOutputs := make([]dbValidSiacoinOutput, len(c.Revision.NewValidProofOutputs))
	for i, sco := range c.Revision.NewValidProofOutputs {
		newValidOutputs[i] = dbValidSiacoinOutput{
			ParentID:   fcid,
			UnlockHash: sco.UnlockHash,
			Value:      sco.Value.Big(),
		}
	}
	newMissedOutputs := make([]dbMissedSiacoinOutput, len(c.Revision.NewMissedProofOutputs))
	for i, sco := range c.Revision.NewMissedProofOutputs {
		newMissedOutputs[i] = dbMissedSiacoinOutput{
			ParentID:   fcid,
			UnlockHash: sco.UnlockHash,
			Value:      sco.Value.Big(),
		}
	}

	// Prepare contract revision.
	revision := dbFileContractRevision{
		ParentID:              fcid,
		Signatures:            c.Signatures[:],
		UnlockConditions:      c.Revision.UnlockConditions,
		NewRevisionNumber:     c.Revision.NewRevisionNumber,
		NewFileSize:           c.Revision.NewFileSize,
		NewFileMerkleRoot:     c.Revision.NewFileMerkleRoot,
		NewWindowStart:        c.Revision.NewWindowStart,
		NewWindowEnd:          c.Revision.NewWindowEnd,
		NewValidProofOutputs:  newValidOutputs,
		NewMissedProofOutputs: newMissedOutputs,
		NewUnlockHash:         c.Revision.NewUnlockHash,
	}

	// Insert contract.
	return s.db.Create(&dbContractRHPv2{
		ID:            fcid,
		GoodForUpload: true, // new contract is always good for upload
		Revision:      revision,
	}).Error
}

// Contract implements the bus.ContractStore interface.
func (s *SQLStore) Contract(id types.FileContractID) (rhpv2.Contract, error) {
	// Fetch contract.
	contract, err := s.contract(id)
	if err != nil {
		return rhpv2.Contract{}, err
	}
	return contract.convert()
}

// Contracts implements the bus.ContractStore interface.
func (s *SQLStore) Contracts() ([]rhpv2.Contract, error) {
	dbContracts, err := s.contracts()
	if err != nil {
		return nil, err
	}
	contracts := make([]rhpv2.Contract, len(dbContracts))
	for i, c := range dbContracts {
		contract, err := c.convert()
		if err != nil {
			return nil, err
		}
		contracts[i] = contract
	}
	return contracts, nil
}

// RemoveContract implements the bus.ContractStore interface.
func (s *SQLStore) RemoveContract(id types.FileContractID) error {
	return s.db.Delete(&dbContractRHPv2{ID: id}).Error
}

func (s *SQLStore) contract(id types.FileContractID) (dbContractRHPv2, error) {
	var contract dbContractRHPv2
	err := s.db.Where(&dbContractRHPv2{ID: id}).
		Preload("Revision.NewValidProofOutputs").
		Preload("Revision.NewMissedProofOutputs").
		Take(&contract).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return contract, ErrContractNotFound
	}
	return contract, err
}

func (s *SQLStore) contracts() ([]dbContractRHPv2, error) {
	var contracts []dbContractRHPv2
	err := s.db.Model(&dbContractRHPv2{}).
		Preload("Revision.NewValidProofOutputs").
		Preload("Revision.NewMissedProofOutputs").
		Find(&contracts).Error
	return contracts, err
}
