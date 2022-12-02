// Package rhp implements the Sia renter-host protocol, version 2.
package rhp

import (
	"bytes"
	"fmt"
	"time"

	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/siad/types"
)

// exported types from internal/consensus
type (
	// A Hash256 is a generic 256-bit cryptographic hash.
	Hash256 = consensus.Hash256
	// A PublicKey is an Ed25519 public key.
	PublicKey = consensus.PublicKey
	// A PrivateKey is an Ed25519 private key.
	PrivateKey = consensus.PrivateKey
	// A Signature is an Ed25519 signature.
	Signature = consensus.Signature
	// ConsensusState represents the full state of the chain as of a particular block.
	ConsensusState = consensus.State
)

func wrapErr(err *error, fnName string) {
	if *err != nil {
		*err = fmt.Errorf("%s: %w", fnName, *err)
	}
}

// A Contract pairs a file contract with its signatures.
type Contract struct {
	Revision   types.FileContractRevision
	Signatures [2]types.TransactionSignature
}

// EndHeight returns the height at which the host is no longer obligated to
// store contract data.
func (c Contract) EndHeight() uint64 {
	return uint64(c.Revision.NewWindowStart)
}

// ID returns the ID of the original FileContract.
func (c Contract) ID() types.FileContractID {
	return c.Revision.ParentID
}

// HostKey returns the public key of the host.
func (c Contract) HostKey() (pk PublicKey) {
	copy(pk[:], c.Revision.UnlockConditions.PublicKeys[1].Key)
	return
}

// RenterFunds returns the funds remaining in the contract's Renter payout.
func (c Contract) RenterFunds() types.Currency {
	return c.Revision.NewValidProofOutputs[0].Value
}

// NumSectors returns the number of sectors covered by the contract.
func (c Contract) NumSectors() uint64 {
	return c.Revision.NewFileSize / SectorSize
}

// HostSettings are the settings and prices used when interacting with a host.
type HostSettings struct {
	AcceptingContracts         bool             `json:"acceptingcontracts"`
	MaxDownloadBatchSize       uint64           `json:"maxdownloadbatchsize"`
	MaxDuration                uint64           `json:"maxduration"`
	MaxReviseBatchSize         uint64           `json:"maxrevisebatchsize"`
	NetAddress                 string           `json:"netaddress"`
	RemainingStorage           uint64           `json:"remainingstorage"`
	SectorSize                 uint64           `json:"sectorsize"`
	TotalStorage               uint64           `json:"totalstorage"`
	UnlockHash                 types.UnlockHash `json:"unlockhash"`
	WindowSize                 uint64           `json:"windowsize"`
	Collateral                 types.Currency   `json:"collateral"`
	MaxCollateral              types.Currency   `json:"maxcollateral"`
	BaseRPCPrice               types.Currency   `json:"baserpcprice"`
	ContractPrice              types.Currency   `json:"contractprice"`
	DownloadBandwidthPrice     types.Currency   `json:"downloadbandwidthprice"`
	SectorAccessPrice          types.Currency   `json:"sectoraccessprice"`
	StoragePrice               types.Currency   `json:"storageprice"`
	UploadBandwidthPrice       types.Currency   `json:"uploadbandwidthprice"`
	EphemeralAccountExpiry     time.Duration    `json:"ephemeralaccountexpiry"`
	MaxEphemeralAccountBalance types.Currency   `json:"maxephemeralaccountbalance"`
	RevisionNumber             uint64           `json:"revisionnumber"`
	Version                    string           `json:"version"`
	SiaMuxPort                 string           `json:"siamuxport"`
}

// A Specifier is a generic identification tag.
type Specifier [16]byte

func (s Specifier) String() string {
	return string(bytes.Trim(s[:], "\x00"))
}

func newSpecifier(str string) Specifier {
	if len(str) > 16 {
		panic("specifier is too long")
	}
	var s Specifier
	copy(s[:], str)
	return s
}

// RPC IDs
var (
	RPCFormContractID       = newSpecifier("LoopFormContract")
	RPCLockID               = newSpecifier("LoopLock")
	RPCReadID               = newSpecifier("LoopRead")
	RPCRenewContractID      = newSpecifier("LoopRenew")
	RPCRenewClearContractID = newSpecifier("LoopRenewClear")
	RPCSectorRootsID        = newSpecifier("LoopSectorRoots")
	RPCSettingsID           = newSpecifier("LoopSettings")
	RPCUnlockID             = newSpecifier("LoopUnlock")
	RPCWriteID              = newSpecifier("LoopWrite")
)

// Read/Write actions
var (
	RPCWriteActionAppend = newSpecifier("Append")
	RPCWriteActionTrim   = newSpecifier("Trim")
	RPCWriteActionSwap   = newSpecifier("Swap")
	RPCWriteActionUpdate = newSpecifier("Update")

	RPCReadStop = newSpecifier("ReadStop")
)

// RPC request/response objects
type (
	// RPCFormContractRequest contains the request parameters for the
	// FormContract and RenewContract RPCs.
	RPCFormContractRequest struct {
		Transactions []types.Transaction
		RenterKey    types.SiaPublicKey
	}

	// RPCRenewAndClearContractRequest contains the request parameters for the
	// RenewAndClearContract RPC.
	RPCRenewAndClearContractRequest struct {
		Transactions           []types.Transaction
		RenterKey              types.SiaPublicKey
		FinalValidProofValues  []types.Currency
		FinalMissedProofValues []types.Currency
	}

	// RPCFormContractAdditions contains the parent transaction, inputs, and
	// outputs added by the host when negotiating a file contract.
	RPCFormContractAdditions struct {
		Parents []types.Transaction
		Inputs  []types.SiacoinInput
		Outputs []types.SiacoinOutput
	}

	// RPCFormContractSignatures contains the signatures for a contract
	// transaction and initial revision. These signatures are sent by both the
	// renter and host during contract formation and renewal.
	RPCFormContractSignatures struct {
		ContractSignatures []types.TransactionSignature
		RevisionSignature  types.TransactionSignature
	}

	// RPCRenewAndClearContractSignatures contains the signatures for a contract
	// transaction, initial revision, and final revision of the contract being
	// renewed. These signatures are sent by both the renter and host during the
	// RenewAndClear RPC.
	RPCRenewAndClearContractSignatures struct {
		ContractSignatures     []types.TransactionSignature
		RevisionSignature      types.TransactionSignature
		FinalRevisionSignature Signature
	}

	// RPCLockRequest contains the request parameters for the Lock RPC.
	RPCLockRequest struct {
		ContractID types.FileContractID
		Signature  Signature
		Timeout    uint64
	}

	// RPCLockResponse contains the response data for the Lock RPC.
	RPCLockResponse struct {
		Acquired     bool
		NewChallenge [16]byte
		Revision     types.FileContractRevision
		Signatures   []types.TransactionSignature
	}

	// RPCReadRequestSection is a section requested in RPCReadRequest.
	RPCReadRequestSection struct {
		MerkleRoot Hash256
		Offset     uint64
		Length     uint64
	}

	// RPCReadRequest contains the request parameters for the Read RPC.
	RPCReadRequest struct {
		Sections    []RPCReadRequestSection
		MerkleProof bool

		NewRevisionNumber    uint64
		NewValidProofValues  []types.Currency
		NewMissedProofValues []types.Currency
		Signature            Signature
	}

	// RPCReadResponse contains the response data for the Read RPC.
	RPCReadResponse struct {
		Signature   Signature
		Data        []byte
		MerkleProof []Hash256
	}

	// RPCSectorRootsRequest contains the request parameters for the SectorRoots RPC.
	RPCSectorRootsRequest struct {
		RootOffset uint64
		NumRoots   uint64

		NewRevisionNumber    uint64
		NewValidProofValues  []types.Currency
		NewMissedProofValues []types.Currency
		Signature            Signature
	}

	// RPCSectorRootsResponse contains the response data for the SectorRoots RPC.
	RPCSectorRootsResponse struct {
		Signature   Signature
		SectorRoots []Hash256
		MerkleProof []Hash256
	}

	// RPCSettingsResponse contains the response data for the SettingsResponse RPC.
	RPCSettingsResponse struct {
		Settings []byte // JSON-encoded hostdb.HostSettings
	}

	// RPCWriteRequest contains the request parameters for the Write RPC.
	RPCWriteRequest struct {
		Actions     []RPCWriteAction
		MerkleProof bool

		NewRevisionNumber    uint64
		NewValidProofValues  []types.Currency
		NewMissedProofValues []types.Currency
	}

	// RPCWriteAction is a generic Write action. The meaning of each field
	// depends on the Type of the action.
	RPCWriteAction struct {
		Type Specifier
		A, B uint64
		Data []byte
	}

	// RPCWriteMerkleProof contains the optional Merkle proof for response data
	// for the Write RPC.
	RPCWriteMerkleProof struct {
		OldSubtreeHashes []Hash256
		OldLeafHashes    []Hash256
		NewMerkleRoot    Hash256
	}

	// RPCWriteResponse contains the response data for the Write RPC.
	RPCWriteResponse struct {
		Signature Signature
	}
)

// MetricRPC contains metrics relating to a single RPC.
type MetricRPC struct {
	HostKey    PublicKey
	RPC        Specifier
	Timestamp  time.Time
	Elapsed    time.Duration
	Contract   types.FileContractID // possibly empty
	Uploaded   uint64
	Downloaded uint64
	Cost       types.Currency
	Collateral types.Currency
	Err        error
}

// IsMetric implements metrics.Metric.
func (MetricRPC) IsMetric() {}
