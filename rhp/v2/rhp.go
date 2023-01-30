// Package rhp implements the Sia renter-host protocol, version 2.
package rhp

import (
	"fmt"
	"net"
	"time"

	"go.sia.tech/core/types"
)

func wrapErr(err *error, fnName string) {
	if *err != nil {
		*err = fmt.Errorf("%s: %w", fnName, *err)
	}
}

// A ContractRevision pairs a file contract with its signatures.
type ContractRevision struct {
	Revision   types.FileContractRevision
	Signatures [2]types.TransactionSignature
}

// EndHeight returns the height at which the host is no longer obligated to
// store contract data.
func (c ContractRevision) EndHeight() uint64 {
	return uint64(c.Revision.WindowStart)
}

// ID returns the ID of the original FileContract.
func (c ContractRevision) ID() types.FileContractID {
	return c.Revision.ParentID
}

// HostKey returns the public key of the host.
func (c ContractRevision) HostKey() (pk types.PublicKey) {
	copy(pk[:], c.Revision.UnlockConditions.PublicKeys[1].Key)
	return
}

// RenterFunds returns the funds remaining in the contract's Renter payout.
func (c ContractRevision) RenterFunds() types.Currency {
	return c.Revision.ValidProofOutputs[0].Value
}

// NumSectors returns the number of sectors covered by the contract.
func (c ContractRevision) NumSectors() uint64 {
	return c.Revision.Filesize / SectorSize
}

// HostSettings are the settings and prices used when interacting with a host.
type HostSettings struct {
	AcceptingContracts         bool           `json:"acceptingcontracts,omitempty"`
	MaxDownloadBatchSize       uint64         `json:"maxdownloadbatchsize,omitempty"`
	MaxDuration                uint64         `json:"maxduration,omitempty"`
	MaxReviseBatchSize         uint64         `json:"maxrevisebatchsize,omitempty"`
	NetAddress                 string         `json:"netaddress,omitempty"`
	RemainingStorage           uint64         `json:"remainingstorage,omitempty"`
	SectorSize                 uint64         `json:"sectorsize,omitempty"`
	TotalStorage               uint64         `json:"totalstorage,omitempty"`
	Address                    types.Address  `json:"unlockhash,omitempty"`
	WindowSize                 uint64         `json:"windowsize,omitempty"`
	Collateral                 types.Currency `json:"collateral,omitempty"`
	MaxCollateral              types.Currency `json:"maxcollateral,omitempty"`
	BaseRPCPrice               types.Currency `json:"baserpcprice,omitempty"`
	ContractPrice              types.Currency `json:"contractprice,omitempty"`
	DownloadBandwidthPrice     types.Currency `json:"downloadbandwidthprice,omitempty"`
	SectorAccessPrice          types.Currency `json:"sectoraccessprice,omitempty"`
	StoragePrice               types.Currency `json:"storageprice,omitempty"`
	UploadBandwidthPrice       types.Currency `json:"uploadbandwidthprice,omitempty"`
	EphemeralAccountExpiry     time.Duration  `json:"ephemeralaccountexpiry,omitempty"`
	MaxEphemeralAccountBalance types.Currency `json:"maxephemeralaccountbalance,omitempty"`
	RevisionNumber             uint64         `json:"revisionnumber,omitempty"`
	Version                    string         `json:"version,omitempty"`
	SiaMuxPort                 string         `json:"siamuxport,omitempty"`
}

// SiamuxAddr is a helper which returns an address that can be used to connect
// to the host's siamux.
func (s HostSettings) SiamuxAddr() string {
	host, _, err := net.SplitHostPort(s.NetAddress)
	if err != nil {
		return ""
	}
	return net.JoinHostPort(host, s.SiaMuxPort)
}

// RPC IDs
var (
	RPCFormContractID       = types.NewSpecifier("LoopFormContract")
	RPCLockID               = types.NewSpecifier("LoopLock")
	RPCReadID               = types.NewSpecifier("LoopRead")
	RPCRenewContractID      = types.NewSpecifier("LoopRenew")
	RPCRenewClearContractID = types.NewSpecifier("LoopRenewClear")
	RPCSectorRootsID        = types.NewSpecifier("LoopSectorRoots")
	RPCSettingsID           = types.NewSpecifier("LoopSettings")
	RPCUnlockID             = types.NewSpecifier("LoopUnlock")
	RPCWriteID              = types.NewSpecifier("LoopWrite")
)

// Read/Write actions
var (
	RPCWriteActionAppend = types.NewSpecifier("Append")
	RPCWriteActionTrim   = types.NewSpecifier("Trim")
	RPCWriteActionSwap   = types.NewSpecifier("Swap")
	RPCWriteActionUpdate = types.NewSpecifier("Update")

	RPCReadStop = types.NewSpecifier("ReadStop")
)

// RPC request/response objects
type (
	// RPCFormContractRequest contains the request parameters for the
	// FormContract and RenewContract RPCs.
	RPCFormContractRequest struct {
		Transactions []types.Transaction
		RenterKey    types.UnlockKey
	}

	// RPCRenewAndClearContractRequest contains the request parameters for the
	// RenewAndClearContract RPC.
	RPCRenewAndClearContractRequest struct {
		Transactions           []types.Transaction
		RenterKey              types.UnlockKey
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
		FinalRevisionSignature types.Signature
	}

	// RPCLockRequest contains the request parameters for the Lock RPC.
	RPCLockRequest struct {
		ContractID types.FileContractID
		Signature  types.Signature
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
		MerkleRoot types.Hash256
		Offset     uint64
		Length     uint64
	}

	// RPCReadRequest contains the request parameters for the Read RPC.
	RPCReadRequest struct {
		Sections    []RPCReadRequestSection
		MerkleProof bool

		RevisionNumber    uint64
		ValidProofValues  []types.Currency
		MissedProofValues []types.Currency
		Signature         types.Signature
	}

	// RPCReadResponse contains the response data for the Read RPC.
	RPCReadResponse struct {
		Signature   types.Signature
		Data        []byte
		MerkleProof []types.Hash256
	}

	// RPCSectorRootsRequest contains the request parameters for the SectorRoots RPC.
	RPCSectorRootsRequest struct {
		RootOffset uint64
		NumRoots   uint64

		RevisionNumber    uint64
		ValidProofValues  []types.Currency
		MissedProofValues []types.Currency
		Signature         types.Signature
	}

	// RPCSectorRootsResponse contains the response data for the SectorRoots RPC.
	RPCSectorRootsResponse struct {
		Signature   types.Signature
		SectorRoots []types.Hash256
		MerkleProof []types.Hash256
	}

	// RPCSettingsResponse contains the response data for the SettingsResponse RPC.
	RPCSettingsResponse struct {
		Settings []byte // JSON-encoded hostdb.HostSettings
	}

	// RPCWriteRequest contains the request parameters for the Write RPC.
	RPCWriteRequest struct {
		Actions     []RPCWriteAction
		MerkleProof bool

		RevisionNumber    uint64
		ValidProofValues  []types.Currency
		MissedProofValues []types.Currency
	}

	// RPCWriteAction is a generic Write action. The meaning of each field
	// depends on the Type of the action.
	RPCWriteAction struct {
		Type types.Specifier
		A, B uint64
		Data []byte
	}

	// RPCWriteMerkleProof contains the optional Merkle proof for response data
	// for the Write RPC.
	RPCWriteMerkleProof struct {
		OldSubtreeHashes []types.Hash256
		OldLeafHashes    []types.Hash256
		NewMerkleRoot    types.Hash256
	}

	// RPCWriteResponse contains the response data for the Write RPC.
	RPCWriteResponse struct {
		Signature types.Signature
	}
)

// MetricRPC contains metrics relating to a single RPC.
type MetricRPC struct {
	HostKey    types.PublicKey
	RPC        types.Specifier
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

// IsMetric implements metrics.Metric.
func (m MetricRPC) IsSuccess() bool { return m.Err == nil }
