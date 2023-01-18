// Package rhp implements the Sia renter-host protocol, version 3.
package rhp

import (
	"bytes"
	"time"

	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/siad/crypto"
	"go.sia.tech/siad/modules"
	"go.sia.tech/siad/types"
	"lukechampine.com/frand"
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
)

// An Account is a public key used to identify an ephemeral account on a host.
type Account PublicKey

// ZeroAccount is a sentinel value that indicates the lack of an account.
var ZeroAccount Account

// A PaymentMethod is a way of paying for an arbitrary host operation.
type PaymentMethod interface {
	protocolObject
	isPaymentMethod()
}

func (PayByEphemeralAccountRequest) isPaymentMethod() {}
func (PayByContractRequest) isPaymentMethod()         {}

// PayByEphemeralAccount creates a PayByEphemeralAccountRequest.
func PayByEphemeralAccount(account Account, amount types.Currency, expiry uint64, sk PrivateKey) PayByEphemeralAccountRequest {
	p := PayByEphemeralAccountRequest{
		Account:  account,
		Expiry:   expiry,
		Amount:   amount,
		Priority: 0, // TODO ???
	}
	frand.Read(p.Nonce[:])
	p.Signature = sk.SignHash(Hash256(crypto.HashAll(p.Account, p.Expiry, p.Account, p.Nonce)))
	return p
}

// PayByContract creates a PayByContractRequest by revising the supplied
// contract.
func PayByContract(rev *types.FileContractRevision, amount types.Currency, refundAcct Account, sk PrivateKey) (PayByContractRequest, bool) {
	if rev.ValidRenterPayout().Cmp(amount) < 0 || rev.MissedRenterPayout().Cmp(amount) < 0 {
		return PayByContractRequest{}, false
	}
	rev.NewValidProofOutputs[0].Value = rev.NewValidProofOutputs[0].Value.Sub(amount)
	rev.NewValidProofOutputs[1].Value = rev.NewValidProofOutputs[1].Value.Add(amount)
	rev.NewMissedProofOutputs[0].Value = rev.NewMissedProofOutputs[0].Value.Sub(amount)
	rev.NewMissedProofOutputs[1].Value = rev.NewMissedProofOutputs[1].Value.Add(amount)
	rev.NewRevisionNumber++

	newValid := make([]types.Currency, len(rev.NewValidProofOutputs))
	for i, o := range rev.NewValidProofOutputs {
		newValid[i] = o.Value
	}
	newMissed := make([]types.Currency, len(rev.NewMissedProofOutputs))
	for i, o := range rev.NewMissedProofOutputs {
		newMissed[i] = o.Value
	}
	ra := modules.ZeroAccountID
	if refundAcct != ZeroAccount {
		ra.FromSPK(types.Ed25519PublicKey(crypto.PublicKey(refundAcct)))
	}
	p := PayByContractRequest{
		ContractID:           rev.ParentID,
		NewRevisionNumber:    rev.NewRevisionNumber,
		NewValidProofValues:  newValid,
		NewMissedProofValues: newMissed,
		RefundAccount:        ra,
	}
	txn := types.Transaction{
		FileContractRevisions: []types.FileContractRevision{*rev},
		TransactionSignatures: []types.TransactionSignature{{
			ParentID:       crypto.Hash(rev.ParentID),
			PublicKeyIndex: 0,
			CoveredFields:  types.CoveredFields{FileContractRevisions: []uint64{0}},
		}},
	}
	sig := sk.SignHash(Hash256(txn.SigHash(0, rev.NewWindowEnd)))
	p.Signature = sig[:]
	return p, true
}

// A SettingsID is a unique identifier for registered host settings used by renters
// when interacting with the host.
type SettingsID [16]byte

// An HostPriceTable contains the host's current prices for each RPC.
type HostPriceTable struct {
	ID                           SettingsID     `json:"uid"`
	Validity                     time.Duration  `json:"validity"`
	HostBlockHeight              uint64         `json:"hostblockheight"`
	UpdatePriceTableCost         types.Currency `json:"updatepricetablecost"`
	AccountBalanceCost           types.Currency `json:"accountbalancecost"`
	FundAccountCost              types.Currency `json:"fundaccountcost"`
	LatestRevisionCost           types.Currency `json:"latestrevisioncost"`
	SubscriptionMemoryCost       types.Currency `json:"subscriptionmemorycost"`
	SubscriptionNotificationCost types.Currency `json:"subscriptionnotificationcost"`
	InitBaseCost                 types.Currency `json:"initbasecost"`
	MemoryTimeCost               types.Currency `json:"memorytimecost"`
	DownloadBandwidthCost        types.Currency `json:"downloadbandwidthcost"`
	UploadBandwidthCost          types.Currency `json:"uploadbandwidthcost"`
	DropSectorsBaseCost          types.Currency `json:"dropsectorsbasecost"`
	DropSectorsUnitCost          types.Currency `json:"dropsectorsunitcost"`
	HasSectorBaseCost            types.Currency `json:"hassectorbasecost"`
	ReadBaseCost                 types.Currency `json:"readbasecost"`
	ReadLengthCost               types.Currency `json:"readlengthcost"`
	RenewContractCost            types.Currency `json:"renewcontractcost"`
	RevisionBaseCost             types.Currency `json:"revisionbasecost"`
	SwapSectorCost               types.Currency `json:"swapsectorcost"`
	WriteBaseCost                types.Currency `json:"writebasecost"`
	WriteLengthCost              types.Currency `json:"writelengthcost"`
	WriteStoreCost               types.Currency `json:"writestorecost"`
	TxnFeeMinRecommended         types.Currency `json:"txnfeeminrecommended"`
	TxnFeeMaxRecommended         types.Currency `json:"txnfeemaxrecommended"`
	ContractPrice                types.Currency `json:"contractprice"`
	CollateralCost               types.Currency `json:"collateralcost"`
	MaxCollateral                types.Currency `json:"maxcollateral"`
	MaxDuration                  uint64         `json:"maxduration"`
	WindowSize                   uint64         `json:"windowsize"`
	RegistryEntriesLeft          uint64         `json:"registryentriesleft"`
	RegistryEntriesTotal         uint64         `json:"registryentriestotal"`
}

const registryEntrySize = 256

// MDMUpdateRegistryCost is the cost of executing a 'UpdateRegistry'
// instruction on the MDM.
func (pt *HostPriceTable) UpdateRegistryCost() (_, _ types.Currency) {
	// Cost is the same as uploading and storing a registry entry for 5 years.
	writeCost := pt.writeCost(registryEntrySize)
	storeCost := pt.WriteStoreCost.Mul64(registryEntrySize).Mul64(uint64(5 * types.BlocksPerYear))
	return writeCost.Add(storeCost), storeCost
}

// writeCost is the cost of executing a 'Write' instruction of a certain length
// on the MDM.
func (pt *HostPriceTable) writeCost(writeLength uint64) types.Currency {
	// Atomic write size for modern disks is 4kib so we round up.
	atomicWriteSize := uint64(1 << 12)
	if mod := writeLength % atomicWriteSize; mod != 0 {
		writeLength += (atomicWriteSize - mod)
	}
	writeCost := pt.WriteLengthCost.Mul64(writeLength).Add(pt.WriteBaseCost)
	return writeCost
}

type (
	// PayByEphemeralAccountRequest represents a payment made using an ephemeral account.
	PayByEphemeralAccountRequest struct {
		Account   Account
		Expiry    uint64
		Amount    types.Currency
		Nonce     [8]byte
		Signature Signature
		Priority  int64
	}

	// PayByContractRequest represents a payment made using a contract revision.
	PayByContractRequest struct {
		ContractID           types.FileContractID
		NewRevisionNumber    uint64
		NewValidProofValues  []types.Currency
		NewMissedProofValues []types.Currency
		RefundAccount        modules.AccountID
		Signature            []byte
		HostSignature        Signature
	}
)

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
	rpcAccountBalanceID   = newSpecifier("AccountBalance")
	rpcExecuteProgramID   = newSpecifier("ExecuteProgram")
	rpcUpdatePriceTableID = newSpecifier("UpdatePriceTable")
	rpcFundAccountID      = newSpecifier("FundAccount")
	// rpcLatestRevisionID       = newSpecifier("LatestRevision")
	// rpcRegistrySubscriptionID = newSpecifier("Subscription")
	// rpcRenewContractID        = newSpecifier("RenewContract")

	paymentTypeContract         = newSpecifier("PayByContract")
	paymentTypeEphemeralAccount = newSpecifier("PayByEphemAcc")
)

// RPC request/response objects
type (
	paymentResponse struct {
		Signature Signature
	}

	rpcUpdatePriceTableResponse struct {
		PriceTableJSON []byte
	}

	rpcPriceTableResponse struct{}

	rpcFundAccountRequest struct {
		Account modules.AccountID
	}

	rpcFundAccountResponse struct {
		Balance types.Currency
		Receipt struct {
			Host      types.SiaPublicKey
			Account   modules.AccountID
			Amount    types.Currency
			Timestamp int64
		}
		Signature Signature
	}

	instruction struct {
		Specifier Specifier
		Args      []byte
	}

	rpcExecuteProgramRequest struct {
		FileContractID types.FileContractID
		Program        []instruction
		ProgramData    []byte
	}

	rpcExecuteProgramResponse struct {
		AdditionalCollateral types.Currency
		OutputLength         uint64
		NewMerkleRoot        Hash256
		NewSize              uint64
		Proof                []Hash256
		Error                error
		TotalCost            types.Currency
		FailureRefund        types.Currency
	}
)
