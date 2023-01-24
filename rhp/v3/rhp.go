// Package rhp implements the Sia renter-host protocol, version 3.
package rhp

import (
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"go.sia.tech/siad/crypto"
	stypes "go.sia.tech/siad/types"
	"lukechampine.com/frand"
)

// An Account is a public key used to identify an ephemeral account on a host.
type Account types.PublicKey

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
func PayByEphemeralAccount(account Account, amount types.Currency, expiry uint64, sk types.PrivateKey) PayByEphemeralAccountRequest {
	p := PayByEphemeralAccountRequest{
		Account:  account,
		Expiry:   expiry,
		Amount:   amount,
		Priority: 0, // TODO ???
	}
	frand.Read(p.Nonce[:])
	p.Signature = sk.SignHash(types.Hash256(crypto.HashAll(p.Account, p.Expiry, p.Account, p.Nonce)))
	return p
}

// PayByContract creates a PayByContractRequest by revising the supplied
// contract.
func PayByContract(rev *types.FileContractRevision, amount types.Currency, refundAcct Account, sk types.PrivateKey) (PayByContractRequest, bool) {
	if rev.ValidRenterPayout().Cmp(amount) < 0 || rev.MissedRenterPayout().Cmp(amount) < 0 {
		return PayByContractRequest{}, false
	}
	rev.ValidProofOutputs[0].Value = rev.ValidProofOutputs[0].Value.Sub(amount)
	rev.ValidProofOutputs[1].Value = rev.ValidProofOutputs[1].Value.Add(amount)
	rev.MissedProofOutputs[0].Value = rev.MissedProofOutputs[0].Value.Sub(amount)
	rev.MissedProofOutputs[1].Value = rev.MissedProofOutputs[1].Value.Add(amount)
	rev.RevisionNumber++

	newValid := make([]types.Currency, len(rev.ValidProofOutputs))
	for i, o := range rev.ValidProofOutputs {
		newValid[i] = o.Value
	}
	newMissed := make([]types.Currency, len(rev.MissedProofOutputs))
	for i, o := range rev.MissedProofOutputs {
		newMissed[i] = o.Value
	}
	p := PayByContractRequest{
		ContractID:        rev.ParentID,
		RevisionNumber:    rev.RevisionNumber,
		ValidProofValues:  newValid,
		MissedProofValues: newMissed,
		RefundAccount:     refundAcct,
	}
	txn := types.Transaction{
		FileContractRevisions: []types.FileContractRevision{*rev},
	}
	cs := consensus.State{Index: types.ChainIndex{Height: rev.WindowEnd}}
	p.Signature = sk.SignHash(cs.PartialSigHash(txn, types.CoveredFields{FileContractRevisions: []uint64{0}}))
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
func (pt *HostPriceTable) UpdateRegistryCost() (writeCost, storeCost types.Currency) {
	// Cost is the same as uploading and storing a registry entry for 5 years.
	// TODO: remove dependency on stypes
	writeCost = pt.writeCost(registryEntrySize)
	storeCost = pt.WriteStoreCost.Mul64(registryEntrySize).Mul64(uint64(5 * stypes.BlocksPerYear))
	return writeCost.Add(storeCost), storeCost
}

// writeCost is the cost of executing a 'Write' instruction of a certain length
// on the MDM.
func (pt *HostPriceTable) writeCost(writeLength uint64) types.Currency {
	const atomicWriteSize = 1 << 12
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
		Signature types.Signature
		Priority  int64
	}

	// PayByContractRequest represents a payment made using a contract revision.
	PayByContractRequest struct {
		ContractID        types.FileContractID
		RevisionNumber    uint64
		ValidProofValues  []types.Currency
		MissedProofValues []types.Currency
		RefundAccount     Account
		Signature         types.Signature
		HostSignature     types.Signature
	}
)

// RPC IDs
var (
	rpcAccountBalanceID   = types.NewSpecifier("AccountBalance")
	rpcExecuteProgramID   = types.NewSpecifier("ExecuteProgram")
	rpcUpdatePriceTableID = types.NewSpecifier("UpdatePriceTable")
	rpcFundAccountID      = types.NewSpecifier("FundAccount")
	// rpcLatestRevisionID       = types.NewSpecifier("LatestRevision")
	// rpcRegistrySubscriptionID = types.NewSpecifier("Subscription")
	// rpcRenewContractID        = types.NewSpecifier("RenewContract")

	paymentTypeContract         = types.NewSpecifier("PayByContract")
	paymentTypeEphemeralAccount = types.NewSpecifier("PayByEphemAcc")
)

// RPC request/response objects
type (
	paymentResponse struct {
		Signature types.Signature
	}

	rpcUpdatePriceTableResponse struct {
		PriceTableJSON []byte
	}

	rpcPriceTableResponse struct{}

	rpcFundAccountRequest struct {
		Account Account
	}

	rpcFundAccountResponse struct {
		Balance types.Currency
		Receipt struct {
			Host      types.UnlockKey
			Account   Account
			Amount    types.Currency
			Timestamp time.Time
		}
		Signature types.Signature
	}

	instruction struct {
		Specifier types.Specifier
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
		NewMerkleRoot        types.Hash256
		NewSize              uint64
		Proof                []types.Hash256
		Error                error
		TotalCost            types.Currency
		FailureRefund        types.Currency
	}
)
