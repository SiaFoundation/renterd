package rhp

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"go.sia.tech/core/types"
)

// EncodeTo implements types.EncoderTo.
func (r *RPCError) EncodeTo(e *types.Encoder) {
	r.Type.EncodeTo(e)
	e.WriteBytes(r.Data)
	e.WriteString(r.Description)
}

// DecodeFrom implements types.DecoderFrom.
func (r *RPCError) DecodeFrom(d *types.Decoder) {
	r.Type.DecodeFrom(d)
	r.Data = d.ReadBytes()
	r.Description = d.ReadString()
}

// EncodeTo implements types.EncoderTo.
func (s *SettingsID) EncodeTo(e *types.Encoder) { e.Write(s[:]) }

// DecodeFrom implements types.DecoderFrom.
func (s *SettingsID) DecodeFrom(d *types.Decoder) { d.Read(s[:]) }

// String implements fmt.Stringer.
func (s SettingsID) String() string {
	return hex.EncodeToString(s[:])
}

// LoadString loads the unique id from the given string.
func (s *SettingsID) LoadString(input string) error {
	if len(input) != len(s)*2 {
		return errors.New("incorrect length")
	}
	uidBytes, err := hex.DecodeString(input)
	if err != nil {
		return errors.New("could not unmarshal hash: " + err.Error())
	}
	copy(s[:], uidBytes)
	return nil
}

// MarshalJSON marshals an id as a hex string.
func (s SettingsID) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.String())
}

// UnmarshalJSON decodes the json hex string of the id.
func (s *SettingsID) UnmarshalJSON(b []byte) error {
	if len(b) != len(SettingsID{})*2+2 {
		return errors.New("incorrect length")
	}
	return s.LoadString(string(bytes.Trim(b, `"`)))
}

// MarshalSia implements encoding.SiaMarshaler.
func (resp *rpcResponse) EncodeTo(e *types.Encoder) {
	e.WriteBool(resp.err != nil)
	if resp.err != nil {
		resp.err.EncodeTo(e)
		return
	}
	resp.data.EncodeTo(e)
}

func (resp *rpcResponse) DecodeFrom(d *types.Decoder) {
	if d.ReadBool() {
		resp.err = new(RPCError)
		resp.err.DecodeFrom(d)
		return
	}
	resp.data.DecodeFrom(d)
}

// EncodeTo implements types.EncoderTo.
func (a *Account) EncodeTo(e *types.Encoder) {
	var uk types.UnlockKey
	if *a != ZeroAccount {
		uk.Algorithm = types.SpecifierEd25519
		uk.Key = a[:]
	}
	uk.EncodeTo(e)
}

// DecodeFrom implements types.DecoderFrom.
func (a *Account) DecodeFrom(d *types.Decoder) {
	var spk types.UnlockKey
	spk.DecodeFrom(d)
	if spk.Algorithm == (types.Specifier{}) && len(spk.Key) == 0 {
		*a = ZeroAccount
		return
	} else if spk.Algorithm != types.SpecifierEd25519 {
		d.SetErr(fmt.Errorf("unsupported signature algorithm: %v", spk.Algorithm))
		return
	}
	copy(a[:], spk.Key)
}

// MarshalJSON implements json.Marshaler.
func (a Account) MarshalJSON() ([]byte, error) {
	return types.PublicKey(a).MarshalJSON()
}

// MarshalJSON implements json.Marshaler.
func (a *Account) UnmarshalJSON(b []byte) error {
	return (*types.PublicKey)(a).UnmarshalJSON(b)
}

// UnmarshalText implements encoding.TextMarshaler.
func (a Account) MarshalText() ([]byte, error) { return types.PublicKey(a).MarshalText() }

// UnmarshalText implements encoding.TextUnmarshaler.
func (a *Account) UnmarshalText(b []byte) error { return (*types.PublicKey)(a).UnmarshalText(b) }

// String implements fmt.Stringer.
func (a Account) String() string { return types.PublicKey(a).String() }

// EncodeTo implements types.EncoderTo.
func (r *PayByEphemeralAccountRequest) EncodeTo(e *types.Encoder) {
	r.Account.EncodeTo(e)
	e.WriteUint64(r.Expiry)
	r.Amount.EncodeTo(e)
	e.Write(r.Nonce[:])
	r.Signature.EncodeTo(e)
	e.WriteUint64(uint64(r.Priority))
}

// DecodeFrom implements types.DecoderFrom.
func (r *PayByEphemeralAccountRequest) DecodeFrom(d *types.Decoder) {
	r.Account.DecodeFrom(d)
	r.Expiry = d.ReadUint64()
	r.Amount.DecodeFrom(d)
	d.Read(r.Nonce[:])
	r.Signature.DecodeFrom(d)
	r.Priority = int64(d.ReadUint64())
}

// EncodeTo implements types.EncoderTo.
func (r *PayByContractRequest) EncodeTo(e *types.Encoder) {
	r.ContractID.EncodeTo(e)
	e.WriteUint64(r.RevisionNumber)
	e.WritePrefix(len(r.ValidProofValues))
	for i := range r.ValidProofValues {
		r.ValidProofValues[i].EncodeTo(e)
	}
	e.WritePrefix(len(r.MissedProofValues))
	for i := range r.MissedProofValues {
		r.MissedProofValues[i].EncodeTo(e)
	}
	r.RefundAccount.EncodeTo(e)
	e.WriteBytes(r.Signature[:])
	r.HostSignature.EncodeTo(e)
}

// DecodeFrom implements types.DecoderFrom.
func (r *PayByContractRequest) DecodeFrom(d *types.Decoder) {
	r.ContractID.DecodeFrom(d)
	r.RevisionNumber = d.ReadUint64()
	r.ValidProofValues = make([]types.Currency, d.ReadPrefix())
	for i := range r.ValidProofValues {
		r.ValidProofValues[i].DecodeFrom(d)
	}
	r.MissedProofValues = make([]types.Currency, d.ReadPrefix())
	for i := range r.MissedProofValues {
		r.MissedProofValues[i].DecodeFrom(d)
	}
	r.RefundAccount.DecodeFrom(d)
	copy(r.Signature[:], d.ReadBytes())
	r.HostSignature.DecodeFrom(d)
}

func (r *paymentResponse) EncodeTo(e *types.Encoder) {
	r.Signature.EncodeTo(e)
}

func (r *paymentResponse) DecodeFrom(d *types.Decoder) {
	r.Signature.DecodeFrom(d)
}

func paymentType(payment PaymentMethod) *types.Specifier {
	switch payment.(type) {
	case *PayByContractRequest:
		return &paymentTypeContract
	case *PayByEphemeralAccountRequest:
		return &paymentTypeEphemeralAccount
	default:
		panic("unhandled payment method")
	}
}

func (rpcPriceTableResponse) EncodeTo(e *types.Encoder)   {}
func (rpcPriceTableResponse) DecodeFrom(d *types.Decoder) {}

func (r *rpcUpdatePriceTableResponse) EncodeTo(e *types.Encoder) {
	e.WriteBytes(r.PriceTableJSON)
}
func (r *rpcUpdatePriceTableResponse) DecodeFrom(d *types.Decoder) {
	r.PriceTableJSON = d.ReadBytes()
}

func (r *rpcFundAccountRequest) EncodeTo(e *types.Encoder) {
	r.Account.EncodeTo(e)
}

func (r *rpcFundAccountRequest) DecodeFrom(d *types.Decoder) {
	r.Account.DecodeFrom(d)
}

func (r *rpcFundAccountResponse) EncodeTo(e *types.Encoder) {
	r.Balance.EncodeTo(e)
	r.Receipt.Host.EncodeTo(e)
	r.Receipt.Account.EncodeTo(e)
	r.Receipt.Amount.EncodeTo(e)
	e.WriteTime(r.Receipt.Timestamp)
	r.Signature.EncodeTo(e)
}

func (r *rpcFundAccountResponse) DecodeFrom(d *types.Decoder) {
	r.Balance.DecodeFrom(d)
	r.Receipt.Host.DecodeFrom(d)
	r.Receipt.Account.DecodeFrom(d)
	r.Receipt.Amount.DecodeFrom(d)
	r.Receipt.Timestamp = d.ReadTime()
	r.Signature.DecodeFrom(d)
}

func (i *instruction) EncodeTo(e *types.Encoder) {
	i.Specifier.EncodeTo(e)
	e.WriteBytes(i.Args)
}

func (i *instruction) DecodeFrom(d *types.Decoder) {
	i.Specifier.DecodeFrom(d)
	i.Args = d.ReadBytes()
}

func (r *rpcExecuteProgramRequest) EncodeTo(e *types.Encoder) {
	r.FileContractID.EncodeTo(e)
	e.WritePrefix(len(r.Program))
	for i := range r.Program {
		r.Program[i].EncodeTo(e)
	}
	e.WriteBytes(r.ProgramData)
}

func (r *rpcExecuteProgramRequest) DecodeFrom(d *types.Decoder) {
	r.FileContractID.DecodeFrom(d)
	r.Program = make([]instruction, d.ReadPrefix())
	for i := range r.Program {
		r.Program[i].DecodeFrom(d)
	}
	r.ProgramData = d.ReadBytes()
}

func (r *rpcExecuteProgramResponse) EncodeTo(e *types.Encoder) {
	r.AdditionalCollateral.EncodeTo(e)
	e.WriteUint64(r.OutputLength)
	r.NewMerkleRoot.EncodeTo(e)
	e.WriteUint64(r.NewSize)
	e.WritePrefix(len(r.Proof))
	for i := range r.Proof {
		r.Proof[i].EncodeTo(e)
	}
	var errString string
	if r.Error != nil {
		errString = r.Error.Error()
	}
	e.WriteString(errString)
	r.TotalCost.EncodeTo(e)
	r.FailureRefund.EncodeTo(e)
}

func (r *rpcExecuteProgramResponse) DecodeFrom(d *types.Decoder) {
	r.AdditionalCollateral.DecodeFrom(d)
	r.OutputLength = d.ReadUint64()
	r.NewMerkleRoot.DecodeFrom(d)
	r.NewSize = d.ReadUint64()
	r.Proof = make([]types.Hash256, d.ReadPrefix())
	for i := range r.Proof {
		r.Proof[i].DecodeFrom(d)
	}
	if s := d.ReadString(); s != "" {
		r.Error = errors.New(s)
	}
	r.TotalCost.DecodeFrom(d)
	r.FailureRefund.DecodeFrom(d)
}
