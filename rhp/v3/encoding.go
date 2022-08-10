package rhp

import (
	"fmt"
	"io"

	"gitlab.com/NebulousLabs/encoding"
	"go.sia.tech/siad/types"
)

// MarshalSia implements encoding.SiaMarshaler.
func (s *Specifier) MarshalSia(w io.Writer) error {
	_, err := w.Write(s[:])
	return err
}

// UnmarshalSia implements encoding.SiaUnmarshaler.
func (s *Specifier) UnmarshalSia(r io.Reader) error {
	_, err := r.Read(s[:])
	return err
}

// MarshalSia implements encoding.SiaMarshaler.
func (s *SettingsID) MarshalSia(w io.Writer) error {
	_, err := w.Write(s[:])
	return err
}

// UnmarshalSia implements encoding.SiaUnmarshaler.
func (s *SettingsID) UnmarshalSia(r io.Reader) error {
	_, err := r.Read(s[:])
	return err
}

// MarshalSia implements encoding.SiaMarshaler.
func (resp *rpcResponse) MarshalSia(w io.Writer) error {
	if resp.err != nil {
		return encoding.NewEncoder(w).EncodeAll(true, resp.err)
	}
	return encoding.NewEncoder(w).EncodeAll(false, resp.data)
}

func (resp *rpcResponse) UnmarshalSia(r io.Reader) error {
	d := encoding.NewDecoder(r, 1<<20)
	var isError bool
	if err := d.Decode(&isError); err != nil {
		return err
	} else if isError {
		resp.err = new(RPCError)
		return d.Decode(resp.err)
	}
	return d.Decode(resp.data)
}

// MarshalSia im
// MarshalSia implements encoding.SiaMarshaler.plements encoding.SiaMarshaler.
func (a *Account) MarshalSia(w io.Writer) error {
	if *a == ZeroAccount {
		return (types.SiaPublicKey{}).MarshalSia(w)
	}
	return (types.SiaPublicKey{
		Algorithm: types.SignatureEd25519,
		Key:       a[:],
	}).MarshalSia(w)
}

// UnmarshalSia implements encoding.SiaUnmarshaler.
func (a *Account) UnmarshalSia(r io.Reader) error {
	var spk types.SiaPublicKey
	if err := spk.UnmarshalSia(r); err != nil {
		return err
	} else if spk.Algorithm == (types.Specifier{}) && len(spk.Key) == 0 {
		*a = ZeroAccount
		return nil
	} else if spk.Algorithm != types.SignatureEd25519 {
		return fmt.Errorf("unsupported signature algorithm: %v", spk.Algorithm)
	}
	copy(a[:], spk.Key)
	return nil
}

// MarshalSia implements encoding.SiaMarshaler.
func (r *PayByEphemeralAccountRequest) MarshalSia(w io.Writer) error {
	return encoding.NewEncoder(w).EncodeAll(r.Account, r.Expiry, r.Account, r.Nonce, r.Signature, r.Priority)
}

// UnmarshalSia implements encoding.SiaUnmarshaler.
func (r *PayByEphemeralAccountRequest) UnmarshalSia(rd io.Reader) error {
	return encoding.NewDecoder(rd, 4096).DecodeAll(&r.Account, &r.Expiry, &r.Account, &r.Nonce, &r.Signature, &r.Priority)
}

// MarshalSia implements encoding.SiaMarshaler.
func (r *PayByContractRequest) MarshalSia(w io.Writer) error {
	return encoding.NewEncoder(w).EncodeAll(r.ContractID, r.NewRevisionNumber, r.NewValidProofValues, r.NewMissedProofValues, r.RefundAccount, r.Signature)
}

// UnmarshalSia implements encoding.SiaUnmarshaler.
func (r *PayByContractRequest) UnmarshalSia(rd io.Reader) error {
	return encoding.NewDecoder(rd, 4096).DecodeAll(&r.ContractID, &r.NewRevisionNumber, &r.NewValidProofValues, &r.NewMissedProofValues, &r.RefundAccount, &r.Signature)
}

func (r *paymentResponse) MarshalSia(w io.Writer) error {
	return encoding.NewEncoder(w).EncodeAll(r.Signature)
}

func (r *paymentResponse) UnmarshalSia(rd io.Reader) error {
	return encoding.NewDecoder(rd, 4096).DecodeAll(&r.Signature)
}

func paymentType(payment PaymentMethod) *Specifier {
	switch payment.(type) {
	case *PayByContractRequest:
		return &paymentTypeContract
	case *PayByEphemeralAccountRequest:
		return &paymentTypeEphemeralAccount
	default:
		panic("unhandled payment method")
	}
}

func (rpcPriceTableResponse) MarshalSia(w io.Writer) error    { return nil }
func (rpcPriceTableResponse) UnmarshalSia(rd io.Reader) error { return nil }

func (r *rpcFundAccountRequest) MarshalSia(w io.Writer) error {
	return encoding.NewEncoder(w).EncodeAll(r.Account)
}

func (r *rpcFundAccountRequest) UnmarshalSia(rd io.Reader) error {
	return encoding.NewDecoder(rd, 4096).DecodeAll(&r.Account)
}

func (r *rpcFundAccountResponse) MarshalSia(w io.Writer) error {
	return encoding.NewEncoder(w).EncodeAll(r.Balance, r.Receipt, r.Signature)
}

func (r *rpcFundAccountResponse) UnmarshalSia(rd io.Reader) error {
	return encoding.NewDecoder(rd, 4096).DecodeAll(&r.Balance, &r.Receipt, &r.Signature)
}

func (r *rpcExecuteProgramRequest) MarshalSia(w io.Writer) error {
	return encoding.NewEncoder(w).EncodeAll(r.FileContractID, r.Program, r.ProgramData)
}

func (r *rpcExecuteProgramRequest) UnmarshalSia(rd io.Reader) error {
	return encoding.NewDecoder(rd, 4096).DecodeAll(&r.FileContractID, &r.Program, &r.ProgramData)
}

func (r *rpcExecuteProgramResponse) MarshalSia(w io.Writer) error {
	return encoding.NewEncoder(w).EncodeAll(r.AdditionalCollateral, r.OutputLength, r.NewMerkleRoot, r.NewSize, r.Proof, r.Error, r.TotalCost, r.FailureRefund)
}

func (r *rpcExecuteProgramResponse) UnmarshalSia(rd io.Reader) error {
	return encoding.NewDecoder(rd, 4096).DecodeAll(&r.AdditionalCollateral, &r.OutputLength, &r.NewMerkleRoot, &r.NewSize, &r.Proof, &r.Error, &r.TotalCost, &r.FailureRefund)
}
