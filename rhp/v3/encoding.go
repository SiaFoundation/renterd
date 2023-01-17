package rhp

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"gitlab.com/NebulousLabs/encoding"
	"go.sia.tech/renterd/internal/consensus"
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

// String prints the uid in hex.
func (s SettingsID) String() string {
	return hex.EncodeToString(s[:])
}

// LoadString loads the unique id from the given string. It is the inverse of
// the `String` method.
func (s *SettingsID) LoadString(input string) error {
	// *2 because there are 2 hex characters per byte.
	if len(input) != types.SpecifierLen*2 {
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
	// *2 because there are 2 hex characters per byte.
	// +2 because the encoded JSON string is wrapped in `"`.
	if len(b) != len(SettingsID{})*2+2 {
		return errors.New("incorrect length")
	}

	// b[1 : len(b)-1] cuts off the leading and trailing `"` in the JSON string.
	return s.LoadString(string(b[1 : len(b)-1]))
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

// MarshalSia implements encoding.SiaMarshaler.
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

// MarshalJSON implements json.Marshaler.
func (a Account) MarshalJSON() ([]byte, error) {
	return consensus.PublicKey(a).MarshalJSON()
}

// MarshalJSON implements json.Unmarshaler.
func (a *Account) UnmarshalJSON(b []byte) error {
	return (*consensus.PublicKey)(a).UnmarshalJSON(b)
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
