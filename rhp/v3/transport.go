package rhp

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"

	"gitlab.com/NebulousLabs/encoding"
	"go.sia.tech/renterd/internal/mux"
	"go.sia.tech/siad/types"
)

func wrapErr(err *error, fnName string) {
	if *err != nil {
		*err = fmt.Errorf("%s: %w", fnName, *err)
	}
}

// An RPCError may be sent instead of a response object to any RPC.
type RPCError struct {
	Type        Specifier
	Data        []byte // structure depends on Type
	Description string // human-readable error string
}

// Error implements the error interface.
func (e *RPCError) Error() string {
	return e.Description
}

// Is reports whether this error matches target.
func (e *RPCError) Is(target error) bool {
	return strings.Contains(e.Description, target.Error())
}

// helper type for encoding and decoding RPC response messages, which can
// represent either valid data or an error.
type rpcResponse struct {
	err  *RPCError
	data protocolObject
}

type protocolObject interface {
	encoding.SiaMarshaler
	encoding.SiaUnmarshaler
}

func writeRequest(s *mux.Stream, id Specifier, req protocolObject) error {
	if _, err := s.Write(id[:]); err != nil {
		return err
	}
	return req.MarshalSia(s)
}

func readResponse(s *mux.Stream, resp protocolObject) error {
	rr := rpcResponse{nil, resp}
	if err := rr.UnmarshalSia(s); err != nil {
		return err
	} else if rr.err != nil {
		return rr.err
	}
	return nil
}

func writeResponse(s *mux.Stream, resp protocolObject) error {
	return (&rpcResponse{nil, resp}).MarshalSia(s)
}

func writeResponseErr(s *mux.Stream, err error) error {
	return (&rpcResponse{&RPCError{Description: err.Error()}, nil}).MarshalSia(s)
}

func processPayment(s *mux.Stream, payment PaymentMethod) error {
	if err := writeResponse(s, paymentType(payment)); err != nil {
		return err
	} else if err := writeResponse(s, payment); err != nil {
		return err
	}
	if pbcr, ok := payment.(*PayByContractRequest); ok {
		var pr paymentResponse
		if err := readResponse(s, &pr); err != nil {
			return err
		}
		pbcr.HostSignature = pr.Signature
	}
	return nil
}

// A Transport facilitates the exchange of RPCs via the renter-host protocol,
// version 3.
type Transport struct {
	mux *mux.Mux
}

// DialStream opens a new stream with the host.
func (t *Transport) DialStream() *mux.Stream {
	buf := make([]byte, 8+8+len("host"))
	binary.LittleEndian.PutUint64(buf[8:], uint64(len(buf[16:])))
	binary.LittleEndian.PutUint64(buf[:8], uint64(len(buf[8:])))
	copy(buf[16:], "host")

	s := t.mux.DialStream()
	s.Write(buf)
	return s
}

// Close closes the protocol connection.
func (t *Transport) Close() error {
	return t.mux.Close()
}

// NewRenterTransport establishes a new RHPv3 session over the supplied connection.
func NewRenterTransport(conn net.Conn, hostKey PublicKey) (*Transport, error) {
	m, err := mux.Dial(conn, hostKey[:])
	if err != nil {
		return nil, err
	}
	return &Transport{
		mux: m,
	}, nil
}

// RPCPriceTable calls the UpdatePriceTable RPC.
func RPCPriceTable(t *Transport, payment PaymentMethod) (pt HostPriceTable, err error) {
	defer wrapErr(&err, "PriceTable")
	s := t.DialStream()
	defer s.Close()

	var js []byte
	if _, err := s.Write(rpcUpdatePriceTableID[:]); err != nil {
		return HostPriceTable{}, err
	} else if err := encoding.NewDecoder(s, 1<<20).Decode(&js); err != nil {
		return HostPriceTable{}, err
	} else if err := json.Unmarshal(js, &pt); err != nil {
		return HostPriceTable{}, err
	} else if err := processPayment(s, payment); err != nil {
		return HostPriceTable{}, err
	} else if err := readResponse(s, rpcPriceTableResponse{}); err != nil {
		return HostPriceTable{}, err
	}
	return pt, nil
}

// RPCAccountBalance calls the AccountBalance RPC.
func RPCAccountBalance(t *Transport, account Account, price, collateral types.Currency) (bal types.Currency, err error) {
	defer wrapErr(&err, "AccountBalance")
	s := t.DialStream()
	defer s.Close()

	if err := writeRequest(s, rpcAccountBalanceID, &account); err != nil {
		return types.ZeroCurrency, err
	} else if err := readResponse(s, &bal); err != nil {
		return types.ZeroCurrency, err
	}
	return
}

// RPCFundAccount calls the FundAccount RPC.
func RPCFundAccount(t *Transport, payment PaymentMethod, account Account, settingsID SettingsID) (err error) {
	defer wrapErr(&err, "FundAccount")
	s := t.DialStream()
	defer s.Close()

	req := rpcFundAccountRequest{
		Account: account,
	}
	var resp rpcFundAccountResponse
	if _, err := s.Write(rpcFundAccountID[:]); err != nil {
		return err
	} else if err := writeResponse(s, &settingsID); err != nil {
		return err
	} else if err := writeResponse(s, &req); err != nil {
		return err
	} else if err := processPayment(s, payment); err != nil {
		return err
	} else if err := readResponse(s, &resp); err != nil {
		return err
	}
	return nil
}

// RPCReadRegistry calls the ExecuteProgram RPC with an MDM program that reads
// the specified registry value.
func RPCReadRegistry(t *Transport, payment PaymentMethod, key RegistryKey) (rv RegistryValue, err error) {
	defer wrapErr(&err, "ReadRegistry")
	s := t.DialStream()
	defer s.Close()

	req := &rpcExecuteProgramRequest{
		FileContractID: types.FileContractID{},
		Program: []instruction{{
			Specifier: newSpecifier("ReadRegistry"),
			Args:      encoding.MarshalAll(0, 32),
		}},
		ProgramData: encoding.MarshalAll(key.PublicKey, key.Tweak),
	}
	if _, err := s.Write(rpcExecuteProgramID[:]); err != nil {
		return RegistryValue{}, err
	} else if err := processPayment(s, payment); err != nil {
		return RegistryValue{}, err
	} else if err := writeResponse(s, req); err != nil {
		return RegistryValue{}, err
	}

	var cancellationToken Specifier
	readResponse(s, &cancellationToken) // unused

	var resp rpcExecuteProgramResponse
	if err := readResponse(s, &resp); err != nil {
		return RegistryValue{}, err
	} else if resp.OutputLength < 64+8+1 {
		return RegistryValue{}, errors.New("invalid output length")
	}
	buf := make([]byte, resp.OutputLength)
	if _, err := s.Read(buf); err != nil {
		return RegistryValue{}, err
	}
	var sig Signature
	copy(sig[:], buf[:64])
	rev := binary.BigEndian.Uint64(buf[64:72])
	data := buf[72 : len(buf)-1]
	typ := buf[len(buf)-1]
	return RegistryValue{
		Data:      data,
		Revision:  rev,
		Type:      typ,
		Signature: sig,
	}, nil
}

// RPCUpdateRegistry calls the ExecuteProgram RPC with an MDM program that
// updates the specified registry value.
func RPCUpdateRegistry(t *Transport, payment PaymentMethod, key RegistryKey, value RegistryValue) (err error) {
	defer wrapErr(&err, "UpdateRegistry")
	s := t.DialStream()
	defer s.Close()

	req := &rpcExecuteProgramRequest{
		FileContractID: types.FileContractID{},
		Program: []instruction{{
			Specifier: newSpecifier("UpdateRegistry"),
			Args:      encoding.Marshal(0),
		}},
		ProgramData: append(encoding.MarshalAll(key.Tweak, value.Revision, value.Signature, key.PublicKey), value.Data...),
	}
	if _, err := s.Write(rpcExecuteProgramID[:]); err != nil {
		return err
	} else if err := processPayment(s, payment); err != nil {
		return err
	} else if err := writeResponse(s, req); err != nil {
		return err
	}

	var cancellationToken Specifier
	readResponse(s, &cancellationToken) // unused

	var resp rpcExecuteProgramResponse
	if err := readResponse(s, &resp); err != nil {
		return err
	} else if resp.OutputLength != 0 {
		return errors.New("invalid output length")
	}
	return nil
}
