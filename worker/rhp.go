package worker

import (
	"context"
	"errors"
	"net"
	"time"

	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/renterd/metrics"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	rhpv3 "go.sia.tech/renterd/rhp/v3"
	"go.sia.tech/siad/crypto"
	"go.sia.tech/siad/types"
)

// MetricHostDial contains metrics relating to a host dial.
type MetricHostDial struct {
	HostKey   consensus.PublicKey
	HostIP    string
	Timestamp time.Time
	Elapsed   time.Duration
	Err       error
}

// IsMetric implements metrics.Metric.
func (MetricHostDial) IsMetric() {}

func dial(ctx context.Context, hostIP string, hostKey consensus.PublicKey) (net.Conn, error) {
	start := time.Now()
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", hostIP)
	metrics.Record(ctx, MetricHostDial{
		HostKey:   hostKey,
		HostIP:    hostIP,
		Timestamp: start,
		Elapsed:   time.Since(start),
		Err:       err,
	})
	return conn, err
}

type rhpImpl struct{}

func (rhpImpl) withTransportV2(ctx context.Context, hostIP string, hostKey consensus.PublicKey, fn func(*rhpv2.Transport) error) (err error) {
	conn, err := dial(ctx, hostIP, hostKey)
	if err != nil {
		return err
	}
	done := make(chan struct{})
	go func() {
		select {
		case <-done:
		case <-ctx.Done():
			conn.Close()
		}
	}()
	defer func() {
		close(done)
		if ctx.Err() != nil {
			err = ctx.Err()
		}
	}()
	t, err := rhpv2.NewRenterTransport(conn, hostKey)
	if err != nil {
		return err
	}
	defer t.Close()
	return fn(t)
}

func (rhpImpl) withTransportV3(ctx context.Context, hostIP string, hostKey consensus.PublicKey, fn func(*rhpv3.Transport) error) (err error) {
	conn, err := dial(ctx, hostIP, hostKey)
	if err != nil {
		return err
	}
	done := make(chan struct{})
	go func() {
		select {
		case <-done:
		case <-ctx.Done():
			conn.Close()
		}
	}()
	defer func() {
		close(done)
		if ctx.Err() != nil {
			err = ctx.Err()
		}
	}()
	t, err := rhpv3.NewRenterTransport(conn, hostKey)
	if err != nil {
		return err
	}
	defer t.Close()
	return fn(t)
}

func (r rhpImpl) Settings(ctx context.Context, hostIP string, hostKey consensus.PublicKey) (settings rhpv2.HostSettings, err error) {
	err = r.withTransportV2(ctx, hostIP, hostKey, func(t *rhpv2.Transport) error {
		settings, err = rhpv2.RPCSettings(ctx, t)
		return err
	})
	return
}

func (r rhpImpl) FormContract(ctx context.Context, cs consensus.State, hostIP string, hostKey consensus.PublicKey, renterKey consensus.PrivateKey, txns []types.Transaction) (rhpv2.Contract, []types.Transaction, error) {
	var contract rhpv2.Contract
	var txnSet []types.Transaction
	err := r.withTransportV2(ctx, hostIP, hostKey, func(t *rhpv2.Transport) error {
		var err error
		contract, txnSet, err = rhpv2.RPCFormContract(t, cs, renterKey, hostKey, txns)
		return err
	})
	return contract, txnSet, err
}

func (r rhpImpl) RenewContract(ctx context.Context, cs consensus.State, hostIP string, hostKey consensus.PublicKey, renterKey consensus.PrivateKey, contractID types.FileContractID, txns []types.Transaction, finalPayment types.Currency) (rhpv2.Contract, []types.Transaction, error) {
	var contract rhpv2.Contract
	var txnSet []types.Transaction
	err := r.withTransportV2(ctx, hostIP, hostKey, func(t *rhpv2.Transport) error {
		session, err := rhpv2.RPCLock(ctx, t, contractID, renterKey, 5*time.Second)
		if err != nil {
			return err
		}
		contract, txnSet, err = session.RenewContract(cs, txns, finalPayment)
		return err
	})
	return contract, txnSet, err
}

func (r rhpImpl) FundAccount(ctx context.Context, hostIP string, hostKey consensus.PublicKey, contract types.FileContractRevision, renterKey consensus.PrivateKey, account rhpv3.Account, amount types.Currency) (rhpv2.Contract, error) {
	var renterSig, hostSig consensus.Signature
	err := r.withTransportV3(ctx, hostIP, hostKey, func(t *rhpv3.Transport) (err error) {
		// The FundAccount RPC requires a SettingsID, which we also have to pay
		// for. To simplify things, we pay for the SettingsID using the full
		// amount, with the "refund" going to the desired account; we then top
		// up the account to cover the cost of the two RPCs.
		payment, ok := rhpv3.PayByContract(&contract, amount, account, renterKey)
		if !ok {
			return errors.New("insufficient funds")
		}
		priceTable, err := rhpv3.RPCPriceTable(t, &payment)
		if err != nil {
			return err
		}
		payment, ok = rhpv3.PayByContract(&contract, priceTable.UpdatePriceTableCost.Add(priceTable.FundAccountCost), rhpv3.ZeroAccount, renterKey)
		if !ok {
			return errors.New("insufficient funds")
		}
		err = rhpv3.RPCFundAccount(t, &payment, account, priceTable.ID)
		renterSig, hostSig = payment.Signature, payment.HostSignature
		return
	})
	if err != nil {
		return rhpv2.Contract{}, err
	}
	return rhpv2.Contract{
		Revision: contract,
		Signatures: [2]types.TransactionSignature{
			{
				ParentID:       crypto.Hash(contract.ID()),
				CoveredFields:  types.CoveredFields{FileContractRevisions: []uint64{0}},
				PublicKeyIndex: 0,
				Signature:      renterSig[:],
			},
			{
				ParentID:       crypto.Hash(contract.ID()),
				CoveredFields:  types.CoveredFields{FileContractRevisions: []uint64{0}},
				PublicKeyIndex: 1,
				Signature:      hostSig[:],
			},
		},
	}, nil
}

func (r rhpImpl) ReadRegistry(ctx context.Context, hostIP string, hostKey consensus.PublicKey, payment rhpv3.PaymentMethod, registryKey rhpv3.RegistryKey) (rhpv3.RegistryValue, error) {
	var value rhpv3.RegistryValue
	err := r.withTransportV3(ctx, hostIP, hostKey, func(t *rhpv3.Transport) (err error) {
		value, err = rhpv3.RPCReadRegistry(t, payment, registryKey)
		return
	})
	return value, err
}

func (r rhpImpl) UpdateRegistry(ctx context.Context, hostIP string, hostKey consensus.PublicKey, payment rhpv3.PaymentMethod, registryKey rhpv3.RegistryKey, registryValue rhpv3.RegistryValue) error {
	return r.withTransportV3(ctx, hostIP, hostKey, func(t *rhpv3.Transport) (err error) {
		return rhpv3.RPCUpdateRegistry(t, payment, registryKey, registryValue)
	})
}
