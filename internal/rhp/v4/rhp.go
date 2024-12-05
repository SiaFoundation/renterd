package rhp

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"time"

	"go.sia.tech/core/consensus"
	rhp4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	rhp "go.sia.tech/coreutils/rhp/v4"
)

var (
	// errDialTransport is returned when the worker could not dial the host.
	ErrDialTransport = errors.New("could not dial transport")
)

type (
	Dialer interface {
		Dial(ctx context.Context, hk types.PublicKey, address string) (net.Conn, error)
	}

	HostSettings struct {
		rhp4.HostSettings
		Validity time.Duration `json:"validity"`
	}
)

type Client struct {
	tpool *transportPool
}

func New(dialer Dialer) *Client {
	return &Client{
		tpool: newTransportPool(dialer),
	}
}

func (c *Client) Settings(ctx context.Context, hk types.PublicKey, addr string) (hs HostSettings, _ error) {
	err := c.tpool.withTransport(ctx, hk, addr, func(c rhp.TransportClient) error {
		var settings rhp4.HostSettings
		settings, err := rhp.RPCSettings(ctx, c)
		if err != nil {
			return err
		}
		var validity time.Duration
		if v := time.Until(settings.Prices.ValidUntil); v > 0 {
			validity = v
		}
		hs = HostSettings{
			HostSettings: settings,
			Validity:     validity,
		}
		return err
	})
	return hs, err
}

// ReadSector reads a sector from the host.
func (c *Client) ReadSector(ctx context.Context, hk types.PublicKey, hostIP string, prices rhp4.HostPrices, token rhp4.AccountToken, w io.Writer, root types.Hash256, offset, length uint64) (res rhp.RPCReadSectorResult, _ error) {
	err := c.tpool.withTransport(ctx, hk, hostIP, func(t rhp.TransportClient) (err error) {
		res, err = rhp.RPCReadSector(ctx, t, prices, token, w, root, offset, length)
		return
	})
	return res, err
}

// WriteSector writes a sector to the host.
func (c *Client) WriteSector(ctx context.Context, hk types.PublicKey, hostIP string, contract rhp.ContractRevision, prices rhp4.HostPrices, sk types.PrivateKey, token rhp4.AccountToken, rl rhp.ReaderLen, length, duration uint64) (res1 rhp.RPCWriteSectorResult, res2 rhp.RPCAppendSectorsResult, _ error) {
	err := c.tpool.withTransport(ctx, hk, hostIP, func(t rhp.TransportClient) (err error) {
		res1, err = rhp.RPCWriteSector(ctx, t, prices, token, rl, length, duration)
		if err != nil {
			return
		}

		// NOTE: construct an empty state object here to pass to
		// RPCAppendSectors since it only uses it for hashing
		cs := consensus.State{}

		// NOTE: immediately append the sector for the time being, eventually
		// this will be a 2-step process where uploads are unblocked as soon as
		// the sector is on the host, but not yet added to the contract
		res2, err = rhp.RPCAppendSectors(ctx, t, cs, prices, sk, contract, []types.Hash256{res1.Root})
		return
	})
	return res1, res2, err
}

// VerifySector verifies that the host is properly storing a sector
func (c *Client) VerifySector(ctx context.Context, prices rhp4.HostPrices, token rhp4.AccountToken, root types.Hash256) (rhp.RPCVerifySectorResult, error) {
	panic("not implemented")
}

// FreeSectors removes sectors from a contract.
func (c *Client) FreeSectors(ctx context.Context, cs consensus.State, prices rhp4.HostPrices, sk types.PrivateKey, contract rhp.ContractRevision, indices []uint64) (rhp.RPCFreeSectorsResult, error) {
	panic("not implemented")
}

// AppendSectors appends sectors a host is storing to a contract.
func (c *Client) AppendSectors(ctx context.Context, cs consensus.State, prices rhp4.HostPrices, sk types.PrivateKey, contract rhp.ContractRevision, roots []types.Hash256) (rhp.RPCAppendSectorsResult, error) {
	panic("not implemented")
}

// FundAccounts funds accounts on the host.
func (c *Client) FundAccounts(ctx context.Context, hk types.PublicKey, hostIP string, cs consensus.State, signer rhp.ContractSigner, contract rhp.ContractRevision, deposits []rhp4.AccountDeposit) error {
	err := c.tpool.withTransport(ctx, hk, hostIP, func(c rhp.TransportClient) (err error) {
		_, err = rhp.RPCFundAccounts(ctx, c, cs, signer, contract, deposits)
		if err != nil {
			return err
		}
		return nil
	})
	return err
}

// LatestRevision returns the latest revision of a contract.
func (c *Client) LatestRevision(ctx context.Context, hk types.PublicKey, addr string, contractID types.FileContractID) (revision types.V2FileContract, _ error) {
	err := c.tpool.withTransport(ctx, hk, addr, func(c rhp.TransportClient) (err error) {
		revision, err = rhp.RPCLatestRevision(ctx, c, contractID)
		return err
	})
	return revision, err
}

// SectorRoots returns the sector roots for a contract.
func (c *Client) SectorRoots(ctx context.Context, cs consensus.State, prices rhp4.HostPrices, signer rhp.ContractSigner, contract rhp.ContractRevision, offset, length uint64) (rhp.RPCSectorRootsResult, error) {
	panic("not implemented")
}

// AccountBalance returns the balance of an account.
func (c *Client) AccountBalance(ctx context.Context, hk types.PublicKey, hostIP string, account rhp4.Account) (balance types.Currency, _ error) {
	err := c.tpool.withTransport(ctx, hk, hostIP, func(c rhp.TransportClient) (err error) {
		balance, err = rhp.RPCAccountBalance(ctx, c, account)
		if err != nil {
			// TODO: remove this hack once the host is fixed
			if strings.Contains(err.Error(), "internal error") {
				err = nil
				balance = types.ZeroCurrency
			}
			return err
		}
		return err
	})
	return balance, err
}

// FormContract forms a contract with a host
func (c *Client) FormContract(ctx context.Context, hk types.PublicKey, hostIP string, tp rhp.TxPool, signer rhp.FormContractSigner, cs consensus.State, p rhp4.HostPrices, hostAddress types.Address, params rhp4.RPCFormContractParams) (res rhp.RPCFormContractResult, _ error) {
	err := c.tpool.withTransport(ctx, hk, hostIP, func(c rhp.TransportClient) (err error) {
		res, err = rhp.RPCFormContract(ctx, c, tp, signer, cs, p, hk, hostAddress, params)
		if err != nil {
			return err
		}
		return err
	})
	return res, err
}

// RenewContract renews a contract with a host.
func (c *Client) RenewContract(ctx context.Context, hk types.PublicKey, hostIP string, tp rhp.TxPool, signer rhp.FormContractSigner, cs consensus.State, p rhp4.HostPrices, existing types.V2FileContract, params rhp4.RPCRenewContractParams) (res rhp.RPCRenewContractResult, _ error) {
	err := c.tpool.withTransport(ctx, hk, hostIP, func(c rhp.TransportClient) (err error) {
		res, err = rhp.RPCRenewContract(ctx, c, tp, signer, cs, p, existing, params)
		if err != nil {
			return err
		}
		return err
	})
	return res, err
}

// RefreshContract refreshes a contract with a host.
func (c *Client) RefreshContract(ctx context.Context, hk types.PublicKey, hostIP string, tp rhp.TxPool, signer rhp.FormContractSigner, cs consensus.State, p rhp4.HostPrices, existing types.V2FileContract, params rhp4.RPCRefreshContractParams) (res rhp.RPCRefreshContractResult, _ error) {
	err := c.tpool.withTransport(ctx, hk, hostIP, func(c rhp.TransportClient) (err error) {
		res, err = rhp.RPCRefreshContract(ctx, c, tp, signer, cs, p, existing, params)
		if err != nil {
			return err
		}
		return err
	})
	return res, err
}
