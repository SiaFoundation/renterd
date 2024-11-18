package rhp

import (
	"context"
	"errors"
	"io"
	"net"
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
		hs = HostSettings{
			HostSettings: settings,
			Validity:     time.Until(settings.Prices.ValidUntil),
		}
		return err
	})
	return hs, err
}

// ReadSector reads a sector from the host.
func (c *Client) ReadSector(ctx context.Context, prices rhp4.HostPrices, token rhp4.AccountToken, w io.Writer, root types.Hash256, offset, length uint64) (rhp.RPCReadSectorResult, error) {
	panic("not implemented")
}

// WriteSector writes a sector to the host.
func (c *Client) WriteSector(ctx context.Context, prices rhp4.HostPrices, token rhp4.AccountToken, rl rhp.ReaderLen, duration uint64) (rhp.RPCWriteSectorResult, error) {
	panic("not implemented")
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
func (c *Client) FundAccounts(ctx context.Context, cs consensus.State, signer rhp.ContractSigner, contract rhp.ContractRevision, deposits []rhp4.AccountDeposit) (rhp.RPCFundAccountResult, error) {
	panic("not implemented")
}

// LatestRevision returns the latest revision of a contract.
func (c *Client) LatestRevision(ctx context.Context, contractID types.FileContractID) (types.V2FileContract, error) {
	panic("not implemented")
}

// SectorRoots returns the sector roots for a contract.
func (c *Client) SectorRoots(ctx context.Context, cs consensus.State, prices rhp4.HostPrices, signer rhp.ContractSigner, contract rhp.ContractRevision, offset, length uint64) (rhp.RPCSectorRootsResult, error) {
	panic("not implemented")
}

// AccountBalance returns the balance of an account.
func (c *Client) AccountBalance(ctx context.Context, account rhp4.Account) (types.Currency, error) {
	panic("not implemented")
}

// FormContract forms a contract with a host
func (c *Client) FormContract(ctx context.Context, tp rhp.TxPool, signer rhp.FormContractSigner, cs consensus.State, p rhp4.HostPrices, hostKey types.PublicKey, hostAddress types.Address, params rhp4.RPCFormContractParams) (rhp.RPCFormContractResult, error) {
	panic("not implemented")
}

// RenewContract renews a contract with a host.
func (c *Client) RenewContract(ctx context.Context, tp rhp.TxPool, signer rhp.FormContractSigner, cs consensus.State, p rhp4.HostPrices, existing types.V2FileContract, params rhp4.RPCRenewContractParams) (rhp.RPCRenewContractResult, error) {
	panic("not implemented")
}

// RefreshContract refreshes a contract with a host.
func (c *Client) RefreshContract(ctx context.Context, tp rhp.TxPool, signer rhp.FormContractSigner, cs consensus.State, p rhp4.HostPrices, existing types.V2FileContract, params rhp4.RPCRefreshContractParams) (rhp.RPCRefreshContractResult, error) {
	panic("not implemented")
}
