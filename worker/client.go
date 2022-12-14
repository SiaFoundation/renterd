package worker

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"go.sia.tech/jape"
	"go.sia.tech/renterd/bus"
	"go.sia.tech/renterd/object"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	rhpv3 "go.sia.tech/renterd/rhp/v3"
	"go.sia.tech/siad/types"
)

// A Client provides methods for interacting with a renterd API server.
type Client struct {
	c jape.Client
}

// RHPScan scans a host, returning its current settings.
func (c *Client) RHPScan(hostKey PublicKey, hostIP string, timeout time.Duration) (resp RHPScanResponse, err error) {
	err = c.c.POST("/rhp/scan", RHPScanRequest{hostKey, hostIP, timeout}, &resp)
	return
}

// RHPPrepareForm prepares a contract formation transaction.
func (c *Client) RHPPrepareForm(renterKey PrivateKey, hostKey PublicKey, renterFunds types.Currency, renterAddress types.UnlockHash, hostCollateral types.Currency, endHeight uint64, hostSettings rhpv2.HostSettings) (types.FileContract, types.Currency, error) {
	req := RHPPrepareFormRequest{
		RenterKey:      renterKey,
		HostKey:        hostKey,
		RenterFunds:    renterFunds,
		RenterAddress:  renterAddress,
		HostCollateral: hostCollateral,
		EndHeight:      endHeight,
		HostSettings:   hostSettings,
	}
	var resp RHPPrepareFormResponse
	err := c.c.POST("/rhp/prepare/form", req, &resp)
	return resp.Contract, resp.Cost, err
}

// RHPPreparePayment prepares an ephemeral account payment.
func (c *Client) RHPPreparePayment(account rhpv3.Account, amount types.Currency, key PrivateKey) (resp rhpv3.PayByEphemeralAccountRequest, err error) {
	req := RHPPreparePaymentRequest{
		Account:    account,
		Amount:     amount,
		Expiry:     0, // TODO
		AccountKey: key,
	}
	err = c.c.POST("/rhp/prepare/payment", req, &resp)
	return
}

// RHPForm forms a contract with a host.
func (c *Client) RHPForm(renterKey PrivateKey, hostKey PublicKey, hostIP string, transactionSet []types.Transaction) (rhpv2.ContractRevision, []types.Transaction, error) {
	req := RHPFormRequest{
		RenterKey:      renterKey,
		HostKey:        hostKey,
		HostIP:         hostIP,
		TransactionSet: transactionSet,
	}
	var resp RHPFormResponse
	err := c.c.POST("/rhp/form", req, &resp)
	return resp.Contract, resp.TransactionSet, err
}

func (c *Client) withWS(route string, fn func(conn *websocket.Conn) error) error {
	baseURL := strings.TrimPrefix(c.c.BaseURL, "http://") // TODO: cleanup
	h := http.Header{}
	if c.c.Password != "" {
		pw := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf(":%s", c.c.Password)))
		h.Set("Authorization", "Basic "+pw)
	}
	conn, resp, err := websocket.DefaultDialer.Dial(fmt.Sprintf("ws://%s%s", baseURL, route), h)
	if err == websocket.ErrBadHandshake {
		log.Printf("handshake failed with status %d", resp.StatusCode)
	}
	if err != nil {
		return err
	}
	defer conn.Close()
	return fn(conn)
}

func (c *Client) RHPRenew(fcid types.FileContractID, renterKey PrivateKey, hostKey PublicKey, renterFunds types.Currency, renterAddress types.UnlockHash, hostCollateral types.Currency, endHeight uint64, hostSettings rhpv2.HostSettings, fn SigningFn) (rhpv2.ContractRevision, []types.Transaction, error) {
	var newContract rhpv2.ContractRevision
	var txnSet []types.Transaction
	err := c.withWS("/rhp/renew", func(conn *websocket.Conn) error {
		err := conn.WriteJSON(RHPPrepareRenewRequest{
			ContractID:     fcid,
			RenterKey:      renterKey,
			HostKey:        hostKey,
			RenterFunds:    renterFunds,
			RenterAddress:  renterAddress,
			HostCollateral: hostCollateral,
			EndHeight:      endHeight,
			HostSettings:   hostSettings,
		})
		if err != nil {
			return err
		}
		var prrr RHPPrepareRenewResponse
		if err := conn.ReadJSON(&prrr); err != nil {
			return err
		}
		if prrr.Error != "" {
			return errors.New(prrr.Error)
		}
		initialTxns, cleanup, err := fn(prrr.Contract, prrr.Cost)
		if err != nil {
			return err
		}
		err = conn.WriteJSON(RHPRenewRequest{
			TransactionSet: initialTxns,
			FinalPayment:   prrr.FinalPayment,
		})
		if err != nil {
			cleanup()
			return err
		}
		var rrr RHPRenewResponse
		if err := conn.ReadJSON(&rrr); err != nil {
			cleanup()
			return err
		}
		if rrr.Error != "" {
			cleanup()
			return errors.New(rrr.Error)
		}
		newContract = rrr.Contract
		txnSet = rrr.TransactionSet
		return nil
	})
	if err != nil && websocket.IsUnexpectedCloseError(err) {
		msg := err.(*websocket.CloseError).Text
		err = fmt.Errorf("connection was closed unexpectedly: %s; %w", msg, err)
	}
	return newContract, txnSet, err
}

// RHPFund funds an ephemeral account using the supplied contract.
func (c *Client) RHPFund(contract types.FileContractRevision, renterKey PrivateKey, hostKey PublicKey, hostIP string, account rhpv3.Account, amount types.Currency) (err error) {
	req := RHPFundRequest{
		Contract:  contract,
		RenterKey: renterKey,
		HostKey:   hostKey,
		HostIP:    hostIP,
		Account:   account,
		Amount:    amount,
	}
	err = c.c.POST("/rhp/fund", req, nil)
	return
}

// RHPReadRegistry reads a registry value.
func (c *Client) RHPReadRegistry(hostKey PublicKey, hostIP string, key rhpv3.RegistryKey, payment rhpv3.PayByEphemeralAccountRequest) (resp rhpv3.RegistryValue, err error) {
	req := RHPRegistryReadRequest{
		HostKey:     hostKey,
		HostIP:      hostIP,
		RegistryKey: key,
		Payment:     payment,
	}
	err = c.c.POST("/rhp/registry/read", req, &resp)
	return
}

// RHPUpdateRegistry updates a registry value.
func (c *Client) RHPUpdateRegistry(hostKey PublicKey, hostIP string, key rhpv3.RegistryKey, value rhpv3.RegistryValue, payment rhpv3.PayByEphemeralAccountRequest) (err error) {
	req := RHPRegistryUpdateRequest{
		HostKey:       hostKey,
		HostIP:        hostIP,
		RegistryKey:   key,
		RegistryValue: value,
		Payment:       payment,
	}
	err = c.c.POST("/rhp/registry/update", req, nil)
	return
}

// MigrateSlab migrates the specified slab.
func (c *Client) MigrateSlab(slab object.Slab) error {
	return c.c.POST("/slabs/migrate", slab, nil)
}

// UploadObject uploads the data in r, creating an object with the given name.
func (c *Client) UploadObject(r io.Reader, name string) (err error) {
	c.c.Custom("PUT", fmt.Sprintf("/objects/%s", name), []byte{}, nil)

	req, err := http.NewRequest("PUT", fmt.Sprintf("%v/objects/%v", c.c.BaseURL, name), r)
	if err != nil {
		panic(err)
	}
	req.SetBasicAuth("", c.c.Password)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer io.Copy(ioutil.Discard, resp.Body)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		err, _ := ioutil.ReadAll(resp.Body)
		return errors.New(string(err))
	}
	return
}

func (c *Client) object(path string, w io.Writer, entries *[]string) (err error) {
	c.c.Custom("GET", fmt.Sprintf("/objects/%s", path), nil, (*[]string)(nil))

	req, err := http.NewRequest("GET", fmt.Sprintf("%v/objects/%v", c.c.BaseURL, path), nil)
	if err != nil {
		panic(err)
	}
	req.SetBasicAuth("", c.c.Password)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer io.Copy(ioutil.Discard, resp.Body)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		err, _ := ioutil.ReadAll(resp.Body)
		return errors.New(string(err))
	}
	if w != nil {
		_, err = io.Copy(w, resp.Body)
	} else {
		err = json.NewDecoder(resp.Body).Decode(entries)
	}
	return
}

// ObjectEntries returns the entries at the given path, which must end in /.
func (c *Client) ObjectEntries(path string) (entries []string, err error) {
	err = c.object(path, nil, &entries)
	return
}

// DownloadObject downloads the object at the given path, writing its data to
// w.
func (c *Client) DownloadObject(w io.Writer, path string) (err error) {
	err = c.object(path, w, nil)
	return
}

// DeleteObject deletes the object with the given name.
func (c *Client) DeleteObject(name string) (err error) {
	err = c.c.DELETE(fmt.Sprintf("/objects/%s", name))
	return
}

func (c *Client) Revisions(rk [32]byte, contracts []bus.Contract) (revisions []rhpv2.ContractRevision, err error) {
	err = c.c.POST("/rhp/revisions", &RHPRevisionsRequest{
		Contracts: contracts,
		RenterKey: rk,
	}, &revisions)
	return
}

// NewClient returns a client that communicates with a renterd worker server
// listening on the specified address.
func NewClient(addr, password string) *Client {
	return &Client{jape.Client{
		BaseURL:  addr,
		Password: password,
	}}
}
