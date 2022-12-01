package worker

import (
	"strconv"
	"time"

	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/renterd/object"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	rhpv3 "go.sia.tech/renterd/rhp/v3"
	"go.sia.tech/siad/types"
)

// exported types from internal/consensus
type (
	// A ChainIndex pairs a block's height with its ID.
	ChainIndex = consensus.ChainIndex

	// A PublicKey is an Ed25519 public key.
	PublicKey = consensus.PublicKey

	// A PrivateKey is an Ed25519 private key.
	PrivateKey = consensus.PrivateKey
)

// A Duration is the elapsed time between two instants. Durations are encoded as
// an integer number of milliseconds.
type Duration time.Duration

// MarshalText implements encoding.TextMarshaler.
func (d Duration) MarshalText() ([]byte, error) {
	return []byte(strconv.FormatInt(time.Duration(d).Milliseconds(), 10)), nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (d *Duration) UnmarshalText(b []byte) error {
	ms, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return err
	}
	*d = Duration(time.Duration(ms) * time.Millisecond)
	return nil
}

// RHPScanRequest is the request type for the /rhp/scan endpoint.
type RHPScanRequest struct {
	HostKey PublicKey     `json:"hostKey"`
	HostIP  string        `json:"hostIP"`
	Timeout time.Duration `json:"timeout"`
}

// RHPScanResponse is the response type for the /rhp/scan endpoint.
type RHPScanResponse struct {
	Settings rhpv2.HostSettings `json:"settings"`
	Ping     Duration           `json:"ping"`
}

// RHPPrepareFormRequest is the request type for the /rhp/prepare/form endpoint.
type RHPPrepareFormRequest struct {
	RenterKey      PrivateKey         `json:"renterKey"`
	HostKey        PublicKey          `json:"hostKey"`
	RenterFunds    types.Currency     `json:"renterFunds"`
	RenterAddress  types.UnlockHash   `json:"renterAddress"`
	HostCollateral types.Currency     `json:"hostCollateral"`
	EndHeight      uint64             `json:"endHeight"`
	HostSettings   rhpv2.HostSettings `json:"hostSettings"`
}

// RHPPrepareFormResponse is the response type for the /rhp/prepare/form
// endpoint.
type RHPPrepareFormResponse struct {
	Contract types.FileContract `json:"contract"`
	Cost     types.Currency     `json:"cost"`
}

// RHPFormRequest is the request type for the /rhp/form endpoint.
type RHPFormRequest struct {
	RenterKey      PrivateKey          `json:"renterKey"`
	HostKey        PublicKey           `json:"hostKey"`
	HostIP         string              `json:"hostIP"`
	TransactionSet []types.Transaction `json:"transactionSet"`
}

// RHPFormResponse is the response type for the /rhp/form endpoint.
type RHPFormResponse struct {
	ContractID     types.FileContractID `json:"contractID"`
	Contract       rhpv2.Contract       `json:"contract"`
	TransactionSet []types.Transaction  `json:"transactionSet"`
}

// RHPPrepareRenewRequest is the request type for the /rhp/prepare/renew
// endpoint.
type RHPPrepareRenewRequest struct {
	Contract       types.FileContractRevision `json:"contract"`
	RenterKey      PrivateKey                 `json:"renterKey"`
	HostKey        PublicKey                  `json:"hostKey"`
	RenterFunds    types.Currency             `json:"renterFunds"`
	RenterAddress  types.UnlockHash           `json:"renterAddress"`
	HostCollateral types.Currency             `json:"hostCollateral"`
	EndHeight      uint64                     `json:"endHeight"`
	HostSettings   rhpv2.HostSettings         `json:"hostSettings"`
}

// RHPPrepareRenewResponse is the response type for the /rhp/prepare/renew
// endpoint.
type RHPPrepareRenewResponse struct {
	Contract     types.FileContract `json:"contract"`
	Cost         types.Currency     `json:"cost"`
	FinalPayment types.Currency     `json:"finalPayment"`
}

// RHPRenewRequest is the request type for the /rhp/renew endpoint.
type RHPRenewRequest struct {
	RenterKey      PrivateKey           `json:"renterKey"`
	HostKey        PublicKey            `json:"hostKey"`
	HostIP         string               `json:"hostIP"`
	ContractID     types.FileContractID `json:"contractID"`
	TransactionSet []types.Transaction  `json:"transactionSet"`
	FinalPayment   types.Currency       `json:"finalPayment"`
}

// RHPRenewResponse is the response type for the /rhp/renew endpoint.
type RHPRenewResponse struct {
	ContractID     types.FileContractID `json:"contractID"`
	Contract       rhpv2.Contract       `json:"contract"`
	TransactionSet []types.Transaction  `json:"transactionSet"`
}

// RHPFundRequest is the request type for the /rhp/fund endpoint.
type RHPFundRequest struct {
	Contract  types.FileContractRevision `json:"contract"`
	RenterKey PrivateKey                 `json:"renterKey"`
	HostKey   PublicKey                  `json:"hostKey"`
	HostIP    string                     `json:"hostIP"`
	Account   rhpv3.Account              `json:"account"`
	Amount    types.Currency             `json:"amount"`
}

// RHPPreparePaymentRequest is the request type for the /rhp/prepare/payment
// endpoint.
type RHPPreparePaymentRequest struct {
	Account    rhpv3.Account  `json:"account"`
	Amount     types.Currency `json:"amount"`
	Expiry     uint64         `json:"expiry"`
	AccountKey PrivateKey     `json:"accountKey"`
}

// RHPRegistryReadRequest is the request type for the /rhp/registry/read
// endpoint.
type RHPRegistryReadRequest struct {
	HostKey     PublicKey                          `json:"hostKey"`
	HostIP      string                             `json:"hostIP"`
	RegistryKey rhpv3.RegistryKey                  `json:"registryKey"`
	Payment     rhpv3.PayByEphemeralAccountRequest `json:"payment"`
}

// RHPRegistryUpdateRequest is the request type for the /rhp/registry/update
// endpoint.
type RHPRegistryUpdateRequest struct {
	HostKey       PublicKey                          `json:"hostKey"`
	HostIP        string                             `json:"hostIP"`
	RegistryKey   rhpv3.RegistryKey                  `json:"registryKey"`
	RegistryValue rhpv3.RegistryValue                `json:"registryValue"`
	Payment       rhpv3.PayByEphemeralAccountRequest `json:"payment"`
}

// A Contract contains all the information necessary to access and revise an
// existing file contract.
type Contract struct {
	HostKey   PublicKey            `json:"hostKey"`
	HostIP    string               `json:"hostIP"`
	ID        types.FileContractID `json:"id"`
	RenterKey PrivateKey           `json:"renterKey"`
}

// SlabsUploadRequest is the request type for the /slabs/upload endpoint.
type SlabsUploadRequest struct {
	MinShards     uint8      `json:"minShards"`
	TotalShards   uint8      `json:"totalShards"`
	Contracts     []Contract `json:"contracts"`
	CurrentHeight uint64     `json:"currentHeight"`
}

// SlabsDownloadRequest is the request type for the /slabs/download endpoint.
type SlabsDownloadRequest struct {
	Slab      object.SlabSlice `json:"slab"`
	Contracts []Contract       `json:"contracts"`
}

// SlabsDeleteRequest is the request type for the /slabs/delete endpoint.
type SlabsDeleteRequest struct {
	Slabs     []object.Slab `json:"slabs"`
	Contracts []Contract    `json:"contracts"`
}

// SlabsMigrateRequest is the request type for the /slabs/migrate endpoint.
type SlabsMigrateRequest struct {
	Slab          object.Slab `json:"slab"`
	From          []Contract  `json:"from"`
	To            []Contract  `json:"to"`
	CurrentHeight uint64      `json:"currentHeight"`
}
