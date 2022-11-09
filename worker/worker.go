package worker

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"go.sia.tech/jape"
	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/renterd/object"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	rhpv3 "go.sia.tech/renterd/rhp/v3"
	"go.sia.tech/renterd/slab"
	"go.sia.tech/siad/types"
)

type (
	// A ContractStore stores contracts.
	ContractStore interface {
		ContractsForUpload() ([]rhpv2.Contract, error)
		ContractsForDownload(s slab.Slab) ([]rhpv2.Contract, error)
	}

	// An ObjectStore stores objects.
	ObjectStore interface {
		List(key string) []string
		Get(key string) (object.Object, error)
		Put(key string, o object.Object) error
		Delete(key string) error
	}

	// A HostDB stores information about hosts.
	HostDB interface {
		Hosts(notSince time.Time, max int) ([]hostdb.Host, error)
		Host(hostKey consensus.PublicKey) (hostdb.Host, error)
		RecordInteraction(hostKey consensus.PublicKey, hi hostdb.Interaction) error
	}

	// An RHP implements the renter-host protocol.
	RHP interface {
		Settings(ctx context.Context, hostIP string, hostKey consensus.PublicKey) (rhpv2.HostSettings, error)
		FormContract(ctx context.Context, cs consensus.State, hostIP string, hostKey consensus.PublicKey, renterKey consensus.PrivateKey, txns []types.Transaction) (rhpv2.Contract, []types.Transaction, error)
		RenewContract(ctx context.Context, cs consensus.State, hostIP string, hostKey consensus.PublicKey, renterKey consensus.PrivateKey, contractID types.FileContractID, txns []types.Transaction, finalPayment types.Currency) (rhpv2.Contract, []types.Transaction, error)
		FundAccount(ctx context.Context, hostIP string, hostKey consensus.PublicKey, contract types.FileContractRevision, renterKey consensus.PrivateKey, account rhpv3.Account, amount types.Currency) (rhpv2.Contract, error)
		ReadRegistry(ctx context.Context, hostIP string, hostKey consensus.PublicKey, payment rhpv3.PaymentMethod, registryKey rhpv3.RegistryKey) (rhpv3.RegistryValue, error)
		UpdateRegistry(ctx context.Context, hostIP string, hostKey consensus.PublicKey, payment rhpv3.PaymentMethod, registryKey rhpv3.RegistryKey, registryValue rhpv3.RegistryValue) error
	}

	// A SlabMover uploads, downloads, and migrates slabs.
	SlabMover interface {
		UploadSlab(ctx context.Context, r io.Reader, m, n uint8, currentHeight uint64, contracts []Contract) (slab.Slab, error)
		DownloadSlab(ctx context.Context, w io.Writer, slab slab.Slice, contracts []Contract) error
		DeleteSlabs(ctx context.Context, slabs []slab.Slab, contracts []Contract) error
		MigrateSlab(ctx context.Context, s *slab.Slab, currentHeight uint64, from, to []Contract) error
	}
)

type Worker struct {
	cs  ContractStore
	os  ObjectStore
	hdb HostDB
	rhp RHP
	sm  SlabMover
}

func (w *Worker) rhpPrepareFormHandler(jc jape.Context) {
	var rpfr RHPPrepareFormRequest
	if jc.Decode(&rpfr) != nil {
		return
	}
	fc := rhpv2.PrepareContractFormation(rpfr.RenterKey, rpfr.HostKey, rpfr.RenterFunds, rpfr.HostCollateral, rpfr.EndHeight, rpfr.HostSettings, rpfr.RenterAddress)
	cost := rhpv2.ContractFormationCost(fc, rpfr.HostSettings.ContractPrice)
	jc.Encode(RHPPrepareFormResponse{
		Contract: fc,
		Cost:     cost,
	})
}

func (w *Worker) rhpPrepareRenewHandler(jc jape.Context) {
	var rprr RHPPrepareRenewRequest
	if jc.Decode(&rprr) != nil {
		return
	}
	fc := rhpv2.PrepareContractRenewal(rprr.Contract, rprr.RenterKey, rprr.HostKey, rprr.RenterFunds, rprr.HostCollateral, rprr.EndHeight, rprr.HostSettings, rprr.RenterAddress)
	cost := rhpv2.ContractRenewalCost(fc, rprr.HostSettings.ContractPrice)
	finalPayment := rprr.HostSettings.BaseRPCPrice
	if finalPayment.Cmp(rprr.Contract.ValidRenterPayout()) > 0 {
		finalPayment = rprr.Contract.ValidRenterPayout()
	}
	jc.Encode(RHPPrepareRenewResponse{
		Contract:     fc,
		Cost:         cost,
		FinalPayment: finalPayment,
	})
}

func (w *Worker) rhpPreparePaymentHandler(jc jape.Context) {
	var rppr RHPPreparePaymentRequest
	if jc.Decode(&rppr) == nil {
		jc.Encode(rhpv3.PayByEphemeralAccount(rppr.Account, rppr.Amount, rppr.Expiry, rppr.AccountKey))
	}
}

func (w *Worker) rhpScanHandler(jc jape.Context) {
	var rsr RHPScanRequest
	if jc.Decode(&rsr) != nil {
		return
	}
	start := time.Now()
	settings, err := w.rhp.Settings(jc.Request.Context(), rsr.HostIP, rsr.HostKey)
	if jc.Check("couldn't scan host", err) != nil {
		return
	}
	jc.Encode(RHPScanResponse{
		Settings: settings,
		Ping:     Duration(time.Since(start)),
	})
}

func (w *Worker) rhpFormHandler(jc jape.Context) {
	var rfr RHPFormRequest
	if jc.Decode(&rfr) != nil {
		return
	}
	var cs consensus.State
	cs.Index.Height = uint64(rfr.TransactionSet[len(rfr.TransactionSet)-1].FileContracts[0].WindowStart)
	contract, txnSet, err := w.rhp.FormContract(jc.Request.Context(), cs, rfr.HostIP, rfr.HostKey, rfr.RenterKey, rfr.TransactionSet)
	if jc.Check("couldn't form contract", err) != nil {
		return
	}
	jc.Encode(RHPFormResponse{
		ContractID:     contract.ID(),
		Contract:       contract,
		TransactionSet: txnSet,
	})
}

func (w *Worker) rhpRenewHandler(jc jape.Context) {
	var rrr RHPRenewRequest
	if jc.Decode(&rrr) != nil {
		return
	}
	var cs consensus.State
	cs.Index.Height = uint64(rrr.TransactionSet[len(rrr.TransactionSet)-1].FileContracts[0].WindowStart)
	contract, txnSet, err := w.rhp.RenewContract(jc.Request.Context(), cs, rrr.HostIP, rrr.HostKey, rrr.RenterKey, rrr.ContractID, rrr.TransactionSet, rrr.FinalPayment)
	if jc.Check("couldn't renew contract", err) != nil {
		return
	}
	jc.Encode(RHPRenewResponse{
		ContractID:     contract.ID(),
		Contract:       contract,
		TransactionSet: txnSet,
	})
}

func (w *Worker) rhpFundHandler(jc jape.Context) {
	var rfr RHPFundRequest
	if jc.Decode(&rfr) != nil {
		return
	}
	_, err := w.rhp.FundAccount(jc.Request.Context(), rfr.HostIP, rfr.HostKey, rfr.Contract, rfr.RenterKey, rfr.Account, rfr.Amount)
	jc.Check("couldn't fund account", err)
}

func (w *Worker) rhpRegistryReadHandler(jc jape.Context) {
	var rrrr RHPRegistryReadRequest
	if jc.Decode(&rrrr) != nil {
		return
	}
	value, err := w.rhp.ReadRegistry(jc.Request.Context(), rrrr.HostIP, rrrr.HostKey, &rrrr.Payment, rrrr.RegistryKey)
	if jc.Check("couldn't read registry", err) == nil {
		jc.Encode(value)
	}
}

func (w *Worker) rhpRegistryUpdateHandler(jc jape.Context) {
	var rrur RHPRegistryUpdateRequest
	if jc.Decode(&rrur) != nil {
		return
	}
	err := w.rhp.UpdateRegistry(jc.Request.Context(), rrur.HostIP, rrur.HostKey, &rrur.Payment, rrur.RegistryKey, rrur.RegistryValue)
	jc.Check("couldn't update registry", err)
}

func (w *Worker) slabsUploadHandler(jc jape.Context) {
	jc.Custom((*[]byte)(nil), slab.Slab{})

	var sur SlabsUploadRequest
	dec := json.NewDecoder(jc.Request.Body)
	if err := dec.Decode(&sur); err != nil {
		http.Error(jc.ResponseWriter, err.Error(), http.StatusBadRequest)
		return
	}
	data := io.LimitReader(io.MultiReader(dec.Buffered(), jc.Request.Body), int64(sur.MinShards)*rhpv2.SectorSize)
	slab, err := w.sm.UploadSlab(jc.Request.Context(), data, sur.MinShards, sur.TotalShards, sur.CurrentHeight, sur.Contracts)
	if jc.Check("couldn't upload slab", err) == nil {
		jc.Encode(slab)
	}
}

func (w *Worker) slabsDownloadHandler(jc jape.Context) {
	jc.Custom(&SlabsDownloadRequest{}, []byte{})

	var sdr SlabsDownloadRequest
	if jc.Decode(&sdr) != nil {
		return
	}
	err := w.sm.DownloadSlab(jc.Request.Context(), jc.ResponseWriter, sdr.Slab, sdr.Contracts)
	jc.Check("couldn't download slabs", err)
}

func (w *Worker) slabsMigrateHandler(jc jape.Context) {
	var smr SlabsMigrateRequest
	if jc.Decode(&smr) != nil {
		return
	}
	err := w.sm.MigrateSlab(jc.Request.Context(), &smr.Slab, smr.CurrentHeight, smr.From, smr.To)
	if jc.Check("couldn't migrate slabs", err) != nil {
		return
	}
	jc.Encode(smr.Slab)
}

func (w *Worker) slabsDeleteHandler(jc jape.Context) {
	var sdr SlabsDeleteRequest
	if jc.Decode(&sdr) == nil {
		err := w.sm.DeleteSlabs(jc.Request.Context(), sdr.Slabs, sdr.Contracts)
		jc.Check("couldn't delete slabs", err)
	}
}

func (w *Worker) objectsKeyHandlerGET(jc jape.Context) {
	jc.Custom(nil, []byte{})

	o, err := w.os.Get(jc.PathParam("key"))
	if jc.Check("couldn't load object", err) != nil {
		return
	}
	var offset int64 = 0 // TODO
	var length int64 = 0 // TODO
	cw := o.Key.Decrypt(jc.ResponseWriter, offset)
	for _, ss := range slab.SlabsForDownload(o.Slabs, offset, length) {
		var contracts []Contract = nil // TODO: ask bus, using ss.Slab.Shards
		if err := w.sm.DownloadSlab(jc.Request.Context(), cw, ss, contracts); err != nil {
			// TODO: handle error
			return
		}
	}
}

func (w *Worker) objectsKeyHandlerPUT(jc jape.Context) {
	jc.Custom((*[]byte)(nil), nil)

	key := object.GenerateEncryptionKey()
	var slabs []slab.Slab
	var contracts []Contract = nil // TODO: ask bus, using ss.Slab.Shards
	var minShards uint8 = 0        // TODO
	var totalShards uint8 = 0      // TODO
	var currentHeight uint64 = 0   // TODO
	for {
		s, err := w.sm.UploadSlab(jc.Request.Context(), jc.Request.Body, minShards, totalShards, currentHeight, contracts)
		if err == io.EOF {
			break
		} else if jc.Check("couldn't upload slab", err) != nil {
			return
		}
		slabs = append(slabs, s)
	}

	var length int = 0 // TODO
	o := object.Object{
		Key:   key,
		Slabs: object.SingleSlabs(slabs, length),
	}
	jc.Check("couldn't store object", w.os.Put(jc.PathParam("key"), o))
}

func (w *Worker) objectsKeyHandlerDELETE(jc jape.Context) {
	jc.Check("couldn't delete object", w.os.Delete(jc.PathParam("key")))
}

// New returns a new Worker.
func New(masterKey [32]byte) *Worker {
	return &Worker{
		rhp: rhpImpl{},
		sm:  newSlabMover(),
	}
}

// NewServer returns an HTTP handler that serves the renterd worker API.
func NewServer(w *Worker) http.Handler {
	return jape.Mux(map[string]jape.Handler{
		"POST   /rhp/prepare/form":    w.rhpPrepareFormHandler,
		"POST   /rhp/prepare/renew":   w.rhpPrepareRenewHandler,
		"POST   /rhp/prepare/payment": w.rhpPreparePaymentHandler,
		"POST   /rhp/scan":            w.rhpScanHandler,
		"POST   /rhp/form":            w.rhpFormHandler,
		"POST   /rhp/renew":           w.rhpRenewHandler,
		"POST   /rhp/fund":            w.rhpFundHandler,
		"POST   /rhp/registry/read":   w.rhpRegistryReadHandler,
		"POST   /rhp/registry/update": w.rhpRegistryUpdateHandler,

		"POST   /slabs/upload":   w.slabsUploadHandler,
		"POST   /slabs/download": w.slabsDownloadHandler,
		"POST   /slabs/migrate":  w.slabsMigrateHandler,
		"POST   /slabs/delete":   w.slabsDeleteHandler,

		"GET    /objects/*key": w.objectsKeyHandlerGET,
		"PUT    /objects/*key": w.objectsKeyHandlerPUT,
		"DELETE /objects/*key": w.objectsKeyHandlerDELETE,
	})
}
