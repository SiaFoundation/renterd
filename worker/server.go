package worker

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"go.sia.tech/jape"
	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/renterd/object"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	rhpv3 "go.sia.tech/renterd/rhp/v3"
	"go.sia.tech/siad/types"
)

type server struct {
	w *Worker
}

func (s *server) rhpPrepareFormHandler(jc jape.Context) {
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

func (s *server) rhpPrepareRenewHandler(jc jape.Context) {
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

func (s *server) rhpPreparePaymentHandler(jc jape.Context) {
	var rppr RHPPreparePaymentRequest
	if jc.Decode(&rppr) == nil {
		jc.Encode(rhpv3.PayByEphemeralAccount(rppr.Account, rppr.Amount, rppr.Expiry, rppr.AccountKey))
	}
}

func (s *server) rhpScanHandler(jc jape.Context) {
	var rsr RHPScanRequest
	if jc.Decode(&rsr) != nil {
		return
	}
	start := time.Now()
	settings, err := s.w.Settings(jc.Request.Context(), rsr.HostIP, rsr.HostKey)
	if jc.Check("couldn't scan host", err) != nil {
		return
	}
	jc.Encode(RHPScanResponse{
		Settings: settings,
		Ping:     Duration(time.Since(start)),
	})
}

func (s *server) rhpFormHandler(jc jape.Context) {
	var rfr RHPFormRequest
	if jc.Decode(&rfr) != nil {
		return
	}
	var cs consensus.State
	cs.Index.Height = uint64(rfr.TransactionSet[len(rfr.TransactionSet)-1].FileContracts[0].WindowStart)
	contract, txnSet, err := s.w.FormContract(jc.Request.Context(), cs, rfr.HostIP, rfr.HostKey, rfr.RenterKey, rfr.TransactionSet)
	if jc.Check("couldn't form contract", err) != nil {
		return
	}
	jc.Encode(RHPFormResponse{
		ContractID:     contract.ID(),
		Contract:       contract,
		TransactionSet: txnSet,
	})
}

func (s *server) rhpRenewHandler(jc jape.Context) {
	var rrr RHPRenewRequest
	if jc.Decode(&rrr) != nil {
		return
	}
	var cs consensus.State
	cs.Index.Height = uint64(rrr.TransactionSet[len(rrr.TransactionSet)-1].FileContracts[0].WindowStart)
	contract, txnSet, err := s.w.RenewContract(jc.Request.Context(), cs, rrr.HostIP, rrr.HostKey, rrr.RenterKey, rrr.ContractID, rrr.TransactionSet, rrr.FinalPayment)
	if jc.Check("couldn't renew contract", err) != nil {
		return
	}
	jc.Encode(RHPRenewResponse{
		ContractID:     contract.ID(),
		Contract:       contract,
		TransactionSet: txnSet,
	})
}

func (s *server) rhpFundHandler(jc jape.Context) {
	var rfr RHPFundRequest
	if jc.Decode(&rfr) != nil {
		return
	}
	_, err := s.w.FundAccount(jc.Request.Context(), rfr.HostIP, rfr.HostKey, rfr.Contract, rfr.RenterKey, rfr.Account, rfr.Amount)
	jc.Check("couldn't fund account", err)
}

func (s *server) rhpRegistryReadHandler(jc jape.Context) {
	var rrrr RHPRegistryReadRequest
	if jc.Decode(&rrrr) != nil {
		return
	}
	value, err := s.w.ReadRegistry(jc.Request.Context(), rrrr.HostIP, rrrr.HostKey, &rrrr.Payment, rrrr.RegistryKey)
	if jc.Check("couldn't read registry", err) == nil {
		jc.Encode(value)
	}
}

func (s *server) rhpRegistryUpdateHandler(jc jape.Context) {
	var rrur RHPRegistryUpdateRequest
	if jc.Decode(&rrur) != nil {
		return
	}
	err := s.w.UpdateRegistry(jc.Request.Context(), rrur.HostIP, rrur.HostKey, &rrur.Payment, rrur.RegistryKey, rrur.RegistryValue)
	jc.Check("couldn't update registry", err)
}

func (s *server) slabsUploadHandler(jc jape.Context) {
	jc.Custom((*[]byte)(nil), object.Slab{})

	var sur SlabsUploadRequest
	dec := json.NewDecoder(jc.Request.Body)
	if err := dec.Decode(&sur); err != nil {
		http.Error(jc.ResponseWriter, err.Error(), http.StatusBadRequest)
		return
	}

	data := io.LimitReader(io.MultiReader(dec.Buffered(), jc.Request.Body), int64(sur.MinShards)*rhpv2.SectorSize)
	slab, err := s.w.UploadSlab(jc.Request.Context(), data, sur.MinShards, sur.TotalShards, sur.CurrentHeight, sur.Contracts, make(map[consensus.PublicKey]types.FileContractID))
	if jc.Check("couldn't upload slab", err) != nil {
		return
	}
	jc.Encode(slab)
}

func (s *server) slabsDownloadHandler(jc jape.Context) {
	jc.Custom(&SlabsDownloadRequest{}, []byte{})

	var sdr SlabsDownloadRequest
	if jc.Decode(&sdr) != nil {
		return
	}

	err := s.w.DownloadSlab(jc.Request.Context(), jc.ResponseWriter, sdr.Slab, sdr.Contracts)
	if jc.Check("couldn't download slabs", err) != nil {
		return
	}
}

func (s *server) slabsMigrateHandler(jc jape.Context) {
	var smr SlabsMigrateRequest
	if jc.Decode(&smr) != nil {
		return
	}

	err := s.w.MigrateSlab(jc.Request.Context(), &smr.Slab, smr.CurrentHeight, smr.From, smr.To)
	if jc.Check("couldn't migrate slabs", err) != nil {
		return
	}
	jc.Encode(smr.Slab)
}

func (s *server) slabsDeleteHandler(jc jape.Context) {
	var sdr SlabsDeleteRequest
	if jc.Decode(&sdr) != nil {
		return
	}
	err := s.w.DeleteSlabs(jc.Request.Context(), sdr.Slabs, sdr.Contracts)
	if jc.Check("couldn't delete slabs", err) != nil {
		return
	}
}

// parseRange parses a Range header string as per RFC 7233. Only the first range
// is returned. If no range is specified, parseRange returns 0, size.
func parseRange(s string, size int64) (offset, length int64, _ error) {
	if s == "" {
		return 0, size, nil
	}
	const b = "bytes="
	if !strings.HasPrefix(s, b) {
		return 0, 0, errors.New("invalid range")
	}
	rs := strings.Split(s[len(b):], ",")
	if len(rs) == 0 {
		return 0, 0, errors.New("invalid range")
	}
	ra := strings.TrimSpace(rs[0])
	if ra == "" {
		return 0, 0, errors.New("invalid range")
	}
	i := strings.Index(ra, "-")
	if i < 0 {
		return 0, 0, errors.New("invalid range")
	}
	start, end := strings.TrimSpace(ra[:i]), strings.TrimSpace(ra[i+1:])
	if start == "" {
		if end == "" || end[0] == '-' {
			return 0, 0, errors.New("invalid range")
		}
		i, err := strconv.ParseInt(end, 10, 64)
		if i < 0 || err != nil {
			return 0, 0, errors.New("invalid range")
		}
		if i > size {
			i = size
		}
		offset = size - i
		length = size - offset
	} else {
		i, err := strconv.ParseInt(start, 10, 64)
		if err != nil || i < 0 {
			return 0, 0, errors.New("invalid range")
		} else if i >= size {
			return 0, 0, errors.New("invalid range")
		}
		offset = i
		if end == "" {
			length = size - offset
		} else {
			i, err := strconv.ParseInt(end, 10, 64)
			if err != nil || offset > i {
				return 0, 0, errors.New("invalid range")
			}
			if i >= size {
				i = size - 1
			}
			length = i - offset + 1
		}
	}
	return offset, length, nil
}

func (s *server) objectsKeyHandlerGET(jc jape.Context) {
	jc.Custom(nil, []string{})

	o, es, err := s.w.Object(jc.PathParam("key"))
	if jc.Check("couldn't get object or entries", err) != nil {
		return
	}
	if len(es) > 0 {
		jc.Encode(es)
		return
	}

	// NOTE: ideally we would use http.ServeContent in this handler, but that
	// has performance issues. If we implemented io.ReadSeeker in the most
	// straightforward fashion, we would need one (or more!) RHP RPCs for each
	// Read call. We can improve on this to some degree by buffering, but
	// without knowing the exact ranges being requested, this will always be
	// suboptimal. Thus, sadly, we have to roll our own range support.
	offset, length, err := parseRange(jc.Request.Header.Get("Range"), o.Size())
	if err != nil {
		jc.Error(err, http.StatusRequestedRangeNotSatisfiable)
		return
	}
	if length < o.Size() {
		jc.ResponseWriter.WriteHeader(http.StatusPartialContent)
		jc.ResponseWriter.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", offset, offset+length-1, o.Size()))
	}
	jc.ResponseWriter.Header().Set("Content-Length", strconv.FormatInt(length, 10))
	err = s.w.DownloadObject(jc.Request.Context(), jc.ResponseWriter, o, offset, length)
	_ = err // TODO: log?
}

func (s *server) objectsKeyHandlerPUT(jc jape.Context) {
	jc.Custom((*[]byte)(nil), nil)

	err := s.w.AddObject(jc.Request.Context(), jc.PathParam("key"), jc.Request.Body)
	if err != nil {
		jc.Error(err, 500)
		return
	}
}

func (s *server) objectsKeyHandlerDELETE(jc jape.Context) {
	jc.Check("couldn't delete object", s.w.DeleteObject(jc.PathParam("key")))
}

// NewServer returns an HTTP handler that serves the renterd worker API.
func NewServer(w *Worker) http.Handler {
	s := &server{w: w}
	return jape.Mux(map[string]jape.Handler{
		"POST   /rhp/prepare/form":    s.rhpPrepareFormHandler,
		"POST   /rhp/prepare/renew":   s.rhpPrepareRenewHandler,
		"POST   /rhp/prepare/payment": s.rhpPreparePaymentHandler,
		"POST   /rhp/scan":            s.rhpScanHandler,
		"POST   /rhp/form":            s.rhpFormHandler,
		"POST   /rhp/renew":           s.rhpRenewHandler,
		"POST   /rhp/fund":            s.rhpFundHandler,
		"POST   /rhp/registry/read":   s.rhpRegistryReadHandler,
		"POST   /rhp/registry/update": s.rhpRegistryUpdateHandler,

		"POST   /slabs/upload":   s.slabsUploadHandler,
		"POST   /slabs/download": s.slabsDownloadHandler,
		"POST   /slabs/migrate":  s.slabsMigrateHandler,
		"POST   /slabs/delete":   s.slabsDeleteHandler,

		"GET    /objects/*key": s.objectsKeyHandlerGET,
		"PUT    /objects/*key": s.objectsKeyHandlerPUT,
		"DELETE /objects/*key": s.objectsKeyHandlerDELETE,
	})
}
