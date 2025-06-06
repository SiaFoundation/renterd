package bus

import (
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
)

var _ rhp.FormContractSigner = (*formContractSigner)(nil)

type feeRecommender interface {
	RecommendedFee() types.Currency
}

type formContractSigner struct {
	renterKey types.PrivateKey
	r         feeRecommender
	w         rhp.Wallet
}

func NewFormContractSigner(w rhp.Wallet, r feeRecommender, renterKey types.PrivateKey) rhp.FormContractSigner {
	return &formContractSigner{
		renterKey: renterKey,
		w:         w,
	}
}

func (s *formContractSigner) FundV2Transaction(txn *types.V2Transaction, amount types.Currency) (types.ChainIndex, []int, error) {
	return s.w.FundV2Transaction(txn, amount, true)
}

func (s *formContractSigner) RecommendedFee() types.Currency {
	return s.r.RecommendedFee()
}

func (s *formContractSigner) ReleaseInputs(txns []types.V2Transaction) {
	s.w.ReleaseInputs(nil, txns)
}

func (s *formContractSigner) SignHash(h types.Hash256) types.Signature {
	return s.renterKey.SignHash(h)
}

func (s *formContractSigner) SignV2Inputs(txn *types.V2Transaction, toSign []int) {
	s.w.SignV2Inputs(txn, toSign)
}
